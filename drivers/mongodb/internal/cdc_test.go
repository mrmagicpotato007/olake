// mongo_change_stream_test.go
package driver

import (
	"context"
	"testing"
	"time"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/mock"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// ========================= Mock Interfaces ========================= //

// MockMongoClient mocks the MongoClientInterface with only the Database method.
type MockMongoClient struct {
	mock.Mock
}

func (m *MockMongoClient) Database(name string, opts ...*options.DatabaseOptions) *mongo.Database {
	args := m.Called(name, opts)
	return args.Get(0).(*mongo.Database)
}

func (m *MockMongoClient) Disconnect(ctx context.Context) error {
	return nil
}

func (m *MockMongoClient) Ping(ctx context.Context, rp *readpref.ReadPref) error {
	return nil
}

// MockDatabase mocks the DatabaseInterface with only the Collection method.
type MockDatabase struct {
	mock.Mock
}

func (d *MockDatabase) Collection(name string, opts ...*options.CollectionOptions) MongoCollectionInterface {
	args := d.Called(name, opts)
	return args.Get(0).(MongoCollectionInterface)
}

func (d *MockDatabase) RunCommand(ctx context.Context, command interface{}, opts ...*options.RunCmdOptions) *mongo.SingleResult {
	// Not needed for this basic test
	return nil
}

// MockCollection mocks the MongoCollectionInterface with only the Watch method.
type MockCollection struct {
	mock.Mock
}

func (c *MockCollection) Watch(ctx context.Context, pipeline interface{}, opts ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error) {
	args := c.Called(ctx, pipeline, opts)
	return args.Get(0).(*mongo.ChangeStream), args.Error(1)
}

func (c *MockCollection) FindOne(ctx context.Context, filter interface{}, opts ...*options.FindOneOptions) *mongo.SingleResult {
	// Not needed for this basic test
	return nil
}

func (c *MockCollection) Aggregate(ctx context.Context, pipeline interface{}, opts ...*options.AggregateOptions) (*mongo.Cursor, error) {
	// Not needed for this basic test
	return nil, nil
}

// MockChangeStream mocks the mongo.ChangeStream
type MockChangeStream struct {
	mock.Mock
}

func (m *MockChangeStream) Next(ctx context.Context) bool {
	args := m.Called(ctx)
	return args.Bool(0)
}

func (m *MockChangeStream) Decode(val interface{}) error {
	args := m.Called(val)
	return args.Error(0)
}

func (m *MockChangeStream) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockChangeStream) TryNext(ctx context.Context) bool {
	args := m.Called(ctx)
	return args.Bool(0)
}

func (m *MockChangeStream) ResumeToken() bson.Raw {
	args := m.Called()
	return args.Get(0).(bson.Raw)
}

func (m *MockChangeStream) Err() error {
	args := m.Called()
	return args.Error(0)
}

// MockWriterPool mocks the WriterPoolInterface with only the NewThread method.
type MockWriterPool struct {
	mock.Mock
}

func (m *MockWriterPool) NewThread(ctx context.Context, stream protocol.Stream, opts ...protocol.ThreadOptions) (protocol.Writer, error) {
	args := m.Called(ctx, stream, opts)
	return args.Get(0).(protocol.Writer), args.Error(1)
}

// MockWriter mocks the protocol.Writer interface with only the Insert and Close methods.
type MockWriter struct {
	mock.Mock
}

func (m *MockWriter) Insert(record types.Record) (bool, error) {
	args := m.Called(record)
	return args.Bool(0), args.Error(1)
}

func (m *MockWriter) Close() error {
	args := m.Called()
	return args.Error(0)
}

// MockStream mocks the protocol.Stream interface with essential methods.
type MockStream struct {
	mock.Mock
	IDValue        string
	NamespaceValue string
	NameValue      string
	SyncModeValue  types.SyncMode
	StateKeyValue  bson.D
}

func (m *MockStream) ID() string {
	return m.IDValue
}

func (m *MockStream) Namespace() string {
	return m.NamespaceValue
}

func (m *MockStream) Name() string {
	return m.NameValue
}

func (m *MockStream) GetSyncMode() types.SyncMode {
	return m.SyncModeValue
}

func (m *MockStream) GetStateKey(key string) bson.D {
	args := m.Called(key)
	return args.Get(0).(bson.D)
}

func (m *MockStream) SetStateKey(key string, value string) {
	m.Called(key, value)
}

// ========================= Test Cases ========================= //

// TestMongo_RunChangeStream_Success tests the successful execution of RunChangeStream.
func TestMongo_RunChangeStream_Success(t *testing.T) {
	// Define the resume token field name
	const cdcCursorField = "resumeToken"

	// Step 1: Setup mocks
	mockClient := new(MockMongoClient)
	mockDatabase := new(MockDatabase)
	mockCollection := new(MockCollection)
	mockChangeStream := new(MockChangeStream)
	mockWriterPool := new(MockWriterPool)
	mockWriter := new(MockWriter)
	mockStream := &MockStream{
		IDValue:        "test-stream",
		NamespaceValue: "test-db",
		NameValue:      "test-collection",
		SyncModeValue:  types.CDC, // Assuming CDC is a valid SyncMode
	}

	// Step 2: Mock Stream.GetStateKey to return no resume token (empty bson.D)
	mockStream.On("GetStateKey", cdcCursorField).Return(bson.D{})

	// Step 3: Mock client interactions
	mockClient.On("Database", "test-db", mock.Anything).Return(mockDatabase)
	mockDatabase.On("Collection", "test-collection", mock.Anything).Return(mockCollection)

	// Step 4: Mock Watch to return a MockChangeStream
	mockCollection.On("Watch", mock.Anything, mock.Anything, mock.Anything).Return(mockChangeStream, nil)

	// Step 5: Mock ChangeStream behavior
	// Simulate one change document and then end
	mockChangeStream.On("TryNext", mock.Anything).Return(true).Once()
	mockChangeStream.On("Decode", mock.Anything).Run(func(args mock.Arguments) {
		record := args.Get(0).(*CDCDocument)
		*record = CDCDocument{
			OperationType: "insert",
			FullDocument:  map[string]any{"field1": "value1"},
		}
	}).Return(nil)
	mockChangeStream.On("ResumeToken").Return(bson.M{"resumeToken": bson.A{}})
	mockChangeStream.On("TryNext", mock.Anything).Return(false).Once()
	mockChangeStream.On("Err").Return(nil)
	mockChangeStream.On("Close", mock.Anything).Return(nil)

	// Step 6: Mock WriterPool and Writer
	mockWriterPool.On("NewThread", mock.Anything, mockStream, mock.Anything).Return(mockWriter, nil).Once()
	// Simulate Insert success
	mockWriter.On("Insert", mock.Anything).Return(false, nil).Once()
	mockWriter.On("Close").Return(nil).Once()
	mongoInstance := &Mongo{
		Driver: &base.Driver{}, // Use the mocked base.Driver
		config: &Config{
			MaxThreads: 1, // Adjust based on your configuration
		},
		client: mockClient, // Use the mocked client
	}
	// Step 9: Run RunChangeStream in a goroutine
	go func() {
		err := mongoInstance.RunChangeStream(mockWriter, mockWriterPool) // Use mockWriterPool instead of moc
		// In a real scenario, RunChangeStream would run indefinitely
		// For testing, we'll let it run and rely on the mocked ChangeStream to stop
		if err != nil {
			t.Errorf("RunChangeStream returned error: %v", err)
		}
	}()

	// Step 10: Allow some time for the change stream to process
	time.Sleep(100 * time.Millisecond) // Adjust as needed

	// Step 11: Assertions
	// Ensure that Insert was called with the expected document
	mockWriter.AssertCalled(t, "Insert", mock.MatchedBy(func(record types.Record) bool {
		// Verify that the inserted record contains the expected data
		return record["field1"] == "value1" && record["cdc_type"] == "insert"
	}))

	// Ensure that backfill was called
	mockClient.AssertCalled(t, "Database", "test-db", mock.Anything)
	mockDatabase.AssertCalled(t, "Collection", "test-collection", mock.Anything)
	mockCollection.AssertCalled(t, "Watch", mock.Anything, mock.Anything, mock.Anything)
	mockWriterPool.AssertCalled(t, "NewThread", mock.Anything, mockStream, mock.Anything)
	mockWriter.AssertCalled(t, "Close")
	mockChangeStream.AssertCalled(t, "TryNext", mock.Anything)
	mockChangeStream.AssertCalled(t, "Decode", mock.Anything)
	mockChangeStream.AssertCalled(t, "ResumeToken")
	mockChangeStream.AssertCalled(t, "TryNext", mock.Anything)
	mockChangeStream.AssertCalled(t, "Err")
	mockChangeStream.AssertCalled(t, "Close", mock.Anything)
	mockStream.AssertCalled(t, "GetStateKey", cdcCursorField)
}

// ========================= Helper Interfaces ========================= //

// Define the necessary interfaces to match the mocked methods.
// These should match the actual interfaces used in your driver package.

type MongoCollectionInterface interface {
	FindOne(ctx context.Context, filter interface{}, opts ...*options.FindOneOptions) *mongo.SingleResult
	Aggregate(ctx context.Context, pipeline interface{}, opts ...*options.AggregateOptions) (*mongo.Cursor, error)
	Watch(ctx context.Context, pipeline interface{}, opts ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error)
}

type WriterPoolInterface interface {
	NewThread(ctx context.Context, stream protocol.Stream, opts ...protocol.ThreadOptions) protocol.Writer
}

type WriterInterface interface {
	Insert(record types.Record) (bool, error)
	Close() error
}

type StreamInterface interface {
	ID() string
	Namespace() string
	Name() string
	GetSyncMode() types.SyncMode
	GetStateKey(key string) bson.D
	SetStateKey(key string, value string)
}
