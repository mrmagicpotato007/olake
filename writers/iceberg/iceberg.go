package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/typeutils"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/writers/iceberg/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Iceberg struct {
	options *protocol.Options
	config  *Config
	stream  protocol.Stream
	records atomic.Int64
	closed  bool
	cmd     *exec.Cmd
	client  proto.StringArrayServiceClient
	conn    *grpc.ClientConn
	port    int
}

// portMap tracks which ports are in use
var portMap sync.Map

func findAvailablePort() (int, error) {
	for p := 50051; p <= 59051; p++ {
		// Try to store port in map - returns false if already exists
		if _, loaded := portMap.LoadOrStore(p, true); !loaded {
			return p, nil
		}
	}
	return 0, fmt.Errorf("no available ports found between 50051 and 51051")
}

func (i *Iceberg) GetConfigRef() protocol.Config {
	i.config = &Config{}
	return i.config
}

func (i *Iceberg) Spec() any {
	return Config{}
}

func (i *Iceberg) setup(upsert bool) error {
	// Create JSON config for the Java server
	err := i.config.Validate()
	if err != nil {
		return fmt.Errorf("failed to validate config: %s", err)
	}

	// Find an available port
	port, err := findAvailablePort()
	if err != nil {
		return err
	}

	i.port = port

	// Add port to server config
	serverConfig := map[string]string{
		"port":                 fmt.Sprintf("%d", port),
		"catalog-impl":         "org.apache.iceberg.aws.glue.GlueCatalog",
		"io-impl":              "org.apache.iceberg.aws.s3.S3FileIO",
		"warehouse":            i.config.S3Path,
		"table-namespace":      i.config.Database,
		"catalog-name":         "olake_iceberg",
		"glue.region":          i.config.Region,
		"s3.access-key-id":     i.config.AccessKey,
		"s3.secret-access-key": i.config.SecretKey,
		"table-prefix":         "",
		"upsert":               strconv.FormatBool(upsert),
		"upsert-keep-deletes":  "true",
		"write.format.default": "parquet",
		"s3.path-style-access": "true",
	}

	if i.config.SessionToken != "" {
		serverConfig["aws.session-token"] = i.config.SessionToken
	}

	configJSON, err := json.Marshal(serverConfig)
	if err != nil {
		return fmt.Errorf("failed to create server config: %v", err)
	}

	// Start the Java server process
	i.cmd = exec.Command("java", "-jar", i.config.JarPath, string(configJSON))
	i.cmd.Stdout = os.Stdout
	i.cmd.Stderr = os.Stderr

	if err := i.cmd.Start(); err != nil {
		return fmt.Errorf("failed to start Iceberg server: %v", err)
	}

	// Connect to gRPC server
	conn, err := grpc.NewClient(`localhost:`+strconv.Itoa(i.port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))

	if err != nil {
		logger.Errorf("failed to connect to iceberg writer: %v", err)
	}

	logger.Info("Connected to iceberg writer", "port", i.port)
	i.conn = conn
	i.client = proto.NewStringArrayServiceClient(conn)
	return nil
}

func (i *Iceberg) Setup(stream protocol.Stream, options *protocol.Options) error {
	i.options = options
	i.stream = stream

	return i.check(!stream.Self().BackfillInProcess)
}

func (i *Iceberg) Write(ctx context.Context, record types.RawRecord) error {
	// Convert record to Debezium format
	debeziumRecord, err := i.convertToDebeziumFormat(record)
	if err != nil {
		return fmt.Errorf("failed to convert record: %v", err)
	}

	req := &proto.StringArrayRequest{
		Messages: []string{debeziumRecord},
	}

	// Send to gRPC server with timeout
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	res, err := i.client.SendStringArray(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to send record: %v", err)
	}

	log.Printf("Server Response: %s", res.GetResult())

	i.records.Add(1)
	return nil
}

func (i *Iceberg) convertToDebeziumFormat(record types.RawRecord) (string, error) {
	// First create the schema and track field types
	schema := i.createDebeziumSchema(record)

	// Create the payload with the actual data
	payload := make(map[string]interface{})

	// Add olake_id to payload
	payload["olake_id"] = record.OlakeID

	// Copy the data fields
	for key, value := range record.Data {
		payload[key] = value
	}

	// Add the metadata fields
	payload["__deleted"] = record.DeleteTime > 0
	payload["__op"] = record.OperationType // "r" for read/backfill, "c" for create, "u" for update
	payload["__db"] = i.config.Database
	payload["__source_ts_ms"] = record.CdcTimestamp

	// Create Debezium format
	debeziumRecord := map[string]interface{}{
		"destination_table": i.stream.Name(),
		"key": map[string]interface{}{
			"schema": map[string]interface{}{
				"type": "struct",
				"fields": []map[string]interface{}{
					{
						"type":     "string",
						"optional": true,
						"field":    "olake_id",
					},
				},
				"optional": false,
			},
			"payload": map[string]interface{}{
				"olake_id": record.OlakeID,
			},
		},
		"value": map[string]interface{}{
			"schema":  schema,
			"payload": payload,
		},
	}

	jsonBytes, err := json.Marshal(debeziumRecord)
	if err != nil {
		return "", err
	}
	return string(jsonBytes), nil
}

func (i *Iceberg) createDebeziumSchema(record types.RawRecord) map[string]interface{} {
	fields := make([]map[string]interface{}, 0)

	// Add olake_id field first
	fields = append(fields, map[string]interface{}{
		"type":     "string",
		"optional": true,
		"field":    "olake_id",
	})

	// Add data fields
	for key, value := range record.Data {
		field := map[string]interface{}{
			"optional": true,
			"field":    key,
		}

		// Determine type based on the value
		switch value.(type) {
		case bool:
			field["type"] = "boolean"
		case int, int8, int16, int32:
			field["type"] = "int32"
		case int64:
			field["type"] = "int64"
		case float32:
			field["type"] = "float32"
		case float64:
			field["type"] = "float64"
		case map[string]interface{}:
			field["type"] = "string"
		case []interface{}:
			field["type"] = "string"
		default:
			field["type"] = "string"
		}

		fields = append(fields, field)
	}

	// Add metadata fields
	fields = append(fields, []map[string]interface{}{
		{
			"type":     "boolean",
			"optional": true,
			"field":    "__deleted",
		},
		{
			"type":     "string",
			"optional": true,
			"field":    "__op",
		},
		{
			"type":     "string",
			"optional": true,
			"field":    "__db",
		},
		{
			"type":     "int64",
			"optional": true,
			"field":    "__source_ts_ms",
		},
	}...)

	return map[string]interface{}{
		"type":     "struct",
		"fields":   fields,
		"optional": false,
		"name":     fmt.Sprintf("%s.%s", i.config.Database, i.stream.Name()),
	}
}

func (i *Iceberg) Close() error {
	if i.closed {
		return nil
	}
	i.closed = true

	if i.conn != nil {
		i.conn.Close()
	}

	if i.cmd != nil && i.cmd.Process != nil {
		err := i.cmd.Process.Kill()
		if err != nil {
			return fmt.Errorf("failed to kill iceberg server: %v", err)
		}
		logger.Info("Iceberg server killed", "port", i.port)
		return err
	}

	return nil
}

func (i *Iceberg) Check() error {
	return i.check(false)
}

func (i *Iceberg) check(upsert bool) error {

	err := i.setup(upsert)
	if err != nil {
		return fmt.Errorf("failed to setup iceberg: %v", err)
	}

	op := "r"
	if upsert {
		op = "c"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	defer cancel()
	random_id := utils.ULID()
	// Try to send a test message
	req := &proto.StringArrayRequest{
		Messages: []string{`{
			"destination_table": "olake_test_table",
			"key": {
				"schema" : {
						"type" : "struct",
						"fields" : [ {
							"type" : "string",
							"optional" : true,
							"field" : "_id"
						} ],
						"optional" : false
					},
					"payload" : {
						"_id" : "` + random_id + `"
					}
				}
				,
			"value": {
				"schema" : {
					"type" : "struct",
					"fields" : [ {
					"type" : "string",
					"optional" : true,
					"field" : "_id"
					}, {
					"type" : "boolean",
					"optional" : true,
					"field" : "__deleted"
					}, {
					"type" : "string",
					"optional" : true,
					"field" : "__op"
					}, {
					"type" : "string",
					"optional" : true,
					"field" : "__db"
					}, {
					"type" : "int64",
					"optional" : true,
					"field" : "__source_ts_ms"
					} ],
					"optional" : false,
					"name" : "dbz_.incr.incr1"
				},
				"payload" : {
					"_id" : "` + random_id + `",
					"__deleted" : false,
					"__op" : "` + op + `",
					"__db" : "incr",
					"__source_ts_ms" : 1738502494009
				}
			}
		}`},
	}

	// Call the remote procedure
	res, err := i.client.SendStringArray(ctx, req)
	if err != nil {
		log.Fatalf("Error when calling SendStringArray: %v", err)
	}
	// Print the response from the server
	log.Printf("Server Response: %s", res.GetResult())

	return nil
}

func (i *Iceberg) ReInitiationOnTypeChange() bool {
	return true
}

func (i *Iceberg) ReInitiationOnNewColumns() bool {
	return true
}

func (i *Iceberg) Type() string {
	return "iceberg"
}

func (i *Iceberg) Flattener() protocol.FlattenFunction {
	flattener := typeutils.NewFlattener()
	return flattener.Flatten
}

func (i *Iceberg) Normalization() bool {
	return i.config.Normalization
}

func (i *Iceberg) EvolveSchema(addNulls bool, addDefaults bool, properties map[string]*types.Property, record types.Record) error {
	// Schema evolution is handled by Iceberg
	return nil
}

func init() {
	protocol.RegisteredWriters[types.Iceberg] = func() protocol.Writer {
		return new(Iceberg)
	}
}
