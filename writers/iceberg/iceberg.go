package iceberg

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"sync/atomic"
	"time"

	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/typeutils"
	"github.com/datazip-inc/olake/writers/iceberg/proto"
	"google.golang.org/grpc"
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

func (i *Iceberg) GetConfigRef() protocol.Config {
	i.config = &Config{}
	return i.config
}

func (i *Iceberg) Spec() any {
	return Config{}
}

func (i *Iceberg) Setup(stream protocol.Stream, options *protocol.Options) error {
	i.options = options
	i.stream = stream

	return i.SetupIcebergClient(!stream.Self().BackfillInProcess)
}

func (i *Iceberg) Write(ctx context.Context, record types.RawRecord) error {
	// Convert record to Debezium format
	debeziumRecord, err := record.GetDebeziumJSON(i.config.Database, i.stream.Name())
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

func (i *Iceberg) Close() error {
	if i.closed {
		return nil
	}
	i.closed = true

	return i.CloseIcebergClient()
}

func (i *Iceberg) Check() error {
	err := i.SetupIcebergClient(false)
	if err != nil {
		return fmt.Errorf("failed to setup iceberg: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Try to send a test message
	req := &proto.StringArrayRequest{
		Messages: []string{getTestDebeziumRecord()},
	}

	// Call the remote procedure
	res, err := i.client.SendStringArray(ctx, req)
	if err != nil {
		log.Fatalf("Error sending record to Iceberg RPC Server: %v", err)
	}
	// Print the response from the server
	log.Printf("Server Response: %s", res.GetResult())

	return i.CloseIcebergClient()
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
