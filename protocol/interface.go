package protocol

import (
	"github.com/piyushsingariya/shift/jsonschema/schema"
	"github.com/piyushsingariya/shift/types"
)

type Connector interface {
	Setup(config any, catalog *types.Catalog, state types.State, batchSize int64) error
	Spec() (schema.JSONSchema, error)
	Check() error

	Catalog() *types.Catalog
	Type() string
}

type Driver interface {
	Connector
	Discover() ([]*types.Stream, error)
	Read(stream Stream, channel chan<- types.Record) error
	GetState() (*types.State, error)
}

type Adapter interface {
	Connector
	Write(channel <-chan types.Record) error
	Create(streamName string) error
}

type Stream interface {
	Name() string
	Namespace() string
	JSONSchema() *types.Schema
	GetStream() *types.Stream
	GetSyncMode() types.SyncMode
	GetCursorField() string
}
