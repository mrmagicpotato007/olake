package driver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"

	// MySQL driver
	_ "github.com/go-sql-driver/mysql"
)

const (
	discoverTime = 5 * time.Minute // maximum time allowed to discover all the streams
)

// MySQL represents the MySQL database driver
type MySQL struct {
	*base.Driver
	config *Config
	db     *sql.DB
}

// GetConfigRef returns a reference to the configuration
func (m *MySQL) GetConfigRef() protocol.Config {
	m.config = &Config{}
	return m.config
}

// Spec returns the configuration specification
func (m *MySQL) Spec() any {
	return Config{}
}

// Setup establishes the database connection
func (m *MySQL) Setup() error {
	// Open database connection
	db, err := sql.Open("mysql", m.config.URI())
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}
	db.SetConnMaxLifetime(30 * time.Minute)

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	m.db = db
	// Enable CDC support if binlog is configured
	m.CDCSupport = true
	return nil
}

// Check verifies the database connection
func (m *MySQL) Check() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return m.db.PingContext(ctx)
}

// Type returns the database type
func (m *MySQL) Type() string {
	return "MySQL"
}

// Discover finds and catalogs database tables
func (m *MySQL) Discover(discoverSchema bool) ([]*types.Stream, error) {
	streams := m.GetStreams()
	if len(streams) != 0 {
		return streams, nil
	}

	logger.Infof("Starting discover for MySQL database %s", m.config.Database)
	discoverCtx, cancel := context.WithTimeout(context.Background(), discoverTime)
	defer cancel()

	// Query to get all tables in the database
	query := `
		SELECT 
			TABLE_NAME, 
			TABLE_SCHEMA 
		FROM 
			INFORMATION_SCHEMA.TABLES 
		WHERE 
			TABLE_SCHEMA = ? 
			AND TABLE_TYPE = 'BASE TABLE'
	`

	rows, err := m.db.QueryContext(discoverCtx, query, m.config.Database)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tableNames []string
	for rows.Next() {
		var tableName, schemaName string
		if err := rows.Scan(&tableName, &schemaName); err != nil {
			return nil, fmt.Errorf("failed to scan table: %w", err)
		}
		tableNames = append(tableNames, fmt.Sprintf("%s.%s", schemaName, tableName))
	}

	// Concurrent stream schema discovery
	err = utils.Concurrent(discoverCtx, tableNames, len(tableNames), func(ctx context.Context, streamName string, _ int) error {
		stream, err := m.produceTableSchema(ctx, streamName)
		if err != nil && discoverCtx.Err() == nil {
			return fmt.Errorf("failed to process table[%s]: %s", streamName, err)
		}
		stream.SyncMode = m.config.DefaultMode
		// cache stream
		m.AddStream(stream)
		return err
	})

	if err != nil {
		return nil, err
	}

	return m.GetStreams(), nil
}

// Read handles different sync modes for data retrieval
func (m *MySQL) Read(pool *protocol.WriterPool, stream protocol.Stream) error {
	switch stream.GetSyncMode() {
	case types.FULLREFRESH:
		return m.backfill(stream, pool)
	case types.CDC:
		return m.changeStreamSync(stream, pool)
	}

	return nil
}

// produceTableSchema extracts schema information for a given table
func (m *MySQL) produceTableSchema(ctx context.Context, streamName string) (*types.Stream, error) {
	logger.Infof("producing type schema for stream [%s]", streamName)

	// Split stream name into schema and table
	parts := strings.Split(streamName, ".")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid stream name format: %s", streamName)
	}
	schemaName, tableName := parts[0], parts[1]
	// Initialize stream
	stream := types.NewStream(tableName, schemaName).WithSyncMode(types.FULLREFRESH, types.CDC)

	// Query column information
	query := `
		SELECT 
			COLUMN_NAME, 
			COLUMN_TYPE,
			DATA_TYPE, 
			IS_NULLABLE,
			COLUMN_KEY
		FROM 
			INFORMATION_SCHEMA.COLUMNS 
		WHERE 
			TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY 
			ORDINAL_POSITION
	`

	rows, err := m.db.QueryContext(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query column information: %w", err)
	}
	defer rows.Close()

	// Process column information
	for rows.Next() {
		var columnName, dataType, isNullable, columnType, columnKey string
		if err := rows.Scan(&columnName, &dataType, &isNullable, &columnType, &columnKey); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}
		// Map MySQL data type to internal data type
		datatype := types.Unknown
		// Handle TinyInt(1) special case for boolean
		key := strings.ToLower(strings.Split(dataType, " ")[0])
		if key == "tinyint" && strings.ToLower(columnType) == "tinyint(1)" {
			datatype = types.Bool
		} else if val, found := mysqlTypeToDataTypes[key]; found {
			datatype = val
		} else {
			logger.Warnf("failed to map MySQL type '%s' for column '%s', defaulting to String", dataType, columnName)
			datatype = types.String
		}
		// Add field to stream with nullable info
		stream.UpsertField(columnName, datatype, strings.EqualFold("yes", isNullable))

		// Check if column is a primary key
		if columnKey == "PRI" {
			stream.WithPrimaryKey(columnName)
		}
	}

	// Add CDC metadata fields if CDC is supported
	if m.Driver.CDCSupport {
		for column, typ := range base.DefaultColumns {
			stream.UpsertField(column, typ, true)
		}
	}

	// Setup supported sync modes
	stream.SupportedSyncModes.Insert("full_refresh")

	// If we have primary keys or CDC support, enable other sync modes
	if stream.SourceDefinedPrimaryKey.Len() > 0 {
		stream.WithSyncMode(types.INCREMENTAL)
		stream.SupportedSyncModes.Insert("incremental")
	}

	if m.Driver.CDCSupport {
		stream.WithSyncMode(types.CDC)
		stream.SupportedSyncModes.Insert("cdc")
	}

	return stream, nil
}
