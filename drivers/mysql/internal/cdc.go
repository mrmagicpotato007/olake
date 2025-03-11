package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"vitess.io/vitess/go/vt/sqlparser"
)

const (
	cdcPositionKey   = "binlog_position"
	cdcFileKey       = "binlog_file"
	cdcServerIDKey   = "server_id"
	maxRetries       = 3
	retryDelay       = 5 * time.Second
	heartbeatPeriod  = 30 * time.Second
	eventReadTimeout = 500 * time.Millisecond
	metadataStateKey = "table_metadata"
)

// MySQLTableMetadata mirrors Estuary's metadata structure
type MySQLTableMetadata struct {
	Schema MySQLTableSchema `json:"schema"`
}

type MySQLTableSchema struct {
	Columns     []string               `json:"columns"`
	ColumnTypes map[string]interface{} `json:"types"`
}

// RunChangeStream implements the CDC functionality for multiple streams concurrently
func (m *MySQL) RunChangeStream(pool *protocol.WriterPool, streams ...protocol.Stream) error {
	m.metadata = make(map[string]*MySQLTableMetadata)
	return utils.Concurrent(context.TODO(), streams, len(streams), func(ctx context.Context, stream protocol.Stream, executionNumber int) error {
		return m.changeStreamSync(stream, pool)
	})
}

// SetupGlobalState initializes the state for CDC
func (m *MySQL) SetupGlobalState(state *types.State) error {
	return nil
}

// StateType returns the type of state used by the MySQL driver
func (m *MySQL) StateType() types.StateType {
	return types.StreamType
}

// InitializeTableMetadata loads or creates metadata for a table
func (m *MySQL) InitializeTableMetadata(stream protocol.Stream) error {
	streamID := stream.ID()

	m.metadataMu.Lock()
	defer m.metadataMu.Unlock()

	// Check if metadata exists in state
	if metadataJSON, ok := stream.GetStateKey(metadataStateKey).(string); ok {
		var meta MySQLTableMetadata
		if err := json.Unmarshal([]byte(metadataJSON), &meta); err == nil {
			m.metadata[streamID] = &meta
			return nil
		}
	}

	// If no metadata in state, query and initialize
	query := jdbc.MySQLTableColumnsQuery()
	rows, err := m.db.Query(query, stream.Namespace(), stream.Name())
	if err != nil {
		return fmt.Errorf("failed to get columns for table %s.%s: %w", stream.Namespace(), stream.Name(), err)
	}
	defer rows.Close()

	meta := &MySQLTableMetadata{
		Schema: MySQLTableSchema{
			Columns:     []string{},
			ColumnTypes: make(map[string]interface{}),
		},
	}

	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			logger.Errorf("Failed to scan column name: %v", err)
			continue
		}
		meta.Schema.Columns = append(meta.Schema.Columns, colName)
		meta.Schema.ColumnTypes[colName] = "unknown" // Simplified type handling
	}

	m.metadata[streamID] = meta

	// Save to state
	if bs, err := json.Marshal(meta); err == nil {
		stream.SetStateKey(metadataStateKey, string(bs))
	}

	return nil
}

// changeStreamSync implements CDC for a specific stream
func (m *MySQL) changeStreamSync(stream protocol.Stream, pool *protocol.WriterPool) error {
	// Initialize table metadata
	if err := m.InitializeTableMetadata(stream); err != nil {
		return err
	}

	var pos mysql.Position
	serverID := uint32(1000 + time.Now().UnixNano()%9000)

	hasBinlogPosition := false
	if posStr := stream.GetStateKey(cdcPositionKey); posStr != nil {
		switch v := posStr.(type) {
		case float64:
			pos.Pos = uint32(v)
		case string:
			if posInt, err := strconv.ParseUint(v, 10, 32); err == nil {
				pos.Pos = uint32(posInt)
			}
		}
		hasBinlogPosition = true
	}

	if fileStr, ok := stream.GetStateKey(cdcFileKey).(string); ok {
		pos.Name = fileStr
		hasBinlogPosition = true
	}

	if serverIDStr := stream.GetStateKey(cdcServerIDKey); serverIDStr != nil {
		switch v := serverIDStr.(type) {
		case float64:
			serverID = uint32(v)
		case string:
			if idVal, err := strconv.ParseUint(v, 10, 32); err == nil {
				serverID = uint32(idVal)
			}
		}
	} else {
		stream.SetStateKey(cdcServerIDKey, serverID)
	}

	chunks := stream.GetStateChunks()
	if !hasBinlogPosition || chunks == nil || chunks.Len() != 0 {
		var err error
		pos, err = m.getCurrentBinlogPosition()
		if err != nil {
			return fmt.Errorf("failed to get current binlog position: %w", err)
		}

		stream.SetStateKey(cdcFileKey, pos.Name)
		stream.SetStateKey(cdcPositionKey, pos.Pos)

		if err := m.backfill(stream, pool); err != nil {
			return fmt.Errorf("failed to backfill stream %s: %w", stream.ID(), err)
		}

		logger.Infof("Backfill done for stream[%s]", stream.ID())
	}

	logger.Infof("Starting MySQL CDC for stream[%s] from binlog position %s:%d", stream.ID(), pos.Name, pos.Pos)

	syncerConfig := replication.BinlogSyncerConfig{
		ServerID:        serverID,
		Flavor:          "mysql",
		Host:            m.config.Hosts[0],
		Port:            uint16(m.config.Port),
		User:            m.config.Username,
		Password:        m.config.Password,
		Charset:         "utf8mb4",
		VerifyChecksum:  true,
		HeartbeatPeriod: heartbeatPeriod,
	}

	syncer := replication.NewBinlogSyncer(syncerConfig)
	defer syncer.Close()

	streamer, err := syncer.StartSync(pos)
	if err != nil {
		return fmt.Errorf("failed to start binlog sync: %w", err)
	}

	insert, err := pool.NewThread(context.TODO(), stream)
	if err != nil {
		return err
	}
	defer insert.Close()

	currentPos := pos
	consecutiveTimeouts := 0
	for {
		eventCtx, cancel := context.WithTimeout(context.Background(), eventReadTimeout)
		ev, err := streamer.GetEvent(eventCtx)
		cancel()

		if err != nil {
			if err == context.DeadlineExceeded {
				consecutiveTimeouts++
				if consecutiveTimeouts >= 2 {
					consecutiveTimeouts = 0
					masterPos, err := m.getCurrentBinlogPosition()
					if err != nil {
						logger.Errorf("Failed to get master status: %v", err)
						continue
					}

					if currentPos.Name == masterPos.Name && currentPos.Pos == masterPos.Pos {
						logger.Infof("Caught up to master position %s:%d, completing CDC for stream[%s]",
							masterPos.Name, masterPos.Pos, stream.ID())
						break
					}
				}
				continue
			}

			logger.Errorf("Error getting binlog event: %v, retrying...", err)
			success := false
			for i := 0; i < maxRetries; i++ {
				time.Sleep(retryDelay)
				eventCtx, cancel := context.WithTimeout(context.Background(), eventReadTimeout)
				ev, err = streamer.GetEvent(eventCtx)
				cancel()
				if err == nil {
					success = true
					break
				}
				logger.Errorf("Retry %d failed: %v", i+1, err)
			}
			if !success {
				return fmt.Errorf("failed to recover after %d retries: %w", maxRetries, err)
			}
		}

		consecutiveTimeouts = 0
		currentPos.Pos = ev.Header.LogPos

		switch e := ev.Event.(type) {
		case *replication.RotateEvent:
			currentPos.Name = string(e.NextLogName)
			currentPos.Pos = uint32(e.Position)
			logger.Infof("Binlog rotated to %s:%d for stream[%s]", currentPos.Name, currentPos.Pos, stream.ID())

		case *replication.RowsEvent:
			schemaName := string(e.Table.Schema)
			tableName := string(e.Table.Table)
			//streamID := fmt.Sprintf("%s.%s", schemaName, tableName)

			if schemaName == stream.Namespace() && tableName == stream.Name() {
				var operationType string
				switch ev.Header.EventType {
				case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
					operationType = "insert"
				case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
					operationType = "update"
				case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
					operationType = "delete"
				default:
					continue
				}

				var rowsToProcess [][]interface{}
				if operationType == "update" {
					for i := 1; i < len(e.Rows); i += 2 {
						rowsToProcess = append(rowsToProcess, e.Rows[i])
					}
				} else {
					rowsToProcess = e.Rows
				}

				for _, row := range rowsToProcess {
					record := m.convertRowToMap(stream, row, operationType)
					if record == nil {
						continue
					}

					pkColumn := getPrimaryKeyColumn(m.db, tableName)
					recordHash := utils.GetKeysHash(record, pkColumn)
					rawRecord := types.CreateRawRecord(recordHash, record, time.Now().UnixNano())
					exit, err := insert.Insert(rawRecord)
					if err != nil {
						return fmt.Errorf("failed to insert record: %w", err)
					}
					if exit {
						stream.SetStateKey(cdcFileKey, currentPos.Name)
						stream.SetStateKey(cdcPositionKey, currentPos.Pos)
						return nil
					}
				}
			}
		case *replication.QueryEvent:
			if err := m.handleQuery(stream, string(e.Schema), string(e.Query)); err != nil {
				logger.Errorf("Failed to handle query event: %v", err)
				return err
			}
		}

		stream.SetStateKey(cdcFileKey, currentPos.Name)
		stream.SetStateKey(cdcPositionKey, currentPos.Pos)
	}

	stream.SetStateKey(cdcFileKey, currentPos.Name)
	stream.SetStateKey(cdcPositionKey, currentPos.Pos)

	logger.Infof("Completed CDC processing for stream[%s] at position %s:%d",
		stream.ID(), currentPos.Name, currentPos.Pos)

	return nil
}

// handleQuery processes DDL statements similar to Estuary
func (m *MySQL) handleQuery(stream protocol.Stream, _, query string) error {
	streamID := stream.ID()

	// Ignore common queries that don't affect schema
	if strings.HasPrefix(strings.ToUpper(query), "BEGIN") ||
		strings.HasPrefix(strings.ToUpper(query), "COMMIT") ||
		strings.HasPrefix(strings.ToUpper(query), "ROLLBACK") {
		return nil
	}

	stmt, err := sqlparser.Parse(query)
	if err != nil {
		logger.Warnf("Failed to parse query event '%s': %v, ignoring", query, err)
		return nil
	}

	m.metadataMu.Lock()
	defer m.metadataMu.Unlock()

	meta, exists := m.metadata[streamID]
	if !exists {
		return fmt.Errorf("no metadata found for stream %s", streamID)
	}

	switch stmt := stmt.(type) {
	case *sqlparser.AlterTable:
		if schemaName := stmt.Table.Qualifier.String(); schemaName == "" || schemaName == stream.Namespace() {
			tableName := stmt.Table.Name.String()
			if tableName == stream.Name() {
				for _, alterOpt := range stmt.AlterOptions {
					switch alter := alterOpt.(type) {
					case *sqlparser.AddColumns:
						insertAt := len(meta.Schema.Columns)
						if alter.First {
							insertAt = 0
						}
						var newCols []string
						for _, col := range alter.Columns {
							colName := col.Name.String()
							newCols = append(newCols, colName)
							meta.Schema.ColumnTypes[colName] = "unknown"
						}
						meta.Schema.Columns = append(meta.Schema.Columns[:insertAt], append(newCols, meta.Schema.Columns[insertAt:]...)...)
						logger.Infof("Added columns %v to table %s", newCols, streamID)
					case *sqlparser.DropColumn:
						colName := alter.Name.Name.String()
						for i, existingCol := range meta.Schema.Columns {
							if strings.EqualFold(existingCol, colName) {
								meta.Schema.Columns = append(meta.Schema.Columns[:i], meta.Schema.Columns[i+1:]...)
								delete(meta.Schema.ColumnTypes, colName)
								logger.Infof("Dropped column %s from table %s", colName, streamID)
								break
							}
						}
					case *sqlparser.RenameColumn:
						oldName := alter.OldName.Name.String()
						newName := alter.NewName.Name.String()
						for i, existingCol := range meta.Schema.Columns {
							if strings.EqualFold(existingCol, oldName) {
								meta.Schema.Columns[i] = newName
								if colType, ok := meta.Schema.ColumnTypes[oldName]; ok {
									meta.Schema.ColumnTypes[newName] = colType
									delete(meta.Schema.ColumnTypes, oldName)
								}
								logger.Infof("Renamed column %s to %s in table %s", oldName, newName, streamID)
								break
							}
						}
					case *sqlparser.ModifyColumn:
						colName := alter.NewColDefinition.Name.String()
						for _, existingCol := range meta.Schema.Columns {
							if strings.EqualFold(existingCol, colName) {
								meta.Schema.ColumnTypes[colName] = "unknown"
								logger.Infof("Modified column %s in table %s", colName, streamID)
								break
							}
						}
					default:
						logger.Warnf("Unsupported ALTER TABLE operation in query: %s", query)
					}
				}

				// Save updated metadata to state
				if bs, err := json.Marshal(meta); err == nil {
					stream.SetStateKey(metadataStateKey, string(bs))
				}
			}
		}
	case *sqlparser.DropTable:
		for _, table := range stmt.FromTables {
			tableName := table.Name.String()
			schemaName := table.Qualifier.String()
			if schemaName == "" {
				schemaName = stream.Namespace()
			}
			if schemaName == stream.Namespace() && tableName == stream.Name() {
				return fmt.Errorf("table %s was dropped, stopping CDC for this stream", streamID)
			}
		}
	}

	return nil
}

// convertRowToMap converts a binary row to a map using cached metadata
func (m *MySQL) convertRowToMap(stream protocol.Stream, row []interface{}, operation string) map[string]interface{} {
	streamID := stream.ID()

	m.metadataMu.RLock()
	defer m.metadataMu.RUnlock()

	meta, exists := m.metadata[streamID]
	if !exists {
		logger.Errorf("No metadata found for stream %s", streamID)
		return nil
	}

	columns := meta.Schema.Columns
	if len(columns) != len(row) {
		logger.Errorf("Column count mismatch for stream %s: expected %d, got %d", streamID, len(columns), len(row))
		return nil
	}

	record := make(map[string]interface{})
	for i, val := range row {
		if i < len(columns) {
			record[columns[i]] = val
		}
	}

	record["cdc_type"] = operation
	return record
}

// getCurrentBinlogPosition retrieves the current binlog position from MySQL
func (m *MySQL) getCurrentBinlogPosition() (mysql.Position, error) {
	var pos mysql.Position

	rows, err := m.db.Query(jdbc.MySQLMasterStatusQuery())
	if err != nil {
		return pos, fmt.Errorf("failed to get master status: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return pos, fmt.Errorf("no binlog position available")
	}

	var file string
	var position uint32
	var binlogDoDB, binlogIgnoreDB string
	var executeGtidSet string

	err = rows.Scan(&file, &position, &binlogDoDB, &binlogIgnoreDB, &executeGtidSet)
	if err != nil {
		rows, err = m.db.Query(jdbc.MySQLMasterStatusQuery())
		if err != nil {
			return pos, fmt.Errorf("failed to retry master status: %w", err)
		}
		defer rows.Close()

		if !rows.Next() {
			return pos, fmt.Errorf("no binlog position available on retry")
		}

		err = rows.Scan(&file, &position, &binlogDoDB, &binlogIgnoreDB)
		if err != nil {
			return pos, fmt.Errorf("failed to scan binlog position: %w", err)
		}
	}

	pos.Name = file
	pos.Pos = position
	return pos, nil
}

// Close ensures proper cleanup
func (m *MySQL) Close() error {
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}
