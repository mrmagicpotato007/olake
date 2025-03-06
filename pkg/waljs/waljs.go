package waljs

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/logger"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jmoiron/sqlx"
)

const (
	ReplicationSlotTempl = "SELECT plugin, slot_type, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '%s'"
)

var pluginArguments = []string{
	"\"include-lsn\" 'on'",
	"\"pretty-print\" 'off'",
	"\"include-timestamp\" 'on'",
}

type Socket struct {
	*Config
	pgConn                *pgconn.PgConn
	pgxConn               *pgx.Conn
	clientXLogPos         pglogrepl.LSN
	idleStartTime         time.Time
	standbyMessageTimeout time.Duration
	changeFilter          ChangeFilter
	RestartLSN            pglogrepl.LSN
}

func NewConnection(db *sqlx.DB, config *Config) (*Socket, error) {
	conn, err := pgx.Connect(context.Background(), config.Connection.String())
	if err != nil {
		return nil, err
	}

	query := config.Connection.Query()
	query.Add("replication", "database")
	config.Connection.RawQuery = query.Encode()

	cfg, err := pgconn.ParseConfig(config.Connection.String())
	if err != nil {
		return nil, err
	}

	if config.TLSConfig != nil {
		cfg.TLSConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	dbConn, err := pgconn.ConnectConfig(context.Background(), cfg)
	if err != nil {
		return nil, err
	}

	connection := &Socket{
		Config:                config,
		standbyMessageTimeout: time.Second,
		pgConn:                dbConn,
		pgxConn:               conn,
		changeFilter:          NewChangeFilter(config.Tables.Array()...),
	}

	sysident, err := pglogrepl.IdentifySystem(context.Background(), connection.pgConn)
	if err != nil {
		return nil, fmt.Errorf("failed to identify the system: %s", err)
	}

	logger.Infof("system identification result SystemID:%s, Timeline: %d, XLogPos: %s, Database: %s", sysident.SystemID, sysident.Timeline, sysident.XLogPos, sysident.DBName)

	slot := ReplicationSlot{}
	err = db.Get(&slot, fmt.Sprintf(ReplicationSlotTempl, config.ReplicationSlotName))
	if err != nil {
		return nil, err
	}

	connection.RestartLSN = slot.LSN
	connection.clientXLogPos = slot.LSN

	return connection, err
}

// Confirm that Logs has been recorded
func (s *Socket) AcknowledgeLSN() error {
	err := pglogrepl.SendStandbyStatusUpdate(context.Background(), s.pgConn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: s.clientXLogPos,
		WALFlushPosition: s.clientXLogPos,
	})
	if err != nil {
		return fmt.Errorf("failed to send standby status message: %s", err)
	}

	// Update local pointer and state
	logger.Debugf("sent standby status message at LSN#%s", s.clientXLogPos.String())
	return nil
}

func (s *Socket) StreamMessages(ctx context.Context, callback OnMessage) error {
	// Start logical replication with wal2json plugin arguments.
	if err := pglogrepl.StartReplication(
		context.Background(),
		s.pgConn,
		s.ReplicationSlotName,
		s.RestartLSN,
		pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments},
	); err != nil {
		return fmt.Errorf("starting replication slot failed: %s", err)
	}
	logger.Infof("Started logical replication on slot[%s]", s.ReplicationSlotName)
	s.idleStartTime = time.Now()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if time.Since(s.idleStartTime) > s.InitialWaitTime {
				logger.Debug("Idle timeout reached, waiting for new messages")
				return nil
			}
			// Use a context with timeout for receiving a message.
			msg, err := s.pgConn.ReceiveMessage(ctx)
			// If the receive timed out, log the idle state and continue waiting.
			if err != nil {
				return fmt.Errorf("failed to receive message: %w", err)
			}

			// Process only CopyData messages.
			copyData, ok := msg.(*pgproto3.CopyData)
			if !ok {
				return fmt.Errorf("unexpected message type: %T", msg)
			}

			switch copyData.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				// For keepalive messages, process them (but no ack is sent here).
				_, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
				if err != nil {
					return fmt.Errorf("failed to parse primary keepalive message: %w", err)
				}

			case pglogrepl.XLogDataByteID:
				// Reset the idle timer on receiving WAL data.
				s.idleStartTime = time.Now()
				xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
				if err != nil {
					return fmt.Errorf("failed to parse XLogData: %w", err)
				}
				// Calculate new LSN based on the received WAL data.
				newLSN := xld.WALStart + pglogrepl.LSN(len(xld.WALData))
				// Process change with the provided callback.
				if err := s.changeFilter.FilterChange(newLSN, xld.WALData, callback); err != nil {
					return fmt.Errorf("failed to filter change: %w", err)
				}
				// Update the current LSN pointer.
				s.clientXLogPos = newLSN

			default:
				logger.Debugf("Received unhandled message type: %v", copyData.Data[0])
			}
		}
	}
}

// cleanUpOnFailure drops replication slot and publication if database snapshotting was failed for any reason
func (s *Socket) Cleanup() {
	s.pgConn.Close(context.TODO())
	s.pgxConn.Close(context.TODO())
}
