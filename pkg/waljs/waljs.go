package waljs

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
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
	pgConn                     *pgconn.PgConn
	pgxConn                    *pgx.Conn
	waiter                     *time.Timer
	clientXLogPos              pglogrepl.LSN
	standbyMessageTimeout      time.Duration
	nextStandbyMessageDeadline time.Time
	changeFilter               ChangeFilter
	RestartLSN                 pglogrepl.LSN
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

func (s *Socket) StartReplication() error {
	err := pglogrepl.StartReplication(context.Background(), s.pgConn, s.ReplicationSlotName, s.RestartLSN, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		return fmt.Errorf("starting replication slot failed: %s", err)
	}
	logger.Infof("Started logical replication on slot[%s]", s.ReplicationSlotName)

	// Setup initial wait timeout to be the next message deadline to wait for a change log
	s.nextStandbyMessageDeadline = time.Now().Add(s.InitialWaitTime + 2*time.Second)

	logger.Debugf("Setting initial wait timer: %s", s.InitialWaitTime)
	s.waiter = time.AfterFunc(s.InitialWaitTime, func() {
		logger.Info("Closing sync. initial wait timer expired...")
	})
	return nil
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

func (s *Socket) increaseDeadline() {
	s.nextStandbyMessageDeadline = time.Now().Add(s.standbyMessageTimeout)
}

func (s *Socket) deadlineCrossed() bool {
	return time.Now().After(s.nextStandbyMessageDeadline)
}

func (s *Socket) StreamMessages(ctx context.Context, callback OnMessage) error {
	var cachedLSN *pglogrepl.LSN
recordIterator:
	for {
		select {
		case <-ctx.Done():
			break recordIterator
		default:
			exit, err := func() (bool, error) {
				if s.deadlineCrossed() {
					// adjusting with function being retriggered when not even a single message has been received
					s.increaseDeadline()
					return true, nil
				}

				ctx, cancel := context.WithDeadline(context.Background(), s.nextStandbyMessageDeadline)
				defer cancel()

				rawMsg, err := s.pgConn.ReceiveMessage(ctx)
				if err != nil {
					if pgconn.Timeout(err) || err == io.EOF || err == io.ErrUnexpectedEOF {
						return true, nil
					}

					return false, fmt.Errorf("failed to receive messages from PostgreSQL %s", err)
				}

				if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
					return false, fmt.Errorf("received broken Postgres WAL. Error: %+v", errMsg)
				}

				msg, ok := rawMsg.(*pgproto3.CopyData)
				if !ok {
					return false, fmt.Errorf("received unexpected message: %T", rawMsg)
				}

				switch msg.Data[0] {
				case pglogrepl.PrimaryKeepaliveMessageByteID:
					pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
					if err != nil {
						return false, fmt.Errorf("ParsePrimaryKeepaliveMessage failed: %s", err)
					}

					if pkm.ReplyRequested {
						s.nextStandbyMessageDeadline = time.Time{}
					}

				case pglogrepl.XLogDataByteID:
					xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
					if err != nil {
						return false, fmt.Errorf("ParseXLogData failed: %s", err)
					}

					// Cache LSN here to be used during acknowledgement
					clientXLogPos := xld.WALStart + pglogrepl.LSN(len(xld.WALData))
					cachedLSN = &clientXLogPos
					err = s.changeFilter.FilterChange(clientXLogPos, xld.WALData, func(change WalJSChange) {
						callback(change)

						// stop waiter after a record has been recieved
						if s.waiter != nil {
							s.waiter.Stop()
						}
					})
					if err != nil {
						return false, err
					}
				}
				s.increaseDeadline()
				return false, nil
			}()
			if err != nil {
				return err
			}

			// acknowledge and exit only when we can acknowledge a LSN
			// This helps in hooking till atleast getting one message from
			if exit && cachedLSN != nil {
				s.clientXLogPos = *cachedLSN
				break recordIterator
			}
		}
	}

	return nil
}

// cleanUpOnFailure drops replication slot and publication if database snapshotting was failed for any reason
func (s *Socket) Cleanup() {
	s.pgConn.Close(context.TODO())
	s.pgxConn.Close(context.TODO())
}
