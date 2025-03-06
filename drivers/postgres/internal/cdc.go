package driver

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/pkg/waljs"
	"github.com/datazip-inc/olake/protocol"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/jackc/pglogrepl"
	"github.com/jmoiron/sqlx"
)

func (p *Postgres) prepareWALJSConfig(streams ...protocol.Stream) (*waljs.Config, error) {
	if !p.Driver.CDCSupport {
		return nil, fmt.Errorf("invalid call; %s not running in CDC mode", p.Type())
	}

	config := &waljs.Config{
		Connection:          *p.config.Connection,
		ReplicationSlotName: p.cdcConfig.ReplicationSlot,
		InitialWaitTime:     time.Duration(p.cdcConfig.InitialWaitTime) * time.Second,
		Tables:              types.NewSet[protocol.Stream](streams...),
		BatchSize:           p.config.BatchSize,
	}
	return config, nil
}

func (p *Postgres) StateType() types.StateType {
	return types.MixedType
}

// func (p *Postgres) GlobalState() any {
// 	return p.cdcState
// }

func (p *Postgres) SetupGlobalState(state *types.State) error {
	state.Type = p.StateType()
	// Setup raw state
	p.cdcState = types.NewGlobalState(&waljs.WALState{})

	return base.ManageGlobalState(state, p.cdcState, p)
}

// Write Ahead Log Sync
func (p *Postgres) RunChangeStream(pool *protocol.WriterPool, streams ...protocol.Stream) (err error) {
	cdcCtx := context.TODO()
	config, err := p.prepareWALJSConfig(streams...)
	if err != nil {
		return err
	}

	socket, err := waljs.NewConnection(p.client, config)
	if err != nil {
		return err
	}
	defer socket.Cleanup()
	fullLoad := func() error {
		// TODO: Save Global state
		return utils.Concurrent(cdcCtx, streams, len(streams), func(ctx context.Context, stream protocol.Stream, _ int) error {
			err := p.backfill(pool, stream)
			if err != nil {
				return err
			}
			p.cdcState.Streams.Insert(stream.ID())
			return nil
		})
	}
	if p.cdcState.State.LSN != "" {
		stateLSN, err := pglogrepl.ParseLSN(p.cdcState.State.LSN)
		if err != nil {
			return fmt.Errorf("failed to parse State LSN: %s", err)
		}
		// difference in confirmed flush lsn from State and DB
		if stateLSN.String() != socket.RestartLSN.String() {
			if err := fullLoad(); err != nil {
				return err
			}
		}
	} else {
		// save current lsn
		p.cdcState.State.LSN = socket.RestartLSN.String()
		if err := fullLoad(); err != nil {
			return err
		}
	}

	insertionMap := make(map[protocol.Stream]*protocol.ThreadEvent)
	errorChannelMap := make(map[protocol.Stream]chan error)
	for _, stream := range streams {
		errorChannelMap[stream] = make(chan error)
		insert, err := pool.NewThread(cdcCtx, stream, protocol.WithErrorChannel(errorChannelMap[stream]))
		if err != nil {
			return err
		}
		insertionMap[stream] = insert
	}

	defer func() {
		if err == nil {
			for stream, insertThread := range insertionMap {
				insertThread.Close()
				err = <-errorChannelMap[stream]
			}
			if err != nil {
				err = socket.AcknowledgeLSN()
				// if err != nil {
				// TODO: Update state or take action accordingly
				// }
			}
		}

	}()

	return socket.StreamMessages(cdcCtx, func(message waljs.WalJSChange) error {
		var deleteTimeStamp int64
		if message.Kind == "delete" {
			deleteTimeStamp = message.Timestamp.UnixMilli()
		}
		if message.LSN != nil {
			message.Data[constants.CDCLSN] = message.LSN
		}
		message.Data[constants.CDCUpdatedAt] = message.Timestamp

		// get olake_key_id
		olakeID := utils.GetKeysHash(message.Data, message.Stream.GetStream().SourceDefinedPrimaryKey.Array()...)

		// insert record
		rawRecord := types.CreateRawRecord(olakeID, message.Data, deleteTimeStamp)
		err := insertionMap[message.Stream].Insert(rawRecord)
		if err != nil {
			return err
		}

		return nil
	})
}

func doesReplicationSlotExists(conn *sqlx.DB, slotName string) (bool, error) {
	var exists bool
	err := conn.QueryRow(
		"SELECT EXISTS(Select 1 from pg_replication_slots where slot_name = $1)",
		slotName,
	).Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, validateReplicationSlot(conn, slotName)
}

func validateReplicationSlot(conn *sqlx.DB, slotName string) error {
	slot := waljs.ReplicationSlot{}
	err := conn.Get(&slot, fmt.Sprintf(waljs.ReplicationSlotTempl, slotName))
	if err != nil {
		return err
	}

	if slot.Plugin != "wal2json" {
		return fmt.Errorf("plugin not supported[%s]: driver only supports wal2json", slot.Plugin)
	}

	if slot.SlotType != "logical" {
		return fmt.Errorf("only logical slots are supported: %s", slot.SlotType)
	}

	return nil
}
