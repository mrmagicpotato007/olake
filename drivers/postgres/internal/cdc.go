package driver

import (
	"context"
	"fmt"
	"time"

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
	return types.GlobalType
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

func (p *Postgres) RunChangeStream(pool *protocol.WriterPool, streams ...protocol.Stream) (err error) {
	ctx := context.TODO()

	// Initialize replication configuration
	config, err := p.prepareWALJSConfig(streams...)
	if err != nil {
		return fmt.Errorf("failed to prepare WAL config: %w", err)
	}

	// Establish replication connection
	socket, err := waljs.NewConnection(p.client, config)
	if err != nil {
		return fmt.Errorf("failed to create replication connection: %w", err)
	}
	defer socket.Cleanup()

	fullLoad := func(ctx context.Context, pool *protocol.WriterPool, streams []protocol.Stream) error {
		return utils.Concurrent(ctx, streams, len(streams), func(ctx context.Context, s protocol.Stream, _ int) error {
			if err := p.backfill(pool, s); err != nil {
				return fmt.Errorf("backfill failed for %s: %w", s.ID(), err)
			}
			p.cdcState.Streams.Insert(s.ID())
			return nil
		})
	}

	// Handle initial data load if needed
	currentLSN := socket.RestartLSN.String()
	if p.cdcState.State.LSN == "" {
		p.cdcState.State.LSN = currentLSN
		if err := fullLoad(ctx, pool, streams); err != nil {
			return fmt.Errorf("initial full load failed: %w", err)
		}
	} else if storedLSN, parseErr := pglogrepl.ParseLSN(p.cdcState.State.LSN); parseErr != nil {
		return fmt.Errorf("invalid stored LSN format: %w", parseErr)
	} else if storedLSN.String() != currentLSN {
		if err := fullLoad(ctx, pool, streams); err != nil {
			return fmt.Errorf("delta load failed: %w", err)
		}
	}

	// Create parallel inserters for each stream
	inserters := make(map[protocol.Stream]*protocol.ThreadEvent)
	errChannels := make(map[protocol.Stream]chan error)

	for _, stream := range streams {
		errChan := make(chan error)
		inserter, err := pool.NewThread(ctx, stream, protocol.WithErrorChannel(errChan))
		if err != nil {
			return fmt.Errorf("failed to create inserter for %s: %w", stream.ID(), err)
		}
		inserters[stream] = inserter
		errChannels[stream] = errChan
	}

	// Cleanup and final error collection
	defer func() {
		for stream, inserter := range inserters {
			inserter.Close()
			if threadErr := <-errChannels[stream]; threadErr != nil && err == nil {
				err = fmt.Errorf("inserter error for %s: %w", stream.ID(), threadErr)
			}
		}

		if err == nil {
			// TODO : Save Global State before lsn marked
			err = socket.AcknowledgeLSN()
		}
	}()

	// Process incoming changes
	return socket.StreamMessages(ctx, func(msg waljs.WalJSChange) error {
		pkFields := msg.Stream.GetStream().SourceDefinedPrimaryKey.Array()
		record := types.CreateRawRecord(
			utils.GetKeysHash(msg.Data, pkFields...),
			msg.Data,
			utils.IfThenElse(msg.Kind == "delete", msg.Timestamp.UnixMilli(), 0),
		)

		// Insert using appropriate thread
		return inserters[msg.Stream].Insert(record)
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
