package replication

import (
	"arctic-mirror/config"
	"arctic-mirror/iceberg"
	"arctic-mirror/schema"
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type Replicator struct {
	config          *config.Config
	dbConn          *pgx.Conn
	replicationConn *pgconn.PgConn
	writer          *iceberg.Writer
	schemaManager   *schema.Manager
}

func NewReplicator(cfg *config.Config) (*Replicator, error) {
	// Create a regular connection for querying the database
	dbConn, err := pgx.Connect(context.Background(), fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s",
		cfg.Postgres.User,
		cfg.Postgres.Password,
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.Database,
	))
	if err != nil {
		return nil, fmt.Errorf("connecting to postgres: %w", err)
	}

	// Initialize schema manager
	schemaManager := schema.NewSchemaManager(dbConn)

	// Initialize schemas for configured tables
	for _, table := range cfg.Tables {
		if err := schemaManager.InitializeSchema(context.Background(),
			table.Schema, table.Name); err != nil {
			return nil, fmt.Errorf("initializing schema for %s.%s: %w",
				table.Schema, table.Name, err)
		}
	}

	// Create a replication connection using pgconn
	replicationConn, err := pgconn.Connect(context.Background(), fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?replication=database",
		cfg.Postgres.User,
		cfg.Postgres.Password,
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.Database,
	))
	if err != nil {
		return nil, fmt.Errorf("connecting to postgres for replication: %w", err)
	}

	// Initialize Iceberg writer
	writer, err := iceberg.NewWriter(cfg.Iceberg.Path, schemaManager)
	if err != nil {
		return nil, fmt.Errorf("creating iceberg writer: %w", err)
	}

	return &Replicator{
		config:          cfg,
		dbConn:          dbConn,
		replicationConn: replicationConn,
		writer:          writer,
		schemaManager:   schemaManager,
	}, nil
}

func (r *Replicator) Start(ctx context.Context) error {
	defer r.dbConn.Close(context.Background())
	defer r.replicationConn.Close(context.Background())

	// Create replication slot if needed
	if err := r.createReplicationSlot(ctx); err != nil {
		return fmt.Errorf("creating replication slot: %w", err)
	}

	// Start replication
	return r.startReplication(ctx)
}

func (r *Replicator) createReplicationSlot(ctx context.Context) error {
	_, err := pglogrepl.CreateReplicationSlot(ctx, r.replicationConn, r.config.Postgres.Slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Temporary: true,
		Mode:      pglogrepl.LogicalReplication,
	})
	if err != nil {
		// Ignore if slot already exists
		var pgerr *pgconn.PgError
		if errors.As(err, &pgerr) {
			if pgerr.Code == "42710" {
				// Duplicate object error, slot already exists
				return nil
			}
		}
		return fmt.Errorf("error creating replication slot: %w", err)
	}
	return nil
}

func (r *Replicator) startReplication(ctx context.Context) error {
	// Get the current WAL position (LSN)
	var startLSN pglogrepl.LSN = 0 // Replace with your starting LSN

	// Start replication
	err := pglogrepl.StartReplication(ctx, r.replicationConn, r.config.Postgres.Slot, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '2'",
			"messages 'true'",
			"streaming 'true'",
			fmt.Sprintf("publication_names '%s'", r.config.Postgres.Publication), // Use the actual publication name
		},
	})
	if err != nil {
		return fmt.Errorf("starting replication: %w", err)
	}

	return r.handleReplication(ctx)
}

func (r *Replicator) handleReplication(ctx context.Context) error {
	clientXLogPos := pglogrepl.LSN(0) // Starting LSN; you might want to initialize this properly
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)
	relations := make(map[uint32]*pglogrepl.RelationMessageV2)
	inStream := false

	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err := pglogrepl.SendStandbyStatusUpdate(ctx, r.replicationConn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: clientXLogPos,
			})
			if err != nil {
				return fmt.Errorf("SendStandbyStatusUpdate failed: %w", err)
			}
			log.Printf("Sent Standby status message at %s\n", clientXLogPos.String())
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		rawMsg, err := r.replicationConn.ReceiveMessage(ctx)
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return fmt.Errorf("ReceiveMessage failed: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("received Postgres WAL error: %+v", errMsg)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			// Unexpected message type
			continue
		}

		if len(msg.Data) == 0 {
			return fmt.Errorf("empty CopyData message received")
		}

		// The first byte of msg.Data indicates the message type
		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID: // 'k'
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("ParsePrimaryKeepaliveMessage failed: %w", err)
			}
			log.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)
			if pkm.ServerWALEnd > clientXLogPos {
				clientXLogPos = pkm.ServerWALEnd
			}
			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{}
			}

		case pglogrepl.XLogDataByteID: // 'w'
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("ParseXLogData failed: %w", err)
			}

			log.Printf("XLogData => WALStart %s ServerWALEnd %s ServerTime %s WALData:\n", xld.WALStart, xld.ServerWALEnd, xld.ServerTime)
			if xld.WALStart > clientXLogPos {
				clientXLogPos = xld.WALStart
			}

			walData := xld.WALData
			logicalMsg, err := pglogrepl.ParseV2(walData, inStream)
			if err != nil {
				return fmt.Errorf("parsing logical replication message: %w", err)
			}

			switch m := logicalMsg.(type) {
			case *pglogrepl.RelationMessageV2:
				relations[m.RelationID] = m
				// Update schema if necessary
				if err := r.schemaManager.HandleRelationMessage(m); err != nil {
					return fmt.Errorf("handling relation message: %w", err)
				}

			case *pglogrepl.BeginMessage:
				// Handle begin
				log.Println("Begin transaction")

			case *pglogrepl.CommitMessage:
				// Handle commit
				if err := r.writer.Commit(); err != nil {
					return fmt.Errorf("committing: %w", err)
				}

			case *pglogrepl.InsertMessageV2:
				log.Printf("insert for xid %d\n", m.Xid)
				rel, ok := relations[m.RelationID]
				if !ok {
					return fmt.Errorf("unknown relation ID %d", m.RelationID)
				}
				if err := r.writer.WriteInsert(m, rel); err != nil {
					return fmt.Errorf("writing insert: %w", err)
				}

			case *pglogrepl.UpdateMessageV2:
				log.Printf("update for xid %d\n", m.Xid)
				rel, ok := relations[m.RelationID]
				if !ok {
					return fmt.Errorf("unknown relation ID %d", m.RelationID)
				}
				if err := r.writer.WriteUpdate(m, rel); err != nil {
					return fmt.Errorf("writing update: %w", err)
				}

			case *pglogrepl.DeleteMessageV2:
				log.Printf("delete for xid %d\n", m.Xid)
				rel, ok := relations[m.RelationID]
				if !ok {
					return fmt.Errorf("unknown relation ID %d", m.RelationID)
				}
				if err := r.writer.WriteDelete(m, rel); err != nil {
					return fmt.Errorf("writing delete: %w", err)
				}

			case *pglogrepl.LogicalDecodingMessageV2:
				log.Printf("Logical decoding message")

			case *pglogrepl.StreamStartMessageV2:
				inStream = true
				log.Printf("Stream start message")
			case *pglogrepl.StreamStopMessageV2:
				inStream = false
				log.Printf("Stream stop message")
			case *pglogrepl.StreamCommitMessageV2:
				log.Printf("Stream commit message")
			case *pglogrepl.StreamAbortMessageV2:
				log.Printf("Stream abort message")
			default:
				log.Printf("Unknown message type in pgoutput stream")
			}

		default:
			// Unknown message type
			return fmt.Errorf("unknown replication message type: %c", msg.Data[0])
		}
	}
}
