package proxy

import (
	"context"
	"database/sql"
	"fmt"
	"net"

	"arctic-mirror/config"

	"github.com/jackc/pgx/v5/pgproto3"
	_ "github.com/marcboeker/go-duckdb"
)

type DuckDBProxy struct {
	config   *config.Config
	db       *sql.DB
	listener net.Listener
}

func NewDuckDBProxy(cfg *config.Config) (*DuckDBProxy, error) {
	// Initialize DuckDB
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("opening duckdb: %w", err)
	}

	// Install and load extensions
	if err := loadExtensions(db); err != nil {
		return nil, fmt.Errorf("loading extensions: %w", err)
	}

	// Create listener
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Proxy.Port))
	if err != nil {
		return nil, fmt.Errorf("creating listener: %w", err)
	}

	return &DuckDBProxy{
		config:   cfg,
		db:       db,
		listener: listener,
	}, nil
}

func loadExtensions(db *sql.DB) error {
	extensions := []string{"iceberg", "parquet"}
	for _, ext := range extensions {
		if _, err := db.Exec(fmt.Sprintf("INSTALL %s; LOAD %s;", ext, ext)); err != nil {
			return fmt.Errorf("loading extension %s: %w", ext, err)
		}
	}
	return nil
}

func (p *DuckDBProxy) Start(ctx context.Context) error {
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				continue
			}
		}

		go p.handleConnection(ctx, conn)
	}
}

func (p *DuckDBProxy) handleConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	backend := pgproto3.NewBackend(conn, conn)

	// Handle startup
	_, err := backend.ReceiveStartupMessage()
	if err != nil {
		return
	}

	// Send authentication OK
	backend.Send(&pgproto3.AuthenticationOk{})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := backend.Flush(); err != nil {
		return
	}

	// Main message loop
	for {
		msg, err := backend.Receive()
		if err != nil {
			return
		}

		switch msg := msg.(type) {
		case *pgproto3.Query:
			if err := p.handleQuery(ctx, backend, msg.String); err != nil {
				p.sendError(backend, err)
				continue
			}

		case *pgproto3.Terminate:
			return
		}
	}
}

func (p *DuckDBProxy) handleQuery(ctx context.Context, backend *pgproto3.Backend, query string) error {
	// Execute query using DuckDB
	rows, err := p.db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Get column descriptions
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	// Send row description
	if err := p.sendRowDescription(backend, columnTypes); err != nil {
		return err
	}

	// Send data rows
	values := make([]interface{}, len(columnTypes))
	scanArgs := make([]interface{}, len(columnTypes))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Send data rows
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}

		// Create data row message
		dataRow := &pgproto3.DataRow{
			Values: make([][]byte, len(columnTypes)),
		}

		// Convert values to bytes
		for i, val := range values {
			if val == nil {
				dataRow.Values[i] = nil
				continue
			}

			// Convert value to string representation
			dataRow.Values[i] = []byte(fmt.Sprintf("%v", val))
		}

		backend.Send(dataRow)
	}

	// Check for errors after iterating over rows
	if err := rows.Err(); err != nil {
		return err
	}

	// Send command complete
	backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT")})

	// Send ready for query
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})

	// Flush all sent messages
	if err := backend.Flush(); err != nil {
		return err
	}

	return nil
}

func (p *DuckDBProxy) sendRowDescription(backend *pgproto3.Backend, columns []*sql.ColumnType) error {
	fields := make([]pgproto3.FieldDescription, len(columns))
	for i, col := range columns {
		dataTypeOID := uint32(25) // Default to TEXT OID
		if databaseTypeName := col.DatabaseTypeName(); databaseTypeName != "" {
			// Map database type name to OID if necessary
			dataTypeOID = mapDataTypeToOID(databaseTypeName)
		}

		fields[i] = pgproto3.FieldDescription{
			Name:                 []byte(col.Name()),
			TableOID:             0,
			TableAttributeNumber: 0,
			DataTypeOID:          dataTypeOID,
			DataTypeSize:         -1,
			TypeModifier:         -1,
			Format:               0,
		}
	}

	backend.Send(&pgproto3.RowDescription{Fields: fields})
	return backend.Flush()
}

func (p *DuckDBProxy) sendError(backend *pgproto3.Backend, err error) {
	backend.Send(&pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     "XX000",
		Message:  err.Error(),
	})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	_ = backend.Flush()
}

func mapDataTypeToOID(databaseTypeName string) uint32 {
	switch databaseTypeName {
	case "BOOL":
		return 16 // BOOL OID
	case "INT8":
		return 20 // BIGINT OID
	case "INT4":
		return 23 // INTEGER OID
	case "FLOAT4":
		return 700 // REAL OID
	case "FLOAT8":
		return 701 // DOUBLE PRECISION OID
	case "VARCHAR", "TEXT":
		return 25 // TEXT OID
	case "DATE":
		return 1082 // DATE OID
	case "TIMESTAMP":
		return 1114 // TIMESTAMP OID
	default:
		return 25 // Default to TEXT OID
	}
}
