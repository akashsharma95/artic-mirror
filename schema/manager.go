package schema

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

// Manager is a manager for PostgreSQL schemas.
type Manager struct {
	conn          *pgx.Conn
	schemas       map[uint32]*TableSchema // Maps relation ID to schema
	schemasByName map[string]*TableSchema // Maps "schema.table" to schema
	mu            sync.RWMutex
}

func NewSchemaManager(conn *pgx.Conn) *Manager {
	return &Manager{
		conn:          conn,
		schemas:       make(map[uint32]*TableSchema),
		schemasByName: make(map[string]*TableSchema),
	}
}

// GetSchema returns schema by relation ID, loading it if necessary
func (m *Manager) GetSchema(relationID uint32) (*TableSchema, error) {
	m.mu.RLock()
	schema, exists := m.schemas[relationID]
	m.mu.RUnlock()

	if exists {
		return schema, nil
	}

	return nil, fmt.Errorf("schema not found for relation ID: %d", relationID)
}

// HandleRelationMessage processes PostgreSQL relation messages to cache schemas
func (m *Manager) HandleRelationMessage(msg *pglogrepl.RelationMessageV2) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	schema := &TableSchema{
		Schema:  msg.Namespace,
		Name:    msg.RelationName,
		Columns: make([]Column, len(msg.Columns)),
	}

	for i, col := range msg.Columns {
		schema.Columns[i] = Column{
			Name:     col.Name,
			TypeOID:  col.DataType,
			Nullable: (col.Flags & pglogrepl.TupleDataTypeNull) == 0,
		}
	}

	m.schemas[msg.RelationID] = schema
	m.schemasByName[fmt.Sprintf("%s.%s", msg.Namespace, msg.RelationName)] = schema

	return nil
}

// InitializeSchema loads schema for specified tables
func (m *Manager) InitializeSchema(ctx context.Context, schemaName, tableName string) error {
	schema, err := GetTableSchema(ctx, m.conn, schemaName, tableName)
	if err != nil {
		return fmt.Errorf("getting table schema: %w", err)
	}

	// Get relation ID
	var relationID uint32
	err = m.conn.QueryRow(ctx, `
        SELECT c.oid 
        FROM pg_class c 
        JOIN pg_namespace n ON n.oid = c.relnamespace 
        WHERE n.nspname = $1 AND c.relname = $2
    `, schemaName, tableName).Scan(&relationID)
	if err != nil {
		return fmt.Errorf("getting relation ID: %w", err)
	}

	m.mu.Lock()
	m.schemas[relationID] = schema
	m.schemasByName[fmt.Sprintf("%s.%s", schemaName, tableName)] = schema
	m.mu.Unlock()

	return nil
}
