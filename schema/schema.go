package schema

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type Column struct {
	Name     string
	TypeOID  uint32
	TypeName string
	Nullable bool
}

type TableSchema struct {
	Schema  string
	Name    string
	Columns []Column
}

func GetTableSchema(ctx context.Context, conn *pgx.Conn, schemaName, tableName string) (*TableSchema, error) {
	query := `
        SELECT 
            c.column_name,
            c.is_nullable,
            t.oid AS type_oid,
            t.typname AS data_type
        FROM information_schema.columns c
        JOIN pg_catalog.pg_type t ON c.udt_name = t.typname
        WHERE c.table_schema = $1 AND c.table_name = $2
        ORDER BY c.ordinal_position;
    `

	rows, err := conn.Query(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("querying schema: %w", err)
	}
	defer rows.Close()

	schema := &TableSchema{
		Schema:  schemaName,
		Name:    tableName,
		Columns: make([]Column, 0),
	}

	for rows.Next() {
		var col Column
		var nullable string
		if err := rows.Scan(&col.Name, &nullable, &col.TypeOID, &col.TypeName); err != nil {
			return nil, fmt.Errorf("scanning column: %w", err)
		}
		col.Nullable = nullable == "YES"
		schema.Columns = append(schema.Columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading rows: %w", err)
	}

	return schema, nil
}
