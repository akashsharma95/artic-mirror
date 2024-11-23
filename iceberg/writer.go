package iceberg

import (
	"arctic-mirror/schema"
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"github.com/parquet-go/parquet-go"
)

type Writer struct {
	basePath      string
	writers       map[uint32]*tableWriter
	mu            sync.RWMutex
	schemaManager *schema.Manager
}

type tableWriter struct {
	schema        *SchemaV2
	parquetSchema *parquet.Schema
	writer        *parquet.GenericWriter[map[string]interface{}]
	path          string
	records       int64
	metadata      *TableMetadata
	manifests     []ManifestEntry
	mu            sync.Mutex
	file          *os.File
}

func NewWriter(basePath string, schemaManager *schema.Manager) (*Writer, error) {
	return &Writer{
		basePath:      basePath,
		writers:       make(map[uint32]*tableWriter),
		schemaManager: schemaManager,
	}, nil
}

func (w *Writer) WriteInsert(msg *pglogrepl.InsertMessageV2, rel *pglogrepl.RelationMessageV2) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	tw, err := w.getTableWriter(msg.RelationID)
	if err != nil {
		return err
	}

	record, err := tw.mapTupleToRecord(msg.Tuple, rel)
	if err != nil {
		return fmt.Errorf("mapping tuple to record: %w", err)
	}

	if _, err := tw.writer.Write([]map[string]interface{}{record}); err != nil {
		return fmt.Errorf("writing record: %w", err)
	}

	tw.records++
	return nil
}

func (w *Writer) WriteUpdate(msg *pglogrepl.UpdateMessageV2, rel *pglogrepl.RelationMessageV2) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	tw, err := w.getTableWriter(msg.RelationID)
	if err != nil {
		return err
	}

	record, err := tw.mapTupleToRecord(msg.NewTuple, rel)
	if err != nil {
		return fmt.Errorf("mapping tuple to record: %w", err)
	}

	if _, err := tw.writer.Write([]map[string]interface{}{record}); err != nil {
		return fmt.Errorf("writing record: %w", err)
	}

	tw.records++
	return nil
}

func (w *Writer) WriteDelete(msg *pglogrepl.DeleteMessageV2, rel *pglogrepl.RelationMessageV2) error {
	// Implement delete handling if necessary
	// For simplicity, you might log or skip deletes
	return nil
}

func (w *Writer) Commit() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, tw := range w.writers {
		if err := tw.commit(context.Background()); err != nil {
			return err
		}
	}

	// Clear writers after commit
	w.writers = make(map[uint32]*tableWriter)
	return nil
}

func (w *Writer) getTableWriter(relationID uint32) (*tableWriter, error) {
	if tw, exists := w.writers[relationID]; exists {
		return tw, nil
	}

	tw, err := w.createWriter(relationID)
	if err != nil {
		return nil, err
	}

	w.writers[relationID] = tw
	return tw, nil
}

func (w *Writer) createWriter(relationID uint32) (*tableWriter, error) {
	// Get PostgreSQL schema
	pgSchema, err := w.schemaManager.GetSchema(relationID)
	if err != nil {
		return nil, fmt.Errorf("getting schema: %w", err)
	}

	// Create Iceberg schema
	schema := SchemaV2{
		SchemaID: 0,
		Fields:   make([]Field, 0, len(pgSchema.Columns)),
	}

	// Map PostgreSQL types to Iceberg types
	for i, col := range pgSchema.Columns {
		field := Field{
			ID:       i + 1,
			Name:     col.Name,
			Required: !col.Nullable,
			Type:     postgresTypeToIceberg(col.TypeOID),
		}
		schema.Fields = append(schema.Fields, field)
	}

	// Create Parquet schema from Iceberg schema
	parquetSchema, err := createParquetSchema(schema)
	if err != nil {
		return nil, fmt.Errorf("creating parquet schema: %w", err)
	}

	// Create data path
	dataPath := fmt.Sprintf(
		"data/%s.%s/%s.parquet",
		pgSchema.Schema,
		pgSchema.Name,
		time.Now().Format("20060102150405"),
	)
	fullPath := filepath.Join(w.basePath, dataPath)

	// Initialize table metadata if not exists
	metadata, err := w.getOrCreateMetadata(pgSchema, schema)
	if err != nil {
		return nil, fmt.Errorf("initializing metadata: %w", err)
	}

	// Create directories if they don't exist
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return nil, fmt.Errorf("creating directories: %w", err)
	}

	// Open Parquet file
	file, err := os.Create(fullPath)
	if err != nil {
		return nil, fmt.Errorf("creating parquet file: %w", err)
	}

	// Initialize Parquet writer
	pw := parquet.NewGenericWriter[map[string]interface{}](file, parquetSchema)

	return &tableWriter{
		schema:        &schema,
		parquetSchema: parquetSchema,
		writer:        pw,
		path:          dataPath,
		metadata:      metadata,
		file:          file,
		manifests:     make([]ManifestEntry, 0),
	}, nil
}

func (w *Writer) getOrCreateMetadata(pgSchema *schema.TableSchema, icebergSchema SchemaV2) (*TableMetadata, error) {
	metadataPath := filepath.Join(w.basePath, pgSchema.Schema, pgSchema.Name, "metadata", "metadata.json")

	// Check if metadata exists
	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		// Create new metadata
		metadata := &TableMetadata{
			FormatVersion: 2,
			TableUUID:     uuid.New().String(),
			Location:      filepath.Dir(metadataPath),
			LastUpdated:   time.Now().UnixNano() / int64(time.Millisecond),
			LastColumnID:  len(icebergSchema.Fields),
			SchemaID:      icebergSchema.SchemaID,
			Schemas:       []SchemaV2{icebergSchema},
			CurrentSchema: icebergSchema,
			PartitionSpec: []PartitionSpec{}, // No partitioning
			Properties:    map[string]string{},
			Snapshots:     []*Snapshot{},
		}

		// Write metadata
		if err := w.writeMetadata(context.Background(), metadata, metadataPath); err != nil {
			return nil, fmt.Errorf("writing metadata: %w", err)
		}

		return metadata, nil
	} else if err != nil {
		return nil, fmt.Errorf("checking metadata: %w", err)
	}

	// Load existing metadata
	file, err := os.Open(metadataPath)
	if err != nil {
		return nil, fmt.Errorf("opening metadata: %w", err)
	}
	defer file.Close()

	var metadata TableMetadata
	if err := json.NewDecoder(file).Decode(&metadata); err != nil {
		return nil, fmt.Errorf("decoding metadata: %w", err)
	}

	return &metadata, nil
}

func (w *Writer) writeMetadata(ctx context.Context, metadata *TableMetadata, metadataPath string) error {
	metadataDir := filepath.Dir(metadataPath)
	if err := os.MkdirAll(metadataDir, 0755); err != nil {
		return fmt.Errorf("creating metadata directory: %w", err)
	}

	file, err := os.Create(metadataPath)
	if err != nil {
		return fmt.Errorf("creating metadata file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(metadata); err != nil {
		return fmt.Errorf("encoding metadata: %w", err)
	}

	return nil
}

func (tw *tableWriter) mapTupleToRecord(tuple *pglogrepl.TupleData, rel *pglogrepl.RelationMessageV2) (map[string]interface{}, error) {
	typeMap := pgtype.NewMap()

	record := make(map[string]interface{})

	for idx, col := range tuple.Columns {
		colName := rel.Columns[idx].Name
		dataType := rel.Columns[idx].DataType
		formatCode := pgtype.TextFormatCode // or pgtype.BinaryFormatCode, depending on `col.DataType`

		switch col.DataType {
		case 'n': // null
			record[colName] = nil
		case 't': // text
			// Decode the column data according to its PostgreSQL data type
			val, err := decodeColumnData(typeMap, col.Data, dataType, int16(formatCode))
			if err != nil {
				return nil, fmt.Errorf("decoding column data for %s: %w", colName, err)
			}
			record[colName] = val
		case 'b': // binary
			// Handle binary data if necessary
			record[colName] = col.Data
		case 'u': // unchanged TOAST data
			record[colName] = nil
		default:
			return nil, fmt.Errorf("unknown column data type: %v", col.DataType)
		}
	}
	return record, nil
}

func decodeColumnData(typeMap *pgtype.Map, data []byte, dataTypeOID uint32, formatCode int16) (interface{}, error) {
	// Retrieve the DataType for the given OID
	dataType, ok := typeMap.TypeForOID(dataTypeOID)
	if !ok {
		// If the data type is unknown, default to returning the data as a string
		return string(data), nil
	}

	// Use the Codec's DecodeValue method to decode the data directly
	value, err := dataType.Codec.DecodeValue(typeMap, dataTypeOID, formatCode, data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode value for OID %d: %w", dataTypeOID, err)
	}

	// Return the decoded Go value
	return value, nil
}

func (tw *tableWriter) commit(ctx context.Context) error {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	// Close Parquet writer
	if err := tw.writer.Close(); err != nil {
		return fmt.Errorf("closing parquet writer: %w", err)
	}

	// Collect metrics
	metrics := tw.collectMetrics()

	fileInfo, err := tw.file.Stat()
	if err != nil {
		return fmt.Errorf("getting file size: %w", err)
	}

	// Create manifest entry
	entry := ManifestEntry{
		Status:     1, // Added
		SnapshotID: time.Now().UnixNano() / int64(time.Millisecond),
		DataFile: DataFile{
			FilePath:      tw.path,
			FileFormat:    "PARQUET",
			RecordCount:   tw.records,
			FileSizeBytes: fileInfo.Size(),
			Metrics:       metrics,
		},
	}

	tw.manifests = append(tw.manifests, entry)

	// Write manifest file
	manifestPath := fmt.Sprintf("manifests/manifest_%d.avro", entry.SnapshotID)
	if err := tw.writeManifest(ctx, manifestPath); err != nil {
		return fmt.Errorf("writing manifest: %w", err)
	}

	// Update metadata
	tw.metadata.CurrentSnapshot = &Snapshot{
		SnapshotID:   entry.SnapshotID,
		TimestampMs:  entry.SnapshotID,
		ManifestList: manifestPath,
		Summary: map[string]string{
			"added-data-files": "1",
			"total-data-files": fmt.Sprintf("%d", len(tw.manifests)),
			"total-records":    fmt.Sprintf("%d", tw.records),
		},
	}
	tw.metadata.Snapshots = append(tw.metadata.Snapshots, tw.metadata.CurrentSnapshot)

	// Write metadata
	metadataPath := filepath.Join(tw.metadata.Location, "metadata.json")
	if err := tw.writeMetadata(ctx, tw.metadata, metadataPath); err != nil {
		return fmt.Errorf("writing metadata: %w", err)
	}

	return nil
}

func (tw *tableWriter) collectMetrics() FileMetrics {
	// Collect actual metrics from Parquet writer (simplified)
	return FileMetrics{
		ColumnSizes:     make(map[int]int64),
		ValueCounts:     make(map[int]int64),
		NullValueCounts: make(map[int]int64),
		LowerBounds:     make(map[int][]byte),
		UpperBounds:     make(map[int][]byte),
	}
}

func (tw *tableWriter) writeManifest(ctx context.Context, manifestPath string) error {
	// Implement writing manifest entries using Avro (simplified)
	// For the sake of example, we'll write JSON
	fullPath := filepath.Join(tw.metadata.Location, manifestPath)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return fmt.Errorf("creating manifest directory: %w", err)
	}

	file, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("creating manifest file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(tw.manifests); err != nil {
		return fmt.Errorf("encoding manifest: %w", err)
	}

	return nil
}

func (tw *tableWriter) writeMetadata(ctx context.Context, metadata *TableMetadata, metadataPath string) error {
	file, err := os.Create(metadataPath)
	if err != nil {
		return fmt.Errorf("creating metadata file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(metadata); err != nil {
		return fmt.Errorf("encoding metadata: %w", err)
	}

	return nil
}

// Helper functions
func postgresTypeToIceberg(pgTypeOID uint32) string {
	switch pgTypeOID {
	case pgtype.Int4OID, pgtype.Int8OID:
		return "int"
	case pgtype.Int2OID:
		return "int"
	case pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID:
		return "string"
	case pgtype.Float8OID:
		return "double"
	case pgtype.Float4OID:
		return "float"
	case pgtype.BoolOID:
		return "boolean"
	case pgtype.DateOID:
		return "date"
	case pgtype.TimestampOID, pgtype.TimestamptzOID:
		return "timestamp"
	case pgtype.NumericOID:
		return "double" // Simplify decimal to double
	case pgtype.ByteaOID:
		return "binary"
	default:
		return "string" // Default to string for unknown types
	}
}

func createParquetSchema(schema SchemaV2) (*parquet.Schema, error) {
	root := make(parquet.Group)

	for _, field := range schema.Fields {
		var node parquet.Node

		switch field.Type {
		case "int":
			node = parquet.Leaf(parquet.Int32Type)
		case "long":
			node = parquet.Leaf(parquet.Int64Type)
		case "string":
			node = parquet.Leaf(parquet.ByteArrayType)
		case "double":
			node = parquet.Leaf(parquet.DoubleType)
		case "float":
			node = parquet.Leaf(parquet.FloatType)
		case "boolean":
			node = parquet.Leaf(parquet.BooleanType)
		case "date":
			node = parquet.Date()
		case "timestamp":
			node = parquet.Timestamp(parquet.Millisecond)
		case "binary":
			node = parquet.Leaf(parquet.ByteArrayType)
		default:
			return nil, fmt.Errorf("unsupported type: %s", field.Type)
		}

		if !field.Required {
			node = parquet.Optional(node)
		}
		root[field.Name] = node
	}

	return parquet.NewSchema("schema", root), nil
}
