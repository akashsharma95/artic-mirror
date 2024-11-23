package iceberg

type PartitionSpec struct {
	SpecID int              `json:"spec-id"`
	Fields []PartitionField `json:"fields"`
}

type PartitionField struct {
	SourceID    int    `json:"source-id"` // ID from the schema
	FieldID     int    `json:"field-id"`  // Unique ID for partition field
	Name        string `json:"name"`      // Partition name (e.g. "year", "month", "day")
	Transform   string `json:"transform"` // year, month, day, bucket, truncate
	SourceField string `json:"source"`    // Source column name
}

type TableMetadata struct {
	FormatVersion   int               `json:"format-version"`
	TableUUID       string            `json:"table-uuid"`
	Location        string            `json:"location"`
	LastUpdated     int64             `json:"last-updated-ms"`
	LastColumnID    int               `json:"last-column-id"`
	SchemaID        int               `json:"schema-id"`
	Schemas         []SchemaV2        `json:"schemas"`
	CurrentSchema   SchemaV2          `json:"current-schema"`
	PartitionSpec   []PartitionSpec   `json:"partition-spec"`
	Properties      map[string]string `json:"properties"`
	CurrentSnapshot *Snapshot         `json:"current-snapshot"`
	Snapshots       []*Snapshot       `json:"snapshots"`
}

type SchemaV2 struct {
	SchemaID int     `json:"schema-id"`
	Fields   []Field `json:"fields"`
}

type Field struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
}

type Snapshot struct {
	SnapshotID       int64             `json:"snapshot-id"`
	ParentSnapshotID int64             `json:"parent-snapshot-id"`
	SequenceNumber   int64             `json:"sequence-number"`
	TimestampMs      int64             `json:"timestamp-ms"`
	ManifestList     string            `json:"manifest-list"`
	Summary          map[string]string `json:"summary"`
}
