package iceberg

type ManifestEntry struct {
	Status       int32    `avro:"status"` // 1=added, 2=existing, 3=deleted
	SnapshotID   int64    `avro:"snapshot_id"`
	SequenceNum  int64    `avro:"sequence_number"`
	FileSequence int64    `avro:"file_sequence_number"`
	DataFile     DataFile `avro:"data_file"`
}

type DataFile struct {
	FilePath      string            `avro:"file_path"`
	FileFormat    string            `avro:"file_format"`
	Partition     map[string]string `avro:"partition"`
	RecordCount   int64             `avro:"record_count"`
	FileSizeBytes int64             `avro:"file_size_bytes"`
	Metrics       FileMetrics       `avro:"metrics"`
}

type FileMetrics struct {
	ColumnSizes     map[int]int64  `avro:"column_sizes"`
	ValueCounts     map[int]int64  `avro:"value_counts"`
	NullValueCounts map[int]int64  `avro:"null_value_counts"`
	LowerBounds     map[int][]byte `avro:"lower_bounds"`
	UpperBounds     map[int][]byte `avro:"upper_bounds"`
}
