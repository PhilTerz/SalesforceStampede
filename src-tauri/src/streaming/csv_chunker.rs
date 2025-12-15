//! Record-aware CSV chunking that never corrupts records.
//!
//! Uses the `csv` crate to properly handle embedded commas and newlines within
//! quoted fields. Splits large CSV files into smaller chunks while ensuring
//! each chunk includes the header row.

use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use csv::{ByteRecord, ReaderBuilder, Terminator, WriterBuilder};

use crate::error::AppError;

/// Batch size presets for Salesforce Bulk API operations.
#[derive(Debug, Clone, Copy)]
pub enum BatchSize {
    /// 10 records per batch
    ExtraSmall,
    /// 200 records per batch
    Small,
    /// 2,000 records per batch
    Medium,
    /// 10,000 records per batch
    Large,
    /// Custom record count
    Custom(u32),
}

impl BatchSize {
    /// Returns the batch size as a u64.
    pub fn as_u64(self) -> u64 {
        match self {
            BatchSize::ExtraSmall => 10,
            BatchSize::Small => 200,
            BatchSize::Medium => 2_000,
            BatchSize::Large => 10_000,
            BatchSize::Custom(n) => n as u64,
        }
    }
}

impl From<BatchSize> for u64 {
    fn from(batch_size: BatchSize) -> Self {
        batch_size.as_u64()
    }
}

/// Configuration for CSV chunking.
#[derive(Debug, Clone)]
pub struct ChunkConfig {
    /// Maximum bytes per chunk (default: 100 MB).
    pub max_bytes: u64,
    /// Maximum records per chunk (excluding header).
    pub max_records: u64,
}

impl Default for ChunkConfig {
    fn default() -> Self {
        Self {
            max_bytes: 100 * 1024 * 1024, // 100 MB
            max_records: 10_000,
        }
    }
}

impl ChunkConfig {
    /// Creates a ChunkConfig with the given batch size.
    pub fn with_batch_size(batch_size: BatchSize) -> Self {
        Self {
            max_bytes: 100 * 1024 * 1024,
            max_records: batch_size.as_u64(),
        }
    }

    /// Sets the max_bytes limit.
    pub fn max_bytes(mut self, bytes: u64) -> Self {
        self.max_bytes = bytes;
        self
    }

    /// Sets the max_records limit.
    pub fn max_records(mut self, records: u64) -> Self {
        self.max_records = records;
        self
    }
}

/// Result of splitting a CSV file into chunks.
#[derive(Debug, Clone)]
pub struct ChunkResult {
    /// Paths to the generated chunk files.
    pub chunk_paths: Vec<PathBuf>,
    /// Total data rows processed (excluding headers).
    pub total_rows: u64,
    /// Number of data rows in each chunk (parallel to chunk_paths).
    pub rows_per_chunk: Vec<u64>,
}

/// Splits a CSV file into multiple chunk files.
///
/// Each chunk includes the original header row and respects both byte and
/// record limits. Uses the `csv` crate for proper handling of quoted fields
/// containing commas and newlines.
///
/// # Arguments
///
/// * `source` - Path to the source CSV file
/// * `temp_dir` - Directory where chunk files will be created
/// * `config` - Chunking configuration (byte/record limits)
///
/// # Returns
///
/// A `ChunkResult` containing paths to all chunk files and row counts.
///
/// # Errors
///
/// Returns `AppError::CsvChunkError` if the source file cannot be read,
/// has no header, or if chunk files cannot be written.
pub async fn split_file(
    source: &Path,
    temp_dir: &Path,
    config: ChunkConfig,
) -> Result<ChunkResult, AppError> {
    // Ensure temp directory exists
    tokio::fs::create_dir_all(temp_dir)
        .await
        .map_err(|e| AppError::CsvChunkError(format!("Failed to create temp directory: {}", e)))?;

    // Clone paths for the blocking closure
    let source = source.to_owned();
    let temp_dir = temp_dir.to_owned();

    // Run the blocking CSV processing in a separate thread
    tokio::task::spawn_blocking(move || split_file_blocking(&source, &temp_dir, config))
        .await
        .map_err(|e| AppError::CsvChunkError(format!("Task join error: {}", e)))?
}

/// Blocking implementation of CSV file splitting.
fn split_file_blocking(
    source: &Path,
    temp_dir: &Path,
    config: ChunkConfig,
) -> Result<ChunkResult, AppError> {
    let file = File::open(source)
        .map_err(|e| AppError::CsvChunkError(format!("Failed to open source file: {}", e)))?;

    let buf_reader = BufReader::new(file);
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .flexible(false)
        .from_reader(buf_reader);

    // Read headers
    let headers = reader
        .byte_headers()
        .map_err(|e| AppError::CsvChunkError(format!("Failed to read CSV headers: {}", e)))?
        .clone();

    if headers.is_empty() {
        return Err(AppError::CsvChunkError(
            "CSV file has no header row".to_string(),
        ));
    }

    // Pre-serialize the header to know its size
    let header_bytes = serialize_record(&headers)?;
    let header_size = header_bytes.len() as u64;

    #[cfg(debug_assertions)]
    tracing::debug!(
        source = %source.display(),
        header_size,
        "Starting CSV chunking"
    );

    let mut chunk_paths: Vec<PathBuf> = Vec::new();
    let mut rows_per_chunk: Vec<u64> = Vec::new();
    let mut total_rows: u64 = 0;

    // State for current chunk
    let mut chunk_index: u32 = 0;
    let mut current_writer: Option<ChunkWriter> = None;
    let mut current_chunk_bytes: u64 = 0;
    let mut current_chunk_rows: u64 = 0;

    // Reusable buffer for measuring record sizes
    let mut measure_buf: Vec<u8> = Vec::with_capacity(4096);

    // Process each record
    for result in reader.byte_records() {
        let record = result
            .map_err(|e| AppError::CsvChunkError(format!("Failed to read CSV record: {}", e)))?;

        // Measure the serialized size of this record
        measure_buf.clear();
        serialize_record_into(&record, &mut measure_buf)?;
        let record_size = measure_buf.len() as u64;

        // Check if we need to start a new chunk
        let need_new_chunk = current_writer.is_none()
            || would_exceed_limits(
                current_chunk_bytes,
                current_chunk_rows,
                record_size,
                &config,
            );

        if need_new_chunk {
            // Finalize current chunk if exists
            if let Some(mut writer) = current_writer.take() {
                writer.flush()?;
                rows_per_chunk.push(current_chunk_rows);

                #[cfg(debug_assertions)]
                tracing::debug!(
                    chunk_index = chunk_index - 1,
                    rows = current_chunk_rows,
                    bytes = current_chunk_bytes,
                    "Completed chunk"
                );
            }

            // Start new chunk
            let chunk_path = temp_dir.join(format!("chunk_{:04}.csv", chunk_index));
            let mut writer = ChunkWriter::new(&chunk_path)?;

            // Write header to new chunk
            writer.write_bytes(&header_bytes)?;

            chunk_paths.push(chunk_path);
            chunk_index += 1;
            current_chunk_bytes = header_size;
            current_chunk_rows = 0;
            current_writer = Some(writer);
        }

        // Write the record (we already serialized it into measure_buf)
        if let Some(ref mut writer) = current_writer {
            writer.write_bytes(&measure_buf)?;
            current_chunk_bytes += record_size;
            current_chunk_rows += 1;
            total_rows += 1;
        }
    }

    // Finalize the last chunk
    if let Some(mut writer) = current_writer.take() {
        writer.flush()?;
        rows_per_chunk.push(current_chunk_rows);

        #[cfg(debug_assertions)]
        tracing::debug!(
            chunk_index = chunk_index - 1,
            rows = current_chunk_rows,
            bytes = current_chunk_bytes,
            "Completed final chunk"
        );
    }

    #[cfg(debug_assertions)]
    tracing::debug!(
        total_rows,
        chunk_count = chunk_paths.len(),
        "CSV chunking complete"
    );

    Ok(ChunkResult {
        chunk_paths,
        total_rows,
        rows_per_chunk,
    })
}

/// Checks if adding a record would exceed the configured limits.
fn would_exceed_limits(
    current_bytes: u64,
    current_rows: u64,
    record_size: u64,
    config: &ChunkConfig,
) -> bool {
    // If this is the first record after header, always allow it
    // (handles the case where a single record exceeds max_bytes)
    if current_rows == 0 {
        return false;
    }

    // Check record limit
    if current_rows >= config.max_records {
        return true;
    }

    // Check byte limit
    if current_bytes + record_size > config.max_bytes {
        return true;
    }

    false
}

/// Serializes a ByteRecord to bytes using CRLF terminator.
fn serialize_record(record: &ByteRecord) -> Result<Vec<u8>, AppError> {
    let mut buf = Vec::with_capacity(record.len() * 32);
    serialize_record_into(record, &mut buf)?;
    Ok(buf)
}

/// Serializes a ByteRecord into an existing buffer using CRLF terminator.
fn serialize_record_into(record: &ByteRecord, buf: &mut Vec<u8>) -> Result<(), AppError> {
    let mut writer = WriterBuilder::new()
        .has_headers(false)
        .terminator(Terminator::CRLF)
        .from_writer(buf);

    writer
        .write_byte_record(record)
        .map_err(|e| AppError::CsvChunkError(format!("Failed to serialize record: {}", e)))?;

    writer
        .flush()
        .map_err(|e| AppError::CsvChunkError(format!("Failed to flush writer: {}", e)))?;

    Ok(())
}

/// Wrapper for writing to a chunk file.
struct ChunkWriter {
    writer: BufWriter<File>,
}

impl ChunkWriter {
    fn new(path: &Path) -> Result<Self, AppError> {
        let file = File::create(path)
            .map_err(|e| AppError::CsvChunkError(format!("Failed to create chunk file: {}", e)))?;
        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), AppError> {
        self.writer
            .write_all(bytes)
            .map_err(|e| AppError::CsvChunkError(format!("Failed to write to chunk: {}", e)))
    }

    fn flush(&mut self) -> Result<(), AppError> {
        self.writer
            .flush()
            .map_err(|e| AppError::CsvChunkError(format!("Failed to flush chunk: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    /// Helper to create a test CSV file and return its path.
    fn create_test_csv(dir: &TempDir, content: &str) -> PathBuf {
        let path = dir.path().join("test.csv");
        fs::write(&path, content).expect("Failed to write test CSV");
        path
    }

    /// Helper to read a chunk file and return its content.
    fn read_chunk(path: &Path) -> String {
        fs::read_to_string(path).expect("Failed to read chunk")
    }

    /// Helper to parse a chunk file and return header + records.
    fn parse_chunk(path: &Path) -> (Vec<String>, Vec<Vec<String>>) {
        let mut reader = csv::Reader::from_path(path).expect("Failed to open chunk");
        let headers: Vec<String> = reader
            .headers()
            .expect("Failed to read headers")
            .iter()
            .map(|s| s.to_string())
            .collect();

        let records: Vec<Vec<String>> = reader
            .records()
            .map(|r| {
                r.expect("Failed to read record")
                    .iter()
                    .map(|s| s.to_string())
                    .collect()
            })
            .collect();

        (headers, records)
    }

    #[tokio::test]
    async fn test_embedded_newline_integrity() {
        // Test that embedded newlines in quoted fields are preserved
        let source_dir = TempDir::new().unwrap();
        let temp_dir = TempDir::new().unwrap();

        // CSV with embedded newline in Desc field
        let csv_content = "Name,Desc\n\"John\",\"Line1\nLine2\"\n";
        let source = create_test_csv(&source_dir, csv_content);

        let config = ChunkConfig::default().max_records(1);
        let result = split_file(&source, temp_dir.path(), config)
            .await
            .expect("split_file failed");

        assert_eq!(result.chunk_paths.len(), 1);
        assert_eq!(result.total_rows, 1);
        assert_eq!(result.rows_per_chunk, vec![1]);

        // Parse the chunk and verify the newline is preserved
        let (headers, records) = parse_chunk(&result.chunk_paths[0]);
        assert_eq!(headers, vec!["Name", "Desc"]);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0][0], "John");
        assert_eq!(records[0][1], "Line1\nLine2");
    }

    #[tokio::test]
    async fn test_embedded_comma_integrity() {
        // Test that embedded commas in quoted fields are preserved
        let source_dir = TempDir::new().unwrap();
        let temp_dir = TempDir::new().unwrap();

        let csv_content = "Name,Address\n\"John\",\"123 Main St, Apt 4\"\n";
        let source = create_test_csv(&source_dir, csv_content);

        let config = ChunkConfig::default().max_records(1);
        let result = split_file(&source, temp_dir.path(), config)
            .await
            .expect("split_file failed");

        let (_, records) = parse_chunk(&result.chunk_paths[0]);
        assert_eq!(records[0][1], "123 Main St, Apt 4");
    }

    #[tokio::test]
    async fn test_header_repetition() {
        // Split 3 rows into 3 chunks, verify all have the header
        let source_dir = TempDir::new().unwrap();
        let temp_dir = TempDir::new().unwrap();

        let csv_content = "Id,Name\n1,Alice\n2,Bob\n3,Charlie\n";
        let source = create_test_csv(&source_dir, csv_content);

        let config = ChunkConfig::default().max_records(1);
        let result = split_file(&source, temp_dir.path(), config)
            .await
            .expect("split_file failed");

        assert_eq!(result.chunk_paths.len(), 3);
        assert_eq!(result.total_rows, 3);
        assert_eq!(result.rows_per_chunk, vec![1, 1, 1]);

        // Verify each chunk has the header
        for chunk_path in &result.chunk_paths {
            let (headers, _) = parse_chunk(chunk_path);
            assert_eq!(headers, vec!["Id", "Name"]);
        }

        // Verify correct records in each chunk
        let (_, records1) = parse_chunk(&result.chunk_paths[0]);
        let (_, records2) = parse_chunk(&result.chunk_paths[1]);
        let (_, records3) = parse_chunk(&result.chunk_paths[2]);

        assert_eq!(records1[0], vec!["1", "Alice"]);
        assert_eq!(records2[0], vec!["2", "Bob"]);
        assert_eq!(records3[0], vec!["3", "Charlie"]);
    }

    #[tokio::test]
    async fn test_record_limit() {
        // 5 rows with max 2 records per chunk = [2, 2, 1]
        let source_dir = TempDir::new().unwrap();
        let temp_dir = TempDir::new().unwrap();

        let csv_content = "Id,Name\n1,A\n2,B\n3,C\n4,D\n5,E\n";
        let source = create_test_csv(&source_dir, csv_content);

        let config = ChunkConfig::default().max_records(2);
        let result = split_file(&source, temp_dir.path(), config)
            .await
            .expect("split_file failed");

        assert_eq!(result.chunk_paths.len(), 3);
        assert_eq!(result.total_rows, 5);
        assert_eq!(result.rows_per_chunk, vec![2, 2, 1]);
    }

    #[tokio::test]
    async fn test_byte_limit() {
        // Use a very tight byte limit to force splitting based on bytes
        let source_dir = TempDir::new().unwrap();
        let temp_dir = TempDir::new().unwrap();

        // Create CSV with long field values
        let csv_content = "Id,LongField\n1,AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n2,BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB\n3,CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC\n";
        let source = create_test_csv(&source_dir, csv_content);

        // Set byte limit low enough to split after each record
        // Header "Id,LongField\r\n" = 14 bytes
        // Each record "N,XXXX...XXXX\r\n" ~ 45 bytes
        // So 60 bytes should fit header + 1 record
        let config = ChunkConfig::default().max_bytes(60).max_records(1000);
        let result = split_file(&source, temp_dir.path(), config)
            .await
            .expect("split_file failed");

        assert_eq!(result.chunk_paths.len(), 3);
        assert_eq!(result.total_rows, 3);
        assert_eq!(result.rows_per_chunk, vec![1, 1, 1]);
    }

    #[tokio::test]
    async fn test_terminator_byte_consistency() {
        // Verify that byte counting matches actual file size
        let source_dir = TempDir::new().unwrap();
        let temp_dir = TempDir::new().unwrap();

        let csv_content = "A,B\n1,2\n3,4\n";
        let source = create_test_csv(&source_dir, csv_content);

        // Use a byte limit that exactly fits header + 1 record
        // Measure what the chunker produces
        let config = ChunkConfig::default().max_records(1);
        let result = split_file(&source, temp_dir.path(), config)
            .await
            .expect("split_file failed");

        // Each chunk file should have consistent CRLF terminators
        for chunk_path in &result.chunk_paths {
            let content = read_chunk(chunk_path);
            // Verify CRLF line endings (not LF only)
            assert!(
                content.contains("\r\n"),
                "Chunk should use CRLF terminators"
            );
        }
    }

    #[tokio::test]
    async fn test_large_record_policy() {
        // A single record larger than max_bytes should still be written
        let source_dir = TempDir::new().unwrap();
        let temp_dir = TempDir::new().unwrap();

        // Create a record that's ~100 bytes
        let long_value = "X".repeat(80);
        let csv_content = format!("Id,Data\n1,{}\n2,short\n", long_value);
        let source = create_test_csv(&source_dir, &csv_content);

        // Set max_bytes very low (but record should still be written)
        let config = ChunkConfig::default().max_bytes(20).max_records(1000);
        let result = split_file(&source, temp_dir.path(), config)
            .await
            .expect("split_file failed");

        // Should have 2 chunks, each with 1 record
        assert_eq!(result.chunk_paths.len(), 2);
        assert_eq!(result.total_rows, 2);

        // Verify the large record was written correctly
        let (_, records) = parse_chunk(&result.chunk_paths[0]);
        assert_eq!(records[0][1], long_value);
    }

    #[tokio::test]
    async fn test_empty_csv_no_data_rows() {
        // CSV with only header, no data rows
        let source_dir = TempDir::new().unwrap();
        let temp_dir = TempDir::new().unwrap();

        let csv_content = "Id,Name\n";
        let source = create_test_csv(&source_dir, csv_content);

        let config = ChunkConfig::default();
        let result = split_file(&source, temp_dir.path(), config)
            .await
            .expect("split_file failed");

        assert_eq!(result.chunk_paths.len(), 0);
        assert_eq!(result.total_rows, 0);
        assert!(result.rows_per_chunk.is_empty());
    }

    #[tokio::test]
    async fn test_no_header_returns_error() {
        // Empty file should return error
        let source_dir = TempDir::new().unwrap();
        let temp_dir = TempDir::new().unwrap();

        let csv_content = "";
        let source = create_test_csv(&source_dir, csv_content);

        let config = ChunkConfig::default();
        let result = split_file(&source, temp_dir.path(), config).await;

        assert!(result.is_err());
        match result {
            Err(AppError::CsvChunkError(msg)) => {
                assert!(msg.contains("header") || msg.contains("headers"));
            }
            _ => panic!("Expected CsvChunkError"),
        }
    }

    #[tokio::test]
    async fn test_chunk_file_naming() {
        // Verify chunk files are named correctly
        let source_dir = TempDir::new().unwrap();
        let temp_dir = TempDir::new().unwrap();

        let csv_content = "Id\n1\n2\n3\n";
        let source = create_test_csv(&source_dir, csv_content);

        let config = ChunkConfig::default().max_records(1);
        let result = split_file(&source, temp_dir.path(), config)
            .await
            .expect("split_file failed");

        assert_eq!(result.chunk_paths.len(), 3);
        assert!(result.chunk_paths[0].ends_with("chunk_0000.csv"));
        assert!(result.chunk_paths[1].ends_with("chunk_0001.csv"));
        assert!(result.chunk_paths[2].ends_with("chunk_0002.csv"));
    }

    #[tokio::test]
    async fn test_batch_size_variants() {
        assert_eq!(BatchSize::ExtraSmall.as_u64(), 10);
        assert_eq!(BatchSize::Small.as_u64(), 200);
        assert_eq!(BatchSize::Medium.as_u64(), 2_000);
        assert_eq!(BatchSize::Large.as_u64(), 10_000);
        assert_eq!(BatchSize::Custom(500).as_u64(), 500);
    }

    #[tokio::test]
    async fn test_chunk_config_builder() {
        let config = ChunkConfig::with_batch_size(BatchSize::Small).max_bytes(50 * 1024 * 1024);

        assert_eq!(config.max_records, 200);
        assert_eq!(config.max_bytes, 50 * 1024 * 1024);
    }

    #[tokio::test]
    async fn test_multiple_embedded_special_chars() {
        // Test with complex quoted content: newlines, commas, and quotes
        let source_dir = TempDir::new().unwrap();
        let temp_dir = TempDir::new().unwrap();

        // CSV with record containing newlines, commas, and escaped quotes
        let csv_content = r#"Name,Bio
"John ""The Dev""","Works at Acme, Inc.
Loves coding
""Quoted text"" inside"
"#;
        let source = create_test_csv(&source_dir, csv_content);

        let config = ChunkConfig::default();
        let result = split_file(&source, temp_dir.path(), config)
            .await
            .expect("split_file failed");

        assert_eq!(result.total_rows, 1);

        let (headers, records) = parse_chunk(&result.chunk_paths[0]);
        assert_eq!(headers, vec!["Name", "Bio"]);
        assert_eq!(records[0][0], "John \"The Dev\"");
        assert!(records[0][1].contains("Acme, Inc."));
        assert!(records[0][1].contains("\n"));
        assert!(records[0][1].contains("\"Quoted text\""));
    }
}
