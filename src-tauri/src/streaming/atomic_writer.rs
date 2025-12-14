//! Atomic CSV file writer with automatic cleanup on failure.
//!
//! Writes to a temporary file in the same directory as the destination,
//! then atomically replaces the destination on `finish()`. If dropped
//! before finishing, the temporary file is automatically cleaned up.

use std::io::BufWriter;
use std::path::{Path, PathBuf};

use csv::Writer;
use tempfile::NamedTempFile;

use crate::error::AppError;

/// An atomic CSV writer that ensures data integrity.
///
/// Writes to a temporary file and atomically persists to the final path
/// on `finish()`. If dropped without calling `finish()`, the temporary
/// file is automatically deleted.
pub struct AtomicCsvWriter {
    writer: Writer<BufWriter<NamedTempFile>>,
    final_path: PathBuf,
}

impl AtomicCsvWriter {
    /// Creates a new atomic CSV writer targeting the specified path.
    ///
    /// The temporary file is created in the same directory as `final_path`
    /// to ensure atomic persistence (same filesystem requirement).
    ///
    /// # Errors
    ///
    /// Returns `AppError::CsvChunkError` if the parent directory cannot be
    /// determined or the temporary file cannot be created.
    pub fn new(final_path: impl AsRef<Path>) -> Result<Self, AppError> {
        let final_path = final_path.as_ref().to_path_buf();

        let parent_dir = final_path.parent().ok_or_else(|| {
            AppError::CsvChunkError(format!(
                "Cannot determine parent directory for: {}",
                final_path.display()
            ))
        })?;

        let temp_file = NamedTempFile::new_in(parent_dir).map_err(|e| {
            AppError::CsvChunkError(format!("Failed to create temporary file: {}", e))
        })?;

        let buf_writer = BufWriter::new(temp_file);
        let csv_writer = Writer::from_writer(buf_writer);

        Ok(Self {
            writer: csv_writer,
            final_path,
        })
    }

    /// Returns a mutable reference to the underlying CSV writer.
    ///
    /// Use this to write headers and records to the CSV file.
    pub fn writer_mut(&mut self) -> &mut Writer<BufWriter<NamedTempFile>> {
        &mut self.writer
    }

    /// Flushes all buffers and atomically persists the file to the final path.
    ///
    /// This method consumes the writer. On success, the destination file
    /// is atomically replaced with the contents written to the temporary file.
    ///
    /// # Returns
    ///
    /// Returns the final path on success.
    ///
    /// # Errors
    ///
    /// Returns `AppError::CsvChunkError` if flushing or persisting fails.
    /// On error, the temporary file is cleaned up automatically.
    pub fn finish(self) -> Result<PathBuf, AppError> {
        // Flush the CSV writer and get the BufWriter
        let buf_writer = self.writer.into_inner().map_err(|e| {
            AppError::CsvChunkError(format!("Failed to flush CSV writer: {}", e.error()))
        })?;

        // Flush the BufWriter and get the NamedTempFile
        let named_temp = buf_writer.into_inner().map_err(|e| {
            AppError::CsvChunkError(format!("Failed to flush buffer: {}", e.error()))
        })?;

        // Atomically persist to the final path
        named_temp.persist(&self.final_path).map_err(|e| {
            AppError::CsvChunkError(format!(
                "Failed to persist file to {}: {}",
                self.final_path.display(),
                e.error
            ))
        })?;

        Ok(self.final_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_successful_write() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let final_path = temp_dir.path().join("output.csv");

        // Create writer and write some data
        let mut writer = AtomicCsvWriter::new(&final_path).expect("Failed to create writer");

        writer
            .writer_mut()
            .write_record(&["Name", "Age"])
            .expect("Failed to write header");
        writer
            .writer_mut()
            .write_record(&["Alice", "30"])
            .expect("Failed to write record 1");
        writer
            .writer_mut()
            .write_record(&["Bob", "25"])
            .expect("Failed to write record 2");

        // Finish and persist
        let result_path = writer.finish().expect("Failed to finish");

        // Verify the file exists and has correct content
        assert_eq!(result_path, final_path);
        assert!(final_path.exists(), "Final file should exist");

        let content = fs::read_to_string(&final_path).expect("Failed to read file");
        assert!(content.contains("Name,Age"));
        assert!(content.contains("Alice,30"));
        assert!(content.contains("Bob,25"));
    }

    #[test]
    fn test_drop_cleanup() {
        // Create a dedicated empty temp directory
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let final_path = temp_dir.path().join("output.csv");

        // Verify directory starts empty
        let entries_before: Vec<_> = fs::read_dir(temp_dir.path())
            .expect("Failed to read dir")
            .collect();
        assert!(entries_before.is_empty(), "Directory should start empty");

        {
            // Create writer and write some data, but don't call finish()
            let mut writer = AtomicCsvWriter::new(&final_path).expect("Failed to create writer");

            writer
                .writer_mut()
                .write_record(&["Header"])
                .expect("Failed to write");
            writer
                .writer_mut()
                .write_record(&["Data"])
                .expect("Failed to write");

            // Writer is dropped here without calling finish()
        }

        // Verify the directory is empty (temp file was cleaned up)
        let entries_after: Vec<_> = fs::read_dir(temp_dir.path())
            .expect("Failed to read dir")
            .collect();
        assert!(
            entries_after.is_empty(),
            "Directory should be empty after drop (temp file cleaned up)"
        );

        // Final path should not exist
        assert!(
            !final_path.exists(),
            "Final file should not exist since finish() was not called"
        );
    }

    #[test]
    fn test_overwrite_behavior() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let final_path = temp_dir.path().join("output.csv");

        // Create a dummy file with old content
        fs::write(&final_path, "OLD_CONTENT").expect("Failed to write dummy file");
        assert!(final_path.exists());

        // Create writer targeting the same path
        let mut writer = AtomicCsvWriter::new(&final_path).expect("Failed to create writer");

        writer
            .writer_mut()
            .write_record(&["NEW"])
            .expect("Failed to write");
        writer
            .writer_mut()
            .write_record(&["DATA"])
            .expect("Failed to write");

        // Finish and persist
        writer.finish().expect("Failed to finish");

        // Verify the file was overwritten with new content
        let content = fs::read_to_string(&final_path).expect("Failed to read file");
        assert!(
            !content.contains("OLD_CONTENT"),
            "Old content should be gone"
        );
        assert!(content.contains("NEW"), "New content should be present");
        assert!(content.contains("DATA"), "New data should be present");
    }

    #[test]
    fn test_invalid_parent_directory() {
        // Path with no parent (just a filename, which would use current dir)
        // This should actually work since "." is the implicit parent
        // Let's test a truly problematic case - root path on Unix
        #[cfg(unix)]
        {
            // "/" has no parent in the traditional sense, but parent() returns None
            let result = AtomicCsvWriter::new("/");
            assert!(result.is_err(), "Should fail for path with no parent");
        }
    }

    #[test]
    fn test_write_complex_csv_data() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let final_path = temp_dir.path().join("complex.csv");

        let mut writer = AtomicCsvWriter::new(&final_path).expect("Failed to create writer");

        // Write data with special characters
        writer
            .writer_mut()
            .write_record(&["Name", "Description", "Value"])
            .expect("Failed to write header");
        writer
            .writer_mut()
            .write_record(&["Item1", "Contains, comma", "100"])
            .expect("Failed to write record");
        writer
            .writer_mut()
            .write_record(&["Item2", "Has \"quotes\"", "200"])
            .expect("Failed to write record");
        writer
            .writer_mut()
            .write_record(&["Item3", "Multi\nline", "300"])
            .expect("Failed to write record");

        writer.finish().expect("Failed to finish");

        // Read back and verify using csv reader
        let mut reader = csv::Reader::from_path(&final_path).expect("Failed to open reader");

        let headers: Vec<String> = reader
            .headers()
            .expect("Failed to read headers")
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(headers, vec!["Name", "Description", "Value"]);

        let records: Vec<Vec<String>> = reader
            .records()
            .map(|r| {
                r.expect("Failed to read record")
                    .iter()
                    .map(|s| s.to_string())
                    .collect()
            })
            .collect();

        assert_eq!(records.len(), 3);
        assert_eq!(records[0][1], "Contains, comma");
        assert_eq!(records[1][1], "Has \"quotes\"");
        assert_eq!(records[2][1], "Multi\nline");
    }

    #[test]
    fn test_empty_file() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let final_path = temp_dir.path().join("empty.csv");

        // Create writer but don't write anything
        let writer = AtomicCsvWriter::new(&final_path).expect("Failed to create writer");
        writer.finish().expect("Failed to finish");

        // File should exist but be empty
        assert!(final_path.exists());
        let content = fs::read_to_string(&final_path).expect("Failed to read file");
        assert!(content.is_empty(), "File should be empty");
    }
}
