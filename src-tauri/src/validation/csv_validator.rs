//! CSV validation with sample-based approach for large files.
//!
//! This module validates CSV files by reading only a fixed-size sample (512 KB),
//! making it safe for very large files while still catching common issues:
//! - UTF-8 encoding errors
//! - Missing headers
//! - Inconsistent column counts
//! - Line ending detection

use std::io::Cursor;
use std::path::Path;

use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use crate::error::AppError;

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

/// Size of the sample buffer for validation (512 KB).
pub const VALIDATION_SAMPLE_SIZE: usize = 512 * 1024;

/// Maximum number of records to validate in the sample.
const MAX_RECORDS_TO_VALIDATE: usize = 1000;

/// File size threshold for large file warning (100 MB).
const LARGE_FILE_THRESHOLD: u64 = 100 * 1024 * 1024;

/// UTF-8 BOM bytes.
const UTF8_BOM: &[u8] = &[0xEF, 0xBB, 0xBF];

// ─────────────────────────────────────────────────────────────────────────────
// Public Types
// ─────────────────────────────────────────────────────────────────────────────

/// Result of CSV validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsvValidationResult {
    /// Whether the file passed validation (no errors).
    pub ok: bool,
    /// List of validation errors found.
    pub errors: Vec<CsvValidationError>,
    /// List of validation warnings found.
    pub warnings: Vec<CsvValidationWarning>,
    /// Statistics about the file.
    pub stats: CsvValidationStats,
}

/// Statistics collected during validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsvValidationStats {
    /// Total file size in bytes.
    pub file_size_bytes: u64,
    /// Number of bytes actually validated (sample size or file size if smaller).
    pub sample_bytes: u64,
    /// Headers found in the CSV.
    pub headers: Vec<String>,
    /// Number of columns inferred from headers.
    pub inferred_column_count: usize,
    /// Detected line ending style.
    pub line_endings: LineEndings,
    /// Estimated total rows based on sample average.
    pub estimated_total_rows: Option<u64>,
}

/// Detected line ending style.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LineEndings {
    /// Unix-style line endings (\n).
    LF,
    /// Windows-style line endings (\r\n).
    CRLF,
    /// Mixed line endings (both \n and \r\n found).
    Mixed,
    /// No line endings detected (single line or empty).
    Unknown,
}

/// Validation errors that prevent successful processing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CsvValidationError {
    /// File is not valid UTF-8.
    NotUtf8,
    /// File is empty (0 bytes).
    EmptyFile,
    /// No headers found in the CSV.
    NoHeaders,
    /// Row has inconsistent number of columns.
    InconsistentColumns {
        /// Expected number of columns (from header).
        expected: usize,
        /// Actual number of columns found.
        found: usize,
        /// 1-based row number where the error occurred.
        row: u64,
    },
    /// CSV parsing error.
    CsvParseError {
        /// Error message.
        message: String,
    },
    /// I/O error reading the file.
    IoError {
        /// Error message.
        message: String,
    },
}

/// Validation warnings that don't prevent processing but may indicate issues.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CsvValidationWarning {
    /// File starts with UTF-8 BOM.
    HasBom,
    /// File contains mixed line endings.
    MixedLineEndings,
    /// File is larger than 100MB.
    LargeFile {
        /// File size in bytes.
        size_bytes: u64,
    },
    /// Only a sample of the file was validated.
    SampleOnlyValidation {
        /// Number of rows validated in the sample.
        validated_rows: u64,
    },
}

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

/// Validates a CSV file using sample-based approach.
///
/// This function reads only the first `VALIDATION_SAMPLE_SIZE` bytes of the file,
/// making it safe for very large files.
///
/// # Arguments
///
/// * `path` - Path to the CSV file to validate
///
/// # Returns
///
/// A `CsvValidationResult` containing errors, warnings, and statistics.
///
/// # Errors
///
/// Returns `AppError` only for catastrophic I/O failures. Normal validation
/// errors are returned in `CsvValidationResult.errors`.
pub async fn validate(path: &Path) -> Result<CsvValidationResult, AppError> {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    // Step 1: Get file metadata
    let metadata = tokio::fs::metadata(path)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to read file metadata: {}", e)))?;

    let file_size_bytes = metadata.len();

    // Check for empty file
    if file_size_bytes == 0 {
        return Ok(CsvValidationResult {
            ok: false,
            errors: vec![CsvValidationError::EmptyFile],
            warnings: vec![],
            stats: CsvValidationStats {
                file_size_bytes: 0,
                sample_bytes: 0,
                headers: vec![],
                inferred_column_count: 0,
                line_endings: LineEndings::Unknown,
                estimated_total_rows: None,
            },
        });
    }

    // Large file warning
    if file_size_bytes > LARGE_FILE_THRESHOLD {
        warnings.push(CsvValidationWarning::LargeFile {
            size_bytes: file_size_bytes,
        });
    }

    // Step 2: Read sample buffer
    let sample_size = (file_size_bytes as usize).min(VALIDATION_SAMPLE_SIZE);
    let mut buffer = vec![0u8; sample_size];

    let mut file = File::open(path)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to open file: {}", e)))?;

    file.read_exact(&mut buffer)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to read file: {}", e)))?;

    let sample_bytes = sample_size as u64;
    let is_sample_only = file_size_bytes > sample_bytes;

    // Step 3: Check for BOM
    let has_bom = buffer.starts_with(UTF8_BOM);
    if has_bom {
        warnings.push(CsvValidationWarning::HasBom);
    }

    // Get the data without BOM for processing
    let data_start = if has_bom { UTF8_BOM.len() } else { 0 };
    let data = &buffer[data_start..];

    // Step 4: UTF-8 validation
    if std::str::from_utf8(data).is_err() {
        return Ok(CsvValidationResult {
            ok: false,
            errors: vec![CsvValidationError::NotUtf8],
            warnings,
            stats: CsvValidationStats {
                file_size_bytes,
                sample_bytes,
                headers: vec![],
                inferred_column_count: 0,
                line_endings: LineEndings::Unknown,
                estimated_total_rows: None,
            },
        });
    }

    // Step 5: Line endings detection
    let line_endings = detect_line_endings(data);
    if line_endings == LineEndings::Mixed {
        warnings.push(CsvValidationWarning::MixedLineEndings);
    }

    // Step 6: CSV parsing
    let cursor = Cursor::new(data);
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(false) // We want to catch inconsistent columns
        .from_reader(cursor);

    // Parse headers
    let headers: Vec<String> = match reader.headers() {
        Ok(h) => h.iter().map(String::from).collect(),
        Err(e) => {
            // Check if it's just empty
            if e.to_string().contains("empty") || e.to_string().contains("EOF") {
                errors.push(CsvValidationError::NoHeaders);
            } else {
                errors.push(CsvValidationError::CsvParseError {
                    message: e.to_string(),
                });
            }
            return Ok(CsvValidationResult {
                ok: false,
                errors,
                warnings,
                stats: CsvValidationStats {
                    file_size_bytes,
                    sample_bytes,
                    headers: vec![],
                    inferred_column_count: 0,
                    line_endings,
                    estimated_total_rows: None,
                },
            });
        }
    };

    if headers.is_empty() {
        errors.push(CsvValidationError::NoHeaders);
        return Ok(CsvValidationResult {
            ok: false,
            errors,
            warnings,
            stats: CsvValidationStats {
                file_size_bytes,
                sample_bytes,
                headers: vec![],
                inferred_column_count: 0,
                line_endings,
                estimated_total_rows: None,
            },
        });
    }

    let inferred_column_count = headers.len();
    let mut validated_rows: u64 = 0;
    let mut total_record_bytes: usize = 0;

    // Step 7: Validate records
    // We iterate through records and track approximate bytes by summing field lengths
    for result in reader.records().take(MAX_RECORDS_TO_VALIDATE) {
        match result {
            Ok(record) => {
                validated_rows += 1;

                // Estimate bytes for this row (fields + commas + newline)
                let row_bytes: usize = record.iter().map(|f| f.len()).sum::<usize>()
                    + record.len() // commas
                    + 2; // newline (estimate)
                total_record_bytes += row_bytes;

                // Check column count
                if record.len() != inferred_column_count {
                    errors.push(CsvValidationError::InconsistentColumns {
                        expected: inferred_column_count,
                        found: record.len(),
                        row: validated_rows + 1, // +1 for header, making it 1-based
                    });
                }
            }
            Err(e) => {
                // Check if this is a truncation error at the end of a sample
                let is_at_end = is_sample_only && is_likely_truncation_error(&e, data);

                if is_at_end {
                    // Ignore truncation errors at the end of sample
                    break;
                }

                // Real error
                if is_unequal_lengths_error(&e) {
                    // Parse the row number from the error if possible
                    errors.push(CsvValidationError::InconsistentColumns {
                        expected: inferred_column_count,
                        found: 0, // Unknown, csv crate doesn't expose this easily
                        row: validated_rows + 2, // +1 for header, +1 for 1-based
                    });
                } else {
                    errors.push(CsvValidationError::CsvParseError {
                        message: e.to_string(),
                    });
                }
                break;
            }
        }
    }

    // Step 8: Sample-only warning
    if is_sample_only {
        warnings.push(CsvValidationWarning::SampleOnlyValidation { validated_rows });
    }

    // Step 9: Estimate total rows
    let estimated_total_rows = if validated_rows > 0 && total_record_bytes > 0 {
        let avg_bytes_per_row = total_record_bytes as f64 / validated_rows as f64;
        // Account for header row
        let header_bytes = data.len().saturating_sub(total_record_bytes);
        let data_bytes = file_size_bytes.saturating_sub(header_bytes as u64);
        Some((data_bytes as f64 / avg_bytes_per_row).ceil() as u64)
    } else {
        None
    };

    let ok = errors.is_empty();

    Ok(CsvValidationResult {
        ok,
        errors,
        warnings,
        stats: CsvValidationStats {
            file_size_bytes,
            sample_bytes,
            headers,
            inferred_column_count,
            line_endings,
            estimated_total_rows,
        },
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Detects line ending style in the given bytes.
fn detect_line_endings(data: &[u8]) -> LineEndings {
    let mut has_lf = false;
    let mut has_crlf = false;

    let mut i = 0;
    while i < data.len() {
        if data[i] == b'\r' && i + 1 < data.len() && data[i + 1] == b'\n' {
            has_crlf = true;
            i += 2;
        } else if data[i] == b'\n' {
            has_lf = true;
            i += 1;
        } else {
            i += 1;
        }
    }

    match (has_lf, has_crlf) {
        (true, true) => LineEndings::Mixed,
        (true, false) => LineEndings::LF,
        (false, true) => LineEndings::CRLF,
        (false, false) => LineEndings::Unknown,
    }
}

/// Checks if the error is likely due to buffer truncation.
fn is_likely_truncation_error(err: &csv::Error, _data: &[u8]) -> bool {
    let msg = err.to_string().to_lowercase();
    // Common truncation indicators
    msg.contains("unexpected eof")
        || msg.contains("record ends in a quote")
        || msg.contains("premature eof")
}

/// Checks if the error is an unequal lengths error.
fn is_unequal_lengths_error(err: &csv::Error) -> bool {
    matches!(err.kind(), csv::ErrorKind::UnequalLengths { .. })
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    /// Helper to create a temp file with the given content.
    fn create_temp_csv(content: &[u8]) -> NamedTempFile {
        let mut file = NamedTempFile::new().expect("Failed to create temp file");
        file.write_all(content)
            .expect("Failed to write to temp file");
        file.flush().expect("Failed to flush temp file");
        file
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 1: Non-UTF8
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_non_utf8_returns_error() {
        // Invalid UTF-8 sequence
        let invalid_utf8 = b"Name,Value\n\xff\xfe,123\n";
        let file = create_temp_csv(invalid_utf8);

        let result = validate(file.path())
            .await
            .expect("Validation should not fail");

        assert!(!result.ok, "Validation should fail for non-UTF8");
        assert!(
            result.errors.contains(&CsvValidationError::NotUtf8),
            "Should contain NotUtf8 error"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 2: BOM Detection
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_bom_detection_warning() {
        // UTF-8 BOM followed by valid CSV
        let mut content = Vec::new();
        content.extend_from_slice(UTF8_BOM);
        content.extend_from_slice(b"Name,Value\nAlice,100\nBob,200\n");

        let file = create_temp_csv(&content);

        let result = validate(file.path())
            .await
            .expect("Validation should not fail");

        assert!(result.ok, "Validation should succeed with BOM");
        assert!(
            result.warnings.contains(&CsvValidationWarning::HasBom),
            "Should warn about BOM"
        );
        assert_eq!(result.stats.headers, vec!["Name", "Value"]);
        assert_eq!(result.stats.inferred_column_count, 2);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 3: Truncated Row (False Positive Check)
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_truncated_row_ignored_at_sample_boundary() {
        // Create a file larger than VALIDATION_SAMPLE_SIZE
        // with a row that gets cut off at the boundary
        let mut content = Vec::new();
        content.extend_from_slice(b"Id,Name,Description\n");

        // Fill with rows until we exceed VALIDATION_SAMPLE_SIZE
        let row = b"12345,\"John Doe\",\"Some description here\"\n";
        let rows_needed = (VALIDATION_SAMPLE_SIZE / row.len()) + 100;

        for i in 0..rows_needed {
            let row_content = format!("{},\"Name{}\",\"Description for row {}\"\n", i, i, i);
            content.extend_from_slice(row_content.as_bytes());
        }

        // Ensure file is larger than sample size
        assert!(
            content.len() > VALIDATION_SAMPLE_SIZE,
            "File should be larger than sample size"
        );

        let file = create_temp_csv(&content);

        let result = validate(file.path())
            .await
            .expect("Validation should not fail");

        // Should succeed because truncation at end should be ignored
        assert!(
            result.ok,
            "Validation should succeed despite truncation: {:?}",
            result.errors
        );
        assert!(
            result
                .warnings
                .iter()
                .any(|w| matches!(w, CsvValidationWarning::SampleOnlyValidation { .. })),
            "Should have sample-only warning"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 4: Real Inconsistency
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_inconsistent_columns_detected() {
        let content = b"Name,Value\nAlice,100\nBob,200,ExtraColumn\nCharlie,300\n";
        let file = create_temp_csv(content);

        let result = validate(file.path())
            .await
            .expect("Validation should not fail");

        assert!(
            !result.ok,
            "Validation should fail for inconsistent columns"
        );
        let has_inconsistent_error = result.errors.iter().any(|e| {
            matches!(
                e,
                CsvValidationError::InconsistentColumns {
                    expected: 2,
                    row: 3, // Row 3 (1-based, including header)
                    ..
                }
            )
        });
        assert!(
            has_inconsistent_error,
            "Should detect inconsistent columns on row 3: {:?}",
            result.errors
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 5: Quoted Newlines
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_quoted_newlines_valid() {
        let content = b"Name,Desc\n\"John\",\"Line1\nLine2\"\n";
        let file = create_temp_csv(content);

        let result = validate(file.path())
            .await
            .expect("Validation should not fail");

        assert!(result.ok, "Validation should succeed with quoted newlines");
        assert_eq!(result.stats.headers, vec!["Name", "Desc"]);
        // Should count as 1 data row
        assert!(
            result.stats.estimated_total_rows.is_some(),
            "Should have row estimate"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Additional Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_empty_file_error() {
        let file = create_temp_csv(b"");

        let result = validate(file.path())
            .await
            .expect("Validation should not fail");

        assert!(!result.ok, "Validation should fail for empty file");
        assert!(
            result.errors.contains(&CsvValidationError::EmptyFile),
            "Should contain EmptyFile error"
        );
    }

    #[tokio::test]
    async fn test_headers_only_valid() {
        let content = b"Name,Value,Description\n";
        let file = create_temp_csv(content);

        let result = validate(file.path())
            .await
            .expect("Validation should not fail");

        assert!(result.ok, "Validation should succeed with headers only");
        assert_eq!(result.stats.headers, vec!["Name", "Value", "Description"]);
        assert_eq!(result.stats.inferred_column_count, 3);
    }

    #[tokio::test]
    async fn test_line_endings_lf() {
        let content = b"Name,Value\nAlice,100\nBob,200\n";
        let file = create_temp_csv(content);

        let result = validate(file.path())
            .await
            .expect("Validation should not fail");

        assert_eq!(result.stats.line_endings, LineEndings::LF);
    }

    #[tokio::test]
    async fn test_line_endings_crlf() {
        let content = b"Name,Value\r\nAlice,100\r\nBob,200\r\n";
        let file = create_temp_csv(content);

        let result = validate(file.path())
            .await
            .expect("Validation should not fail");

        assert_eq!(result.stats.line_endings, LineEndings::CRLF);
    }

    #[tokio::test]
    async fn test_line_endings_mixed() {
        let content = b"Name,Value\r\nAlice,100\nBob,200\r\n";
        let file = create_temp_csv(content);

        let result = validate(file.path())
            .await
            .expect("Validation should not fail");

        assert_eq!(result.stats.line_endings, LineEndings::Mixed);
        assert!(
            result
                .warnings
                .contains(&CsvValidationWarning::MixedLineEndings),
            "Should warn about mixed line endings"
        );
    }

    #[tokio::test]
    async fn test_valid_csv_full_check() {
        let content = b"Id,Name,Email,Score\n1,Alice,alice@example.com,95\n2,Bob,bob@example.com,87\n3,Charlie,charlie@example.com,92\n";
        let file = create_temp_csv(content);

        let result = validate(file.path())
            .await
            .expect("Validation should not fail");

        assert!(result.ok, "Validation should succeed");
        assert!(result.errors.is_empty(), "Should have no errors");
        assert_eq!(result.stats.headers.len(), 4);
        assert_eq!(result.stats.inferred_column_count, 4);
        assert!(result.stats.estimated_total_rows.is_some());
    }

    #[test]
    fn test_detect_line_endings_lf() {
        let data = b"line1\nline2\nline3\n";
        assert_eq!(detect_line_endings(data), LineEndings::LF);
    }

    #[test]
    fn test_detect_line_endings_crlf() {
        let data = b"line1\r\nline2\r\nline3\r\n";
        assert_eq!(detect_line_endings(data), LineEndings::CRLF);
    }

    #[test]
    fn test_detect_line_endings_mixed() {
        let data = b"line1\r\nline2\nline3\r\n";
        assert_eq!(detect_line_endings(data), LineEndings::Mixed);
    }

    #[test]
    fn test_detect_line_endings_unknown() {
        let data = b"single line no newline";
        assert_eq!(detect_line_endings(data), LineEndings::Unknown);
    }
}
