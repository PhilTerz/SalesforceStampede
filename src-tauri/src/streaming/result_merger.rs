//! Result merging for bulk ingest operations.
//!
//! After a multi-chunk bulk ingest run, each Salesforce job produces success and failure
//! result CSVs. This module downloads and merges these per-job results into combined files.
//!
//! Key features:
//! - Streaming merge (never loads full files into memory)
//! - Header normalization with BOM stripping
//! - Newline boundary handling to prevent row gluing
//! - Atomic writes using .part files
//! - Best-effort temp cleanup on success or failure

use std::future::Future;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;

use serde::{Deserialize, Serialize};

use crate::error::AppError;
use crate::storage::jobs::{get_group, BulkJobRow};
use crate::storage::Database;

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

/// UTF-8 BOM bytes.
const UTF8_BOM: &[u8] = &[0xEF, 0xBB, 0xBF];

/// Buffer size for reading/writing (64 KB).
const BUFFER_SIZE: usize = 64 * 1024;

// ─────────────────────────────────────────────────────────────────────────────
// Public Types
// ─────────────────────────────────────────────────────────────────────────────

/// Paths to the merged result files from a bulk job group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkGroupResults {
    /// Path to the merged success results CSV.
    pub success_path: PathBuf,
    /// Path to the merged failure results CSV.
    pub failure_path: PathBuf,
    /// Path to the warnings file (if any warnings occurred during merge).
    pub warnings_path: Option<PathBuf>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Provider Trait
// ─────────────────────────────────────────────────────────────────────────────

/// Trait for downloading job results from Salesforce.
///
/// This trait allows the result merger to be decoupled from the actual Salesforce client.
/// The orchestration layer can implement this for the real Bulk client, and tests can
/// provide a fake implementation that writes CSV fixtures.
pub trait JobResultProvider: Send + Sync {
    /// Downloads the success results for a job to the specified path.
    fn download_success<'a>(
        &'a self,
        job_id: &'a str,
        out_path: &'a Path,
    ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + 'a>>;

    /// Downloads the failure results for a job to the specified path.
    fn download_failure<'a>(
        &'a self,
        job_id: &'a str,
        out_path: &'a Path,
    ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + 'a>>;
}

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

/// Downloads and merges results for all jobs in a group.
///
/// # Arguments
///
/// * `db` - Database for fetching group/job info
/// * `provider` - Provider for downloading results from Salesforce
/// * `group_id` - The group ID to download results for
/// * `output_dir` - Directory where merged results will be written
///
/// # Returns
///
/// `BulkGroupResults` containing paths to merged success, failure, and optional warnings files.
///
/// # Errors
///
/// Returns `AppError::NotFound` if the group doesn't exist.
/// Returns `AppError::Internal` for I/O errors during download or merge.
pub async fn download_group_results(
    db: &Database,
    provider: &dyn JobResultProvider,
    group_id: &str,
    output_dir: &Path,
) -> Result<BulkGroupResults, AppError> {
    // 1) Discover jobs in group
    let group_with_jobs = get_group(db, group_id).await?;
    let jobs: Vec<BulkJobRow> = group_with_jobs.jobs;

    if jobs.is_empty() {
        return Err(AppError::NotFound(format!(
            "Group {} has no jobs",
            group_id
        )));
    }

    // Get job_ids in part_number order (already sorted by get_group)
    let job_ids: Vec<&str> = jobs.iter().map(|j| j.job_id.as_str()).collect();

    // 2) Prepare output directory
    tokio::fs::create_dir_all(output_dir)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to create output directory: {}", e)))?;

    // Output file paths
    let success_final = output_dir.join(format!("group_{}_success.csv", group_id));
    let failure_final = output_dir.join(format!("group_{}_failure.csv", group_id));
    let warnings_final = output_dir.join(format!("group_{}_merge_warnings.txt", group_id));

    // Part file paths (for atomic writes)
    let success_part = output_dir.join(format!("group_{}_success.csv.part", group_id));
    let failure_part = output_dir.join(format!("group_{}_failure.csv.part", group_id));

    // Track temp files for cleanup
    let mut temp_files: Vec<PathBuf> = Vec::new();

    // 3) Download all job results to temp files
    let mut success_temps: Vec<PathBuf> = Vec::new();
    let mut failure_temps: Vec<PathBuf> = Vec::new();

    let download_result = download_all_job_results(
        provider,
        &job_ids,
        output_dir,
        &mut success_temps,
        &mut failure_temps,
        &mut temp_files,
    )
    .await;

    if let Err(e) = download_result {
        // Cleanup temps on error
        cleanup_temps(&temp_files).await;
        return Err(e);
    }

    // 4) Merge results (blocking I/O in spawn_blocking)
    let merge_result = {
        let success_temps_clone = success_temps.clone();
        let failure_temps_clone = failure_temps.clone();
        let success_part_clone = success_part.clone();
        let failure_part_clone = failure_part.clone();
        let warnings_final_clone = warnings_final.clone();
        let job_ids_clone: Vec<String> = job_ids.iter().map(|s| s.to_string()).collect();

        tokio::task::spawn_blocking(move || {
            merge_results_blocking(
                &success_temps_clone,
                &failure_temps_clone,
                &success_part_clone,
                &failure_part_clone,
                &warnings_final_clone,
                &job_ids_clone,
            )
        })
        .await
        .map_err(|e| AppError::Internal(format!("Merge task panicked: {}", e)))?
    };

    // Cleanup temps regardless of merge result
    cleanup_temps(&temp_files).await;

    // Handle merge errors
    let has_warnings = merge_result.map_err(|e| {
        // Also cleanup .part files on error
        let _ = std::fs::remove_file(&success_part);
        let _ = std::fs::remove_file(&failure_part);
        e
    })?;

    // 5) Rename .part files to final (atomic)
    tokio::fs::rename(&success_part, &success_final)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to rename success file: {}", e)))?;

    tokio::fs::rename(&failure_part, &failure_final)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to rename failure file: {}", e)))?;

    Ok(BulkGroupResults {
        success_path: success_final,
        failure_path: failure_final,
        warnings_path: if has_warnings {
            Some(warnings_final)
        } else {
            None
        },
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Download Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Downloads all job results to temp files.
async fn download_all_job_results(
    provider: &dyn JobResultProvider,
    job_ids: &[&str],
    output_dir: &Path,
    success_temps: &mut Vec<PathBuf>,
    failure_temps: &mut Vec<PathBuf>,
    all_temps: &mut Vec<PathBuf>,
) -> Result<(), AppError> {
    for job_id in job_ids {
        // Download success results
        let success_temp = output_dir.join(format!("job_{}_success.tmp", job_id));
        provider.download_success(job_id, &success_temp).await?;
        success_temps.push(success_temp.clone());
        all_temps.push(success_temp);

        // Download failure results
        let failure_temp = output_dir.join(format!("job_{}_failure.tmp", job_id));
        provider.download_failure(job_id, &failure_temp).await?;
        failure_temps.push(failure_temp.clone());
        all_temps.push(failure_temp);
    }

    Ok(())
}

/// Best-effort cleanup of temp files.
async fn cleanup_temps(temps: &[PathBuf]) {
    for path in temps {
        let _ = tokio::fs::remove_file(path).await;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Merge Logic (Blocking)
// ─────────────────────────────────────────────────────────────────────────────

/// Merges downloaded temp files into output files.
/// Returns true if warnings were generated.
fn merge_results_blocking(
    success_temps: &[PathBuf],
    failure_temps: &[PathBuf],
    success_out: &Path,
    failure_out: &Path,
    warnings_path: &Path,
    job_ids: &[String],
) -> Result<bool, AppError> {
    let mut warnings: Vec<String> = Vec::new();

    // Merge success files
    merge_files(
        success_temps,
        success_out,
        job_ids,
        "success",
        &mut warnings,
    )?;

    // Merge failure files
    merge_files(
        failure_temps,
        failure_out,
        job_ids,
        "failure",
        &mut warnings,
    )?;

    // Write warnings if any
    if !warnings.is_empty() {
        let mut file = std::fs::File::create(warnings_path)
            .map_err(|e| AppError::Internal(format!("Failed to create warnings file: {}", e)))?;

        for warning in &warnings {
            writeln!(file, "{}", warning)
                .map_err(|e| AppError::Internal(format!("Failed to write warning: {}", e)))?;
        }
    }

    Ok(!warnings.is_empty())
}

/// Merges multiple CSV files into one output file.
fn merge_files(
    temps: &[PathBuf],
    output: &Path,
    job_ids: &[String],
    file_type: &str,
    warnings: &mut Vec<String>,
) -> Result<(), AppError> {
    let mut out_file = std::fs::File::create(output)
        .map_err(|e| AppError::Internal(format!("Failed to create output file: {}", e)))?;

    let mut writer = BufWriter::with_capacity(BUFFER_SIZE, &mut out_file);

    let mut canonical_header: Option<Vec<u8>> = None;
    let mut last_ended_with_newline = true;

    for (i, temp_path) in temps.iter().enumerate() {
        let job_id = &job_ids[i];

        // Open temp file
        let file = match std::fs::File::open(temp_path) {
            Ok(f) => f,
            Err(e) => {
                warnings.push(format!(
                    "Job {} {}: Failed to open temp file: {}",
                    job_id, file_type, e
                ));
                continue;
            }
        };

        let mut reader = BufReader::with_capacity(BUFFER_SIZE, file);

        // Read header line
        let mut header_buf: Vec<u8> = Vec::new();
        let header_bytes = reader
            .read_until(b'\n', &mut header_buf)
            .map_err(|e| AppError::Internal(format!("Failed to read header: {}", e)))?;

        if header_bytes == 0 {
            // Empty file - skip
            continue;
        }

        // Normalize header for comparison (always strip BOM for consistent comparison)
        let normalized_header = normalize_header(&header_buf, true);

        if normalized_header.is_empty() {
            // Header was only whitespace/newlines - skip
            continue;
        }

        match &canonical_header {
            None => {
                // First non-empty header becomes canonical
                canonical_header = Some(normalized_header.clone());

                // Write header to output (with newline)
                writer
                    .write_all(&normalized_header)
                    .map_err(|e| AppError::Internal(format!("Failed to write header: {}", e)))?;
                writer
                    .write_all(b"\n")
                    .map_err(|e| AppError::Internal(format!("Failed to write newline: {}", e)))?;
                last_ended_with_newline = true;
            }
            Some(canonical) => {
                // Compare with canonical
                if &normalized_header != canonical {
                    warnings.push(format!(
                        "Job {} {}: Header mismatch - expected {:?}, got {:?}",
                        job_id,
                        file_type,
                        String::from_utf8_lossy(canonical),
                        String::from_utf8_lossy(&normalized_header)
                    ));
                }
                // Still merge rows even if header differs
            }
        }

        // Ensure newline boundary before appending rows
        if !last_ended_with_newline {
            writer.write_all(b"\n").map_err(|e| {
                AppError::Internal(format!("Failed to write boundary newline: {}", e))
            })?;
            last_ended_with_newline = true;
        }

        // Stream-copy remaining content (rows)
        copy_remaining(&mut reader, &mut writer, &mut last_ended_with_newline)?;
    }

    // Handle case where all files were empty or had no rows
    if canonical_header.is_none() {
        writer
            .write_all(b"# No results\n")
            .map_err(|e| AppError::Internal(format!("Failed to write no-results marker: {}", e)))?;
    }

    writer
        .flush()
        .map_err(|e| AppError::Internal(format!("Failed to flush output: {}", e)))?;

    Ok(())
}

/// Normalizes a header line for comparison.
///
/// - Strips UTF-8 BOM when `strip_bom` is true (should be true for consistent comparison)
/// - Removes trailing newline characters (\n and \r)
fn normalize_header(header: &[u8], strip_bom: bool) -> Vec<u8> {
    let mut data = header;

    // Strip BOM if requested and present
    if strip_bom && data.starts_with(UTF8_BOM) {
        data = &data[UTF8_BOM.len()..];
    }

    // Remove trailing newline (\n)
    if data.ends_with(b"\n") {
        data = &data[..data.len() - 1];
    }

    // Remove trailing carriage return (\r) if present
    if data.ends_with(b"\r") {
        data = &data[..data.len() - 1];
    }

    data.to_vec()
}

/// Copies remaining content from reader to writer, tracking newline state.
fn copy_remaining<R: Read, W: Write>(
    reader: &mut BufReader<R>,
    writer: &mut BufWriter<W>,
    last_ended_with_newline: &mut bool,
) -> Result<(), AppError> {
    let mut buf = [0u8; BUFFER_SIZE];
    let mut last_byte: Option<u8> = None;

    loop {
        let n = reader
            .read(&mut buf)
            .map_err(|e| AppError::Internal(format!("Failed to read: {}", e)))?;

        if n == 0 {
            break;
        }

        writer
            .write_all(&buf[..n])
            .map_err(|e| AppError::Internal(format!("Failed to write: {}", e)))?;

        last_byte = Some(buf[n - 1]);
    }

    // Update newline tracking
    *last_ended_with_newline = last_byte.map(|b| b == b'\n').unwrap_or(true);

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tempfile::TempDir;

    use crate::salesforce::BulkJobState;
    use crate::storage::jobs::{add_job_to_group, save_group, BulkJobGroupRow, GroupState};

    // ─────────────────────────────────────────────────────────────────────────
    // Fake Provider
    // ─────────────────────────────────────────────────────────────────────────

    struct FakeJobResultProvider {
        success_files: Mutex<HashMap<String, Vec<u8>>>,
        failure_files: Mutex<HashMap<String, Vec<u8>>>,
    }

    impl FakeJobResultProvider {
        fn new() -> Self {
            Self {
                success_files: Mutex::new(HashMap::new()),
                failure_files: Mutex::new(HashMap::new()),
            }
        }

        fn set_success(&self, job_id: &str, content: &[u8]) {
            self.success_files
                .lock()
                .unwrap()
                .insert(job_id.to_string(), content.to_vec());
        }

        fn set_failure(&self, job_id: &str, content: &[u8]) {
            self.failure_files
                .lock()
                .unwrap()
                .insert(job_id.to_string(), content.to_vec());
        }
    }

    impl JobResultProvider for FakeJobResultProvider {
        fn download_success<'a>(
            &'a self,
            job_id: &'a str,
            out_path: &'a Path,
        ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + 'a>> {
            Box::pin(async move {
                let content = self
                    .success_files
                    .lock()
                    .unwrap()
                    .get(job_id)
                    .cloned()
                    .unwrap_or_default();
                tokio::fs::write(out_path, &content)
                    .await
                    .map_err(|e| AppError::Internal(e.to_string()))?;
                Ok(())
            })
        }

        fn download_failure<'a>(
            &'a self,
            job_id: &'a str,
            out_path: &'a Path,
        ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + 'a>> {
            Box::pin(async move {
                let content = self
                    .failure_files
                    .lock()
                    .unwrap()
                    .get(job_id)
                    .cloned()
                    .unwrap_or_default();
                tokio::fs::write(out_path, &content)
                    .await
                    .map_err(|e| AppError::Internal(e.to_string()))?;
                Ok(())
            })
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helper Functions
    // ─────────────────────────────────────────────────────────────────────────

    fn now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs() as i64
    }

    async fn setup_test_group(db: &Database, group_id: &str, job_ids: &[&str]) {
        let timestamp = now();

        let group = BulkJobGroupRow {
            group_id: group_id.to_string(),
            org_id: "test-org".to_string(),
            object: "Account".to_string(),
            operation: "insert".to_string(),
            state: GroupState::Completed,
            batch_size: 1000,
            total_parts: job_ids.len() as i64,
            created_at: timestamp,
            updated_at: timestamp,
        };
        save_group(db, &group).await.expect("Failed to save group");

        for (i, job_id) in job_ids.iter().enumerate() {
            let job = BulkJobRow {
                job_id: job_id.to_string(),
                group_id: group_id.to_string(),
                part_number: (i + 1) as i64,
                state: BulkJobState::JobComplete,
                processed_records: Some(100),
                failed_records: Some(0),
                error_message: None,
                created_at: timestamp,
                updated_at: timestamp,
            };
            add_job_to_group(db, &job).await.expect("Failed to add job");
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 1: Merge success with matching headers
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_merge_success_matching_headers() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = Database::init(db_path).await.expect("Failed to init DB");

        let output_dir = temp_dir.path().join("output");

        // Setup group with 2 jobs
        setup_test_group(&db, "group-1", &["job-a", "job-b"]).await;

        // Setup fake provider
        let provider = FakeJobResultProvider::new();

        // job-a success: header + 2 rows (ends with newline)
        provider.set_success("job-a", b"Id,Name,Success\n001,Alice,true\n002,Bob,true\n");

        // job-b success: header + 1 row
        provider.set_success("job-b", b"Id,Name,Success\n003,Charlie,true\n");

        // Empty failures
        provider.set_failure("job-a", b"Id,Name,Error\n");
        provider.set_failure("job-b", b"Id,Name,Error\n");

        // Download and merge
        let results = download_group_results(&db, &provider, "group-1", &output_dir)
            .await
            .expect("Merge should succeed");

        // Verify success file
        let success_content = tokio::fs::read_to_string(&results.success_path)
            .await
            .expect("Should read success file");

        // Should have 1 header + 3 rows
        let lines: Vec<&str> = success_content.lines().collect();
        assert_eq!(lines.len(), 4, "Should have 4 lines (1 header + 3 rows)");
        assert_eq!(lines[0], "Id,Name,Success");
        assert_eq!(lines[1], "001,Alice,true");
        assert_eq!(lines[2], "002,Bob,true");
        assert_eq!(lines[3], "003,Charlie,true");

        // No warnings expected
        assert!(results.warnings_path.is_none());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 2: Header mismatch creates warnings
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_header_mismatch_creates_warnings() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = Database::init(db_path).await.expect("Failed to init DB");

        let output_dir = temp_dir.path().join("output");

        setup_test_group(&db, "group-2", &["job-a", "job-b"]).await;

        let provider = FakeJobResultProvider::new();

        // job-a success with one header
        provider.set_success("job-a", b"Id,Name,Success\n001,Alice,true\n");

        // job-b success with DIFFERENT header (different column name)
        provider.set_success("job-b", b"Id,FullName,Status\n002,Bob,true\n");

        provider.set_failure("job-a", b"");
        provider.set_failure("job-b", b"");

        let results = download_group_results(&db, &provider, "group-2", &output_dir)
            .await
            .expect("Merge should succeed despite mismatch");

        // Should have warnings
        assert!(results.warnings_path.is_some());

        let warnings_content = tokio::fs::read_to_string(results.warnings_path.as_ref().unwrap())
            .await
            .expect("Should read warnings file");

        assert!(warnings_content.contains("job-b"));
        assert!(warnings_content.contains("success"));
        assert!(warnings_content.contains("Header mismatch"));

        // Verify both rows are still in output
        let success_content = tokio::fs::read_to_string(&results.success_path)
            .await
            .expect("Should read success file");

        let lines: Vec<&str> = success_content.lines().collect();
        assert_eq!(lines.len(), 3, "Should have 3 lines (1 header + 2 rows)");
        assert!(lines[1].contains("Alice"));
        assert!(lines[2].contains("Bob"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 3: Newline boundary correctness
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_newline_boundary_correctness() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = Database::init(db_path).await.expect("Failed to init DB");

        let output_dir = temp_dir.path().join("output");

        setup_test_group(&db, "group-3", &["job-a", "job-b"]).await;

        let provider = FakeJobResultProvider::new();

        // job-a success: does NOT end with newline
        provider.set_success("job-a", b"Id,Name\n001,Alice"); // No trailing \n

        // job-b success: has header + rows
        provider.set_success("job-b", b"Id,Name\n002,Bob\n");

        provider.set_failure("job-a", b"");
        provider.set_failure("job-b", b"");

        let results = download_group_results(&db, &provider, "group-3", &output_dir)
            .await
            .expect("Merge should succeed");

        let success_content = tokio::fs::read_to_string(&results.success_path)
            .await
            .expect("Should read success file");

        // Should have clean row boundaries (no glued lines)
        let lines: Vec<&str> = success_content.lines().collect();
        assert_eq!(lines.len(), 3, "Should have 3 lines");
        assert_eq!(lines[0], "Id,Name");
        assert_eq!(lines[1], "001,Alice");
        assert_eq!(lines[2], "002,Bob");

        // Ensure lines are not glued together
        assert!(
            !success_content.contains("Alice002"),
            "Rows should not be glued"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 4: All empty results
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_all_empty_results() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = Database::init(db_path).await.expect("Failed to init DB");

        let output_dir = temp_dir.path().join("output");

        setup_test_group(&db, "group-4", &["job-a", "job-b"]).await;

        let provider = FakeJobResultProvider::new();

        // All success files are empty or header-only
        provider.set_success("job-a", b"Id,Name\n"); // Header only
        provider.set_success("job-b", b""); // Completely empty

        // All failure files are empty
        provider.set_failure("job-a", b"");
        provider.set_failure("job-b", b"");

        let results = download_group_results(&db, &provider, "group-4", &output_dir)
            .await
            .expect("Merge should succeed");

        // Success file should exist with canonical header
        let success_content = tokio::fs::read_to_string(&results.success_path)
            .await
            .expect("Should read success file");

        // Should have the header from job-a
        assert!(success_content.contains("Id,Name"));

        // Failure file should exist with "# No results" marker
        let failure_content = tokio::fs::read_to_string(&results.failure_path)
            .await
            .expect("Should read failure file");

        assert!(failure_content.contains("# No results"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 5: Failure merge works similarly
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_failure_merge() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = Database::init(db_path).await.expect("Failed to init DB");

        let output_dir = temp_dir.path().join("output");

        setup_test_group(&db, "group-5", &["job-a", "job-b"]).await;

        let provider = FakeJobResultProvider::new();

        // Empty success files
        provider.set_success("job-a", b"");
        provider.set_success("job-b", b"");

        // Failure files with data
        provider.set_failure("job-a", b"Id,Error\n001,Invalid field\n");
        provider.set_failure(
            "job-b",
            b"Id,Error\n002,Duplicate ID\n003,Missing required field\n",
        );

        let results = download_group_results(&db, &provider, "group-5", &output_dir)
            .await
            .expect("Merge should succeed");

        // Verify failure file
        let failure_content = tokio::fs::read_to_string(&results.failure_path)
            .await
            .expect("Should read failure file");

        let lines: Vec<&str> = failure_content.lines().collect();
        assert_eq!(lines.len(), 4, "Should have 4 lines (1 header + 3 rows)");
        assert_eq!(lines[0], "Id,Error");
        assert!(lines[1].contains("Invalid field"));
        assert!(lines[2].contains("Duplicate ID"));
        assert!(lines[3].contains("Missing required field"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 6: BOM stripping
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_bom_stripping() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = Database::init(db_path).await.expect("Failed to init DB");

        let output_dir = temp_dir.path().join("output");

        setup_test_group(&db, "group-6", &["job-a", "job-b"]).await;

        let provider = FakeJobResultProvider::new();

        // job-a success: starts with BOM
        let mut with_bom = Vec::new();
        with_bom.extend_from_slice(UTF8_BOM);
        with_bom.extend_from_slice(b"Id,Name\n001,Alice\n");
        provider
            .success_files
            .lock()
            .unwrap()
            .insert("job-a".to_string(), with_bom);

        // job-b success: no BOM
        provider.set_success("job-b", b"Id,Name\n002,Bob\n");

        provider.set_failure("job-a", b"");
        provider.set_failure("job-b", b"");

        let results = download_group_results(&db, &provider, "group-6", &output_dir)
            .await
            .expect("Merge should succeed");

        let success_bytes = tokio::fs::read(&results.success_path)
            .await
            .expect("Should read success file");

        // Output should NOT start with BOM
        assert!(
            !success_bytes.starts_with(UTF8_BOM),
            "Output should not have BOM"
        );

        let success_content = String::from_utf8(success_bytes).expect("Valid UTF-8");
        let lines: Vec<&str> = success_content.lines().collect();
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0], "Id,Name");

        // No warnings expected (BOMs are silently stripped, not warned about)
        assert!(results.warnings_path.is_none());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 7: CRLF handling
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_crlf_handling() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = Database::init(db_path).await.expect("Failed to init DB");

        let output_dir = temp_dir.path().join("output");

        setup_test_group(&db, "group-7", &["job-a", "job-b"]).await;

        let provider = FakeJobResultProvider::new();

        // job-a with CRLF line endings
        provider.set_success("job-a", b"Id,Name\r\n001,Alice\r\n");

        // job-b with LF line endings
        provider.set_success("job-b", b"Id,Name\n002,Bob\n");

        provider.set_failure("job-a", b"");
        provider.set_failure("job-b", b"");

        let results = download_group_results(&db, &provider, "group-7", &output_dir)
            .await
            .expect("Merge should succeed");

        let success_content = tokio::fs::read_to_string(&results.success_path)
            .await
            .expect("Should read success file");

        // Should merge correctly despite different line endings
        let lines: Vec<&str> = success_content.lines().collect();
        assert_eq!(lines.len(), 3);
        assert!(lines[1].contains("Alice"));
        assert!(lines[2].contains("Bob"));

        // No warnings expected (headers normalize CRLF)
        assert!(results.warnings_path.is_none());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Unit test for normalize_header
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn test_normalize_header() {
        // Basic case
        assert_eq!(normalize_header(b"Id,Name\n", false), b"Id,Name");

        // With CRLF
        assert_eq!(normalize_header(b"Id,Name\r\n", false), b"Id,Name");

        // With BOM (strip_bom = true)
        let mut with_bom = Vec::new();
        with_bom.extend_from_slice(UTF8_BOM);
        with_bom.extend_from_slice(b"Id,Name\n");
        assert_eq!(normalize_header(&with_bom, true), b"Id,Name");

        // With BOM but strip_bom = false
        let mut with_bom2 = Vec::new();
        with_bom2.extend_from_slice(UTF8_BOM);
        with_bom2.extend_from_slice(b"Id,Name\n");
        let result = normalize_header(&with_bom2, false);
        assert!(result.starts_with(UTF8_BOM));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 8: BOM in subsequent file doesn't cause false mismatch
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_bom_in_subsequent_file_no_false_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = Database::init(db_path).await.expect("Failed to init DB");

        let output_dir = temp_dir.path().join("output");

        setup_test_group(&db, "group-8", &["job-a", "job-b"]).await;

        let provider = FakeJobResultProvider::new();

        // job-a success: NO BOM
        provider.set_success("job-a", b"Id,Name\n001,Alice\n");

        // job-b success: HAS BOM (simulating different processing)
        let mut with_bom = Vec::new();
        with_bom.extend_from_slice(UTF8_BOM);
        with_bom.extend_from_slice(b"Id,Name\n002,Bob\n");
        provider
            .success_files
            .lock()
            .unwrap()
            .insert("job-b".to_string(), with_bom);

        provider.set_failure("job-a", b"");
        provider.set_failure("job-b", b"");

        let results = download_group_results(&db, &provider, "group-8", &output_dir)
            .await
            .expect("Merge should succeed");

        // Should NOT have warnings - headers are textually identical
        assert!(
            results.warnings_path.is_none(),
            "BOM in subsequent file should not cause false header mismatch warning"
        );

        // Verify content is correct
        let success_content = tokio::fs::read_to_string(&results.success_path)
            .await
            .expect("Should read success file");

        let lines: Vec<&str> = success_content.lines().collect();
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0], "Id,Name");
        assert!(lines[1].contains("Alice"));
        assert!(lines[2].contains("Bob"));
    }
}
