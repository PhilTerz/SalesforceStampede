//! Salesforce Bulk API v2 Query Client for large data exports.
//!
//! This module provides functionality to:
//! - Create bulk query jobs from SOQL
//! - Poll job status
//! - Stream CSV results to disk without loading into memory
//! - Support job abort (best-effort)
//!
//! # Security
//!
//! - Raw SOQL queries are never logged
//! - Auth headers and tokens are never logged
//! - Only HTTP method, path, and status codes are logged

use std::path::Path;
use std::sync::Arc;

use futures_util::StreamExt;
use reqwest::header::HeaderMap;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tracing::info;
use url::Url;

use crate::error::AppError;
use crate::salesforce::{BulkJobState, API_VERSION};

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

/// Header name for the locator used in result pagination.
const SFORCE_LOCATOR_HEADER: &str = "Sforce-Locator";

/// Header name for the number of records in current result set.
#[allow(dead_code)]
const SFORCE_NUMBER_OF_RECORDS_HEADER: &str = "Sforce-NumberOfRecords";

// ─────────────────────────────────────────────────────────────────────────────
// Public Types
// ─────────────────────────────────────────────────────────────────────────────

/// Information about a Bulk API v2 query job.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BulkQueryJobInfo {
    /// Unique identifier for the job.
    pub id: String,
    /// Current state of the job.
    pub state: BulkJobState,
    /// Number of records processed so far.
    #[serde(default)]
    pub number_records_processed: Option<u64>,
    /// Number of records that failed processing.
    #[serde(default)]
    pub number_records_failed: Option<u64>,
    /// Error message if job failed.
    #[serde(default)]
    pub error_message: Option<String>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal Wire Types
// ─────────────────────────────────────────────────────────────────────────────

/// Request body for creating a bulk query job.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CreateQueryJobRequest<'a> {
    operation: &'static str,
    query: &'a str,
    content_type: &'static str,
}

/// Request body for aborting a bulk query job.
#[derive(Debug, Serialize)]
struct AbortJobRequest {
    state: &'static str,
}

/// Salesforce API error response format.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SalesforceError {
    message: String,
    error_code: String,
}

// ─────────────────────────────────────────────────────────────────────────────
// BulkQueryV2Client
// ─────────────────────────────────────────────────────────────────────────────

/// Client for Salesforce Bulk API v2 query operations.
///
/// Provides methods to create, monitor, and download results from bulk query jobs.
/// Results are streamed directly to disk to handle large data volumes efficiently.
#[derive(Clone)]
pub struct BulkQueryV2Client {
    /// Shared HTTP client.
    client: Arc<Client>,
    /// Base instance URL (e.g., "https://na1.salesforce.com").
    base_url: Url,
    /// Access token for authentication.
    access_token: String,
}

impl BulkQueryV2Client {
    /// Creates a new Bulk API v2 client.
    ///
    /// # Arguments
    ///
    /// * `client` - Shared HTTP client
    /// * `base_url` - Salesforce instance URL
    /// * `access_token` - OAuth access token
    pub fn new(client: Arc<Client>, base_url: Url, access_token: String) -> Self {
        Self {
            client,
            base_url,
            access_token,
        }
    }

    /// Creates a new bulk query job.
    ///
    /// # Arguments
    ///
    /// * `soql` - The SOQL query to execute
    ///
    /// # Returns
    ///
    /// The job ID on success.
    ///
    /// # Errors
    ///
    /// - `AppError::SalesforceError` - API error
    /// - `AppError::RateLimited` - Rate limit exceeded
    /// - `AppError::ConnectionFailed` - Network error
    pub async fn create_query_job(&self, soql: &str) -> Result<String, AppError> {
        let url = self.build_jobs_url()?;

        let request_body = CreateQueryJobRequest {
            operation: "query",
            query: soql,
            content_type: "CSV",
        };

        info!("[BULK] POST /jobs/query (creating job)");

        let response = self
            .client
            .post(url)
            .bearer_auth(&self.access_token)
            .json(&request_body)
            .send()
            .await
            .map_err(|e| AppError::ConnectionFailed(format!("Bulk job creation failed: {}", e)))?;

        let status = response.status();
        info!("[BULK] POST /jobs/query -> {}", status.as_u16());

        if !status.is_success() {
            return Err(self.parse_error_response(response, status).await);
        }

        let job_info: BulkQueryJobInfo = response.json().await.map_err(|e| {
            AppError::SalesforceError(format!("Failed to parse job creation response: {}", e))
        })?;

        Ok(job_info.id)
    }

    /// Gets the current status of a bulk query job.
    ///
    /// # Arguments
    ///
    /// * `job_id` - The job ID to check
    ///
    /// # Returns
    ///
    /// Job information including state and record counts.
    pub async fn get_query_job_status(&self, job_id: &str) -> Result<BulkQueryJobInfo, AppError> {
        let url = self.build_job_url(job_id)?;

        info!("[BULK] GET /jobs/query/{} (status)", redact_id(job_id));

        let response = self
            .client
            .get(url)
            .bearer_auth(&self.access_token)
            .send()
            .await
            .map_err(|e| AppError::ConnectionFailed(format!("Job status check failed: {}", e)))?;

        let status = response.status();
        info!(
            "[BULK] GET /jobs/query/{} -> {}",
            redact_id(job_id),
            status.as_u16()
        );

        if !status.is_success() {
            return Err(self.parse_error_response(response, status).await);
        }

        let job_info: BulkQueryJobInfo = response.json().await.map_err(|e| {
            AppError::SalesforceError(format!("Failed to parse job status response: {}", e))
        })?;

        // Check for failed state
        if job_info.state == BulkJobState::Failed {
            return Err(AppError::JobFailed {
                job_id: job_id.to_string(),
                message: job_info
                    .error_message
                    .clone()
                    .unwrap_or_else(|| "Unknown error".to_string()),
            });
        }

        Ok(job_info)
    }

    /// Downloads query results to a file, streaming to avoid memory issues.
    ///
    /// Handles Salesforce result pagination automatically and strips duplicate
    /// CSV headers from subsequent pages.
    ///
    /// # Arguments
    ///
    /// * `job_id` - The completed job ID
    /// * `output_path` - Path where the CSV file will be written
    ///
    /// # Implementation
    ///
    /// - Uses atomic write pattern: writes to `.part` file, renames on success
    /// - Streams data directly to disk without buffering entire response
    /// - Handles pagination via `Sforce-Locator` header
    /// - Strips CSV headers from page 2+ to produce valid merged output
    pub async fn download_query_results(
        &self,
        job_id: &str,
        output_path: &Path,
    ) -> Result<(), AppError> {
        // Create temporary file path for atomic write
        let temp_path = output_path.with_extension("part");

        // Ensure parent directory exists
        if let Some(parent) = temp_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                AppError::Internal(format!("Failed to create output directory: {}", e))
            })?;
        }

        // Create/truncate temp file
        let mut file = File::create(&temp_path)
            .await
            .map_err(|e| AppError::Internal(format!("Failed to create temp file: {}", e)))?;

        // Download with pagination
        let mut locator: Option<String> = None;
        let mut is_first_page = true;

        loop {
            let (next_locator, bytes_written) = self
                .download_result_page(job_id, locator.as_deref(), &mut file, is_first_page)
                .await?;

            info!(
                "[BULK] Downloaded page for job {} ({} bytes)",
                redact_id(job_id),
                bytes_written
            );

            // Check if there are more pages
            match next_locator {
                Some(loc) if loc != "null" && !loc.is_empty() => {
                    locator = Some(loc);
                    is_first_page = false;
                }
                _ => break,
            }
        }

        // Flush and close the file
        file.flush()
            .await
            .map_err(|e| AppError::Internal(format!("Failed to flush output file: {}", e)))?;
        drop(file);

        // Atomic rename to final path
        tokio::fs::rename(&temp_path, output_path)
            .await
            .map_err(|e| {
                AppError::Internal(format!("Failed to rename temp file to final path: {}", e))
            })?;

        info!(
            "[BULK] Download complete for job {}: {:?}",
            redact_id(job_id),
            output_path
        );

        Ok(())
    }

    /// Downloads a single page of results.
    ///
    /// Returns the next locator (if any) and number of bytes written.
    async fn download_result_page(
        &self,
        job_id: &str,
        locator: Option<&str>,
        file: &mut File,
        is_first_page: bool,
    ) -> Result<(Option<String>, usize), AppError> {
        let url = self.build_results_url(job_id, locator)?;

        info!(
            "[BULK] GET /jobs/query/{}/results{}",
            redact_id(job_id),
            if locator.is_some() {
                " (paginated)"
            } else {
                ""
            }
        );

        let response = self
            .client
            .get(url)
            .bearer_auth(&self.access_token)
            .send()
            .await
            .map_err(|e| AppError::ConnectionFailed(format!("Results download failed: {}", e)))?;

        let status = response.status();
        info!(
            "[BULK] GET /jobs/query/{}/results -> {}",
            redact_id(job_id),
            status.as_u16()
        );

        if !status.is_success() {
            return Err(self.parse_error_response(response, status).await);
        }

        // Extract locator for next page
        let next_locator = extract_locator(response.headers());

        // Stream response body to file
        let bytes_written = if is_first_page {
            // First page: write everything including headers
            self.stream_response_to_file(response, file).await?
        } else {
            // Subsequent pages: skip the CSV header row
            self.stream_response_skip_header(response, file).await?
        };

        Ok((next_locator, bytes_written))
    }

    /// Streams response body directly to file.
    async fn stream_response_to_file(
        &self,
        response: reqwest::Response,
        file: &mut File,
    ) -> Result<usize, AppError> {
        let mut stream = response.bytes_stream();
        let mut total_bytes = 0usize;

        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result.map_err(|e| {
                AppError::ConnectionFailed(format!("Error reading response stream: {}", e))
            })?;
            file.write_all(&chunk)
                .await
                .map_err(|e| AppError::Internal(format!("Error writing to file: {}", e)))?;
            total_bytes += chunk.len();
        }

        Ok(total_bytes)
    }

    /// Streams response body to file, skipping the first line (CSV header).
    async fn stream_response_skip_header(
        &self,
        response: reqwest::Response,
        file: &mut File,
    ) -> Result<usize, AppError> {
        let mut stream = response.bytes_stream();
        let mut total_bytes = 0usize;
        let mut header_skipped = false;
        let mut pending_bytes: Vec<u8> = Vec::new();

        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result.map_err(|e| {
                AppError::ConnectionFailed(format!("Error reading response stream: {}", e))
            })?;

            if header_skipped {
                // Header already skipped, write directly
                file.write_all(&chunk)
                    .await
                    .map_err(|e| AppError::Internal(format!("Error writing to file: {}", e)))?;
                total_bytes += chunk.len();
            } else {
                // Accumulate bytes until we find the first newline
                pending_bytes.extend_from_slice(&chunk);

                // Look for newline to skip header
                if let Some(newline_pos) = find_line_end(&pending_bytes) {
                    // Write everything after the first line
                    let after_header = &pending_bytes[newline_pos..];
                    if !after_header.is_empty() {
                        file.write_all(after_header).await.map_err(|e| {
                            AppError::Internal(format!("Error writing to file: {}", e))
                        })?;
                        total_bytes += after_header.len();
                    }
                    header_skipped = true;
                    pending_bytes.clear();
                }
            }
        }

        // If we never found a newline but have pending bytes, write them
        // (edge case: single line without newline)
        if !header_skipped && !pending_bytes.is_empty() {
            // This shouldn't happen for CSV with multiple rows, but handle gracefully
            file.write_all(&pending_bytes)
                .await
                .map_err(|e| AppError::Internal(format!("Error writing to file: {}", e)))?;
            total_bytes += pending_bytes.len();
        }

        Ok(total_bytes)
    }

    /// Aborts a bulk query job (best-effort).
    ///
    /// # Arguments
    ///
    /// * `job_id` - The job ID to abort
    ///
    /// # Note
    ///
    /// Abort is best-effort. The job may have already completed or failed.
    pub async fn abort_query_job(&self, job_id: &str) -> Result<(), AppError> {
        let url = self.build_job_url(job_id)?;

        let request_body = AbortJobRequest { state: "Aborted" };

        info!("[BULK] PATCH /jobs/query/{} (aborting)", redact_id(job_id));

        let response = self
            .client
            .patch(url)
            .bearer_auth(&self.access_token)
            .json(&request_body)
            .send()
            .await
            .map_err(|e| AppError::ConnectionFailed(format!("Job abort failed: {}", e)))?;

        let status = response.status();
        info!(
            "[BULK] PATCH /jobs/query/{} -> {}",
            redact_id(job_id),
            status.as_u16()
        );

        if !status.is_success() {
            return Err(self.parse_error_response(response, status).await);
        }

        Ok(())
    }

    // ─────────────────────────────────────────────────────────────────────────
    // URL Builders
    // ─────────────────────────────────────────────────────────────────────────

    /// Builds the base jobs URL: /services/data/vXX.X/jobs/query
    fn build_jobs_url(&self) -> Result<Url, AppError> {
        let path = format!("/services/data/{}/jobs/query", API_VERSION);
        self.base_url
            .join(&path)
            .map_err(|e| AppError::Internal(format!("Failed to build jobs URL: {}", e)))
    }

    /// Builds a specific job URL: /services/data/vXX.X/jobs/query/{job_id}
    fn build_job_url(&self, job_id: &str) -> Result<Url, AppError> {
        let path = format!("/services/data/{}/jobs/query/{}", API_VERSION, job_id);
        self.base_url
            .join(&path)
            .map_err(|e| AppError::Internal(format!("Failed to build job URL: {}", e)))
    }

    /// Builds the results URL with optional locator for pagination.
    fn build_results_url(&self, job_id: &str, locator: Option<&str>) -> Result<Url, AppError> {
        let path = format!(
            "/services/data/{}/jobs/query/{}/results",
            API_VERSION, job_id
        );
        let mut url = self
            .base_url
            .join(&path)
            .map_err(|e| AppError::Internal(format!("Failed to build results URL: {}", e)))?;

        if let Some(loc) = locator {
            url.query_pairs_mut().append_pair("locator", loc);
        }

        Ok(url)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Error Handling
    // ─────────────────────────────────────────────────────────────────────────

    /// Parses an error response and maps to appropriate AppError.
    async fn parse_error_response(
        &self,
        response: reqwest::Response,
        status: reqwest::StatusCode,
    ) -> AppError {
        // Check for rate limiting
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after = response
                .headers()
                .get("Retry-After")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok());
            return AppError::RateLimited {
                retry_after_secs: retry_after,
            };
        }

        // Check for not found
        if status == reqwest::StatusCode::NOT_FOUND {
            return AppError::NotFound("Bulk query job not found".to_string());
        }

        // Try to parse Salesforce error response
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| String::from("Unable to read error body"));

        if let Ok(errors) = serde_json::from_str::<Vec<SalesforceError>>(&body) {
            if let Some(first_error) = errors.first() {
                // Check for specific error codes
                if first_error.error_code == "REQUEST_LIMIT_EXCEEDED" {
                    return AppError::RateLimited {
                        retry_after_secs: None,
                    };
                }

                return AppError::SalesforceError(format!(
                    "[{}] {}",
                    first_error.error_code, first_error.message
                ));
            }
        }

        // Fallback to generic error
        AppError::SalesforceError(format!(
            "HTTP {} - {}",
            status.as_u16(),
            status.canonical_reason().unwrap_or("Unknown error")
        ))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper Functions
// ─────────────────────────────────────────────────────────────────────────────

/// Extracts the Sforce-Locator header value from response headers.
fn extract_locator(headers: &HeaderMap) -> Option<String> {
    headers
        .get(SFORCE_LOCATOR_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

/// Finds the position after the first line ending (LF or CRLF).
fn find_line_end(bytes: &[u8]) -> Option<usize> {
    for (i, &b) in bytes.iter().enumerate() {
        if b == b'\n' {
            return Some(i + 1);
        }
        if b == b'\r' && bytes.get(i + 1) == Some(&b'\n') {
            return Some(i + 2);
        }
    }
    None
}

/// Redacts a job ID for logging (shows first 8 chars).
fn redact_id(id: &str) -> String {
    if id.len() > 8 {
        format!("{}...", &id[..8])
    } else {
        id.to_string()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::TempDir;
    use wiremock::matchers::{body_json, header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Helper to create a test client pointing to mock server.
    fn create_test_client(mock_url: &str) -> BulkQueryV2Client {
        let client = Arc::new(Client::new());
        let base_url = Url::parse(mock_url).unwrap();
        BulkQueryV2Client::new(client, base_url, "test_token".to_string())
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Job Creation Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_create_query_job_success() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        let response_body = serde_json::json!({
            "id": "750xx000000001ABC",
            "state": "Open",
            "numberRecordsProcessed": 0,
            "numberRecordsFailed": 0
        });

        Mock::given(method("POST"))
            .and(path(format!("/services/data/{}/jobs/query", API_VERSION)))
            .and(header("Authorization", "Bearer test_token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = client.create_query_job("SELECT Id FROM Account").await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "750xx000000001ABC");
    }

    #[tokio::test]
    async fn test_create_query_job_sends_correct_body() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        let expected_request = serde_json::json!({
            "operation": "query",
            "query": "SELECT Id, Name FROM Account",
            "contentType": "CSV"
        });

        let response_body = serde_json::json!({
            "id": "750xx000000001ABC",
            "state": "Open"
        });

        Mock::given(method("POST"))
            .and(path(format!("/services/data/{}/jobs/query", API_VERSION)))
            .and(body_json(&expected_request))
            .respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = client
            .create_query_job("SELECT Id, Name FROM Account")
            .await;

        assert!(result.is_ok());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Job State Enum Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn test_job_state_deserialization() {
        let test_cases = [
            (r#""Open""#, BulkJobState::Open),
            (r#""UploadComplete""#, BulkJobState::UploadComplete),
            (r#""InProgress""#, BulkJobState::InProgress),
            (r#""JobComplete""#, BulkJobState::JobComplete),
            (r#""Aborted""#, BulkJobState::Aborted),
            (r#""Failed""#, BulkJobState::Failed),
            (r#""SomeNewState""#, BulkJobState::Unknown),
        ];

        for (json, expected) in test_cases {
            let result: BulkJobState = serde_json::from_str(json).unwrap();
            assert_eq!(result, expected, "Failed for input: {}", json);
        }
    }

    #[test]
    fn test_job_info_deserialization() {
        let json = r#"{
            "id": "750xx000000001ABC",
            "state": "InProgress",
            "numberRecordsProcessed": 1000,
            "numberRecordsFailed": 5,
            "errorMessage": null
        }"#;

        let info: BulkQueryJobInfo = serde_json::from_str(json).unwrap();

        assert_eq!(info.id, "750xx000000001ABC");
        assert_eq!(info.state, BulkJobState::InProgress);
        assert_eq!(info.number_records_processed, Some(1000));
        assert_eq!(info.number_records_failed, Some(5));
        assert!(info.error_message.is_none());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Job Status Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_get_query_job_status_success() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        let response_body = serde_json::json!({
            "id": "750xx000000001ABC",
            "state": "JobComplete",
            "numberRecordsProcessed": 5000,
            "numberRecordsFailed": 0
        });

        Mock::given(method("GET"))
            .and(path(format!(
                "/services/data/{}/jobs/query/750xx000000001ABC",
                API_VERSION
            )))
            .respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = client.get_query_job_status("750xx000000001ABC").await;

        assert!(result.is_ok());
        let info = result.unwrap();
        assert_eq!(info.state, BulkJobState::JobComplete);
        assert_eq!(info.number_records_processed, Some(5000));
    }

    #[tokio::test]
    async fn test_get_query_job_status_failed_returns_error() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        let response_body = serde_json::json!({
            "id": "750xx000000001ABC",
            "state": "Failed",
            "errorMessage": "Query syntax error"
        });

        Mock::given(method("GET"))
            .and(path(format!(
                "/services/data/{}/jobs/query/750xx000000001ABC",
                API_VERSION
            )))
            .respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
            .mount(&mock_server)
            .await;

        let result = client.get_query_job_status("750xx000000001ABC").await;

        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::JobFailed { job_id, message } => {
                assert_eq!(job_id, "750xx000000001ABC");
                assert!(message.contains("syntax error"));
            }
            e => panic!("Expected JobFailed, got: {:?}", e),
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Streaming & Pagination Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_download_results_single_page() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("results.csv");

        let csv_content = "Id,Name\n001xx1,Account1\n001xx2,Account2";

        Mock::given(method("GET"))
            .and(path(format!(
                "/services/data/{}/jobs/query/750xx000000001ABC/results",
                API_VERSION
            )))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Type", "text/csv")
                    .insert_header("Sforce-Locator", "null")
                    .set_body_string(csv_content),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = client
            .download_query_results("750xx000000001ABC", &output_path)
            .await;

        assert!(result.is_ok());
        let content = tokio::fs::read_to_string(&output_path).await.unwrap();
        assert_eq!(content, csv_content);
    }

    #[tokio::test]
    async fn test_download_results_with_pagination() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("results.csv");

        let results_path = format!(
            "/services/data/{}/jobs/query/750xx000000001ABC/results",
            API_VERSION
        );

        // Mount page 2 mock FIRST (will be tried second in LIFO)
        // This mock has a specific query_param constraint
        let page2_content = "A,B\n3,4\n";
        Mock::given(method("GET"))
            .and(path(&results_path))
            .and(query_param("locator", "locator123"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Type", "text/csv")
                    .insert_header("Sforce-Locator", "null")
                    .set_body_string(page2_content),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        // Mount page 1 mock SECOND (will be tried first in LIFO)
        // Use up_to_n_times(1) so it only responds once, then falls through
        let page1_content = "A,B\n1,2\n";
        Mock::given(method("GET"))
            .and(path(&results_path))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Type", "text/csv")
                    .insert_header("Sforce-Locator", "locator123")
                    .set_body_string(page1_content),
            )
            .up_to_n_times(1)
            .mount(&mock_server)
            .await;

        let result = client
            .download_query_results("750xx000000001ABC", &output_path)
            .await;

        assert!(result.is_ok());

        // Verify content: header only once, both data rows present
        let content = tokio::fs::read_to_string(&output_path).await.unwrap();
        assert_eq!(content, "A,B\n1,2\n3,4\n");
    }

    #[tokio::test]
    async fn test_download_uses_atomic_write() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("results.csv");
        let temp_path = output_path.with_extension("part");

        Mock::given(method("GET"))
            .and(path(format!(
                "/services/data/{}/jobs/query/750xx000000001ABC/results",
                API_VERSION
            )))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Sforce-Locator", "null")
                    .set_body_string("Id\n001"),
            )
            .mount(&mock_server)
            .await;

        // Before download, neither file should exist
        assert!(!output_path.exists());
        assert!(!temp_path.exists());

        let result = client
            .download_query_results("750xx000000001ABC", &output_path)
            .await;

        assert!(result.is_ok());
        // After download, final file should exist, temp should be gone
        assert!(output_path.exists());
        assert!(!temp_path.exists());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Abort Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_abort_query_job_success() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        let expected_request = serde_json::json!({ "state": "Aborted" });

        Mock::given(method("PATCH"))
            .and(path(format!(
                "/services/data/{}/jobs/query/750xx000000001ABC",
                API_VERSION
            )))
            .and(body_json(&expected_request))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "750xx000000001ABC",
                "state": "Aborted"
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = client.abort_query_job("750xx000000001ABC").await;

        assert!(result.is_ok());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Error Handling Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_rate_limited_error() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        Mock::given(method("POST"))
            .and(path(format!("/services/data/{}/jobs/query", API_VERSION)))
            .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "60"))
            .mount(&mock_server)
            .await;

        let result = client.create_query_job("SELECT Id FROM Account").await;

        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::RateLimited { retry_after_secs } => {
                assert_eq!(retry_after_secs, Some(60));
            }
            e => panic!("Expected RateLimited, got: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_not_found_error() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        Mock::given(method("GET"))
            .and(path(format!(
                "/services/data/{}/jobs/query/invalid_id",
                API_VERSION
            )))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;

        let result = client.get_query_job_status("invalid_id").await;

        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::NotFound(msg) => {
                assert!(msg.contains("not found"));
            }
            e => panic!("Expected NotFound, got: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_salesforce_error_parsing() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        let error_response = serde_json::json!([{
            "errorCode": "MALFORMED_QUERY",
            "message": "unexpected token: SELECT"
        }]);

        Mock::given(method("POST"))
            .and(path(format!("/services/data/{}/jobs/query", API_VERSION)))
            .respond_with(ResponseTemplate::new(400).set_body_json(&error_response))
            .mount(&mock_server)
            .await;

        let result = client.create_query_job("SELECT").await;

        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::SalesforceError(msg) => {
                assert!(msg.contains("MALFORMED_QUERY"));
                assert!(msg.contains("unexpected token"));
            }
            e => panic!("Expected SalesforceError, got: {:?}", e),
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helper Function Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn test_find_line_end_lf() {
        assert_eq!(find_line_end(b"A,B\n1,2"), Some(4));
    }

    #[test]
    fn test_find_line_end_crlf() {
        assert_eq!(find_line_end(b"A,B\r\n1,2"), Some(5));
    }

    #[test]
    fn test_find_line_end_no_newline() {
        assert_eq!(find_line_end(b"A,B"), None);
    }

    #[test]
    fn test_redact_id_long() {
        assert_eq!(redact_id("750xx000000001ABC"), "750xx000...");
    }

    #[test]
    fn test_redact_id_short() {
        assert_eq!(redact_id("short"), "short");
    }
}
