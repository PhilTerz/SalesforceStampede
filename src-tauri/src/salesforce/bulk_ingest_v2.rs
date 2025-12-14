//! Salesforce Bulk API v2 Ingest Client for large data imports.
//!
//! This module provides functionality to:
//! - Create bulk ingest jobs (insert, update, upsert, delete)
//! - Stream-upload CSV data to Salesforce without loading full files into memory
//! - Close and abort jobs
//! - Poll job status
//! - Stream-download success and failure result CSVs to disk
//!
//! # Security
//!
//! - Raw CSV contents are never logged
//! - Auth headers and tokens are never logged
//! - Only HTTP method, path, and status codes are logged

use std::path::Path;
use std::sync::Arc;

use futures_util::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;
use tracing::info;
use url::Url;

use crate::error::AppError;
use crate::salesforce::{BulkJobState, API_VERSION};

// ─────────────────────────────────────────────────────────────────────────────
// Public Types
// ─────────────────────────────────────────────────────────────────────────────

/// Bulk ingest operation type.
///
/// IMPORTANT: Uses `#[serde(rename_all = "lowercase")]` to match Salesforce API
/// which expects lowercase values ("insert", "update", "upsert", "delete").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BulkOperation {
    /// Insert new records.
    Insert,
    /// Update existing records by ID.
    Update,
    /// Insert or update records based on external ID field.
    Upsert,
    /// Delete records by ID.
    Delete,
}

/// Line ending format for CSV files.
#[derive(Debug, Clone, Copy, Serialize)]
pub enum LineEnding {
    /// Unix-style line ending (\\n).
    LF,
    /// Windows-style line ending (\\r\\n).
    CRLF,
}

/// Request body for creating an ingest job.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateIngestJobRequest {
    /// The Salesforce object name (e.g., "Account", "Contact").
    pub object: String,
    /// The operation to perform.
    pub operation: BulkOperation,
    /// External ID field name (required for upsert operations).
    /// IMPORTANT: Skips serialization when None because Salesforce rejects
    /// null values for this field on insert operations.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_id_field_name: Option<String>,
    /// Line ending format for the CSV data.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line_ending: Option<LineEnding>,
}

/// Information about a Bulk API v2 ingest job.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BulkIngestJobInfo {
    /// Unique identifier for the job.
    pub id: String,
    /// Current state of the job.
    pub state: BulkJobState,
    /// The Salesforce object being processed.
    pub object: String,
    /// The operation being performed.
    pub operation: BulkOperation,
    /// Number of records processed so far.
    #[serde(default, rename = "numberRecordsProcessed")]
    pub processed_records: Option<u64>,
    /// Number of records that failed processing.
    #[serde(default, rename = "numberRecordsFailed")]
    pub failed_records: Option<u64>,
    /// Error message if job failed.
    #[serde(default)]
    pub error_message: Option<String>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal Wire Types
// ─────────────────────────────────────────────────────────────────────────────

/// Request body for changing job state (close or abort).
#[derive(Debug, Serialize)]
struct UpdateJobStateRequest {
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
// BulkIngestV2Client
// ─────────────────────────────────────────────────────────────────────────────

/// Client for Salesforce Bulk API v2 ingest operations.
///
/// Provides methods to create ingest jobs, upload CSV data via streaming,
/// monitor job status, and download success/failure results.
#[derive(Clone)]
pub struct BulkIngestV2Client {
    /// Shared HTTP client.
    client: Arc<Client>,
    /// Base instance URL (e.g., "https://na1.salesforce.com").
    base_url: Url,
    /// Access token for authentication.
    access_token: String,
}

impl BulkIngestV2Client {
    /// Creates a new Bulk API v2 ingest client.
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

    /// Creates a new bulk ingest job.
    ///
    /// # Arguments
    ///
    /// * `req` - The job creation request specifying object, operation, etc.
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
    pub async fn create_ingest_job(&self, req: CreateIngestJobRequest) -> Result<String, AppError> {
        let url = self.build_jobs_url()?;

        // Build request body - merge with contentType: "CSV"
        let mut body = serde_json::to_value(&req).map_err(|e| {
            AppError::Internal(format!("Failed to serialize job request: {}", e))
        })?;
        body.as_object_mut()
            .ok_or_else(|| AppError::Internal("Expected object in JSON".to_string()))?
            .insert("contentType".to_string(), serde_json::json!("CSV"));

        info!(
            "[BULK-INGEST] POST /jobs/ingest (creating {} job for {})",
            format!("{:?}", req.operation).to_lowercase(),
            req.object
        );

        let response = self
            .client
            .post(url)
            .bearer_auth(&self.access_token)
            .json(&body)
            .send()
            .await
            .map_err(|e| AppError::ConnectionFailed(format!("Ingest job creation failed: {}", e)))?;

        let status = response.status();
        info!("[BULK-INGEST] POST /jobs/ingest -> {}", status.as_u16());

        if !status.is_success() {
            return Err(self.parse_error_response(response, status).await);
        }

        let job_info: BulkIngestJobInfo = response.json().await.map_err(|e| {
            AppError::SalesforceError(format!("Failed to parse job creation response: {}", e))
        })?;

        Ok(job_info.id)
    }

    /// Uploads CSV data to the job via streaming PUT.
    ///
    /// This method streams the file directly from disk to the network without
    /// loading the entire file into memory.
    ///
    /// # Arguments
    ///
    /// * `job_id` - The ID of the ingest job
    /// * `csv_path` - Path to the CSV file to upload
    ///
    /// # Errors
    ///
    /// - `AppError::Internal` - File read error
    /// - `AppError::SalesforceError` - API error
    /// - `AppError::ConnectionFailed` - Network error
    pub async fn upload_job_data(&self, job_id: &str, csv_path: &Path) -> Result<(), AppError> {
        let url = self.build_batches_url(job_id)?;

        // Open file for streaming
        let file = tokio::fs::File::open(csv_path).await.map_err(|e| {
            AppError::Internal(format!("Failed to open CSV file: {}", e))
        })?;

        // Get file size for logging (optional, but helpful)
        let metadata = file.metadata().await.map_err(|e| {
            AppError::Internal(format!("Failed to get file metadata: {}", e))
        })?;
        let file_size = metadata.len();

        // Create a stream from the file
        let stream = ReaderStream::new(file);
        let body = reqwest::Body::wrap_stream(stream);

        info!(
            "[BULK-INGEST] PUT /jobs/ingest/{}/batches ({} bytes)",
            redact_id(job_id),
            file_size
        );

        let response = self
            .client
            .put(url)
            .bearer_auth(&self.access_token)
            .header("Content-Type", "text/csv")
            .body(body)
            .send()
            .await
            .map_err(|e| AppError::ConnectionFailed(format!("CSV upload failed: {}", e)))?;

        let status = response.status();
        info!(
            "[BULK-INGEST] PUT /jobs/ingest/{}/batches -> {}",
            redact_id(job_id),
            status.as_u16()
        );

        if !status.is_success() {
            return Err(self.parse_error_response(response, status).await);
        }

        Ok(())
    }

    /// Marks the job upload complete so Salesforce starts processing.
    ///
    /// # Arguments
    ///
    /// * `job_id` - The ID of the ingest job
    ///
    /// # Errors
    ///
    /// - `AppError::SalesforceError` - API error
    /// - `AppError::ConnectionFailed` - Network error
    pub async fn close_job(&self, job_id: &str) -> Result<(), AppError> {
        let url = self.build_job_url(job_id)?;

        let request_body = UpdateJobStateRequest {
            state: "UploadComplete",
        };

        info!(
            "[BULK-INGEST] PATCH /jobs/ingest/{} (closing)",
            redact_id(job_id)
        );

        let response = self
            .client
            .patch(url)
            .bearer_auth(&self.access_token)
            .json(&request_body)
            .send()
            .await
            .map_err(|e| AppError::ConnectionFailed(format!("Job close failed: {}", e)))?;

        let status = response.status();
        info!(
            "[BULK-INGEST] PATCH /jobs/ingest/{} -> {}",
            redact_id(job_id),
            status.as_u16()
        );

        if !status.is_success() {
            return Err(self.parse_error_response(response, status).await);
        }

        Ok(())
    }

    /// Gets the current status of a bulk ingest job.
    ///
    /// # Arguments
    ///
    /// * `job_id` - The job ID to check
    ///
    /// # Returns
    ///
    /// Job information including state and record counts.
    ///
    /// # Errors
    ///
    /// - `AppError::NotFound` - Job not found
    /// - `AppError::JobFailed` - Job is in failed state
    /// - `AppError::SalesforceError` - API error
    pub async fn get_job_status(&self, job_id: &str) -> Result<BulkIngestJobInfo, AppError> {
        let url = self.build_job_url(job_id)?;

        info!(
            "[BULK-INGEST] GET /jobs/ingest/{} (status)",
            redact_id(job_id)
        );

        let response = self
            .client
            .get(url)
            .bearer_auth(&self.access_token)
            .send()
            .await
            .map_err(|e| AppError::ConnectionFailed(format!("Job status check failed: {}", e)))?;

        let status = response.status();
        info!(
            "[BULK-INGEST] GET /jobs/ingest/{} -> {}",
            redact_id(job_id),
            status.as_u16()
        );

        if !status.is_success() {
            return Err(self.parse_error_response(response, status).await);
        }

        let job_info: BulkIngestJobInfo = response.json().await.map_err(|e| {
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

    /// Aborts a bulk ingest job (best-effort).
    ///
    /// # Arguments
    ///
    /// * `job_id` - The job ID to abort
    ///
    /// # Note
    ///
    /// Abort is best-effort. The job may have already completed or failed.
    pub async fn abort_job(&self, job_id: &str) -> Result<(), AppError> {
        let url = self.build_job_url(job_id)?;

        let request_body = UpdateJobStateRequest { state: "Aborted" };

        info!(
            "[BULK-INGEST] PATCH /jobs/ingest/{} (aborting)",
            redact_id(job_id)
        );

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
            "[BULK-INGEST] PATCH /jobs/ingest/{} -> {}",
            redact_id(job_id),
            status.as_u16()
        );

        if !status.is_success() {
            return Err(self.parse_error_response(response, status).await);
        }

        Ok(())
    }

    /// Streams the "successfulResults" CSV to disk.
    ///
    /// Uses atomic write pattern: writes to temp file, then renames on success.
    ///
    /// # Arguments
    ///
    /// * `job_id` - The completed job ID
    /// * `output_path` - Path where the CSV file will be written
    ///
    /// # Errors
    ///
    /// - `AppError::NotFound` - Job not found
    /// - `AppError::SalesforceError` - API error
    /// - `AppError::Internal` - File write error
    pub async fn get_success_results(
        &self,
        job_id: &str,
        output_path: &Path,
    ) -> Result<(), AppError> {
        self.download_results(job_id, "successfulResults", output_path)
            .await
    }

    /// Streams the "failedResults" CSV to disk.
    ///
    /// Uses atomic write pattern: writes to temp file, then renames on success.
    ///
    /// # Arguments
    ///
    /// * `job_id` - The completed job ID
    /// * `output_path` - Path where the CSV file will be written
    ///
    /// # Errors
    ///
    /// - `AppError::NotFound` - Job not found
    /// - `AppError::SalesforceError` - API error
    /// - `AppError::Internal` - File write error
    pub async fn get_failure_results(
        &self,
        job_id: &str,
        output_path: &Path,
    ) -> Result<(), AppError> {
        self.download_results(job_id, "failedResults", output_path)
            .await
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Private Helpers
    // ─────────────────────────────────────────────────────────────────────────

    /// Downloads results to disk using atomic write pattern with tempfile.
    async fn download_results(
        &self,
        job_id: &str,
        result_type: &str,
        output_path: &Path,
    ) -> Result<(), AppError> {
        let url = self.build_results_url(job_id, result_type)?;

        info!(
            "[BULK-INGEST] GET /jobs/ingest/{}/{} (downloading)",
            redact_id(job_id),
            result_type
        );

        let response = self
            .client
            .get(url)
            .bearer_auth(&self.access_token)
            .send()
            .await
            .map_err(|e| {
                AppError::ConnectionFailed(format!("{} download failed: {}", result_type, e))
            })?;

        let status = response.status();
        info!(
            "[BULK-INGEST] GET /jobs/ingest/{}/{} -> {}",
            redact_id(job_id),
            result_type,
            status.as_u16()
        );

        if !status.is_success() {
            return Err(self.parse_error_response(response, status).await);
        }

        // Ensure parent directory exists
        if let Some(parent) = output_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                AppError::Internal(format!("Failed to create output directory: {}", e))
            })?;
        }

        // Create temp file in the same directory for atomic rename
        let parent_dir = output_path
            .parent()
            .unwrap_or_else(|| Path::new("."));
        let temp_file = tempfile::NamedTempFile::new_in(parent_dir).map_err(|e| {
            AppError::Internal(format!("Failed to create temp file: {}", e))
        })?;

        // Convert to tokio file for async writing
        let std_file = temp_file.reopen().map_err(|e| {
            AppError::Internal(format!("Failed to reopen temp file: {}", e))
        })?;
        let mut async_file = File::from_std(std_file);

        // Stream response body to file
        let mut stream = response.bytes_stream();
        let mut total_bytes = 0usize;

        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result.map_err(|e| {
                AppError::ConnectionFailed(format!("Error reading response stream: {}", e))
            })?;
            async_file.write_all(&chunk).await.map_err(|e| {
                AppError::Internal(format!("Error writing to file: {}", e))
            })?;
            total_bytes += chunk.len();
        }

        // Flush and sync
        async_file.flush().await.map_err(|e| {
            AppError::Internal(format!("Failed to flush output file: {}", e))
        })?;
        async_file.sync_all().await.map_err(|e| {
            AppError::Internal(format!("Failed to sync output file: {}", e))
        })?;

        // Atomic rename via persist
        temp_file.persist(output_path).map_err(|e| {
            AppError::Internal(format!("Failed to persist temp file: {}", e))
        })?;

        info!(
            "[BULK-INGEST] {} download complete for job {}: {} bytes",
            result_type,
            redact_id(job_id),
            total_bytes
        );

        Ok(())
    }

    // ─────────────────────────────────────────────────────────────────────────
    // URL Builders
    // ─────────────────────────────────────────────────────────────────────────

    /// Builds the base jobs URL: /services/data/vXX.X/jobs/ingest
    fn build_jobs_url(&self) -> Result<Url, AppError> {
        let path = format!("/services/data/{}/jobs/ingest", API_VERSION);
        self.base_url.join(&path).map_err(|e| {
            AppError::Internal(format!("Failed to build jobs URL: {}", e))
        })
    }

    /// Builds a specific job URL: /services/data/vXX.X/jobs/ingest/{job_id}
    fn build_job_url(&self, job_id: &str) -> Result<Url, AppError> {
        let path = format!("/services/data/{}/jobs/ingest/{}", API_VERSION, job_id);
        self.base_url.join(&path).map_err(|e| {
            AppError::Internal(format!("Failed to build job URL: {}", e))
        })
    }

    /// Builds the batches URL: /services/data/vXX.X/jobs/ingest/{job_id}/batches
    fn build_batches_url(&self, job_id: &str) -> Result<Url, AppError> {
        let path = format!(
            "/services/data/{}/jobs/ingest/{}/batches",
            API_VERSION, job_id
        );
        self.base_url.join(&path).map_err(|e| {
            AppError::Internal(format!("Failed to build batches URL: {}", e))
        })
    }

    /// Builds the results URL: /services/data/vXX.X/jobs/ingest/{job_id}/{result_type}
    fn build_results_url(&self, job_id: &str, result_type: &str) -> Result<Url, AppError> {
        let path = format!(
            "/services/data/{}/jobs/ingest/{}/{}",
            API_VERSION, job_id, result_type
        );
        self.base_url.join(&path).map_err(|e| {
            AppError::Internal(format!("Failed to build results URL: {}", e))
        })
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
            return AppError::NotFound("Bulk ingest job not found".to_string());
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
    use std::io::Write;
    use std::sync::Arc;
    use tempfile::TempDir;
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Helper to create a test client pointing to mock server.
    fn create_test_client(mock_url: &str) -> BulkIngestV2Client {
        let client = Arc::new(Client::new());
        let base_url = Url::parse(mock_url).unwrap();
        BulkIngestV2Client::new(client, base_url, "test_token".to_string())
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Create Job Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_create_ingest_job_success() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        let response_body = serde_json::json!({
            "id": "750xx000000001ABC",
            "state": "Open",
            "object": "Account",
            "operation": "insert",
            "numberRecordsProcessed": 0,
            "numberRecordsFailed": 0
        });

        Mock::given(method("POST"))
            .and(path(format!("/services/data/{}/jobs/ingest", API_VERSION)))
            .and(header("Authorization", "Bearer test_token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
            .expect(1)
            .mount(&mock_server)
            .await;

        let req = CreateIngestJobRequest {
            object: "Account".to_string(),
            operation: BulkOperation::Insert,
            external_id_field_name: None,
            line_ending: None,
        };

        let result = client.create_ingest_job(req).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "750xx000000001ABC");
    }

    #[tokio::test]
    async fn test_create_job_sends_lowercase_operation() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        // Verify the request body has lowercase operation
        let expected_request = serde_json::json!({
            "object": "Account",
            "operation": "insert",
            "contentType": "CSV"
        });

        let response_body = serde_json::json!({
            "id": "750xx000000001ABC",
            "state": "Open",
            "object": "Account",
            "operation": "insert"
        });

        Mock::given(method("POST"))
            .and(path(format!("/services/data/{}/jobs/ingest", API_VERSION)))
            .and(body_json(&expected_request))
            .respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
            .expect(1)
            .mount(&mock_server)
            .await;

        let req = CreateIngestJobRequest {
            object: "Account".to_string(),
            operation: BulkOperation::Insert,
            external_id_field_name: None,
            line_ending: None,
        };

        let result = client.create_ingest_job(req).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_job_skips_null_external_id() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        // The request should NOT contain externalIdFieldName when it's None
        let expected_request = serde_json::json!({
            "object": "Contact",
            "operation": "insert",
            "contentType": "CSV"
        });

        let response_body = serde_json::json!({
            "id": "750xx000000002DEF",
            "state": "Open",
            "object": "Contact",
            "operation": "insert"
        });

        Mock::given(method("POST"))
            .and(path(format!("/services/data/{}/jobs/ingest", API_VERSION)))
            .and(body_json(&expected_request))
            .respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
            .expect(1)
            .mount(&mock_server)
            .await;

        let req = CreateIngestJobRequest {
            object: "Contact".to_string(),
            operation: BulkOperation::Insert,
            external_id_field_name: None,
            line_ending: None,
        };

        let result = client.create_ingest_job(req).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_upsert_job_includes_external_id() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        let expected_request = serde_json::json!({
            "object": "Account",
            "operation": "upsert",
            "externalIdFieldName": "External_Id__c",
            "contentType": "CSV"
        });

        let response_body = serde_json::json!({
            "id": "750xx000000003GHI",
            "state": "Open",
            "object": "Account",
            "operation": "upsert"
        });

        Mock::given(method("POST"))
            .and(path(format!("/services/data/{}/jobs/ingest", API_VERSION)))
            .and(body_json(&expected_request))
            .respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
            .expect(1)
            .mount(&mock_server)
            .await;

        let req = CreateIngestJobRequest {
            object: "Account".to_string(),
            operation: BulkOperation::Upsert,
            external_id_field_name: Some("External_Id__c".to_string()),
            line_ending: None,
        };

        let result = client.create_ingest_job(req).await;
        assert!(result.is_ok());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Upload Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_upload_job_data_success() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());
        let temp_dir = TempDir::new().unwrap();
        let csv_path = temp_dir.path().join("data.csv");

        // Create test CSV file
        let mut file = std::fs::File::create(&csv_path).unwrap();
        writeln!(file, "Id,Name").unwrap();
        writeln!(file, "001xx1,Account1").unwrap();

        Mock::given(method("PUT"))
            .and(path(format!(
                "/services/data/{}/jobs/ingest/750xx000000001ABC/batches",
                API_VERSION
            )))
            .and(header("Content-Type", "text/csv"))
            .and(header("Authorization", "Bearer test_token"))
            .respond_with(ResponseTemplate::new(201))
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = client
            .upload_job_data("750xx000000001ABC", &csv_path)
            .await;

        assert!(result.is_ok());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Close Job Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_close_job_success() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        let expected_request = serde_json::json!({ "state": "UploadComplete" });

        Mock::given(method("PATCH"))
            .and(path(format!(
                "/services/data/{}/jobs/ingest/750xx000000001ABC",
                API_VERSION
            )))
            .and(body_json(&expected_request))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "750xx000000001ABC",
                "state": "UploadComplete",
                "object": "Account",
                "operation": "insert"
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = client.close_job("750xx000000001ABC").await;
        assert!(result.is_ok());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Abort Job Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_abort_job_success() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        let expected_request = serde_json::json!({ "state": "Aborted" });

        Mock::given(method("PATCH"))
            .and(path(format!(
                "/services/data/{}/jobs/ingest/750xx000000001ABC",
                API_VERSION
            )))
            .and(body_json(&expected_request))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "750xx000000001ABC",
                "state": "Aborted",
                "object": "Account",
                "operation": "insert"
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = client.abort_job("750xx000000001ABC").await;
        assert!(result.is_ok());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Status Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_get_job_status_in_progress() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        let response_body = serde_json::json!({
            "id": "750xx000000001ABC",
            "state": "InProgress",
            "object": "Account",
            "operation": "insert",
            "numberRecordsProcessed": 500,
            "numberRecordsFailed": 0
        });

        Mock::given(method("GET"))
            .and(path(format!(
                "/services/data/{}/jobs/ingest/750xx000000001ABC",
                API_VERSION
            )))
            .respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = client.get_job_status("750xx000000001ABC").await;

        assert!(result.is_ok());
        let info = result.unwrap();
        assert_eq!(info.state, BulkJobState::InProgress);
        assert_eq!(info.processed_records, Some(500));
        assert_eq!(info.failed_records, Some(0));
    }

    #[tokio::test]
    async fn test_get_job_status_job_complete() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        let response_body = serde_json::json!({
            "id": "750xx000000001ABC",
            "state": "JobComplete",
            "object": "Account",
            "operation": "insert",
            "numberRecordsProcessed": 1000,
            "numberRecordsFailed": 5
        });

        Mock::given(method("GET"))
            .and(path(format!(
                "/services/data/{}/jobs/ingest/750xx000000001ABC",
                API_VERSION
            )))
            .respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = client.get_job_status("750xx000000001ABC").await;

        assert!(result.is_ok());
        let info = result.unwrap();
        assert_eq!(info.state, BulkJobState::JobComplete);
        assert_eq!(info.processed_records, Some(1000));
        assert_eq!(info.failed_records, Some(5));
    }

    #[tokio::test]
    async fn test_get_job_status_failed_returns_error() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        let response_body = serde_json::json!({
            "id": "750xx000000001ABC",
            "state": "Failed",
            "object": "Account",
            "operation": "insert",
            "errorMessage": "Invalid CSV format"
        });

        Mock::given(method("GET"))
            .and(path(format!(
                "/services/data/{}/jobs/ingest/750xx000000001ABC",
                API_VERSION
            )))
            .respond_with(ResponseTemplate::new(200).set_body_json(&response_body))
            .mount(&mock_server)
            .await;

        let result = client.get_job_status("750xx000000001ABC").await;

        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::JobFailed { job_id, message } => {
                assert_eq!(job_id, "750xx000000001ABC");
                assert!(message.contains("Invalid CSV"));
            }
            e => panic!("Expected JobFailed, got: {:?}", e),
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Download Results Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_get_success_results_downloads_file() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("success.csv");

        let csv_content = "sf__Id,sf__Created,Id,Name\n001xx1,true,001xx1,Account1";

        Mock::given(method("GET"))
            .and(path(format!(
                "/services/data/{}/jobs/ingest/750xx000000001ABC/successfulResults",
                API_VERSION
            )))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Type", "text/csv")
                    .set_body_string(csv_content),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = client
            .get_success_results("750xx000000001ABC", &output_path)
            .await;

        assert!(result.is_ok());
        let content = tokio::fs::read_to_string(&output_path).await.unwrap();
        assert_eq!(content, csv_content);
    }

    #[tokio::test]
    async fn test_get_failure_results_downloads_file() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("failures.csv");

        let csv_content = "sf__Id,sf__Error,Id,Name\n,\"REQUIRED_FIELD_MISSING\",001xx1,";

        Mock::given(method("GET"))
            .and(path(format!(
                "/services/data/{}/jobs/ingest/750xx000000001ABC/failedResults",
                API_VERSION
            )))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Type", "text/csv")
                    .set_body_string(csv_content),
            )
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = client
            .get_failure_results("750xx000000001ABC", &output_path)
            .await;

        assert!(result.is_ok());
        let content = tokio::fs::read_to_string(&output_path).await.unwrap();
        assert_eq!(content, csv_content);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Error Handling Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_rate_limited_error() {
        let mock_server = MockServer::start().await;
        let client = create_test_client(&mock_server.uri());

        Mock::given(method("POST"))
            .and(path(format!("/services/data/{}/jobs/ingest", API_VERSION)))
            .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "60"))
            .mount(&mock_server)
            .await;

        let req = CreateIngestJobRequest {
            object: "Account".to_string(),
            operation: BulkOperation::Insert,
            external_id_field_name: None,
            line_ending: None,
        };

        let result = client.create_ingest_job(req).await;

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
                "/services/data/{}/jobs/ingest/invalid_id",
                API_VERSION
            )))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;

        let result = client.get_job_status("invalid_id").await;

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
            "errorCode": "INVALID_FIELD",
            "message": "No such column 'InvalidField'"
        }]);

        Mock::given(method("POST"))
            .and(path(format!("/services/data/{}/jobs/ingest", API_VERSION)))
            .respond_with(ResponseTemplate::new(400).set_body_json(&error_response))
            .mount(&mock_server)
            .await;

        let req = CreateIngestJobRequest {
            object: "Account".to_string(),
            operation: BulkOperation::Insert,
            external_id_field_name: None,
            line_ending: None,
        };

        let result = client.create_ingest_job(req).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::SalesforceError(msg) => {
                assert!(msg.contains("INVALID_FIELD"));
                assert!(msg.contains("No such column"));
            }
            e => panic!("Expected SalesforceError, got: {:?}", e),
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Type Serialization Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn test_bulk_operation_serializes_lowercase() {
        assert_eq!(
            serde_json::to_string(&BulkOperation::Insert).unwrap(),
            r#""insert""#
        );
        assert_eq!(
            serde_json::to_string(&BulkOperation::Update).unwrap(),
            r#""update""#
        );
        assert_eq!(
            serde_json::to_string(&BulkOperation::Upsert).unwrap(),
            r#""upsert""#
        );
        assert_eq!(
            serde_json::to_string(&BulkOperation::Delete).unwrap(),
            r#""delete""#
        );
    }

    #[test]
    fn test_bulk_operation_deserializes_from_lowercase() {
        assert_eq!(
            serde_json::from_str::<BulkOperation>(r#""insert""#).unwrap(),
            BulkOperation::Insert
        );
        assert_eq!(
            serde_json::from_str::<BulkOperation>(r#""update""#).unwrap(),
            BulkOperation::Update
        );
        assert_eq!(
            serde_json::from_str::<BulkOperation>(r#""upsert""#).unwrap(),
            BulkOperation::Upsert
        );
        assert_eq!(
            serde_json::from_str::<BulkOperation>(r#""delete""#).unwrap(),
            BulkOperation::Delete
        );
    }

    #[test]
    fn test_job_info_deserialization() {
        let json = r#"{
            "id": "750xx000000001ABC",
            "state": "InProgress",
            "object": "Account",
            "operation": "insert",
            "numberRecordsProcessed": 1000,
            "numberRecordsFailed": 5,
            "errorMessage": null
        }"#;

        let info: BulkIngestJobInfo = serde_json::from_str(json).unwrap();

        assert_eq!(info.id, "750xx000000001ABC");
        assert_eq!(info.state, BulkJobState::InProgress);
        assert_eq!(info.object, "Account");
        assert_eq!(info.operation, BulkOperation::Insert);
        assert_eq!(info.processed_records, Some(1000));
        assert_eq!(info.failed_records, Some(5));
        assert!(info.error_message.is_none());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helper Function Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn test_redact_id_long() {
        assert_eq!(redact_id("750xx000000001ABC"), "750xx000...");
    }

    #[test]
    fn test_redact_id_short() {
        assert_eq!(redact_id("short"), "short");
    }
}
