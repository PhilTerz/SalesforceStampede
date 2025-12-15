//! Bulk upload orchestration commands.
//!
//! Provides Tauri commands for bulk CSV uploads to Salesforce using Bulk API v2.
//! The orchestrator validates CSV, chunks it, creates a persistent job group,
//! and runs each chunk as a Salesforce Bulk API v2 ingest job with concurrency limits.

use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use reqwest::Client;
use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Emitter, Manager};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::info;
use url::Url;
use uuid::Uuid;

use crate::error::AppError;
use crate::salesforce::bulk_ingest_v2::{
    BulkIngestJobInfo, BulkIngestV2Client, BulkOperation, CreateIngestJobRequest,
};
use crate::salesforce::bulk_scheduler::{BulkJobPermit, BulkJobScheduler};
use crate::salesforce::BulkJobState;
use crate::state::AppState;
use crate::storage::jobs::{
    add_job_to_group, get_group, save_group, update_group_state, update_job_state, BulkJobGroupRow,
    BulkJobGroupWithJobs, BulkJobRow, GroupState,
};
use crate::storage::{credentials, Database};
use crate::streaming::{split_file, BatchSize, ChunkConfig};
use crate::validation::validate;

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

/// Maximum concurrent bulk jobs to run.
const MAX_CONCURRENT_JOBS: usize = 3;

/// Maximum polling duration before timeout (10 minutes).
const MAX_POLLING_DURATION: Duration = Duration::from_secs(10 * 60);

/// Event name for bulk progress updates.
const BULK_PROGRESS_EVENT: &str = "bulk-progress";

// ─────────────────────────────────────────────────────────────────────────────
// Request/Response Types
// ─────────────────────────────────────────────────────────────────────────────

/// Request to start a bulk upload operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkUploadRequest {
    /// The Salesforce object name (e.g., "Account", "Contact").
    pub object: String,
    /// The bulk operation type.
    pub operation: BulkOperation,
    /// Path to the CSV file to upload.
    pub csv_path: String,
    /// Batch size for chunking.
    pub batch_size: BatchSizeDto,
    /// External ID field name (required for upsert operations).
    pub external_id_field_name: Option<String>,
}

/// Batch size DTO for serialization.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchSizeDto {
    ExtraSmall,
    Small,
    Medium,
    Large,
    Custom(u32),
}

impl From<BatchSizeDto> for BatchSize {
    fn from(dto: BatchSizeDto) -> Self {
        match dto {
            BatchSizeDto::ExtraSmall => BatchSize::ExtraSmall,
            BatchSizeDto::Small => BatchSize::Small,
            BatchSizeDto::Medium => BatchSize::Medium,
            BatchSizeDto::Large => BatchSize::Large,
            BatchSizeDto::Custom(n) => BatchSize::Custom(n),
        }
    }
}

/// Response when bulk upload starts successfully.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkUploadStarted {
    /// Unique identifier for this upload group.
    pub group_id: String,
    /// Total number of chunks/parts.
    pub total_parts: u64,
}

/// Progress event emitted during bulk upload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkProgressEvent {
    /// The group ID this event belongs to.
    pub group_id: String,
    /// Current part being processed (1-based).
    pub current_part: u64,
    /// Total number of parts.
    pub total_parts: u64,
    /// Number of active jobs.
    pub active_jobs: u64,
    /// Maximum concurrent jobs allowed.
    pub max_jobs: u64,
    /// Current phase of the operation.
    pub phase: String,
    /// Optional message with additional details.
    pub message: Option<String>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Cancellation Token Storage
// ─────────────────────────────────────────────────────────────────────────────

/// Storage for cancellation tokens, keyed by group_id.
pub struct CancellationTokens {
    tokens: Mutex<HashMap<String, CancellationToken>>,
}

impl CancellationTokens {
    /// Creates a new empty token storage.
    pub fn new() -> Self {
        Self {
            tokens: Mutex::new(HashMap::new()),
        }
    }

    /// Inserts a cancellation token for the given group_id.
    /// Returns the token for use.
    pub async fn insert(&self, group_id: String, token: CancellationToken) {
        let mut guard = self.tokens.lock().await;
        guard.insert(group_id, token);
        // Lock released here
    }

    /// Gets and clones a cancellation token for the given group_id.
    pub async fn get(&self, group_id: &str) -> Option<CancellationToken> {
        let guard = self.tokens.lock().await;
        guard.get(group_id).cloned()
        // Lock released here
    }

    /// Removes the cancellation token for the given group_id.
    pub async fn remove(&self, group_id: &str) {
        let mut guard = self.tokens.lock().await;
        guard.remove(group_id);
        // Lock released here
    }
}

impl Default for CancellationTokens {
    fn default() -> Self {
        Self::new()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Traits for Testing
// ─────────────────────────────────────────────────────────────────────────────

/// Trait for ingest client operations, allowing test fakes.
pub trait IngestClientOps: Send + Sync + Clone {
    /// Creates a new ingest job.
    fn create_ingest_job(
        &self,
        req: CreateIngestJobRequest,
    ) -> Pin<Box<dyn Future<Output = Result<String, AppError>> + Send + '_>>;

    /// Uploads CSV data to the job.
    fn upload_job_data<'a>(
        &'a self,
        job_id: &'a str,
        csv_path: &'a Path,
    ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + 'a>>;

    /// Closes the job to start processing.
    fn close_job<'a>(
        &'a self,
        job_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + 'a>>;

    /// Gets the current job status.
    fn get_job_status<'a>(
        &'a self,
        job_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<BulkIngestJobInfo, AppError>> + Send + 'a>>;

    /// Aborts a job (best-effort).
    fn abort_job<'a>(
        &'a self,
        job_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + 'a>>;
}

/// Implementation of IngestClientOps for the real BulkIngestV2Client.
impl IngestClientOps for BulkIngestV2Client {
    fn create_ingest_job(
        &self,
        req: CreateIngestJobRequest,
    ) -> Pin<Box<dyn Future<Output = Result<String, AppError>> + Send + '_>> {
        Box::pin(BulkIngestV2Client::create_ingest_job(self, req))
    }

    fn upload_job_data<'a>(
        &'a self,
        job_id: &'a str,
        csv_path: &'a Path,
    ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + 'a>> {
        Box::pin(BulkIngestV2Client::upload_job_data(self, job_id, csv_path))
    }

    fn close_job<'a>(
        &'a self,
        job_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + 'a>> {
        Box::pin(BulkIngestV2Client::close_job(self, job_id))
    }

    fn get_job_status<'a>(
        &'a self,
        job_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<BulkIngestJobInfo, AppError>> + Send + 'a>> {
        // Note: get_job_status returns AppError::JobFailed for failed jobs,
        // but for polling we need the actual status. We'll handle this in the orchestrator.
        Box::pin(BulkIngestV2Client::get_job_status(self, job_id))
    }

    fn abort_job<'a>(
        &'a self,
        job_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + 'a>> {
        Box::pin(BulkIngestV2Client::abort_job(self, job_id))
    }
}

/// Trait for job persistence operations, allowing test fakes.
pub trait JobStoreOps: Send + Sync {
    fn save_group(
        &self,
        group: &BulkJobGroupRow,
    ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + '_>>;

    fn add_job(
        &self,
        job: &BulkJobRow,
    ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + '_>>;

    fn update_job_state(
        &self,
        job_id: &str,
        state: BulkJobState,
        processed_records: Option<i64>,
        failed_records: Option<i64>,
        error_message: Option<&str>,
    ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + '_>>;

    fn update_group_state(
        &self,
        group_id: &str,
        state: GroupState,
    ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + '_>>;

    fn get_group(
        &self,
        group_id: &str,
    ) -> Pin<Box<dyn Future<Output = Result<BulkJobGroupWithJobs, AppError>> + Send + '_>>;
}

/// Real implementation of JobStoreOps using the database.
pub struct DatabaseJobStore {
    db: Arc<Database>,
}

impl DatabaseJobStore {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}

impl JobStoreOps for DatabaseJobStore {
    fn save_group(
        &self,
        group: &BulkJobGroupRow,
    ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + '_>> {
        let db = self.db.clone();
        let group = group.clone();
        Box::pin(async move { save_group(&db, &group).await })
    }

    fn add_job(
        &self,
        job: &BulkJobRow,
    ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + '_>> {
        let db = self.db.clone();
        let job = job.clone();
        Box::pin(async move { add_job_to_group(&db, &job).await })
    }

    fn update_job_state(
        &self,
        job_id: &str,
        state: BulkJobState,
        processed_records: Option<i64>,
        failed_records: Option<i64>,
        error_message: Option<&str>,
    ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + '_>> {
        let db = self.db.clone();
        let job_id = job_id.to_string();
        let error_message = error_message.map(|s| s.to_string());
        Box::pin(async move {
            update_job_state(
                &db,
                &job_id,
                state,
                processed_records,
                failed_records,
                error_message.as_deref(),
            )
            .await
        })
    }

    fn update_group_state(
        &self,
        group_id: &str,
        state: GroupState,
    ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + '_>> {
        let db = self.db.clone();
        let group_id = group_id.to_string();
        Box::pin(async move { update_group_state(&db, &group_id, state).await })
    }

    fn get_group(
        &self,
        group_id: &str,
    ) -> Pin<Box<dyn Future<Output = Result<BulkJobGroupWithJobs, AppError>> + Send + '_>> {
        let db = self.db.clone();
        let group_id = group_id.to_string();
        Box::pin(async move { get_group(&db, &group_id).await })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Bulk Upload Orchestrator
// ─────────────────────────────────────────────────────────────────────────────

/// Information about a chunk to be processed.
#[derive(Debug, Clone)]
struct ChunkInfo {
    part_number: u64,
    path: PathBuf,
}

/// Result of processing a single chunk.
#[derive(Debug)]
struct ChunkResult {
    part_number: u64,
    job_id: String,
    result: Result<(), AppError>,
}

/// Orchestrates the bulk upload process.
struct BulkUploadOrchestrator<C: IngestClientOps, S: JobStoreOps> {
    app: Option<AppHandle>,
    store: Arc<S>,
    org_id: String,
    client: C,
    scheduler: BulkJobScheduler,
    cancel: CancellationToken,
    group_id: String,
    max_jobs: u64,
    // Request details
    object: String,
    operation: BulkOperation,
    external_id_field_name: Option<String>,
    // Progress tracking
    last_emit_time: Mutex<Instant>,
}

impl<C: IngestClientOps + 'static, S: JobStoreOps + 'static> BulkUploadOrchestrator<C, S> {
    fn new(
        app: Option<AppHandle>,
        store: Arc<S>,
        org_id: String,
        client: C,
        scheduler: BulkJobScheduler,
        cancel: CancellationToken,
        group_id: String,
        max_jobs: u64,
        object: String,
        operation: BulkOperation,
        external_id_field_name: Option<String>,
    ) -> Self {
        Self {
            app,
            store,
            org_id,
            client,
            scheduler,
            cancel,
            group_id,
            max_jobs,
            object,
            operation,
            external_id_field_name,
            last_emit_time: Mutex::new(Instant::now()),
        }
    }

    /// Emits a progress event to the frontend.
    fn emit_progress(&self, event: &BulkProgressEvent) {
        if let Some(ref app) = self.app {
            let _ = app.emit(BULK_PROGRESS_EVENT, event);
        }
    }

    /// Emits a progress event with throttling (max once per second).
    async fn emit_progress_throttled(&self, event: &BulkProgressEvent) {
        let mut last_time = self.last_emit_time.lock().await;
        let now = Instant::now();
        if now.duration_since(*last_time) >= Duration::from_secs(1) {
            *last_time = now;
            drop(last_time); // Release lock before emit
            self.emit_progress(event);
        }
    }

    /// Runs the bulk upload orchestration.
    async fn run(
        self: Arc<Self>,
        csv_path: PathBuf,
        batch_size: BatchSize,
        temp_base_dir: PathBuf,
    ) -> Result<BulkUploadStarted, AppError> {
        // Step 1: Validate CSV
        self.emit_progress(&BulkProgressEvent {
            group_id: self.group_id.clone(),
            current_part: 0,
            total_parts: 0,
            active_jobs: 0,
            max_jobs: self.max_jobs,
            phase: "validating".to_string(),
            message: Some("Validating CSV file...".to_string()),
        });

        let validation = validate(&csv_path).await?;
        if !validation.ok {
            let error_messages: Vec<String> = validation
                .errors
                .iter()
                .map(|e| format!("{:?}", e))
                .collect();
            return Err(AppError::CsvInvalid(error_messages.join("; ")));
        }

        // Check for cancellation
        if self.cancel.is_cancelled() {
            return Err(AppError::Cancelled);
        }

        // Step 2: Chunk CSV
        self.emit_progress(&BulkProgressEvent {
            group_id: self.group_id.clone(),
            current_part: 0,
            total_parts: 0,
            active_jobs: 0,
            max_jobs: self.max_jobs,
            phase: "chunking".to_string(),
            message: Some("Splitting CSV into chunks...".to_string()),
        });

        let chunk_dir = temp_base_dir.join(&self.group_id).join("chunks");
        tokio::fs::create_dir_all(&chunk_dir)
            .await
            .map_err(|e| AppError::Internal(format!("Failed to create chunk directory: {}", e)))?;

        let config = ChunkConfig::with_batch_size(batch_size).max_bytes(100 * 1024 * 1024);
        let chunk_result = split_file(&csv_path, &chunk_dir, config).await?;

        let total_parts = chunk_result.chunk_paths.len() as u64;

        // Handle empty CSV (no data rows)
        if total_parts == 0 {
            return Err(AppError::CsvInvalid(
                "CSV file has no data rows".to_string(),
            ));
        }

        // Check for cancellation
        if self.cancel.is_cancelled() {
            // Clean up temp dir
            let _ = tokio::fs::remove_dir_all(temp_base_dir.join(&self.group_id)).await;
            return Err(AppError::Cancelled);
        }

        // Step 3: Create job group in database
        let timestamp = current_timestamp();
        let group_row = BulkJobGroupRow {
            group_id: self.group_id.clone(),
            org_id: self.org_id.clone(),
            object: self.object.clone(),
            operation: format!("{:?}", self.operation).to_lowercase(),
            state: GroupState::InProgress,
            batch_size: batch_size.as_u64() as i64,
            total_parts: total_parts as i64,
            created_at: timestamp,
            updated_at: timestamp,
        };

        self.store.save_group(&group_row).await?;

        info!(
            "[BULK-ORCHESTRATOR] Created group {} with {} parts for {} {}",
            &self.group_id[..8.min(self.group_id.len())],
            total_parts,
            self.operation.as_str(),
            self.object
        );

        // Build chunk info list
        let chunks: Vec<ChunkInfo> = chunk_result
            .chunk_paths
            .into_iter()
            .enumerate()
            .map(|(i, path)| ChunkInfo {
                part_number: (i + 1) as u64,
                path,
            })
            .collect();

        // Step 4: Upload chunks with concurrency limit
        self.emit_progress(&BulkProgressEvent {
            group_id: self.group_id.clone(),
            current_part: 0,
            total_parts,
            active_jobs: 0,
            max_jobs: self.max_jobs,
            phase: "uploading".to_string(),
            message: Some(format!("Starting upload of {} chunks...", total_parts)),
        });

        let job_ids = self.clone().upload_chunks(chunks, total_parts).await?;

        // Step 5: Poll for completion
        self.clone().poll_jobs(job_ids, total_parts).await?;

        // Step 6: Compute final state
        let final_phase = self.clone().finalize_group(total_parts).await?;

        // Step 7: Cleanup temp directory (best-effort)
        let temp_group_dir = temp_base_dir.join(&self.group_id);
        if let Err(e) = tokio::fs::remove_dir_all(&temp_group_dir).await {
            // Log but don't fail
            info!(
                "[BULK-ORCHESTRATOR] Failed to cleanup temp dir: {} (continuing)",
                e
            );
        }

        // Return appropriate result based on final state
        match final_phase.as_str() {
            "completed" => Ok(BulkUploadStarted {
                group_id: self.group_id.clone(),
                total_parts,
            }),
            "cancelled" => Err(AppError::Cancelled),
            "failed" => {
                // Get error details from jobs
                let group = self.store.get_group(&self.group_id).await?;
                let errors: Vec<String> = group
                    .jobs
                    .iter()
                    .filter(|j| j.state == BulkJobState::Failed)
                    .filter_map(|j| j.error_message.clone())
                    .collect();
                let error_msg = if errors.is_empty() {
                    "One or more jobs failed".to_string()
                } else {
                    errors.join("; ")
                };
                Err(AppError::JobFailed {
                    job_id: self.group_id.clone(),
                    message: error_msg,
                })
            }
            _ => Ok(BulkUploadStarted {
                group_id: self.group_id.clone(),
                total_parts,
            }),
        }
    }

    /// Uploads chunks with concurrency control using JoinSet.
    async fn upload_chunks(
        self: Arc<Self>,
        chunks: Vec<ChunkInfo>,
        total_parts: u64,
    ) -> Result<Vec<String>, AppError> {
        let mut job_ids: Vec<String> = Vec::with_capacity(chunks.len());
        let mut join_set: JoinSet<ChunkResult> = JoinSet::new();
        let mut chunks_iter = chunks.into_iter().peekable();
        let mut completed_parts = 0u64;

        loop {
            // Check for cancellation before spawning new tasks
            if self.cancel.is_cancelled() {
                break;
            }

            // Spawn tasks up to max concurrent limit
            while join_set.len() < self.max_jobs as usize {
                if self.cancel.is_cancelled() {
                    break;
                }

                if let Some(chunk) = chunks_iter.next() {
                    let orchestrator = self.clone();
                    let cancel = self.cancel.clone();
                    let scheduler = self.scheduler.clone();

                    join_set.spawn(async move {
                        orchestrator.process_chunk(chunk, scheduler, cancel).await
                    });
                } else {
                    break;
                }
            }

            // If no tasks running and no more chunks, we're done
            if join_set.is_empty() && chunks_iter.peek().is_none() {
                break;
            }

            // Wait for at least one task to complete
            if let Some(result) = join_set.join_next().await {
                match result {
                    Ok(chunk_result) => {
                        completed_parts += 1;

                        // Track job_id only if it exists (creation succeeded)
                        if !chunk_result.job_id.is_empty() {
                            job_ids.push(chunk_result.job_id.clone());
                        }

                        // Emit progress
                        self.emit_progress(&BulkProgressEvent {
                            group_id: self.group_id.clone(),
                            current_part: completed_parts,
                            total_parts,
                            active_jobs: join_set.len() as u64,
                            max_jobs: self.max_jobs,
                            phase: "uploading".to_string(),
                            message: Some(format!(
                                "Completed part {}/{}",
                                completed_parts, total_parts
                            )),
                        });

                        // Log errors but continue with other chunks
                        if let Err(e) = chunk_result.result {
                            info!(
                                "[BULK-ORCHESTRATOR] Chunk {} failed: {:?}",
                                chunk_result.part_number, e
                            );
                        }
                    }
                    Err(e) => {
                        info!("[BULK-ORCHESTRATOR] Task join error: {:?}", e);
                    }
                }
            }
        }

        // Wait for remaining in-flight tasks to complete
        while let Some(result) = join_set.join_next().await {
            if let Ok(chunk_result) = result {
                completed_parts += 1;
                if !chunk_result.job_id.is_empty() {
                    job_ids.push(chunk_result.job_id.clone());
                }

                self.emit_progress(&BulkProgressEvent {
                    group_id: self.group_id.clone(),
                    current_part: completed_parts,
                    total_parts,
                    active_jobs: join_set.len() as u64,
                    max_jobs: self.max_jobs,
                    phase: "uploading".to_string(),
                    message: Some(format!(
                        "Completed part {}/{}",
                        completed_parts, total_parts
                    )),
                });
            }
        }

        Ok(job_ids)
    }

    /// Processes a single chunk: create job, upload data, close job.
    async fn process_chunk(
        &self,
        chunk: ChunkInfo,
        scheduler: BulkJobScheduler,
        cancel: CancellationToken,
    ) -> ChunkResult {
        // Acquire scheduler permit
        let _permit: BulkJobPermit = scheduler.acquire().await;

        // Check cancellation after acquiring permit
        if cancel.is_cancelled() {
            return ChunkResult {
                part_number: chunk.part_number,
                job_id: String::new(),
                result: Err(AppError::Cancelled),
            };
        }

        let timestamp = current_timestamp();

        // Create the Salesforce job
        let create_req = CreateIngestJobRequest {
            object: self.object.clone(),
            operation: self.operation,
            external_id_field_name: self.external_id_field_name.clone(),
            line_ending: None,
        };

        let job_id = match self.client.create_ingest_job(create_req).await {
            Ok(id) => id,
            Err(e) => {
                return ChunkResult {
                    part_number: chunk.part_number,
                    job_id: String::new(),
                    result: Err(e),
                };
            }
        };

        // Insert job row into database
        let job_row = BulkJobRow {
            job_id: job_id.clone(),
            group_id: self.group_id.clone(),
            part_number: chunk.part_number as i64,
            state: BulkJobState::Open,
            processed_records: None,
            failed_records: None,
            error_message: None,
            created_at: timestamp,
            updated_at: timestamp,
        };

        if let Err(e) = self.store.add_job(&job_row).await {
            info!("[BULK-ORCHESTRATOR] Failed to persist job row: {:?}", e);
            // Continue anyway - Salesforce job was created
        }

        // Upload CSV data
        if let Err(e) = self.client.upload_job_data(&job_id, &chunk.path).await {
            // Update job state to Failed
            let _ = self
                .store
                .update_job_state(
                    &job_id,
                    BulkJobState::Failed,
                    None,
                    None,
                    Some(&format!("Upload failed: {:?}", e)),
                )
                .await;

            return ChunkResult {
                part_number: chunk.part_number,
                job_id,
                result: Err(e),
            };
        }

        // Close job to start processing
        if let Err(e) = self.client.close_job(&job_id).await {
            // Update job state to Failed
            let _ = self
                .store
                .update_job_state(
                    &job_id,
                    BulkJobState::Failed,
                    None,
                    None,
                    Some(&format!("Close failed: {:?}", e)),
                )
                .await;

            return ChunkResult {
                part_number: chunk.part_number,
                job_id,
                result: Err(e),
            };
        }

        // Update job state to UploadComplete
        let _ = self
            .store
            .update_job_state(&job_id, BulkJobState::UploadComplete, None, None, None)
            .await;

        ChunkResult {
            part_number: chunk.part_number,
            job_id,
            result: Ok(()),
        }
    }

    /// Polls all jobs until they reach terminal states.
    async fn poll_jobs(
        self: Arc<Self>,
        job_ids: Vec<String>,
        total_parts: u64,
    ) -> Result<(), AppError> {
        if job_ids.is_empty() {
            return Ok(());
        }

        self.emit_progress(&BulkProgressEvent {
            group_id: self.group_id.clone(),
            current_part: 0,
            total_parts,
            active_jobs: 0,
            max_jobs: self.max_jobs,
            phase: "polling".to_string(),
            message: Some("Waiting for Salesforce to process...".to_string()),
        });

        let poll_start = Instant::now();
        let mut backoff_ms = 500u64;
        let max_backoff_ms = 2000u64;

        // Track which jobs are still non-terminal
        let mut pending_jobs: Vec<String> = job_ids;

        loop {
            // Check timeout
            if poll_start.elapsed() > MAX_POLLING_DURATION {
                return Err(AppError::Internal(
                    "Polling timeout - jobs did not complete within 10 minutes".to_string(),
                ));
            }

            // Check cancellation
            if self.cancel.is_cancelled() {
                // Emit aborting phase
                self.emit_progress(&BulkProgressEvent {
                    group_id: self.group_id.clone(),
                    current_part: 0,
                    total_parts,
                    active_jobs: pending_jobs.len() as u64,
                    max_jobs: self.max_jobs,
                    phase: "aborting".to_string(),
                    message: Some("Cancellation requested, aborting jobs...".to_string()),
                });

                // Best-effort abort pending jobs
                for job_id in &pending_jobs {
                    let _ = self.client.abort_job(job_id).await;
                    let _ = self
                        .store
                        .update_job_state(job_id, BulkJobState::Aborted, None, None, None)
                        .await;
                }

                return Ok(()); // Let finalize_group handle the state
            }

            // Check status of pending jobs
            let mut still_pending = Vec::new();
            let mut any_state_changed = false;

            for job_id in pending_jobs {
                match self.client.get_job_status(&job_id).await {
                    Ok(info) => {
                        if info.state.is_terminal() {
                            // Update database with final state
                            let _ = self
                                .store
                                .update_job_state(
                                    &job_id,
                                    info.state,
                                    info.processed_records.map(|v| v as i64),
                                    info.failed_records.map(|v| v as i64),
                                    info.error_message.as_deref(),
                                )
                                .await;
                            any_state_changed = true;
                        } else {
                            still_pending.push(job_id);
                        }
                    }
                    Err(AppError::JobFailed { job_id: _, message }) => {
                        // Job failed - update state
                        let _ = self
                            .store
                            .update_job_state(
                                &job_id,
                                BulkJobState::Failed,
                                None,
                                None,
                                Some(&message),
                            )
                            .await;
                        any_state_changed = true;
                    }
                    Err(_) => {
                        // Network error or other issue - keep polling
                        still_pending.push(job_id);
                    }
                }
            }

            pending_jobs = still_pending;

            // Emit progress if state changed or throttle time passed
            if any_state_changed {
                self.emit_progress(&BulkProgressEvent {
                    group_id: self.group_id.clone(),
                    current_part: (total_parts as usize - pending_jobs.len()) as u64,
                    total_parts,
                    active_jobs: pending_jobs.len() as u64,
                    max_jobs: self.max_jobs,
                    phase: "polling".to_string(),
                    message: Some(format!("{} jobs remaining", pending_jobs.len())),
                });
            } else {
                // Throttled progress update
                self.emit_progress_throttled(&BulkProgressEvent {
                    group_id: self.group_id.clone(),
                    current_part: (total_parts as usize - pending_jobs.len()) as u64,
                    total_parts,
                    active_jobs: pending_jobs.len() as u64,
                    max_jobs: self.max_jobs,
                    phase: "polling".to_string(),
                    message: Some(format!("{} jobs remaining", pending_jobs.len())),
                })
                .await;
            }

            // All done?
            if pending_jobs.is_empty() {
                break;
            }

            // Wait with backoff
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            backoff_ms = (backoff_ms * 2).min(max_backoff_ms);
        }

        Ok(())
    }

    /// Computes and persists the final group state.
    async fn finalize_group(self: Arc<Self>, total_parts: u64) -> Result<String, AppError> {
        // Get all jobs from database
        let group = self.store.get_group(&self.group_id).await?;

        // Compute final state
        let has_failed = group.jobs.iter().any(|j| j.state == BulkJobState::Failed);
        let has_aborted = group.jobs.iter().any(|j| j.state == BulkJobState::Aborted);

        let (final_group_state, phase) = if has_failed {
            (GroupState::Failed, "failed")
        } else if has_aborted || self.cancel.is_cancelled() {
            (GroupState::Aborted, "cancelled")
        } else {
            (GroupState::Completed, "completed")
        };

        // Update group state
        self.store
            .update_group_state(&self.group_id, final_group_state)
            .await?;

        // Emit final progress
        self.emit_progress(&BulkProgressEvent {
            group_id: self.group_id.clone(),
            current_part: total_parts,
            total_parts,
            active_jobs: 0,
            max_jobs: self.max_jobs,
            phase: phase.to_string(),
            message: Some(format!(
                "Bulk upload {}",
                match phase {
                    "completed" => "completed successfully",
                    "cancelled" => "was cancelled",
                    "failed" => "failed",
                    _ => "finished",
                }
            )),
        });

        info!(
            "[BULK-ORCHESTRATOR] Group {} finalized with state: {:?}",
            &self.group_id[..8.min(self.group_id.len())],
            final_group_state
        );

        Ok(phase.to_string())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper Functions
// ─────────────────────────────────────────────────────────────────────────────

/// Returns current unix timestamp in seconds.
fn current_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64
}

/// Gets the app data temp directory for bulk uploads.
fn get_temp_dir(app: &AppHandle) -> Result<PathBuf, AppError> {
    let app_data = app
        .path()
        .app_data_dir()
        .map_err(|e| AppError::Internal(format!("Failed to get app data dir: {}", e)))?;
    Ok(app_data.join("stampede_tmp"))
}

// ─────────────────────────────────────────────────────────────────────────────
// Tauri Commands
// ─────────────────────────────────────────────────────────────────────────────

/// Starts a bulk upload operation.
///
/// This command:
/// 1. Validates the CSV file
/// 2. Chunks it into smaller files
/// 3. Creates a persistent job group
/// 4. Uploads each chunk to Salesforce (max 3 concurrent)
/// 5. Polls for completion
/// 6. Returns the group ID and total parts
#[tauri::command]
pub async fn start_bulk_upload(
    app: AppHandle,
    state: tauri::State<'_, AppState>,
    cancel_tokens: tauri::State<'_, CancellationTokens>,
    req: BulkUploadRequest,
) -> Result<BulkUploadStarted, AppError> {
    // Step 0: Check for active org
    let org_id = state
        .get_active_org_id()
        .await
        .ok_or(AppError::NoActiveOrg)?;

    // Get org from database
    let org = state
        .db
        .get_org(&org_id)
        .await?
        .ok_or(AppError::NotFound(format!("Org {} not found", org_id)))?;

    // Get tokens from keychain
    let tokens = credentials::get_tokens(&org_id).await?;

    // Create HTTP client
    let http_client = Arc::new(Client::new());
    let base_url = Url::parse(&org.instance_url)
        .map_err(|e| AppError::Internal(format!("Invalid instance URL: {}", e)))?;

    // Use access token from keychain
    let access_token = tokens.access_token;

    // Create ingest client
    let client = BulkIngestV2Client::new(http_client, base_url, access_token);

    // Create scheduler and cancellation token
    let scheduler = BulkJobScheduler::new(MAX_CONCURRENT_JOBS);
    let cancel = CancellationToken::new();
    let group_id = Uuid::new_v4().to_string();

    // Store cancellation token
    cancel_tokens.insert(group_id.clone(), cancel.clone()).await;

    // Create job store
    let store = Arc::new(DatabaseJobStore::new(state.db.clone()));

    // Get temp directory
    let temp_dir = get_temp_dir(&app)?;

    // Create orchestrator
    let orchestrator = Arc::new(BulkUploadOrchestrator::new(
        Some(app.clone()),
        store,
        org_id,
        client,
        scheduler,
        cancel.clone(),
        group_id.clone(),
        MAX_CONCURRENT_JOBS as u64,
        req.object,
        req.operation,
        req.external_id_field_name,
    ));

    // Run orchestration
    let csv_path = PathBuf::from(&req.csv_path);
    let batch_size: BatchSize = req.batch_size.into();

    let result = orchestrator.run(csv_path, batch_size, temp_dir).await;

    // Remove cancellation token (best-effort)
    cancel_tokens.remove(&group_id).await;

    result
}

/// Cancels an in-progress bulk upload.
///
/// This command signals cancellation and returns immediately.
/// The orchestrator will stop launching new jobs and best-effort abort in-flight jobs.
#[tauri::command]
pub async fn cancel_bulk_upload(
    cancel_tokens: tauri::State<'_, CancellationTokens>,
    group_id: String,
) -> Result<(), AppError> {
    if let Some(token) = cancel_tokens.get(&group_id).await {
        token.cancel();
        info!(
            "[BULK-ORCHESTRATOR] Cancellation requested for group {}",
            &group_id[..8.min(group_id.len())]
        );
        Ok(())
    } else {
        // Token not found - might already be finished or invalid group_id
        Err(AppError::NotFound(format!(
            "No active upload found for group {}",
            group_id
        )))
    }
}

/// Validates a CSV file without holding an open file handle.
///
/// This command reads a sample of the file, validates it, and closes the file.
/// Safe for large files and Windows file locking.
#[tauri::command]
pub async fn validate_csv(path: String) -> Result<crate::validation::CsvValidationResult, AppError> {
    let path = PathBuf::from(path);
    crate::validation::validate(&path).await
}

/// Response from generate_bulk_results command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkGroupResultPaths {
    /// Path to the merged success results CSV.
    pub success_path: String,
    /// Path to the merged failure results CSV.
    pub failure_path: String,
    /// Path to the warnings file (if any warnings occurred during merge).
    pub warnings_path: Option<String>,
}

/// Generates merged result files for a completed bulk job group.
///
/// Downloads success/failure results from Salesforce for each job in the group,
/// merges them into combined files, and returns the file paths.
#[tauri::command]
pub async fn generate_bulk_results(
    app: AppHandle,
    state: tauri::State<'_, AppState>,
    group_id: String,
) -> Result<BulkGroupResultPaths, AppError> {
    use crate::salesforce::bulk_ingest_v2::BulkIngestV2Client;
    use crate::streaming::{download_group_results, JobResultProvider};
    use std::future::Future;
    use std::pin::Pin;

    // Get org info for building the client
    let org_id = state
        .get_active_org_id()
        .await
        .ok_or(AppError::NoActiveOrg)?;

    let org = state
        .db
        .get_org(&org_id)
        .await?
        .ok_or(AppError::NotFound(format!("Org {} not found", org_id)))?;

    let tokens = credentials::get_tokens(&org_id).await?;

    // Create HTTP client and bulk client
    let http_client = Arc::new(Client::new());
    let base_url = Url::parse(&org.instance_url)
        .map_err(|e| AppError::Internal(format!("Invalid instance URL: {}", e)))?;
    let bulk_client = BulkIngestV2Client::new(http_client, base_url, tokens.access_token);

    // Create provider adapter
    struct BulkClientProvider(BulkIngestV2Client);

    impl JobResultProvider for BulkClientProvider {
        fn download_success<'a>(
            &'a self,
            job_id: &'a str,
            out_path: &'a Path,
        ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + 'a>> {
            Box::pin(async move { self.0.get_success_results(job_id, out_path).await })
        }

        fn download_failure<'a>(
            &'a self,
            job_id: &'a str,
            out_path: &'a Path,
        ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + 'a>> {
            Box::pin(async move { self.0.get_failure_results(job_id, out_path).await })
        }
    }

    let provider = BulkClientProvider(bulk_client);

    // Get output directory
    let app_data_dir = app
        .path()
        .app_data_dir()
        .map_err(|e| AppError::Internal(format!("Failed to get app data dir: {}", e)))?;
    let output_dir = app_data_dir
        .join("stampede_exports")
        .join(&org_id)
        .join("bulk_results")
        .join(&group_id);

    // Download and merge results
    let results = download_group_results(&state.db, &provider, &group_id, &output_dir).await?;

    Ok(BulkGroupResultPaths {
        success_path: results.success_path.to_string_lossy().to_string(),
        failure_path: results.failure_path.to_string_lossy().to_string(),
        warnings_path: results.warnings_path.map(|p| p.to_string_lossy().to_string()),
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// BulkOperation Helper
// ─────────────────────────────────────────────────────────────────────────────

impl BulkOperation {
    fn as_str(&self) -> &'static str {
        match self {
            BulkOperation::Insert => "insert",
            BulkOperation::Update => "update",
            BulkOperation::Upsert => "upsert",
            BulkOperation::Delete => "delete",
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    // ─────────────────────────────────────────────────────────────────────────
    // Fake Implementations for Testing
    // ─────────────────────────────────────────────────────────────────────────

    /// Fake ingest client for testing.
    #[derive(Clone)]
    struct FakeIngestClient {
        created_jobs: Arc<Mutex<Vec<String>>>,
        job_counter: Arc<AtomicU64>,
        should_fail_upload: bool,
        cancel_token: CancellationToken,
    }

    impl FakeIngestClient {
        fn new(cancel_token: CancellationToken) -> Self {
            Self {
                created_jobs: Arc::new(Mutex::new(Vec::new())),
                job_counter: Arc::new(AtomicU64::new(0)),
                should_fail_upload: false,
                cancel_token,
            }
        }

        async fn get_created_jobs(&self) -> Vec<String> {
            self.created_jobs.lock().await.clone()
        }
    }

    impl IngestClientOps for FakeIngestClient {
        fn create_ingest_job(
            &self,
            _req: CreateIngestJobRequest,
        ) -> Pin<Box<dyn Future<Output = Result<String, AppError>> + Send + '_>> {
            let created_jobs = self.created_jobs.clone();
            let job_counter = self.job_counter.clone();
            Box::pin(async move {
                let job_id = format!("fake-job-{}", job_counter.fetch_add(1, Ordering::SeqCst));
                created_jobs.lock().await.push(job_id.clone());
                // Small delay to simulate network
                tokio::time::sleep(Duration::from_millis(10)).await;
                Ok(job_id)
            })
        }

        fn upload_job_data(
            &self,
            _job_id: &str,
            _csv_path: &Path,
        ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + '_>> {
            let should_fail = self.should_fail_upload;
            Box::pin(async move {
                // Small delay to simulate upload
                tokio::time::sleep(Duration::from_millis(10)).await;
                if should_fail {
                    Err(AppError::Internal("Upload failed".to_string()))
                } else {
                    Ok(())
                }
            })
        }

        fn close_job(
            &self,
            _job_id: &str,
        ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + '_>> {
            Box::pin(async move { Ok(()) })
        }

        fn get_job_status(
            &self,
            job_id: &str,
        ) -> Pin<Box<dyn Future<Output = Result<BulkIngestJobInfo, AppError>> + Send + '_>>
        {
            let job_id = job_id.to_string();
            let cancel_token = self.cancel_token.clone();
            Box::pin(async move {
                // Return JobComplete or Aborted based on cancellation
                let state = if cancel_token.is_cancelled() {
                    BulkJobState::Aborted
                } else {
                    BulkJobState::JobComplete
                };
                Ok(BulkIngestJobInfo {
                    id: job_id,
                    state,
                    object: "Account".to_string(),
                    operation: BulkOperation::Insert,
                    processed_records: Some(100),
                    failed_records: Some(0),
                    error_message: None,
                })
            })
        }

        fn abort_job(
            &self,
            _job_id: &str,
        ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + '_>> {
            Box::pin(async move { Ok(()) })
        }
    }

    /// Fake job store for testing.
    struct FakeJobStore {
        groups: Mutex<HashMap<String, BulkJobGroupRow>>,
        jobs: Mutex<HashMap<String, BulkJobRow>>,
    }

    impl FakeJobStore {
        fn new() -> Self {
            Self {
                groups: Mutex::new(HashMap::new()),
                jobs: Mutex::new(HashMap::new()),
            }
        }
    }

    impl JobStoreOps for FakeJobStore {
        fn save_group(
            &self,
            group: &BulkJobGroupRow,
        ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + '_>> {
            let group = group.clone();
            Box::pin(async move {
                self.groups
                    .lock()
                    .await
                    .insert(group.group_id.clone(), group);
                Ok(())
            })
        }

        fn add_job(
            &self,
            job: &BulkJobRow,
        ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + '_>> {
            let job = job.clone();
            Box::pin(async move {
                self.jobs.lock().await.insert(job.job_id.clone(), job);
                Ok(())
            })
        }

        fn update_job_state(
            &self,
            job_id: &str,
            state: BulkJobState,
            processed_records: Option<i64>,
            failed_records: Option<i64>,
            error_message: Option<&str>,
        ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + '_>> {
            let job_id = job_id.to_string();
            let error_message = error_message.map(|s| s.to_string());
            Box::pin(async move {
                let mut jobs = self.jobs.lock().await;
                if let Some(job) = jobs.get_mut(&job_id) {
                    job.state = state;
                    job.processed_records = processed_records;
                    job.failed_records = failed_records;
                    job.error_message = error_message;
                }
                Ok(())
            })
        }

        fn update_group_state(
            &self,
            group_id: &str,
            state: GroupState,
        ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + '_>> {
            let group_id = group_id.to_string();
            Box::pin(async move {
                let mut groups = self.groups.lock().await;
                if let Some(group) = groups.get_mut(&group_id) {
                    group.state = state;
                }
                Ok(())
            })
        }

        fn get_group(
            &self,
            group_id: &str,
        ) -> Pin<Box<dyn Future<Output = Result<BulkJobGroupWithJobs, AppError>> + Send + '_>>
        {
            let group_id = group_id.to_string();
            Box::pin(async move {
                let groups = self.groups.lock().await;
                let jobs = self.jobs.lock().await;

                let group = groups
                    .get(&group_id)
                    .cloned()
                    .ok_or(AppError::NotFound("Group not found".to_string()))?;

                let group_jobs: Vec<BulkJobRow> = jobs
                    .values()
                    .filter(|j| j.group_id == group_id)
                    .cloned()
                    .collect();

                Ok(BulkJobGroupWithJobs {
                    group,
                    jobs: group_jobs,
                })
            })
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 1: Cancellation stops scheduling
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_cancellation_stops_scheduling() {
        // Create 5 chunks, scheduler max=1
        // Cancel after first job starts
        // Assert only 1-2 jobs created

        let cancel = CancellationToken::new();
        let client = FakeIngestClient::new(cancel.clone());
        let store = Arc::new(FakeJobStore::new());
        let scheduler = BulkJobScheduler::new(1); // Only 1 concurrent job

        let group_id = "test-cancel-group".to_string();

        let orchestrator = Arc::new(BulkUploadOrchestrator::new(
            None,
            store.clone(),
            "test-org".to_string(),
            client.clone(),
            scheduler,
            cancel.clone(),
            group_id.clone(),
            1,
            "Account".to_string(),
            BulkOperation::Insert,
            None,
        ));

        // Create temp dir with fake chunks
        let temp_dir = tempfile::TempDir::new().unwrap();
        let chunk_dir = temp_dir.path().join(&group_id).join("chunks");
        tokio::fs::create_dir_all(&chunk_dir).await.unwrap();

        // Create 5 chunk files
        let mut chunks = Vec::new();
        for i in 0..5 {
            let chunk_path = chunk_dir.join(format!("chunk_{:04}.csv", i));
            tokio::fs::write(&chunk_path, "Id,Name\n1,Test\n")
                .await
                .unwrap();
            chunks.push(ChunkInfo {
                part_number: (i + 1) as u64,
                path: chunk_path,
            });
        }

        // Spawn upload task
        let orchestrator_clone = orchestrator.clone();
        let upload_handle =
            tokio::spawn(async move { orchestrator_clone.upload_chunks(chunks, 5).await });

        // Wait a bit then cancel
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();

        // Wait for upload to finish
        let _ = upload_handle.await;

        // Check how many jobs were created
        let created_jobs = client.get_created_jobs().await;

        // With max 1 concurrent and cancellation, should have created fewer than total chunks
        // The exact number depends on timing, but it should be significantly less than 5
        assert!(
            created_jobs.len() < 5,
            "Expected fewer than 5 jobs (cancellation should stop scheduling), got {}",
            created_jobs.len()
        );
        assert!(
            !created_jobs.is_empty(),
            "At least one job should have been created before cancellation"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 2: Final state computation
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_final_state_any_failed_becomes_failed() {
        let cancel = CancellationToken::new();
        let store = Arc::new(FakeJobStore::new());

        // Setup: Create group and jobs
        let group_id = "test-failed-group".to_string();
        let timestamp = current_timestamp();

        store
            .save_group(&BulkJobGroupRow {
                group_id: group_id.clone(),
                org_id: "test-org".to_string(),
                object: "Account".to_string(),
                operation: "insert".to_string(),
                state: GroupState::InProgress,
                batch_size: 1000,
                total_parts: 3,
                created_at: timestamp,
                updated_at: timestamp,
            })
            .await
            .unwrap();

        // Add jobs: 2 complete, 1 failed
        for (i, state) in [
            BulkJobState::JobComplete,
            BulkJobState::Failed,
            BulkJobState::JobComplete,
        ]
        .iter()
        .enumerate()
        {
            store
                .add_job(&BulkJobRow {
                    job_id: format!("job-{}", i),
                    group_id: group_id.clone(),
                    part_number: (i + 1) as i64,
                    state: *state,
                    processed_records: Some(100),
                    failed_records: Some(0),
                    error_message: if *state == BulkJobState::Failed {
                        Some("Test failure".to_string())
                    } else {
                        None
                    },
                    created_at: timestamp,
                    updated_at: timestamp,
                })
                .await
                .unwrap();
        }

        let client = FakeIngestClient::new(cancel.clone());
        let scheduler = BulkJobScheduler::new(3);

        let orchestrator = Arc::new(BulkUploadOrchestrator::new(
            None,
            store.clone(),
            "test-org".to_string(),
            client,
            scheduler,
            cancel,
            group_id.clone(),
            3,
            "Account".to_string(),
            BulkOperation::Insert,
            None,
        ));

        // Finalize
        let phase = orchestrator.finalize_group(3).await.unwrap();

        assert_eq!(
            phase, "failed",
            "Group with any Failed job should be Failed"
        );

        // Verify group state in store
        let group = store.get_group(&group_id).await.unwrap();
        assert_eq!(group.group.state, GroupState::Failed);
    }

    #[tokio::test]
    async fn test_final_state_any_aborted_becomes_aborted() {
        let cancel = CancellationToken::new();
        let store = Arc::new(FakeJobStore::new());

        let group_id = "test-aborted-group".to_string();
        let timestamp = current_timestamp();

        store
            .save_group(&BulkJobGroupRow {
                group_id: group_id.clone(),
                org_id: "test-org".to_string(),
                object: "Account".to_string(),
                operation: "insert".to_string(),
                state: GroupState::InProgress,
                batch_size: 1000,
                total_parts: 3,
                created_at: timestamp,
                updated_at: timestamp,
            })
            .await
            .unwrap();

        // Add jobs: 2 complete, 1 aborted (no failed)
        for (i, state) in [
            BulkJobState::JobComplete,
            BulkJobState::Aborted,
            BulkJobState::JobComplete,
        ]
        .iter()
        .enumerate()
        {
            store
                .add_job(&BulkJobRow {
                    job_id: format!("job-{}", i),
                    group_id: group_id.clone(),
                    part_number: (i + 1) as i64,
                    state: *state,
                    processed_records: Some(100),
                    failed_records: Some(0),
                    error_message: None,
                    created_at: timestamp,
                    updated_at: timestamp,
                })
                .await
                .unwrap();
        }

        let client = FakeIngestClient::new(cancel.clone());
        let scheduler = BulkJobScheduler::new(3);

        let orchestrator = Arc::new(BulkUploadOrchestrator::new(
            None,
            store.clone(),
            "test-org".to_string(),
            client,
            scheduler,
            cancel,
            group_id.clone(),
            3,
            "Account".to_string(),
            BulkOperation::Insert,
            None,
        ));

        let phase = orchestrator.finalize_group(3).await.unwrap();

        assert_eq!(
            phase, "cancelled",
            "Group with any Aborted job (and no Failed) should be cancelled"
        );

        let group = store.get_group(&group_id).await.unwrap();
        assert_eq!(group.group.state, GroupState::Aborted);
    }

    #[tokio::test]
    async fn test_final_state_all_complete_becomes_completed() {
        let cancel = CancellationToken::new();
        let store = Arc::new(FakeJobStore::new());

        let group_id = "test-complete-group".to_string();
        let timestamp = current_timestamp();

        store
            .save_group(&BulkJobGroupRow {
                group_id: group_id.clone(),
                org_id: "test-org".to_string(),
                object: "Account".to_string(),
                operation: "insert".to_string(),
                state: GroupState::InProgress,
                batch_size: 1000,
                total_parts: 3,
                created_at: timestamp,
                updated_at: timestamp,
            })
            .await
            .unwrap();

        // All jobs complete
        for i in 0..3 {
            store
                .add_job(&BulkJobRow {
                    job_id: format!("job-{}", i),
                    group_id: group_id.clone(),
                    part_number: (i + 1) as i64,
                    state: BulkJobState::JobComplete,
                    processed_records: Some(100),
                    failed_records: Some(0),
                    error_message: None,
                    created_at: timestamp,
                    updated_at: timestamp,
                })
                .await
                .unwrap();
        }

        let client = FakeIngestClient::new(cancel.clone());
        let scheduler = BulkJobScheduler::new(3);

        let orchestrator = Arc::new(BulkUploadOrchestrator::new(
            None,
            store.clone(),
            "test-org".to_string(),
            client,
            scheduler,
            cancel,
            group_id.clone(),
            3,
            "Account".to_string(),
            BulkOperation::Insert,
            None,
        ));

        let phase = orchestrator.finalize_group(3).await.unwrap();

        assert_eq!(
            phase, "completed",
            "Group with all JobComplete should be Completed"
        );

        let group = store.get_group(&group_id).await.unwrap();
        assert_eq!(group.group.state, GroupState::Completed);
    }
}
