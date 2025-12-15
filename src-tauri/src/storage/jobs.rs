//! Bulk job persistence for SQLite.
//!
//! Provides storage and reconciliation for bulk ingest job groups and their individual jobs.
//! Jobs survive app restarts and can be reconciled with Salesforce on startup.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use rusqlite::Connection;
use serde::{Deserialize, Serialize};

use crate::error::AppError;
use crate::salesforce::bulk_ingest_v2::BulkIngestJobInfo;
use crate::salesforce::BulkJobState;
use crate::storage::database::Database;

// ─────────────────────────────────────────────────────────────────────────────
// GroupState Enum
// ─────────────────────────────────────────────────────────────────────────────

/// State of a bulk job group (aggregated state of all jobs in the group).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupState {
    /// Group has been created, jobs are being added.
    Open,
    /// Jobs are actively being processed.
    InProgress,
    /// All jobs completed successfully.
    Completed,
    /// One or more jobs failed.
    Failed,
    /// Group was aborted by user request.
    Aborted,
}

impl GroupState {
    /// Converts the state to its string representation for database storage.
    pub fn as_str(&self) -> &'static str {
        match self {
            GroupState::Open => "Open",
            GroupState::InProgress => "InProgress",
            GroupState::Completed => "Completed",
            GroupState::Failed => "Failed",
            GroupState::Aborted => "Aborted",
        }
    }

    /// Parses a string into a GroupState.
    /// Returns `Open` for unknown strings as a safe default.
    pub fn from_str(s: &str) -> Self {
        match s {
            "Open" => GroupState::Open,
            "InProgress" => GroupState::InProgress,
            "Completed" => GroupState::Completed,
            "Failed" => GroupState::Failed,
            "Aborted" => GroupState::Aborted,
            _ => GroupState::Open, // Safe default for unknown states
        }
    }

    /// Returns true if this is a terminal state (group cannot transition further).
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            GroupState::Completed | GroupState::Aborted | GroupState::Failed
        )
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DTOs
// ─────────────────────────────────────────────────────────────────────────────

/// A bulk job group representing one user-initiated bulk upload operation.
/// May contain multiple Salesforce jobs (one per chunk).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkJobGroupRow {
    pub group_id: String,
    pub org_id: String,
    pub object: String,
    pub operation: String,
    pub state: GroupState,
    pub batch_size: i64,
    pub total_parts: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

/// An individual Salesforce bulk ingest job (one chunk of a group).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkJobRow {
    pub job_id: String,
    pub group_id: String,
    pub part_number: i64,
    pub state: BulkJobState,
    pub processed_records: Option<i64>,
    pub failed_records: Option<i64>,
    pub error_message: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// A job group with all its associated jobs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkJobGroupWithJobs {
    pub group: BulkJobGroupRow,
    pub jobs: Vec<BulkJobRow>,
}

// ─────────────────────────────────────────────────────────────────────────────
// JobStatusProvider Trait
// ─────────────────────────────────────────────────────────────────────────────

/// Trait for fetching job status from Salesforce.
///
/// This trait allows the storage layer to be decoupled from the actual Salesforce client.
/// The orchestration layer can implement this for the real Bulk client, and tests can
/// provide a fake implementation.
pub trait JobStatusProvider: Send + Sync {
    /// Fetches the current status of a bulk ingest job from Salesforce.
    fn get_job_status<'a>(
        &'a self,
        job_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<BulkIngestJobInfo, AppError>> + Send + 'a>>;
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper: Configure Connection
// ─────────────────────────────────────────────────────────────────────────────

/// Configures connection with busy timeout and WAL mode.
fn configure_connection(conn: &Connection) -> Result<(), AppError> {
    conn.busy_timeout(Duration::from_secs(10))
        .map_err(|e| AppError::Internal(format!("Failed to set busy timeout: {e}")))?;

    conn.pragma_update(None, "journal_mode", "WAL")
        .map_err(|e| AppError::Internal(format!("Failed to set WAL mode: {e}")))?;

    Ok(())
}

/// Returns current unix timestamp in seconds.
fn current_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as i64
}

// ─────────────────────────────────────────────────────────────────────────────
// Storage Functions
// ─────────────────────────────────────────────────────────────────────────────

/// Saves a new job group to the database.
pub async fn save_group(db: &Database, group: &BulkJobGroupRow) -> Result<(), AppError> {
    let db_path = db.db_path().clone();
    let group = group.clone();

    tokio::task::spawn_blocking(move || {
        let conn = Connection::open(&db_path)
            .map_err(|e| AppError::Internal(format!("Failed to open database: {e}")))?;

        configure_connection(&conn)?;

        conn.execute(
            r#"
            INSERT INTO bulk_job_groups (group_id, org_id, object, operation, state, batch_size, total_parts, created_at, updated_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            "#,
            rusqlite::params![
                group.group_id,
                group.org_id,
                group.object,
                group.operation,
                group.state.as_str(),
                group.batch_size,
                group.total_parts,
                group.created_at,
                group.updated_at,
            ],
        )
        .map_err(|e| AppError::Internal(format!("Failed to insert job group: {e}")))?;

        Ok::<_, AppError>(())
    })
    .await
    .map_err(|e| AppError::Internal(format!("Save group task failed: {e}")))??;

    Ok(())
}

/// Adds a job to an existing group.
pub async fn add_job_to_group(db: &Database, job: &BulkJobRow) -> Result<(), AppError> {
    let db_path = db.db_path().clone();
    let job = job.clone();

    tokio::task::spawn_blocking(move || {
        let conn = Connection::open(&db_path)
            .map_err(|e| AppError::Internal(format!("Failed to open database: {e}")))?;

        configure_connection(&conn)?;

        conn.execute(
            r#"
            INSERT INTO bulk_jobs (job_id, group_id, part_number, state, processed_records, failed_records, error_message, created_at, updated_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            "#,
            rusqlite::params![
                job.job_id,
                job.group_id,
                job.part_number,
                job.state.as_str(),
                job.processed_records,
                job.failed_records,
                job.error_message,
                job.created_at,
                job.updated_at,
            ],
        )
        .map_err(|e| AppError::Internal(format!("Failed to insert job: {e}")))?;

        Ok::<_, AppError>(())
    })
    .await
    .map_err(|e| AppError::Internal(format!("Add job task failed: {e}")))??;

    Ok(())
}

/// Updates a job's state and counts.
pub async fn update_job_state(
    db: &Database,
    job_id: &str,
    state: BulkJobState,
    processed_records: Option<i64>,
    failed_records: Option<i64>,
    error_message: Option<&str>,
) -> Result<(), AppError> {
    let db_path = db.db_path().clone();
    let job_id = job_id.to_string();
    let state_str = state.as_str().to_string();
    let error_message = error_message.map(|s| s.to_string());
    let updated_at = current_timestamp();

    tokio::task::spawn_blocking(move || {
        let conn = Connection::open(&db_path)
            .map_err(|e| AppError::Internal(format!("Failed to open database: {e}")))?;

        configure_connection(&conn)?;

        conn.execute(
            r#"
            UPDATE bulk_jobs
            SET state = ?1, processed_records = ?2, failed_records = ?3, error_message = ?4, updated_at = ?5
            WHERE job_id = ?6
            "#,
            rusqlite::params![
                state_str,
                processed_records,
                failed_records,
                error_message,
                updated_at,
                job_id,
            ],
        )
        .map_err(|e| AppError::Internal(format!("Failed to update job state: {e}")))?;

        Ok::<_, AppError>(())
    })
    .await
    .map_err(|e| AppError::Internal(format!("Update job state task failed: {e}")))??;

    Ok(())
}

/// Updates a group's state.
pub async fn update_group_state(
    db: &Database,
    group_id: &str,
    state: GroupState,
) -> Result<(), AppError> {
    let db_path = db.db_path().clone();
    let group_id = group_id.to_string();
    let state_str = state.as_str().to_string();
    let updated_at = current_timestamp();

    tokio::task::spawn_blocking(move || {
        let conn = Connection::open(&db_path)
            .map_err(|e| AppError::Internal(format!("Failed to open database: {e}")))?;

        configure_connection(&conn)?;

        conn.execute(
            r#"
            UPDATE bulk_job_groups
            SET state = ?1, updated_at = ?2
            WHERE group_id = ?3
            "#,
            rusqlite::params![state_str, updated_at, group_id],
        )
        .map_err(|e| AppError::Internal(format!("Failed to update group state: {e}")))?;

        Ok::<_, AppError>(())
    })
    .await
    .map_err(|e| AppError::Internal(format!("Update group state task failed: {e}")))??;

    Ok(())
}

/// Gets a group with all its jobs.
pub async fn get_group(db: &Database, group_id: &str) -> Result<BulkJobGroupWithJobs, AppError> {
    let db_path = db.db_path().clone();
    let group_id = group_id.to_string();

    tokio::task::spawn_blocking(move || {
        let conn = Connection::open(&db_path)
            .map_err(|e| AppError::Internal(format!("Failed to open database: {e}")))?;

        configure_connection(&conn)?;

        // Get the group
        let group: BulkJobGroupRow = conn
            .query_row(
                r#"
                SELECT group_id, org_id, object, operation, state, batch_size, total_parts, created_at, updated_at
                FROM bulk_job_groups
                WHERE group_id = ?1
                "#,
                [&group_id],
                |row| {
                    let state_str: String = row.get(4)?;
                    Ok(BulkJobGroupRow {
                        group_id: row.get(0)?,
                        org_id: row.get(1)?,
                        object: row.get(2)?,
                        operation: row.get(3)?,
                        state: GroupState::from_str(&state_str),
                        batch_size: row.get(5)?,
                        total_parts: row.get(6)?,
                        created_at: row.get(7)?,
                        updated_at: row.get(8)?,
                    })
                },
            )
            .map_err(|e| AppError::NotFound(format!("Job group not found: {e}")))?;

        // Get jobs for this group
        let mut stmt = conn
            .prepare(
                r#"
                SELECT job_id, group_id, part_number, state, processed_records, failed_records, error_message, created_at, updated_at
                FROM bulk_jobs
                WHERE group_id = ?1
                ORDER BY part_number ASC
                "#,
            )
            .map_err(|e| AppError::Internal(format!("Failed to prepare query: {e}")))?;

        let jobs = stmt
            .query_map([&group_id], |row| {
                let state_str: String = row.get(3)?;
                Ok(BulkJobRow {
                    job_id: row.get(0)?,
                    group_id: row.get(1)?,
                    part_number: row.get(2)?,
                    state: BulkJobState::from_str(&state_str),
                    processed_records: row.get(4)?,
                    failed_records: row.get(5)?,
                    error_message: row.get(6)?,
                    created_at: row.get(7)?,
                    updated_at: row.get(8)?,
                })
            })
            .map_err(|e| AppError::Internal(format!("Failed to query jobs: {e}")))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| AppError::Internal(format!("Failed to collect jobs: {e}")))?;

        Ok::<_, AppError>(BulkJobGroupWithJobs { group, jobs })
    })
    .await
    .map_err(|e| AppError::Internal(format!("Get group task failed: {e}")))?
}

/// Gets all active (non-terminal) groups for an org.
/// Terminal states are: Completed, Failed, Aborted.
pub async fn get_active_groups(
    db: &Database,
    org_id: &str,
) -> Result<Vec<BulkJobGroupWithJobs>, AppError> {
    let db_path = db.db_path().clone();
    let org_id = org_id.to_string();

    tokio::task::spawn_blocking(move || {
        let conn = Connection::open(&db_path)
            .map_err(|e| AppError::Internal(format!("Failed to open database: {e}")))?;

        configure_connection(&conn)?;

        // Get non-terminal groups (use GroupState enum for consistency)
        let terminal_states = [
            GroupState::Completed.as_str(),
            GroupState::Failed.as_str(),
            GroupState::Aborted.as_str(),
        ];
        let mut stmt = conn
            .prepare(
                r#"
                SELECT group_id, org_id, object, operation, state, batch_size, total_parts, created_at, updated_at
                FROM bulk_job_groups
                WHERE org_id = ?1 AND state NOT IN (?2, ?3, ?4)
                ORDER BY created_at DESC
                "#,
            )
            .map_err(|e| AppError::Internal(format!("Failed to prepare query: {e}")))?;

        let groups: Vec<BulkJobGroupRow> = stmt
            .query_map(rusqlite::params![&org_id, terminal_states[0], terminal_states[1], terminal_states[2]], |row| {
                let state_str: String = row.get(4)?;
                Ok(BulkJobGroupRow {
                    group_id: row.get(0)?,
                    org_id: row.get(1)?,
                    object: row.get(2)?,
                    operation: row.get(3)?,
                    state: GroupState::from_str(&state_str),
                    batch_size: row.get(5)?,
                    total_parts: row.get(6)?,
                    created_at: row.get(7)?,
                    updated_at: row.get(8)?,
                })
            })
            .map_err(|e| AppError::Internal(format!("Failed to query groups: {e}")))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| AppError::Internal(format!("Failed to collect groups: {e}")))?;

        // For each group, get its jobs
        let mut result = Vec::with_capacity(groups.len());
        for group in groups {
            let mut job_stmt = conn
                .prepare(
                    r#"
                    SELECT job_id, group_id, part_number, state, processed_records, failed_records, error_message, created_at, updated_at
                    FROM bulk_jobs
                    WHERE group_id = ?1
                    ORDER BY part_number ASC
                    "#,
                )
                .map_err(|e| AppError::Internal(format!("Failed to prepare job query: {e}")))?;

            let jobs = job_stmt
                .query_map([&group.group_id], |row| {
                    let state_str: String = row.get(3)?;
                    Ok(BulkJobRow {
                        job_id: row.get(0)?,
                        group_id: row.get(1)?,
                        part_number: row.get(2)?,
                        state: BulkJobState::from_str(&state_str),
                        processed_records: row.get(4)?,
                        failed_records: row.get(5)?,
                        error_message: row.get(6)?,
                        created_at: row.get(7)?,
                        updated_at: row.get(8)?,
                    })
                })
                .map_err(|e| AppError::Internal(format!("Failed to query jobs: {e}")))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| AppError::Internal(format!("Failed to collect jobs: {e}")))?;

            result.push(BulkJobGroupWithJobs { group, jobs });
        }

        Ok::<_, AppError>(result)
    })
    .await
    .map_err(|e| AppError::Internal(format!("Get active groups task failed: {e}")))?
}

/// Cleans up old terminal groups and their jobs.
/// Returns the number of deleted groups.
pub async fn cleanup_old_jobs(db: &Database, retention_days: i64) -> Result<u64, AppError> {
    let db_path = db.db_path().clone();
    let cutoff = current_timestamp() - (retention_days * 24 * 60 * 60);

    tokio::task::spawn_blocking(move || {
        let mut conn = Connection::open(&db_path)
            .map_err(|e| AppError::Internal(format!("Failed to open database: {e}")))?;

        configure_connection(&conn)?;

        let tx = conn
            .transaction()
            .map_err(|e| AppError::Internal(format!("Failed to start transaction: {e}")))?;

        // First, get the group IDs to delete (use GroupState enum for consistency)
        let terminal_states = [
            GroupState::Completed.as_str(),
            GroupState::Failed.as_str(),
            GroupState::Aborted.as_str(),
        ];
        let group_ids: Vec<String> = {
            let mut stmt = tx
                .prepare(
                    r#"
                    SELECT group_id FROM bulk_job_groups
                    WHERE state IN (?1, ?2, ?3)
                    AND updated_at < ?4
                    "#,
                )
                .map_err(|e| AppError::Internal(format!("Failed to prepare query: {e}")))?;

            let rows = stmt
                .query_map(
                    rusqlite::params![
                        terminal_states[0],
                        terminal_states[1],
                        terminal_states[2],
                        cutoff
                    ],
                    |row| row.get(0),
                )
                .map_err(|e| AppError::Internal(format!("Failed to query old groups: {e}")))?;

            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|e| AppError::Internal(format!("Failed to collect group IDs: {e}")))?
        };

        let deleted_count = group_ids.len() as u64;

        // Delete jobs for each group
        for group_id in &group_ids {
            tx.execute("DELETE FROM bulk_jobs WHERE group_id = ?1", [group_id])
                .map_err(|e| AppError::Internal(format!("Failed to delete jobs: {e}")))?;
        }

        // Delete the groups
        for group_id in &group_ids {
            tx.execute(
                "DELETE FROM bulk_job_groups WHERE group_id = ?1",
                [group_id],
            )
            .map_err(|e| AppError::Internal(format!("Failed to delete group: {e}")))?;
        }

        tx.commit()
            .map_err(|e| AppError::Internal(format!("Failed to commit cleanup: {e}")))?;

        Ok::<_, AppError>(deleted_count)
    })
    .await
    .map_err(|e| AppError::Internal(format!("Cleanup task failed: {e}")))?
}

// ─────────────────────────────────────────────────────────────────────────────
// Reconciliation
// ─────────────────────────────────────────────────────────────────────────────

/// Reconciles in-progress jobs with Salesforce.
///
/// Fetches current status from Salesforce for all non-terminal jobs and updates
/// local state accordingly. Throttles API calls to avoid rate limiting.
pub async fn reconcile_jobs<P: JobStatusProvider>(
    db: &Database,
    provider: &P,
    org_id: &str,
) -> Result<(), AppError> {
    // Get all non-terminal jobs for this org from the database
    let jobs_to_check = get_non_terminal_jobs(db, org_id).await?;

    if jobs_to_check.is_empty() {
        return Ok(());
    }

    // Process jobs in batches of 10 with 500ms delay between batches
    const BATCH_SIZE: usize = 10;
    const BATCH_DELAY_MS: u64 = 500;

    for chunk in jobs_to_check.chunks(BATCH_SIZE) {
        for job in chunk {
            // Fetch status from Salesforce (OUTSIDE spawn_blocking)
            let status = provider.get_job_status(&job.job_id).await;

            match status {
                Ok(info) => {
                    // Update job state in database (atomic transaction)
                    update_job_and_maybe_group(
                        db,
                        &job.job_id,
                        &job.group_id,
                        info.state,
                        info.processed_records.map(|v| v as i64),
                        info.failed_records.map(|v| v as i64),
                        info.error_message.as_deref(),
                    )
                    .await?;
                }
                Err(AppError::NotFound(_)) => {
                    // Job not found in Salesforce - mark as Failed
                    update_job_and_maybe_group(
                        db,
                        &job.job_id,
                        &job.group_id,
                        BulkJobState::Failed,
                        None,
                        None,
                        Some("Job not found in Salesforce"),
                    )
                    .await?;
                }
                Err(e) => {
                    // Other errors - log and continue (don't fail entire reconciliation)
                    tracing::warn!("Failed to get status for job {}: {:?}", job.job_id, e);
                }
            }
        }

        // Throttle between batches
        if chunk.len() == BATCH_SIZE {
            tokio::time::sleep(Duration::from_millis(BATCH_DELAY_MS)).await;
        }
    }

    Ok(())
}

/// Helper to get non-terminal jobs for an org.
async fn get_non_terminal_jobs(db: &Database, org_id: &str) -> Result<Vec<BulkJobRow>, AppError> {
    let db_path = db.db_path().clone();
    let org_id = org_id.to_string();

    tokio::task::spawn_blocking(move || {
        let conn = Connection::open(&db_path)
            .map_err(|e| AppError::Internal(format!("Failed to open database: {e}")))?;

        configure_connection(&conn)?;

        // Use BulkJobState enum for terminal states
        let terminal_states = [
            BulkJobState::JobComplete.as_str(),
            BulkJobState::Failed.as_str(),
            BulkJobState::Aborted.as_str(),
        ];

        let mut stmt = conn
            .prepare(
                r#"
                SELECT j.job_id, j.group_id, j.part_number, j.state, j.processed_records, j.failed_records, j.error_message, j.created_at, j.updated_at
                FROM bulk_jobs j
                INNER JOIN bulk_job_groups g ON j.group_id = g.group_id
                WHERE g.org_id = ?1 AND j.state NOT IN (?2, ?3, ?4)
                ORDER BY j.created_at ASC
                "#,
            )
            .map_err(|e| AppError::Internal(format!("Failed to prepare query: {e}")))?;

        let jobs = stmt
            .query_map(rusqlite::params![&org_id, terminal_states[0], terminal_states[1], terminal_states[2]], |row| {
                let state_str: String = row.get(3)?;
                Ok(BulkJobRow {
                    job_id: row.get(0)?,
                    group_id: row.get(1)?,
                    part_number: row.get(2)?,
                    state: BulkJobState::from_str(&state_str),
                    processed_records: row.get(4)?,
                    failed_records: row.get(5)?,
                    error_message: row.get(6)?,
                    created_at: row.get(7)?,
                    updated_at: row.get(8)?,
                })
            })
            .map_err(|e| AppError::Internal(format!("Failed to query jobs: {e}")))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| AppError::Internal(format!("Failed to collect jobs: {e}")))?;

        Ok::<_, AppError>(jobs)
    })
    .await
    .map_err(|e| AppError::Internal(format!("Get non-terminal jobs task failed: {e}")))?
}

/// Updates a job state and potentially the group state in a single transaction.
async fn update_job_and_maybe_group(
    db: &Database,
    job_id: &str,
    group_id: &str,
    state: BulkJobState,
    processed_records: Option<i64>,
    failed_records: Option<i64>,
    error_message: Option<&str>,
) -> Result<(), AppError> {
    let db_path = db.db_path().clone();
    let job_id = job_id.to_string();
    let group_id = group_id.to_string();
    let state_str = state.as_str().to_string();
    let error_message = error_message.map(|s| s.to_string());
    let updated_at = current_timestamp();

    // Pre-compute terminal state strings for use in the blocking closure
    let terminal_job_states = [
        BulkJobState::JobComplete.as_str(),
        BulkJobState::Failed.as_str(),
        BulkJobState::Aborted.as_str(),
    ];
    let failed_state = BulkJobState::Failed.as_str();
    let aborted_state = BulkJobState::Aborted.as_str();

    tokio::task::spawn_blocking(move || {
        let mut conn = Connection::open(&db_path)
            .map_err(|e| AppError::Internal(format!("Failed to open database: {e}")))?;

        configure_connection(&conn)?;

        let tx = conn
            .transaction()
            .map_err(|e| AppError::Internal(format!("Failed to start transaction: {e}")))?;

        // Update the job
        tx.execute(
            r#"
            UPDATE bulk_jobs
            SET state = ?1, processed_records = ?2, failed_records = ?3, error_message = ?4, updated_at = ?5
            WHERE job_id = ?6
            "#,
            rusqlite::params![
                state_str,
                processed_records,
                failed_records,
                error_message,
                updated_at,
                job_id,
            ],
        )
        .map_err(|e| AppError::Internal(format!("Failed to update job: {e}")))?;

        // Check if all jobs in the group are now terminal
        let non_terminal_count: i64 = tx
            .query_row(
                r#"
                SELECT COUNT(*) FROM bulk_jobs
                WHERE group_id = ?1 AND state NOT IN (?2, ?3, ?4)
                "#,
                rusqlite::params![&group_id, terminal_job_states[0], terminal_job_states[1], terminal_job_states[2]],
                |row| row.get(0),
            )
            .map_err(|e| AppError::Internal(format!("Failed to count non-terminal jobs: {e}")))?;

        if non_terminal_count == 0 {
            // All jobs are terminal - compute group state
            let has_failed: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM bulk_jobs WHERE group_id = ?1 AND state = ?2)",
                    rusqlite::params![&group_id, failed_state],
                    |row| row.get(0),
                )
                .map_err(|e| AppError::Internal(format!("Failed to check failed jobs: {e}")))?;

            let has_aborted: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM bulk_jobs WHERE group_id = ?1 AND state = ?2)",
                    rusqlite::params![&group_id, aborted_state],
                    |row| row.get(0),
                )
                .map_err(|e| AppError::Internal(format!("Failed to check aborted jobs: {e}")))?;

            let new_group_state = if has_failed {
                GroupState::Failed
            } else if has_aborted {
                GroupState::Aborted
            } else {
                GroupState::Completed
            };

            tx.execute(
                "UPDATE bulk_job_groups SET state = ?1, updated_at = ?2 WHERE group_id = ?3",
                rusqlite::params![new_group_state.as_str(), updated_at, group_id],
            )
            .map_err(|e| AppError::Internal(format!("Failed to update group state: {e}")))?;
        }

        tx.commit()
            .map_err(|e| AppError::Internal(format!("Failed to commit transaction: {e}")))?;

        Ok::<_, AppError>(())
    })
    .await
    .map_err(|e| AppError::Internal(format!("Update job and group task failed: {e}")))??;

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

    use crate::salesforce::bulk_ingest_v2::BulkOperation;

    fn test_db_path() -> (TempDir, std::path::PathBuf) {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test.db");
        (temp_dir, db_path)
    }

    fn now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs() as i64
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Fake JobStatusProvider for tests
    // ─────────────────────────────────────────────────────────────────────────

    struct FakeJobStatusProvider {
        responses: Mutex<HashMap<String, BulkIngestJobInfo>>,
        errors: Mutex<HashMap<String, String>>,
    }

    impl FakeJobStatusProvider {
        fn new() -> Self {
            Self {
                responses: Mutex::new(HashMap::new()),
                errors: Mutex::new(HashMap::new()),
            }
        }

        fn add_response(&self, job_id: &str, response: Result<BulkIngestJobInfo, AppError>) {
            match response {
                Ok(info) => {
                    self.responses
                        .lock()
                        .unwrap()
                        .insert(job_id.to_string(), info);
                }
                Err(e) => {
                    self.errors
                        .lock()
                        .unwrap()
                        .insert(job_id.to_string(), format!("{:?}", e));
                }
            }
        }
    }

    impl JobStatusProvider for FakeJobStatusProvider {
        fn get_job_status<'a>(
            &'a self,
            job_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<BulkIngestJobInfo, AppError>> + Send + 'a>>
        {
            let job_id = job_id.to_string();
            Box::pin(async move {
                // Check for error first
                if let Some(_err) = self.errors.lock().unwrap().get(&job_id) {
                    return Err(AppError::NotFound("Job not found".to_string()));
                }

                // Check for success response
                if let Some(info) = self.responses.lock().unwrap().get(&job_id).cloned() {
                    return Ok(info);
                }

                Err(AppError::NotFound("Job not found".to_string()))
            })
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // CRUD Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_save_group_and_add_jobs() {
        let (_temp_dir, db_path) = test_db_path();
        let db = Database::init(db_path).await.expect("Failed to init DB");

        let timestamp = now();

        // Create a group
        let group = BulkJobGroupRow {
            group_id: "group-123".to_string(),
            org_id: "org-456".to_string(),
            object: "Account".to_string(),
            operation: "insert".to_string(),
            state: GroupState::Open,
            batch_size: 10000,
            total_parts: 2,
            created_at: timestamp,
            updated_at: timestamp,
        };

        save_group(&db, &group).await.expect("Failed to save group");

        // Add jobs
        let job1 = BulkJobRow {
            job_id: "sf-job-1".to_string(),
            group_id: "group-123".to_string(),
            part_number: 1,
            state: BulkJobState::Open,
            processed_records: None,
            failed_records: None,
            error_message: None,
            created_at: timestamp,
            updated_at: timestamp,
        };

        let job2 = BulkJobRow {
            job_id: "sf-job-2".to_string(),
            group_id: "group-123".to_string(),
            part_number: 2,
            state: BulkJobState::Open,
            processed_records: None,
            failed_records: None,
            error_message: None,
            created_at: timestamp,
            updated_at: timestamp,
        };

        add_job_to_group(&db, &job1)
            .await
            .expect("Failed to add job 1");
        add_job_to_group(&db, &job2)
            .await
            .expect("Failed to add job 2");

        // Retrieve and verify
        let result = get_group(&db, "group-123")
            .await
            .expect("Failed to get group");

        assert_eq!(result.group.group_id, "group-123");
        assert_eq!(result.group.object, "Account");
        assert_eq!(result.group.operation, "insert");
        assert_eq!(result.group.total_parts, 2);
        assert_eq!(result.jobs.len(), 2);
        assert_eq!(result.jobs[0].part_number, 1);
        assert_eq!(result.jobs[1].part_number, 2);
    }

    #[tokio::test]
    async fn test_update_job_state() {
        let (_temp_dir, db_path) = test_db_path();
        let db = Database::init(db_path).await.expect("Failed to init DB");

        let timestamp = now();

        // Create group and job
        let group = BulkJobGroupRow {
            group_id: "group-1".to_string(),
            org_id: "org-1".to_string(),
            object: "Contact".to_string(),
            operation: "update".to_string(),
            state: GroupState::InProgress,
            batch_size: 5000,
            total_parts: 1,
            created_at: timestamp,
            updated_at: timestamp,
        };
        save_group(&db, &group).await.expect("Failed to save group");

        let job = BulkJobRow {
            job_id: "sf-job-abc".to_string(),
            group_id: "group-1".to_string(),
            part_number: 1,
            state: BulkJobState::InProgress,
            processed_records: Some(100),
            failed_records: Some(0),
            error_message: None,
            created_at: timestamp,
            updated_at: timestamp,
        };
        add_job_to_group(&db, &job)
            .await
            .expect("Failed to add job");

        // Update job state
        update_job_state(
            &db,
            "sf-job-abc",
            BulkJobState::JobComplete,
            Some(500),
            Some(5),
            None,
        )
        .await
        .expect("Failed to update job state");

        // Verify
        let result = get_group(&db, "group-1")
            .await
            .expect("Failed to get group");
        let updated_job = &result.jobs[0];

        assert_eq!(updated_job.state, BulkJobState::JobComplete);
        assert_eq!(updated_job.processed_records, Some(500));
        assert_eq!(updated_job.failed_records, Some(5));
        assert!(updated_job.updated_at >= timestamp);
    }

    #[tokio::test]
    async fn test_update_job_state_with_error() {
        let (_temp_dir, db_path) = test_db_path();
        let db = Database::init(db_path).await.expect("Failed to init DB");

        let timestamp = now();

        let group = BulkJobGroupRow {
            group_id: "group-err".to_string(),
            org_id: "org-1".to_string(),
            object: "Lead".to_string(),
            operation: "insert".to_string(),
            state: GroupState::InProgress,
            batch_size: 1000,
            total_parts: 1,
            created_at: timestamp,
            updated_at: timestamp,
        };
        save_group(&db, &group).await.unwrap();

        let job = BulkJobRow {
            job_id: "sf-job-err".to_string(),
            group_id: "group-err".to_string(),
            part_number: 1,
            state: BulkJobState::InProgress,
            processed_records: None,
            failed_records: None,
            error_message: None,
            created_at: timestamp,
            updated_at: timestamp,
        };
        add_job_to_group(&db, &job).await.unwrap();

        // Update with error
        update_job_state(
            &db,
            "sf-job-err",
            BulkJobState::Failed,
            Some(0),
            Some(100),
            Some("Invalid field mapping"),
        )
        .await
        .unwrap();

        let result = get_group(&db, "group-err").await.unwrap();
        assert_eq!(result.jobs[0].state, BulkJobState::Failed);
        assert_eq!(
            result.jobs[0].error_message,
            Some("Invalid field mapping".to_string())
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Get Active Groups Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_get_active_groups_excludes_terminal() {
        let (_temp_dir, db_path) = test_db_path();
        let db = Database::init(db_path).await.expect("Failed to init DB");

        let timestamp = now();

        // Create groups with different states
        for (id, state) in [
            ("g1", GroupState::Open),
            ("g2", GroupState::InProgress),
            ("g3", GroupState::Completed),
            ("g4", GroupState::Failed),
            ("g5", GroupState::Aborted),
        ] {
            let group = BulkJobGroupRow {
                group_id: id.to_string(),
                org_id: "test-org".to_string(),
                object: "Account".to_string(),
                operation: "insert".to_string(),
                state,
                batch_size: 1000,
                total_parts: 1,
                created_at: timestamp,
                updated_at: timestamp,
            };
            save_group(&db, &group).await.unwrap();
        }

        // Get active groups
        let active = get_active_groups(&db, "test-org").await.unwrap();

        // Should only return Open and InProgress
        assert_eq!(active.len(), 2);
        let states: Vec<_> = active.iter().map(|g| g.group.state).collect();
        assert!(states.contains(&GroupState::Open));
        assert!(states.contains(&GroupState::InProgress));
        assert!(!states.contains(&GroupState::Completed));
        assert!(!states.contains(&GroupState::Failed));
        assert!(!states.contains(&GroupState::Aborted));
    }

    #[tokio::test]
    async fn test_get_active_groups_filters_by_org() {
        let (_temp_dir, db_path) = test_db_path();
        let db = Database::init(db_path).await.expect("Failed to init DB");

        let timestamp = now();

        // Create groups for different orgs
        for (id, org) in [("g1", "org-a"), ("g2", "org-a"), ("g3", "org-b")] {
            let group = BulkJobGroupRow {
                group_id: id.to_string(),
                org_id: org.to_string(),
                object: "Account".to_string(),
                operation: "insert".to_string(),
                state: GroupState::InProgress,
                batch_size: 1000,
                total_parts: 1,
                created_at: timestamp,
                updated_at: timestamp,
            };
            save_group(&db, &group).await.unwrap();
        }

        let org_a_groups = get_active_groups(&db, "org-a").await.unwrap();
        assert_eq!(org_a_groups.len(), 2);

        let org_b_groups = get_active_groups(&db, "org-b").await.unwrap();
        assert_eq!(org_b_groups.len(), 1);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Cleanup Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_cleanup_old_jobs() {
        let (_temp_dir, db_path) = test_db_path();
        let db = Database::init(db_path).await.expect("Failed to init DB");

        let now_ts = now();
        let old_ts = now_ts - (10 * 24 * 60 * 60); // 10 days ago

        // Create old completed group
        let old_group = BulkJobGroupRow {
            group_id: "old-group".to_string(),
            org_id: "org-1".to_string(),
            object: "Account".to_string(),
            operation: "insert".to_string(),
            state: GroupState::Completed,
            batch_size: 1000,
            total_parts: 1,
            created_at: old_ts,
            updated_at: old_ts,
        };
        save_group(&db, &old_group).await.unwrap();

        let old_job = BulkJobRow {
            job_id: "old-job".to_string(),
            group_id: "old-group".to_string(),
            part_number: 1,
            state: BulkJobState::JobComplete,
            processed_records: Some(100),
            failed_records: Some(0),
            error_message: None,
            created_at: old_ts,
            updated_at: old_ts,
        };
        add_job_to_group(&db, &old_job).await.unwrap();

        // Create recent completed group
        let new_group = BulkJobGroupRow {
            group_id: "new-group".to_string(),
            org_id: "org-1".to_string(),
            object: "Account".to_string(),
            operation: "insert".to_string(),
            state: GroupState::Completed,
            batch_size: 1000,
            total_parts: 1,
            created_at: now_ts,
            updated_at: now_ts,
        };
        save_group(&db, &new_group).await.unwrap();

        // Create active group (should not be deleted regardless of age)
        let active_group = BulkJobGroupRow {
            group_id: "active-group".to_string(),
            org_id: "org-1".to_string(),
            object: "Account".to_string(),
            operation: "insert".to_string(),
            state: GroupState::InProgress,
            batch_size: 1000,
            total_parts: 1,
            created_at: old_ts,
            updated_at: old_ts,
        };
        save_group(&db, &active_group).await.unwrap();

        // Cleanup with 7 day retention
        let deleted = cleanup_old_jobs(&db, 7).await.unwrap();

        // Should have deleted 1 group (old-group)
        assert_eq!(deleted, 1);

        // Verify old-group is gone
        let old_result = get_group(&db, "old-group").await;
        assert!(old_result.is_err());

        // Verify new-group still exists
        let new_result = get_group(&db, "new-group").await;
        assert!(new_result.is_ok());

        // Verify active-group still exists
        let active_result = get_group(&db, "active-group").await;
        assert!(active_result.is_ok());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Reconciliation Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_reconcile_jobs_updates_states() {
        let (_temp_dir, db_path) = test_db_path();
        let db = Database::init(db_path).await.expect("Failed to init DB");

        let timestamp = now();

        // Create a group with 2 in-progress jobs
        let group = BulkJobGroupRow {
            group_id: "reconcile-group".to_string(),
            org_id: "reconcile-org".to_string(),
            object: "Account".to_string(),
            operation: "insert".to_string(),
            state: GroupState::InProgress,
            batch_size: 5000,
            total_parts: 2,
            created_at: timestamp,
            updated_at: timestamp,
        };
        save_group(&db, &group).await.unwrap();

        let job_a = BulkJobRow {
            job_id: "job-a".to_string(),
            group_id: "reconcile-group".to_string(),
            part_number: 1,
            state: BulkJobState::InProgress,
            processed_records: None,
            failed_records: None,
            error_message: None,
            created_at: timestamp,
            updated_at: timestamp,
        };
        let job_b = BulkJobRow {
            job_id: "job-b".to_string(),
            group_id: "reconcile-group".to_string(),
            part_number: 2,
            state: BulkJobState::InProgress,
            processed_records: None,
            failed_records: None,
            error_message: None,
            created_at: timestamp,
            updated_at: timestamp,
        };
        add_job_to_group(&db, &job_a).await.unwrap();
        add_job_to_group(&db, &job_b).await.unwrap();

        // Set up fake provider
        let provider = FakeJobStatusProvider::new();

        // Job A completed successfully
        provider.add_response(
            "job-a",
            Ok(BulkIngestJobInfo {
                id: "job-a".to_string(),
                state: BulkJobState::JobComplete,
                object: "Account".to_string(),
                operation: BulkOperation::Insert,
                processed_records: Some(1000),
                failed_records: Some(0),
                error_message: None,
            }),
        );

        // Job B failed
        provider.add_response(
            "job-b",
            Ok(BulkIngestJobInfo {
                id: "job-b".to_string(),
                state: BulkJobState::Failed,
                object: "Account".to_string(),
                operation: BulkOperation::Insert,
                processed_records: Some(0),
                failed_records: Some(500),
                error_message: Some("Field mapping error".to_string()),
            }),
        );

        // Run reconciliation
        reconcile_jobs(&db, &provider, "reconcile-org")
            .await
            .unwrap();

        // Verify job states updated
        let result = get_group(&db, "reconcile-group").await.unwrap();

        let job_a_updated = result.jobs.iter().find(|j| j.job_id == "job-a").unwrap();
        assert_eq!(job_a_updated.state, BulkJobState::JobComplete);
        assert_eq!(job_a_updated.processed_records, Some(1000));
        assert_eq!(job_a_updated.failed_records, Some(0));

        let job_b_updated = result.jobs.iter().find(|j| j.job_id == "job-b").unwrap();
        assert_eq!(job_b_updated.state, BulkJobState::Failed);
        assert_eq!(job_b_updated.processed_records, Some(0));
        assert_eq!(job_b_updated.failed_records, Some(500));
        assert_eq!(
            job_b_updated.error_message,
            Some("Field mapping error".to_string())
        );

        // Group should be Failed (because one job failed)
        assert_eq!(result.group.state, GroupState::Failed);
    }

    #[tokio::test]
    async fn test_reconcile_jobs_group_completed_when_all_succeed() {
        let (_temp_dir, db_path) = test_db_path();
        let db = Database::init(db_path).await.expect("Failed to init DB");

        let timestamp = now();

        let group = BulkJobGroupRow {
            group_id: "success-group".to_string(),
            org_id: "success-org".to_string(),
            object: "Contact".to_string(),
            operation: "insert".to_string(),
            state: GroupState::InProgress,
            batch_size: 1000,
            total_parts: 1,
            created_at: timestamp,
            updated_at: timestamp,
        };
        save_group(&db, &group).await.unwrap();

        let job = BulkJobRow {
            job_id: "success-job".to_string(),
            group_id: "success-group".to_string(),
            part_number: 1,
            state: BulkJobState::InProgress,
            processed_records: None,
            failed_records: None,
            error_message: None,
            created_at: timestamp,
            updated_at: timestamp,
        };
        add_job_to_group(&db, &job).await.unwrap();

        let provider = FakeJobStatusProvider::new();
        provider.add_response(
            "success-job",
            Ok(BulkIngestJobInfo {
                id: "success-job".to_string(),
                state: BulkJobState::JobComplete,
                object: "Contact".to_string(),
                operation: BulkOperation::Insert,
                processed_records: Some(500),
                failed_records: Some(0),
                error_message: None,
            }),
        );

        reconcile_jobs(&db, &provider, "success-org").await.unwrap();

        let result = get_group(&db, "success-group").await.unwrap();
        assert_eq!(result.group.state, GroupState::Completed);
    }
}
