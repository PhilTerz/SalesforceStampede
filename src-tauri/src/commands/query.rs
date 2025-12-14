//! Query execution and management Tauri commands.
//!
//! These commands handle:
//! - SOQL query execution (REST and Bulk API)
//! - Query strategy selection (Auto/REST/Bulk)
//! - Saved queries management
//! - Query history

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Emitter, State};
use tracing::info;
use url::Url;

use crate::error::AppError;
use crate::salesforce::client::{OrgCredentials, SalesforceClient};
use crate::salesforce::query_strategy::{
    count_query, determine_strategy, QueryPreferences, QueryStrategy, StrategyDecision,
};
use crate::salesforce::rest::RestQueryClient;
use crate::salesforce::bulk_query_v2::BulkQueryV2Client;
use crate::salesforce::BulkJobState;
use crate::salesforce::API_VERSION;
use crate::state::AppState;
use crate::storage::credentials as keychain;
use crate::storage::{QueryHistoryEntry, SavedQuery};

// ─────────────────────────────────────────────────────────────────────────────
// Helper: Build SalesforceClient for the active org
// ─────────────────────────────────────────────────────────────────────────────

/// Retrieves the SalesforceClient for the currently active org.
///
/// # Errors
/// - `AppError::NoActiveOrg` if no org is selected
/// - `AppError::NotAuthenticated` if tokens not found
/// - `AppError::NotFound` if org not in database
async fn get_active_client(state: &AppState) -> Result<SalesforceClient, AppError> {
    // Get active org ID
    let org_id = state
        .get_active_org_id()
        .await
        .ok_or(AppError::NoActiveOrg)?;

    // Get org from database
    let org = state
        .db
        .get_org(&org_id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("Org {} not in database", org_id)))?;

    // Get tokens from keychain
    let tokens = keychain::get_tokens(&org_id).await?;

    // Build OrgCredentials
    let creds = OrgCredentials {
        org_id: org.id.clone(),
        user_id: String::new(), // Not stored in database
        username: org.username.clone(),
        instance_url: org.instance_url.clone(),
        access_token: SecretString::from(tokens.access_token),
        refresh_token: Some(SecretString::from(tokens.refresh_token)),
        api_version: API_VERSION.to_string(),
    };

    SalesforceClient::new(creds)
}

/// Creates a BulkQueryV2Client from the active org credentials.
async fn get_bulk_client(state: &AppState) -> Result<BulkQueryV2Client, AppError> {
    // Get active org ID
    let org_id = state
        .get_active_org_id()
        .await
        .ok_or(AppError::NoActiveOrg)?;

    // Get org from database
    let org = state
        .db
        .get_org(&org_id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("Org {} not in database", org_id)))?;

    // Get tokens from keychain
    let tokens = keychain::get_tokens(&org_id).await?;

    // Parse instance URL
    let base_url = Url::parse(&org.instance_url)
        .map_err(|_| AppError::Internal("Invalid instance URL".to_string()))?;

    // Create HTTP client
    let http_client = Arc::new(
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(300))
            .build()
            .map_err(|e| AppError::Internal(format!("Failed to create HTTP client: {}", e)))?
    );

    Ok(BulkQueryV2Client::new(http_client, base_url, tokens.access_token))
}

/// Returns the active org ID or errors.
async fn require_active_org(state: &AppState) -> Result<String, AppError> {
    state.get_active_org_id().await.ok_or(AppError::NoActiveOrg)
}

// ─────────────────────────────────────────────────────────────────────────────
// Request/Response Types
// ─────────────────────────────────────────────────────────────────────────────

/// Request for executing a query.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecuteQueryRequest {
    /// The SOQL query to execute.
    pub soql: String,
    /// Query strategy: "auto", "rest", or "bulk".
    #[serde(default = "default_strategy")]
    pub strategy: String,
    /// Maximum rows to fetch (optional, for preview).
    pub max_rows: Option<u64>,
    /// Output path for bulk export (optional).
    pub export_path: Option<String>,
}

fn default_strategy() -> String {
    "auto".to_string()
}

/// Result of a query execution.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecuteQueryResult {
    /// Records fetched (empty for bulk exports to file).
    pub records: Vec<serde_json::Value>,
    /// Total size reported by Salesforce.
    pub total_size: u64,
    /// Whether all records were fetched.
    pub done: bool,
    /// Whether the result was truncated due to max_rows.
    pub truncated: bool,
    /// Strategy used: "rest" or "bulk".
    pub strategy_used: String,
    /// Duration in milliseconds.
    pub duration_ms: u64,
    /// Export file path (for bulk exports).
    pub export_path: Option<String>,
}

/// Progress event emitted during query execution.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryProgressEvent {
    /// Number of records fetched so far.
    pub records_fetched: u64,
    /// Total expected (if known).
    pub total_expected: Option<u64>,
    /// Current phase: "counting", "fetching", "downloading", "complete".
    pub phase: String,
    /// Bulk job state (for bulk queries).
    pub job_state: Option<String>,
}

/// Saved query info returned to frontend.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SavedQueryInfo {
    pub id: String,
    pub name: String,
    pub soql: String,
    pub created_at: i64,
}

impl From<SavedQuery> for SavedQueryInfo {
    fn from(sq: SavedQuery) -> Self {
        Self {
            id: sq.id,
            name: sq.name,
            soql: sq.soql,
            created_at: sq.created_at,
        }
    }
}

/// Query history entry returned to frontend.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryHistoryInfo {
    pub id: String,
    pub soql: String,
    pub executed_at: i64,
    pub row_count: Option<i64>,
    pub strategy: Option<String>,
    pub duration_ms: Option<i64>,
    pub truncated: bool,
    pub export_path: Option<String>,
}

impl From<QueryHistoryEntry> for QueryHistoryInfo {
    fn from(entry: QueryHistoryEntry) -> Self {
        Self {
            id: entry.id,
            soql: entry.soql,
            executed_at: entry.executed_at,
            row_count: entry.row_count,
            strategy: entry.strategy,
            duration_ms: entry.duration_ms,
            truncated: entry.truncated,
            export_path: entry.export_path,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Commands
// ─────────────────────────────────────────────────────────────────────────────

/// Executes a SOQL query using the specified strategy.
///
/// # Arguments
///
/// * `state` - Application state
/// * `app_handle` - Tauri app handle for emitting events
/// * `request` - Query execution request
///
/// # Returns
///
/// Query execution result with records (or export path for bulk).
///
/// # Events
///
/// Emits `query:progress` events during execution.
#[tauri::command]
pub async fn execute_query(
    state: State<'_, AppState>,
    app_handle: AppHandle,
    request: ExecuteQueryRequest,
) -> Result<ExecuteQueryResult, AppError> {
    let start = Instant::now();
    let org_id = require_active_org(&state).await?;
    let client = get_active_client(&state).await?;

    info!("[Query] Executing query with strategy: {}", request.strategy);

    // Emit initial progress
    let _ = app_handle.emit("query:progress", QueryProgressEvent {
        records_fetched: 0,
        total_expected: None,
        phase: "starting".to_string(),
        job_state: None,
    });

    // Determine strategy
    let chosen_strategy = match request.strategy.to_lowercase().as_str() {
        "rest" => QueryStrategy::Rest,
        "bulk" => QueryStrategy::Bulk,
        _ => {
            // Auto: determine based on count
            let _ = app_handle.emit("query:progress", QueryProgressEvent {
                records_fetched: 0,
                total_expected: None,
                phase: "counting".to_string(),
                job_state: None,
            });

            let rest_client = RestQueryClient::new(client.clone());
            let prefs = QueryPreferences::default();

            // Execute COUNT query
            let count_result = count_query(&rest_client, &request.soql, &prefs).await?;

            // Determine strategy from count result
            match determine_strategy(&request.soql, count_result) {
                StrategyDecision::Determined(strategy) => strategy,
                StrategyDecision::NeedUserDecision { reason, suggested } => {
                    return Err(AppError::StrategySelectionRequired {
                        reason,
                        suggested_strategy: format!("{:?}", suggested),
                    });
                }
            }
        }
    };

    let result = match chosen_strategy {
        QueryStrategy::Rest => {
            execute_rest_query(&client, &app_handle, &request).await?
        }
        QueryStrategy::Bulk => {
            execute_bulk_query(&state, &app_handle, &request).await?
        }
        QueryStrategy::Auto => {
            // This shouldn't happen since Auto is handled above
            execute_rest_query(&client, &app_handle, &request).await?
        }
    };

    let duration_ms = start.elapsed().as_millis() as u64;

    // Record in history
    let history_entry = QueryHistoryEntry {
        id: generate_id(),
        org_id,
        soql: request.soql.clone(),
        executed_at: current_timestamp(),
        row_count: Some(result.total_size as i64),
        strategy: Some(result.strategy_used.clone()),
        duration_ms: Some(duration_ms as i64),
        truncated: result.truncated,
        export_path: result.export_path.clone(),
    };
    // Ignore history insert errors (don't fail the query)
    let _ = state.db.insert_query_history(history_entry).await;

    // Emit completion
    let _ = app_handle.emit("query:progress", QueryProgressEvent {
        records_fetched: result.records.len() as u64,
        total_expected: Some(result.total_size),
        phase: "complete".to_string(),
        job_state: None,
    });

    Ok(ExecuteQueryResult {
        duration_ms,
        ..result
    })
}

/// Executes a query using the REST API.
async fn execute_rest_query(
    client: &SalesforceClient,
    app_handle: &AppHandle,
    request: &ExecuteQueryRequest,
) -> Result<ExecuteQueryResult, AppError> {
    let rest_client = RestQueryClient::new(client.clone());

    // Clone app_handle for the closure
    let app = app_handle.clone();

    // Execute with progress callback
    let query_result = rest_client
        .query_with_progress(
            &request.soql,
            request.max_rows,
            Some(move |count| {
                let _ = app.emit("query:progress", QueryProgressEvent {
                    records_fetched: count,
                    total_expected: None,
                    phase: "fetching".to_string(),
                    job_state: None,
                });
            }),
        )
        .await?;

    let truncated = !query_result.done && request.max_rows.is_some();

    Ok(ExecuteQueryResult {
        records: query_result.records,
        total_size: query_result.total_size,
        done: query_result.done,
        truncated,
        strategy_used: "rest".to_string(),
        duration_ms: 0, // Filled in by caller
        export_path: None,
    })
}

/// Executes a query using the Bulk API v2.
async fn execute_bulk_query(
    state: &AppState,
    app_handle: &AppHandle,
    request: &ExecuteQueryRequest,
) -> Result<ExecuteQueryResult, AppError> {
    let bulk_client = get_bulk_client(state).await?;

    // Emit job creation phase
    let _ = app_handle.emit("query:progress", QueryProgressEvent {
        records_fetched: 0,
        total_expected: None,
        phase: "creating_job".to_string(),
        job_state: Some("UploadComplete".to_string()),
    });

    // Create the bulk query job (returns job ID as String)
    let job_id = bulk_client.create_query_job(&request.soql).await?;

    info!("[Query] Created bulk query job: {}", job_id);

    // Poll for job completion with exponential backoff
    let mut poll_interval_ms = 1000u64; // Start at 1 second
    const MAX_POLL_INTERVAL_MS: u64 = 10000; // Max 10 seconds

    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(poll_interval_ms)).await;

        let status = bulk_client.get_query_job_status(&job_id).await?;

        // Emit progress
        let _ = app_handle.emit("query:progress", QueryProgressEvent {
            records_fetched: status.number_records_processed.unwrap_or(0),
            total_expected: None,
            phase: "polling".to_string(),
            job_state: Some(format!("{:?}", status.state)),
        });

        match status.state {
            BulkJobState::JobComplete => {
                info!("[Query] Bulk job complete: {} records", status.number_records_processed.unwrap_or(0));
                break;
            }
            BulkJobState::Failed | BulkJobState::Aborted => {
                return Err(AppError::JobFailed {
                    job_id: job_id.clone(),
                    message: format!("Job state: {:?}", status.state),
                });
            }
            _ => {
                // Still in progress, increase poll interval (exponential backoff)
                poll_interval_ms = (poll_interval_ms * 2).min(MAX_POLL_INTERVAL_MS);
            }
        }
    }

    // Download results
    let _ = app_handle.emit("query:progress", QueryProgressEvent {
        records_fetched: 0,
        total_expected: None,
        phase: "downloading".to_string(),
        job_state: Some("JobComplete".to_string()),
    });

    // Determine output path
    let export_path = if let Some(path) = &request.export_path {
        PathBuf::from(path)
    } else {
        // Use temp directory with job ID
        std::env::temp_dir().join(format!("stampede_query_{}.csv", job_id))
    };

    // Download to file
    bulk_client
        .download_query_results(&job_id, &export_path)
        .await?;

    info!("[Query] Downloaded results to {:?}", export_path);

    // Get final job status for record count
    let final_status = bulk_client.get_query_job_status(&job_id).await?;
    let total_records = final_status.number_records_processed.unwrap_or(0);

    Ok(ExecuteQueryResult {
        records: Vec::new(), // Bulk exports don't return records in memory
        total_size: total_records,
        done: true,
        truncated: false,
        strategy_used: "bulk".to_string(),
        duration_ms: 0, // Filled in by caller
        export_path: Some(export_path.to_string_lossy().to_string()),
    })
}

/// Saves a query for later use.
///
/// # Arguments
///
/// * `state` - Application state
/// * `name` - Display name for the saved query
/// * `soql` - The SOQL query to save
///
/// # Returns
///
/// The saved query info.
#[tauri::command]
pub async fn save_query(
    state: State<'_, AppState>,
    name: String,
    soql: String,
) -> Result<SavedQueryInfo, AppError> {
    let org_id = require_active_org(&state).await?;

    let saved_query = SavedQuery {
        id: generate_id(),
        org_id,
        name: name.clone(),
        soql: soql.clone(),
        created_at: current_timestamp(),
    };

    state.db.insert_saved_query(saved_query.clone()).await?;

    info!("[Query] Saved query: {}", name);

    Ok(SavedQueryInfo::from(saved_query))
}

/// Lists all saved queries for the active org.
///
/// # Returns
///
/// List of saved queries, ordered by name.
#[tauri::command]
pub async fn get_saved_queries(
    state: State<'_, AppState>,
) -> Result<Vec<SavedQueryInfo>, AppError> {
    let org_id = require_active_org(&state).await?;

    let queries = state.db.list_saved_queries(&org_id).await?;

    Ok(queries.into_iter().map(SavedQueryInfo::from).collect())
}

/// Deletes a saved query.
///
/// # Arguments
///
/// * `query_id` - The ID of the saved query to delete
#[tauri::command]
pub async fn delete_saved_query(
    state: State<'_, AppState>,
    query_id: String,
) -> Result<(), AppError> {
    // Verify active org (for consistency)
    let _ = require_active_org(&state).await?;

    state.db.delete_saved_query(&query_id).await?;

    info!("[Query] Deleted saved query: {}", query_id);

    Ok(())
}

/// Gets query history for the active org.
///
/// # Arguments
///
/// * `limit` - Maximum number of entries to return (default 50)
/// * `offset` - Offset for pagination (default 0)
///
/// # Returns
///
/// List of query history entries, most recent first.
#[tauri::command]
pub async fn get_query_history(
    state: State<'_, AppState>,
    limit: Option<u32>,
    offset: Option<u32>,
) -> Result<Vec<QueryHistoryInfo>, AppError> {
    let org_id = require_active_org(&state).await?;

    let limit = limit.unwrap_or(50);
    let offset = offset.unwrap_or(0);

    let entries = state.db.list_query_history(&org_id, limit, offset).await?;

    Ok(entries.into_iter().map(QueryHistoryInfo::from).collect())
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper Functions
// ─────────────────────────────────────────────────────────────────────────────

/// Generates a unique ID for database records.
fn generate_id() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let bytes: [u8; 16] = rng.gen();
    base64::Engine::encode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, bytes)
}

/// Returns current timestamp in milliseconds.
fn current_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
