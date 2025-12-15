//! Salesforce HTTP client and API interaction layer.
//!
//! This module provides a thread-safe HTTP client for communicating with
//! Salesforce APIs. Key features:
//!
//! - **Secure credential handling** via `secrecy::SecretString`
//! - **Safe logging** that never leaks tokens or sensitive URL parameters
//! - **Thread-safe** design with `RwLock` for credentials and `Mutex` for refresh
//! - **OAuth 2.0 with PKCE** for secure authentication
//! - **Automatic token refresh** with double-checked locking
//! - **REST API client** with pagination support for SOQL queries
//! - **Query strategy** for automatic REST vs Bulk API selection
//! - **Bulk API v2** for large data exports and ingest operations
//! - **Bulk job scheduler** for concurrency control

use serde::{Deserialize, Serialize};

pub mod auth;
pub mod bulk_ingest_v2;
pub mod bulk_query_v2;
pub mod bulk_scheduler;
pub mod client;
pub mod query_strategy;
pub(crate) mod refresh;
pub mod rest;

/// Salesforce REST API version used for all API calls.
/// Single source of truth for the API version across the application.
pub const API_VERSION: &str = "v60.0";

// ─────────────────────────────────────────────────────────────────────────────
// Shared Bulk API Types
// ─────────────────────────────────────────────────────────────────────────────

/// State of a Bulk API v2 job (shared by query and ingest operations).
///
/// This enum is used by both `BulkQueryV2Client` and `BulkIngestV2Client`
/// to represent job status from the Salesforce Bulk API v2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BulkJobState {
    /// Job has been created but data is not yet uploaded.
    Open,
    /// Data has been uploaded, job is queued for processing.
    UploadComplete,
    /// Job is actively being processed.
    InProgress,
    /// Job completed successfully.
    JobComplete,
    /// Job was aborted by user request.
    Aborted,
    /// Job failed due to an error.
    Failed,
    /// Catch-all for unexpected states to prevent deserialization panic.
    #[serde(other)]
    Unknown,
}

impl BulkJobState {
    /// Converts the state to its string representation for database storage.
    pub fn as_str(&self) -> &'static str {
        match self {
            BulkJobState::Open => "Open",
            BulkJobState::UploadComplete => "UploadComplete",
            BulkJobState::InProgress => "InProgress",
            BulkJobState::JobComplete => "JobComplete",
            BulkJobState::Aborted => "Aborted",
            BulkJobState::Failed => "Failed",
            BulkJobState::Unknown => "Unknown",
        }
    }

    /// Parses a string into a BulkJobState.
    pub fn from_str(s: &str) -> Self {
        match s {
            "Open" => BulkJobState::Open,
            "UploadComplete" => BulkJobState::UploadComplete,
            "InProgress" => BulkJobState::InProgress,
            "JobComplete" => BulkJobState::JobComplete,
            "Aborted" => BulkJobState::Aborted,
            "Failed" => BulkJobState::Failed,
            _ => BulkJobState::Unknown,
        }
    }

    /// Returns true if this is a terminal state (job cannot transition further).
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            BulkJobState::JobComplete | BulkJobState::Aborted | BulkJobState::Failed
        )
    }
}

pub use auth::{start_login_flow, LoginType};
pub use bulk_ingest_v2::{
    BulkIngestJobInfo, BulkIngestV2Client, BulkOperation, CreateIngestJobRequest, LineEnding,
};
pub use bulk_query_v2::{BulkQueryJobInfo, BulkQueryV2Client};
pub use bulk_scheduler::{BulkJobPermit, BulkJobScheduler};
pub use client::{LoggingMode, OrgCredentials, SalesforceClient};
pub use query_strategy::{
    count_query, determine_strategy, determine_strategy_from_count, make_count_soql, CountResult,
    QueryPreferences, QueryStrategy, StrategyDecision,
};
pub use rest::{QueryResult, RestQueryClient};
