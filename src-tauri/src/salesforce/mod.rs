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
//! - **Bulk API v2** for large data exports with streaming to disk

pub mod auth;
pub mod bulk_query_v2;
pub mod client;
pub mod query_strategy;
pub(crate) mod refresh;
pub mod rest;

/// Salesforce REST API version used for all API calls.
/// Single source of truth for the API version across the application.
pub const API_VERSION: &str = "v60.0";

pub use auth::{start_login_flow, LoginType};
pub use bulk_query_v2::{BulkQueryJobInfo, BulkQueryJobState, BulkQueryV2Client};
pub use client::{LoggingMode, OrgCredentials, SalesforceClient};
pub use query_strategy::{
    count_query, determine_strategy, determine_strategy_from_count, make_count_soql,
    CountResult, QueryPreferences, QueryStrategy, StrategyDecision,
};
pub use rest::{QueryResult, RestQueryClient};
