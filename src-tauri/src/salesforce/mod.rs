//! Salesforce HTTP client and API interaction layer.
//!
//! This module provides a thread-safe HTTP client for communicating with
//! Salesforce APIs. Key features:
//!
//! - **Secure credential handling** via `secrecy::SecretString`
//! - **Safe logging** that never leaks tokens or sensitive URL parameters
//! - **Thread-safe** design with `RwLock` for credentials and `Mutex` for refresh
//! - **OAuth 2.0 with PKCE** for secure authentication

pub mod auth;
pub mod client;

pub use auth::{start_login_flow, LoginType};
pub use client::{LoggingMode, OrgCredentials, SalesforceClient};
