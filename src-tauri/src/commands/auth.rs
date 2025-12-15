//! Authentication and org management Tauri commands.
//!
//! These commands handle:
//! - OAuth login flow
//! - Listing connected orgs
//! - Switching active org
//! - Logging out (removing org and tokens)

use serde::Serialize;
use tauri::State;
use tracing::info;

use crate::error::AppError;
use crate::salesforce::auth::{start_login_flow, LoginType};
use crate::state::AppState;
use crate::storage::credentials as keychain;
use crate::storage::Org;

// ─────────────────────────────────────────────────────────────────────────────
// Response Types
// ─────────────────────────────────────────────────────────────────────────────

/// Org info returned to the frontend (no sensitive data).
#[derive(Debug, Clone, Serialize)]
pub struct OrgInfo {
    pub id: String,
    pub name: String,
    pub org_type: String,
    pub instance_url: String,
    pub username: String,
}

impl From<Org> for OrgInfo {
    fn from(org: Org) -> Self {
        Self {
            id: org.id,
            name: org.name,
            org_type: org.org_type,
            instance_url: org.instance_url,
            username: org.username,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Commands
// ─────────────────────────────────────────────────────────────────────────────

/// Initiates the OAuth login flow for a Salesforce org.
///
/// # Arguments
///
/// * `state` - Application state
/// * `login_type` - "production", "sandbox", or a custom domain
///
/// # Returns
///
/// The newly authenticated org info on success.
#[tauri::command]
pub async fn login(state: State<'_, AppState>, login_type: String) -> Result<OrgInfo, AppError> {
    let lt = parse_login_type(&login_type);

    info!("Starting login flow for {:?}", lt);

    // Run the OAuth flow
    let creds = start_login_flow(lt).await?;

    // Determine org type based on instance URL
    let org_type = determine_org_type(&creds.instance_url);

    // Get current timestamp
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    // Build org record
    let org = Org {
        id: creds.org_id.clone(),
        name: extract_org_name(&creds.instance_url),
        org_type,
        instance_url: creds.instance_url.clone(),
        login_url: determine_login_url(&creds.instance_url),
        username: creds.username.clone(),
        api_version: creds.api_version.clone(),
        created_at: now,
        updated_at: now,
    };

    // Upsert the org (handles both new and re-auth)
    state.db.upsert_org(org.clone()).await?;

    // Set as active org
    state.set_active_org_id(Some(creds.org_id.clone())).await;

    info!("Login successful for org: {}", creds.org_id);

    Ok(OrgInfo::from(org))
}

/// Lists all connected Salesforce orgs.
///
/// # Returns
///
/// A list of org info for all stored orgs.
#[tauri::command]
pub async fn list_orgs(state: State<'_, AppState>) -> Result<Vec<OrgInfo>, AppError> {
    let orgs = state.db.list_orgs().await?;
    Ok(orgs.into_iter().map(OrgInfo::from).collect())
}

/// Switches the active org.
///
/// # Arguments
///
/// * `org_id` - The ID of the org to switch to
///
/// # Returns
///
/// The org info for the newly active org.
#[tauri::command]
pub async fn switch_org(state: State<'_, AppState>, org_id: String) -> Result<OrgInfo, AppError> {
    // Verify the org exists
    let org = state
        .db
        .get_org(&org_id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("Org not found: {}", org_id)))?;

    // Set as active
    state.set_active_org_id(Some(org_id.clone())).await;

    info!("Switched to org: {}", org_id);

    Ok(OrgInfo::from(org))
}

/// Gets the currently active org, if any.
///
/// # Returns
///
/// The active org info, or None if no org is active.
#[tauri::command]
pub async fn get_active_org(state: State<'_, AppState>) -> Result<Option<OrgInfo>, AppError> {
    let active_id = state.get_active_org_id().await;

    match active_id {
        Some(org_id) => {
            let org = state.db.get_org(&org_id).await?;
            Ok(org.map(OrgInfo::from))
        }
        None => Ok(None),
    }
}

/// Logs out from an org, removing it and its tokens.
///
/// # Arguments
///
/// * `org_id` - The ID of the org to log out from
#[tauri::command]
pub async fn logout(state: State<'_, AppState>, org_id: String) -> Result<(), AppError> {
    info!("Logging out from org: {}", org_id);

    // Delete tokens from keychain (ignore errors - might not exist)
    let _ = keychain::delete_tokens(&org_id).await;

    // Delete org from database
    state.db.delete_org(&org_id).await?;

    // Clear active org if this was it
    let active_id = state.get_active_org_id().await;
    if active_id.as_deref() == Some(&org_id) {
        state.set_active_org_id(None).await;
    }

    info!("Logout complete for org: {}", org_id);

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper Functions
// ─────────────────────────────────────────────────────────────────────────────

/// Parses the login type string from the frontend.
fn parse_login_type(s: &str) -> LoginType {
    match s.to_lowercase().as_str() {
        "production" => LoginType::Production,
        "sandbox" => LoginType::Sandbox,
        _ => {
            // Treat as custom domain
            LoginType::Custom(s.to_string())
        }
    }
}

/// Determines the org type (Production/Sandbox) from the instance URL.
fn determine_org_type(instance_url: &str) -> String {
    if instance_url.contains(".sandbox.")
        || instance_url.contains("test.salesforce.com")
        || instance_url.contains("--")
    {
        "Sandbox".to_string()
    } else {
        "Production".to_string()
    }
}

/// Determines the login URL to use for this org.
fn determine_login_url(instance_url: &str) -> String {
    if instance_url.contains(".sandbox.")
        || instance_url.contains("test.salesforce.com")
        || instance_url.contains("--")
    {
        "https://test.salesforce.com".to_string()
    } else {
        "https://login.salesforce.com".to_string()
    }
}

/// Extracts a display name from the instance URL.
fn extract_org_name(instance_url: &str) -> String {
    // Try to extract the subdomain as the org name
    if let Ok(url) = url::Url::parse(instance_url) {
        if let Some(host) = url.host_str() {
            // e.g., "na1.salesforce.com" -> "na1"
            // e.g., "mycompany.my.salesforce.com" -> "mycompany"
            // e.g., "mycompany--dev.sandbox.my.salesforce.com" -> "mycompany--dev"
            let parts: Vec<&str> = host.split('.').collect();
            if !parts.is_empty() {
                return parts[0].to_string();
            }
        }
    }
    // Fallback: use the full URL
    instance_url.to_string()
}
