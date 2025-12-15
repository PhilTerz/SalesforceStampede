//! Secure credential storage using the OS keychain.
//!
//! Stores OAuth tokens per Salesforce org using the `keyring` crate.
//! Tokens are never logged - the `TokenPair` type implements a custom
//! `Debug` that redacts all secret values.

use crate::error::AppError;
use serde::{Deserialize, Serialize};
use std::fmt;

/// The service name used for all keychain entries.
const SERVICE_NAME: &str = "salesforce-power-toolkit";

/// Returns the namespaced keychain key for an org's tokens.
fn keychain_key(org_id: &str) -> String {
    format!("tokens:{org_id}")
}

/// OAuth token pair for a Salesforce org.
///
/// # Security
/// This type's `Debug` implementation redacts all token values to prevent
/// accidental logging of secrets.
#[derive(Clone)]
pub struct TokenPair {
    pub access_token: String,
    pub refresh_token: String,
}

impl fmt::Debug for TokenPair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenPair")
            .field("access_token", &"***")
            .field("refresh_token", &"***")
            .finish()
    }
}

/// Internal type for JSON serialization of tokens.
#[derive(Serialize, Deserialize)]
struct StoredTokens {
    access_token: String,
    refresh_token: String,
}

impl From<StoredTokens> for TokenPair {
    fn from(stored: StoredTokens) -> Self {
        TokenPair {
            access_token: stored.access_token,
            refresh_token: stored.refresh_token,
        }
    }
}

/// Stores OAuth tokens for a Salesforce org in the OS keychain.
///
/// # Arguments
/// * `org_id` - The Salesforce org ID (used as the keychain entry identifier)
/// * `access` - The OAuth access token
/// * `refresh` - The OAuth refresh token
///
/// # Errors
/// Returns `AppError::Internal` if the keychain operation fails.
pub async fn store_tokens(org_id: &str, access: &str, refresh: &str) -> Result<(), AppError> {
    let key = keychain_key(org_id);
    let stored = StoredTokens {
        access_token: access.to_string(),
        refresh_token: refresh.to_string(),
    };

    let json = serde_json::to_string(&stored)
        .map_err(|_| AppError::Internal("Failed to serialize tokens.".into()))?;

    tokio::task::spawn_blocking(move || {
        let entry = keyring::Entry::new(SERVICE_NAME, &key)
            .map_err(|_| AppError::Internal("Failed to access keychain.".into()))?;

        entry
            .set_password(&json)
            .map_err(|_| AppError::Internal("Failed to store credentials.".into()))?;

        Ok(())
    })
    .await
    .map_err(|_| AppError::Internal("Keychain task failed.".into()))?
}

/// Retrieves OAuth tokens for a Salesforce org from the OS keychain.
///
/// # Arguments
/// * `org_id` - The Salesforce org ID
///
/// # Errors
/// Returns `AppError::NotAuthenticated` if no tokens exist for the org.
/// Returns `AppError::Internal` for other keychain errors.
pub async fn get_tokens(org_id: &str) -> Result<TokenPair, AppError> {
    let key = keychain_key(org_id);

    tokio::task::spawn_blocking(move || {
        let entry = keyring::Entry::new(SERVICE_NAME, &key)
            .map_err(|_| AppError::Internal("Failed to access keychain.".into()))?;

        let json = entry.get_password().map_err(|e| match e {
            keyring::Error::NoEntry => AppError::NotAuthenticated,
            _ => AppError::Internal("Failed to retrieve credentials.".into()),
        })?;

        let stored: StoredTokens = serde_json::from_str(&json)
            .map_err(|_| AppError::Internal("Failed to parse stored tokens.".into()))?;

        Ok(stored.into())
    })
    .await
    .map_err(|_| AppError::Internal("Keychain task failed.".into()))?
}

/// Deletes OAuth tokens for a Salesforce org from the OS keychain.
///
/// This operation is idempotent: deleting non-existent tokens succeeds silently.
///
/// # Arguments
/// * `org_id` - The Salesforce org ID
///
/// # Errors
/// Returns `AppError::Internal` for keychain access errors.
pub async fn delete_tokens(org_id: &str) -> Result<(), AppError> {
    let key = keychain_key(org_id);

    tokio::task::spawn_blocking(move || {
        let entry = keyring::Entry::new(SERVICE_NAME, &key)
            .map_err(|_| AppError::Internal("Failed to access keychain.".into()))?;

        match entry.delete_credential() {
            Ok(()) => Ok(()),
            Err(keyring::Error::NoEntry) => Ok(()), // Idempotent: already deleted
            Err(_) => Err(AppError::Internal("Failed to delete credentials.".into())),
        }
    })
    .await
    .map_err(|_| AppError::Internal("Keychain task failed.".into()))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_pair_debug_redacts_secrets() {
        let tokens = TokenPair {
            access_token: "super_secret_access_token_12345".to_string(),
            refresh_token: "super_secret_refresh_token_67890".to_string(),
        };

        let debug_output = format!("{:?}", tokens);

        // The actual token values must NOT appear in debug output
        // Note: Don't print debug_output in failure message to avoid leaking secrets in CI
        assert!(
            !debug_output.contains("super_secret_access_token_12345"),
            "Debug output leaked access token"
        );
        assert!(
            !debug_output.contains("super_secret_refresh_token_67890"),
            "Debug output leaked refresh token"
        );

        // Should show redacted placeholder
        assert!(
            debug_output.contains("***"),
            "Debug output missing redaction marker"
        );

        // Should still identify the struct
        assert!(
            debug_output.contains("TokenPair"),
            "Debug output missing type identifier"
        );
    }

    #[test]
    fn stored_tokens_json_roundtrip() {
        let original = StoredTokens {
            access_token: "access_abc123".to_string(),
            refresh_token: "refresh_xyz789".to_string(),
        };

        // Serialize to JSON
        let json = serde_json::to_string(&original).expect("Failed to serialize");

        // Verify JSON structure
        assert!(json.contains("access_token"));
        assert!(json.contains("refresh_token"));
        assert!(json.contains("access_abc123"));
        assert!(json.contains("refresh_xyz789"));

        // Deserialize back
        let parsed: StoredTokens = serde_json::from_str(&json).expect("Failed to deserialize");

        assert_eq!(parsed.access_token, original.access_token);
        assert_eq!(parsed.refresh_token, original.refresh_token);
    }

    #[test]
    fn stored_tokens_converts_to_token_pair() {
        let stored = StoredTokens {
            access_token: "access".to_string(),
            refresh_token: "refresh".to_string(),
        };

        let pair: TokenPair = stored.into();

        assert_eq!(pair.access_token, "access");
        assert_eq!(pair.refresh_token, "refresh");
    }

    /// Integration test for real keychain operations.
    ///
    /// This test is ignored by default because:
    /// - It requires OS keychain access (may prompt for permission)
    /// - It modifies the actual keychain
    /// - It may be flaky in CI environments
    ///
    /// To run locally:
    /// ```
    /// cargo test --package salesforcestampede -- storage::credentials::tests::keychain_integration --ignored --nocapture
    /// ```
    #[tokio::test]
    #[ignore]
    async fn keychain_integration() {
        let test_org_id = "test_org_00Dxxxxxxxxxx";
        let access = "test_access_token";
        let refresh = "test_refresh_token";

        // Clean up any previous test data (idempotent, won't fail if missing)
        delete_tokens(test_org_id)
            .await
            .expect("Cleanup should succeed");

        // Store tokens
        store_tokens(test_org_id, access, refresh)
            .await
            .expect("Failed to store tokens");

        // Retrieve tokens
        let retrieved = get_tokens(test_org_id).await.expect("Failed to get tokens");

        assert_eq!(retrieved.access_token, access);
        assert_eq!(retrieved.refresh_token, refresh);

        // Delete tokens
        delete_tokens(test_org_id)
            .await
            .expect("Failed to delete tokens");

        // Verify deletion - get_tokens should return NotAuthenticated
        let result = get_tokens(test_org_id).await;
        assert!(
            matches!(result, Err(AppError::NotAuthenticated)),
            "Expected NotAuthenticated after deletion"
        );

        // Verify delete is idempotent
        delete_tokens(test_org_id)
            .await
            .expect("Deleting non-existent tokens should succeed");
    }
}
