//! Token refresh logic for Salesforce OAuth.
//!
//! Handles exchanging a refresh token for a new access token without
//! requiring user interaction.

use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use tracing::{error, info};

use crate::error::AppError;

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

/// OAuth Client ID for the Salesforce Connected App.
/// Must match the CLIENT_ID used in auth.rs.
pub(crate) const CLIENT_ID: &str = "3MVG9PwZx9R6_Ur.SFyEjmQZHmZd3aODqPzxLowB4KLoLmG8LTk.7n_XNfBF5cRFq5TqX3Hk62I33LIb_85V4";

// ─────────────────────────────────────────────────────────────────────────────
// Response Types
// ─────────────────────────────────────────────────────────────────────────────

/// Response from the token refresh endpoint.
#[derive(Debug, Deserialize)]
pub struct AccessTokenResponse {
    /// The new access token.
    pub access_token: String,
    /// The instance URL (may change due to org migrations).
    pub instance_url: String,
    /// Token type (usually "Bearer").
    #[serde(default)]
    #[allow(dead_code)]
    pub token_type: String,
    /// Issued at timestamp.
    #[serde(default)]
    #[allow(dead_code)]
    pub issued_at: String,
}

// ─────────────────────────────────────────────────────────────────────────────
// Token Refresh
// ─────────────────────────────────────────────────────────────────────────────

/// Refreshes an access token using a refresh token.
///
/// # Arguments
///
/// * `http` - The HTTP client to use
/// * `login_url` - The Salesforce login URL (e.g., "https://login.salesforce.com")
/// * `refresh_token` - The refresh token from the original OAuth flow
/// * `client_id` - The OAuth client ID
///
/// # Returns
///
/// The new access token and instance URL on success.
///
/// # Errors
///
/// - `AppError::SessionExpired` - The refresh token is invalid or expired
/// - `AppError::ConnectionFailed` - Network error during refresh
///
/// # Security
///
/// This function never logs the refresh token or the new access token.
pub async fn refresh_access_token(
    http: &reqwest::Client,
    login_url: &str,
    refresh_token: &SecretString,
    client_id: &str,
) -> Result<AccessTokenResponse, AppError> {
    let token_url = format!("{}/services/oauth2/token", login_url);

    info!("[SFDC] Refreshing access token...");

    let params = [
        ("grant_type", "refresh_token"),
        ("client_id", client_id),
        ("refresh_token", refresh_token.expose_secret()),
    ];

    let response = http
        .post(&token_url)
        .form(&params)
        .send()
        .await
        .map_err(|_| {
            error!("[SFDC] Token refresh request failed");
            AppError::ConnectionFailed("Failed to connect for token refresh".to_string())
        })?;

    let status = response.status();

    if status.is_success() {
        let token_response: AccessTokenResponse = response.json().await.map_err(|_| {
            error!("[SFDC] Failed to parse token refresh response");
            AppError::Internal("Invalid token refresh response".to_string())
        })?;

        info!("[SFDC] Token refresh successful");
        Ok(token_response)
    } else if status == reqwest::StatusCode::BAD_REQUEST
        || status == reqwest::StatusCode::UNAUTHORIZED
    {
        // Refresh token is invalid or expired
        error!("[SFDC] Token refresh failed: {}", status);
        Err(AppError::SessionExpired)
    } else {
        error!("[SFDC] Token refresh failed with status: {}", status);
        Err(AppError::OAuthError("Token refresh failed".to_string()))
    }
}

/// Determines the login URL to use for token refresh.
///
/// Salesforce requires using the original login domain for refresh,
/// not the instance URL.
pub fn get_login_url_for_refresh(instance_url: &str) -> String {
    // Check if this is a sandbox instance
    if instance_url.contains(".sandbox.")
        || instance_url.contains("test.salesforce.com")
        || instance_url.contains("--")
    {
        // Sandbox instances typically use scratch org URLs with "--" or ".sandbox."
        "https://test.salesforce.com".to_string()
    } else {
        "https://login.salesforce.com".to_string()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_login_url_production() {
        assert_eq!(
            get_login_url_for_refresh("https://na1.salesforce.com"),
            "https://login.salesforce.com"
        );
        assert_eq!(
            get_login_url_for_refresh("https://na99.salesforce.com"),
            "https://login.salesforce.com"
        );
    }

    #[test]
    fn get_login_url_sandbox() {
        assert_eq!(
            get_login_url_for_refresh("https://test.salesforce.com"),
            "https://test.salesforce.com"
        );
        assert_eq!(
            get_login_url_for_refresh("https://cs1.salesforce.com"),
            "https://login.salesforce.com" // CS instances are production
        );
    }

    #[test]
    fn get_login_url_scratch_org() {
        // Scratch orgs use "--" in their URL
        assert_eq!(
            get_login_url_for_refresh("https://ability-inspiration-1234--dev.scratch.my.salesforce.com"),
            "https://test.salesforce.com"
        );
    }

    #[test]
    fn get_login_url_sandbox_mydomain() {
        assert_eq!(
            get_login_url_for_refresh("https://mycompany.sandbox.my.salesforce.com"),
            "https://test.salesforce.com"
        );
    }
}

#[cfg(test)]
mod wiremock_tests {
    use super::*;
    use wiremock::matchers::{body_string_contains, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn refresh_token_success() {
        // Start mock server
        let mock_server = MockServer::start().await;

        // Configure mock to return success
        Mock::given(method("POST"))
            .and(path("/services/oauth2/token"))
            .and(body_string_contains("grant_type=refresh_token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "new_access_token_xyz",
                "instance_url": "https://na99.salesforce.com",
                "token_type": "Bearer",
                "issued_at": "1234567890"
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        // Create HTTP client and call refresh
        let http = reqwest::Client::new();
        let refresh_token = SecretString::from("test_refresh_token".to_string());

        let result = refresh_access_token(
            &http,
            &mock_server.uri(),
            &refresh_token,
            "test_client_id",
        )
        .await;

        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.access_token, "new_access_token_xyz");
        assert_eq!(response.instance_url, "https://na99.salesforce.com");
    }

    #[tokio::test]
    async fn refresh_token_expired_returns_session_expired() {
        let mock_server = MockServer::start().await;

        // Configure mock to return 401 (invalid/expired refresh token)
        Mock::given(method("POST"))
            .and(path("/services/oauth2/token"))
            .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
                "error": "invalid_grant",
                "error_description": "expired access/refresh token"
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let http = reqwest::Client::new();
        let refresh_token = SecretString::from("expired_token".to_string());

        let result = refresh_access_token(
            &http,
            &mock_server.uri(),
            &refresh_token,
            "test_client_id",
        )
        .await;

        assert!(result.is_err());
        assert!(matches!(result, Err(AppError::SessionExpired)));
    }

    #[tokio::test]
    async fn refresh_token_bad_request_returns_session_expired() {
        let mock_server = MockServer::start().await;

        // Configure mock to return 400 (bad request - invalid refresh token)
        Mock::given(method("POST"))
            .and(path("/services/oauth2/token"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "invalid_grant",
                "error_description": "refresh token invalid"
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let http = reqwest::Client::new();
        let refresh_token = SecretString::from("invalid_token".to_string());

        let result = refresh_access_token(
            &http,
            &mock_server.uri(),
            &refresh_token,
            "test_client_id",
        )
        .await;

        assert!(result.is_err());
        assert!(matches!(result, Err(AppError::SessionExpired)));
    }

    #[tokio::test]
    async fn refresh_token_sends_correct_params() {
        let mock_server = MockServer::start().await;

        // Configure mock to verify request parameters
        Mock::given(method("POST"))
            .and(path("/services/oauth2/token"))
            .and(body_string_contains("grant_type=refresh_token"))
            .and(body_string_contains("client_id=my_client_id"))
            .and(body_string_contains("refresh_token=my_refresh_token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "new_token",
                "instance_url": "https://test.salesforce.com"
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let http = reqwest::Client::new();
        let refresh_token = SecretString::from("my_refresh_token".to_string());

        let result = refresh_access_token(
            &http,
            &mock_server.uri(),
            &refresh_token,
            "my_client_id",
        )
        .await;

        // If the mock matched, the request had correct params
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn refresh_token_handles_instance_url_change() {
        let mock_server = MockServer::start().await;

        // Simulate instance URL change (org migration)
        Mock::given(method("POST"))
            .and(path("/services/oauth2/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "new_token",
                "instance_url": "https://new-instance.salesforce.com"
            })))
            .expect(1)
            .mount(&mock_server)
            .await;

        let http = reqwest::Client::new();
        let refresh_token = SecretString::from("token".to_string());

        let result = refresh_access_token(
            &http,
            &mock_server.uri(),
            &refresh_token,
            "client_id",
        )
        .await;

        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.instance_url, "https://new-instance.salesforce.com");
    }

    #[tokio::test]
    async fn refresh_token_server_error_returns_oauth_error() {
        let mock_server = MockServer::start().await;

        // Configure mock to return 500
        Mock::given(method("POST"))
            .and(path("/services/oauth2/token"))
            .respond_with(ResponseTemplate::new(500))
            .expect(1)
            .mount(&mock_server)
            .await;

        let http = reqwest::Client::new();
        let refresh_token = SecretString::from("token".to_string());

        let result = refresh_access_token(
            &http,
            &mock_server.uri(),
            &refresh_token,
            "client_id",
        )
        .await;

        assert!(result.is_err());
        assert!(matches!(result, Err(AppError::OAuthError(_))));
    }
}
