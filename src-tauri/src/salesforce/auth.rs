//! OAuth 2.0 authentication flow with PKCE for Salesforce.
//!
//! Implements the Web Server OAuth flow with:
//! - PKCE (Proof Key for Code Exchange) for enhanced security
//! - Port fallback strategy for the callback server
//! - Secure credential handling with `SecretString`

use std::time::Duration;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use rand::Rng;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tracing::{error, info, warn};
use url::Url;

use crate::error::AppError;
use crate::salesforce::client::OrgCredentials;
use crate::storage::credentials as keychain;

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

/// OAuth Client ID for the Salesforce Connected App.
/// In production, this should be loaded from configuration.
const CLIENT_ID: &str =
    "3MVG9PwZx9R6_Ur.SFyEjmQZHmZd3aODqPzxLowB4KLoLmG8LTk.7n_XNfBF5cRFq5TqX3Hk62I33LIb_85V4";

/// Ports to try for the OAuth callback server, in order of preference.
/// Port 0 means "let the OS choose an available port".
const CALLBACK_PORTS: &[u16] = &[8477, 8478, 8479, 9876, 0];

/// Timeout for waiting for the OAuth callback (2 minutes).
const CALLBACK_TIMEOUT_SECS: u64 = 120;

/// Scopes requested from Salesforce.
const OAUTH_SCOPES: &[&str] = &["api", "refresh_token", "openid"];

/// Default API version.
const DEFAULT_API_VERSION: &str = "v60.0";

// ─────────────────────────────────────────────────────────────────────────────
// LoginType
// ─────────────────────────────────────────────────────────────────────────────

/// Specifies the Salesforce login environment.
#[derive(Debug, Clone)]
pub enum LoginType {
    /// Production environment (login.salesforce.com)
    Production,
    /// Sandbox environment (test.salesforce.com)
    Sandbox,
    /// Custom domain (e.g., my-domain.my.salesforce.com)
    Custom(String),
}

impl LoginType {
    /// Returns the login domain for this login type.
    pub fn domain(&self) -> &str {
        match self {
            LoginType::Production => "login.salesforce.com",
            LoginType::Sandbox => "test.salesforce.com",
            LoginType::Custom(domain) => domain,
        }
    }

    /// Returns the authorization URL for this login type.
    fn auth_url(&self) -> String {
        format!("https://{}/services/oauth2/authorize", self.domain())
    }

    /// Returns the token URL for this login type.
    fn token_url(&self) -> String {
        format!("https://{}/services/oauth2/token", self.domain())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PKCE
// ─────────────────────────────────────────────────────────────────────────────

/// PKCE challenge and verifier pair.
struct PkceChallenge {
    /// The verifier (kept secret, sent during token exchange).
    verifier: String,
    /// The challenge (sent during authorization, derived from verifier).
    challenge: String,
}

impl PkceChallenge {
    /// Generates a new PKCE challenge using S256 method.
    fn generate() -> Self {
        // Generate a random 32-byte verifier
        let mut rng = rand::thread_rng();
        let verifier_bytes: [u8; 32] = rng.gen();
        let verifier = URL_SAFE_NO_PAD.encode(verifier_bytes);

        // Create SHA256 hash and base64url encode it
        let mut hasher = Sha256::new();
        hasher.update(verifier.as_bytes());
        let hash = hasher.finalize();
        let challenge = URL_SAFE_NO_PAD.encode(hash);

        Self {
            verifier,
            challenge,
        }
    }
}

/// Generates a random state token for CSRF protection.
fn generate_state() -> String {
    let mut rng = rand::thread_rng();
    let state_bytes: [u8; 16] = rng.gen();
    URL_SAFE_NO_PAD.encode(state_bytes)
}

// ─────────────────────────────────────────────────────────────────────────────
// Port Binding
// ─────────────────────────────────────────────────────────────────────────────

/// Finds an available port for the OAuth callback server.
///
/// Tries ports in the priority list. If port 0 is used, the OS assigns an
/// available port.
///
/// # Returns
///
/// A tuple of (TcpListener, port) on success.
///
/// # Errors
///
/// Returns `AppError::PortBindFailed` if no port could be bound.
pub async fn find_available_port() -> Result<(TcpListener, u16), AppError> {
    for &port in CALLBACK_PORTS {
        let addr = format!("127.0.0.1:{}", port);
        match TcpListener::bind(&addr).await {
            Ok(listener) => {
                let actual_port = listener
                    .local_addr()
                    .map_err(|_| AppError::PortBindFailed)?
                    .port();
                info!("OAuth callback server bound to port {}", actual_port);
                return Ok((listener, actual_port));
            }
            Err(_) => {
                if port != 0 {
                    warn!("Port {} unavailable, trying next...", port);
                }
            }
        }
    }

    error!("Failed to bind to any callback port");
    Err(AppError::PortBindFailed)
}

// ─────────────────────────────────────────────────────────────────────────────
// Callback Parsing
// ─────────────────────────────────────────────────────────────────────────────

/// Parsed callback parameters from the OAuth redirect.
#[derive(Debug)]
pub struct CallbackParams {
    pub code: String,
    pub state: String,
}

/// Parses the callback URL query string to extract code and state.
///
/// # Arguments
///
/// * `request_line` - The HTTP request line (e.g., "GET /callback?code=...&state=... HTTP/1.1")
///
/// # Errors
///
/// Returns `AppError::OAuthError` if parsing fails or required parameters are missing.
pub fn parse_callback_request(request_line: &str) -> Result<CallbackParams, AppError> {
    // Extract the path and query from the request line
    // Format: "GET /callback?code=XXX&state=YYY HTTP/1.1"
    let parts: Vec<&str> = request_line.split_whitespace().collect();
    if parts.len() < 2 {
        return Err(AppError::OAuthError("Invalid request format".into()));
    }

    let path_and_query = parts[1];

    // Parse as URL to extract query parameters
    let fake_base = format!("http://localhost{}", path_and_query);
    let url = Url::parse(&fake_base).map_err(|_| AppError::OAuthError("Invalid URL".into()))?;

    // Check path
    if url.path() != "/callback" {
        return Err(AppError::OAuthError(format!(
            "Unexpected path: {}",
            url.path()
        )));
    }

    // Extract query parameters
    let mut code = None;
    let mut state = None;

    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "code" => code = Some(value.to_string()),
            "state" => state = Some(value.to_string()),
            "error" => {
                let error_desc = url
                    .query_pairs()
                    .find(|(k, _)| k == "error_description")
                    .map(|(_, v)| v.to_string())
                    .unwrap_or_else(|| value.to_string());
                return Err(AppError::OAuthError(error_desc));
            }
            _ => {}
        }
    }

    let code = code.ok_or_else(|| AppError::OAuthError("Missing authorization code".into()))?;
    let state = state.ok_or_else(|| AppError::OAuthError("Missing state parameter".into()))?;

    Ok(CallbackParams { code, state })
}

// ─────────────────────────────────────────────────────────────────────────────
// Token Exchange Response
// ─────────────────────────────────────────────────────────────────────────────

/// Response from the OAuth token endpoint.
#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: Option<String>,
    instance_url: String,
    id: String, // Identity URL: https://login.salesforce.com/id/ORG_ID/USER_ID
    #[serde(default)]
    #[allow(dead_code)]
    token_type: String,
}

/// Response from the Salesforce Identity endpoint.
#[derive(Debug, Deserialize)]
struct IdentityResponse {
    user_id: String,
    organization_id: String,
    username: String,
}

// ─────────────────────────────────────────────────────────────────────────────
// OAuth Flow
// ─────────────────────────────────────────────────────────────────────────────

/// Starts the Salesforce OAuth login flow.
///
/// This function:
/// 1. Binds a local callback server
/// 2. Opens the browser to the Salesforce login page
/// 3. Waits for the OAuth callback
/// 4. Exchanges the authorization code for tokens
/// 5. Fetches user identity information
/// 6. Stores tokens in the keychain
///
/// # Arguments
///
/// * `login_type` - The type of Salesforce environment to log into
///
/// # Returns
///
/// The authenticated user's credentials on success.
///
/// # Errors
///
/// Returns various `AppError` variants for different failure modes:
/// - `PortBindFailed` - Could not bind callback server
/// - `OAuthError` - OAuth flow failed (user denied, invalid response, etc.)
/// - `ConnectionFailed` - Network error during token exchange
pub async fn start_login_flow(login_type: LoginType) -> Result<OrgCredentials, AppError> {
    // Step A: Setup
    let (listener, port) = find_available_port().await?;
    let redirect_uri = format!("http://127.0.0.1:{}/callback", port);

    let pkce = PkceChallenge::generate();
    let state = generate_state();

    // Build authorization URL
    let auth_url = build_auth_url(&login_type, &redirect_uri, &pkce.challenge, &state)?;

    // Step B: Open browser
    info!("Opening browser for Salesforce login...");
    open::that(&auth_url).map_err(|e| {
        error!("Failed to open browser: {}", e);
        AppError::OAuthError("Failed to open browser for login".into())
    })?;

    info!("Waiting for callback on port {}...", port);

    // Step C: Wait for callback
    let callback_params =
        wait_for_callback(listener, &state, Duration::from_secs(CALLBACK_TIMEOUT_SECS)).await?;

    // Step D: Exchange code for tokens
    let token_response = exchange_code(
        &login_type,
        &callback_params.code,
        &redirect_uri,
        &pkce.verifier,
    )
    .await?;

    // Convert tokens to SecretString immediately
    let access_token = SecretString::from(token_response.access_token);
    let refresh_token = token_response.refresh_token.map(SecretString::from);

    // Step E: Fetch identity
    let identity = fetch_identity(&token_response.id, &access_token).await?;

    info!("Login successful for user: {}", identity.username);

    // Step F: Store in keychain
    if let Some(ref refresh) = refresh_token {
        keychain::store_tokens(
            &identity.organization_id,
            access_token.expose_secret(),
            refresh.expose_secret(),
        )
        .await?;
    }

    // Build and return credentials
    Ok(OrgCredentials {
        org_id: identity.organization_id,
        user_id: identity.user_id,
        username: identity.username,
        instance_url: token_response.instance_url,
        access_token,
        refresh_token,
        api_version: DEFAULT_API_VERSION.to_string(),
    })
}

/// Builds the OAuth authorization URL with PKCE.
fn build_auth_url(
    login_type: &LoginType,
    redirect_uri: &str,
    code_challenge: &str,
    state: &str,
) -> Result<String, AppError> {
    let mut url = Url::parse(&login_type.auth_url())
        .map_err(|_| AppError::OAuthError("Invalid auth URL".into()))?;

    url.query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", CLIENT_ID)
        .append_pair("redirect_uri", redirect_uri)
        .append_pair("scope", &OAUTH_SCOPES.join(" "))
        .append_pair("state", state)
        .append_pair("code_challenge", code_challenge)
        .append_pair("code_challenge_method", "S256");

    Ok(url.to_string())
}

/// Waits for the OAuth callback on the given listener.
async fn wait_for_callback(
    listener: TcpListener,
    expected_state: &str,
    timeout: Duration,
) -> Result<CallbackParams, AppError> {
    let result = tokio::time::timeout(timeout, async {
        loop {
            let (mut stream, _addr) = listener.accept().await.map_err(|e| {
                error!("Failed to accept connection: {}", e);
                AppError::OAuthError("Callback server error".into())
            })?;

            let mut reader = BufReader::new(&mut stream);
            let mut request_line = String::new();

            reader.read_line(&mut request_line).await.map_err(|e| {
                error!("Failed to read request: {}", e);
                AppError::OAuthError("Failed to read callback".into())
            })?;

            // Parse the callback
            match parse_callback_request(&request_line) {
                Ok(params) => {
                    // Validate state
                    if params.state != expected_state {
                        warn!("State mismatch - possible CSRF attack");
                        send_error_response(&mut stream, "State mismatch").await;
                        return Err(AppError::OAuthError("State validation failed".into()));
                    }

                    // Send success response
                    send_success_response(&mut stream).await;
                    return Ok(params);
                }
                Err(e) => {
                    // Check if this is just a favicon request or similar
                    if request_line.contains("/favicon") {
                        send_not_found_response(&mut stream).await;
                        continue;
                    }

                    send_error_response(&mut stream, "Invalid callback").await;
                    return Err(e);
                }
            }
        }
    })
    .await;

    result.map_err(|_| {
        error!("OAuth callback timed out");
        AppError::OAuthError("Login timed out - please try again".into())
    })?
}

/// Sends an HTTP 200 success response.
async fn send_success_response(stream: &mut tokio::net::TcpStream) {
    let body = r#"<!DOCTYPE html>
<html>
<head><title>Login Successful</title></head>
<body style="font-family: system-ui; text-align: center; padding: 50px;">
<h1>Login Successful</h1>
<p>You may close this window and return to Stampede.</p>
</body>
</html>"#;

    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );

    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.flush().await;
}

/// Sends an HTTP 400 error response.
async fn send_error_response(stream: &mut tokio::net::TcpStream, message: &str) {
    let body = format!(
        r#"<!DOCTYPE html>
<html>
<head><title>Login Failed</title></head>
<body style="font-family: system-ui; text-align: center; padding: 50px;">
<h1>Login Failed</h1>
<p>{}</p>
<p>Please close this window and try again.</p>
</body>
</html>"#,
        message
    );

    let response = format!(
        "HTTP/1.1 400 Bad Request\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );

    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.flush().await;
}

/// Sends an HTTP 404 response for non-callback requests.
async fn send_not_found_response(stream: &mut tokio::net::TcpStream) {
    let response = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.flush().await;
}

/// Exchanges the authorization code for access and refresh tokens.
async fn exchange_code(
    login_type: &LoginType,
    code: &str,
    redirect_uri: &str,
    code_verifier: &str,
) -> Result<TokenResponse, AppError> {
    let client = reqwest::Client::new();

    let params = [
        ("grant_type", "authorization_code"),
        ("client_id", CLIENT_ID),
        ("code", code),
        ("redirect_uri", redirect_uri),
        ("code_verifier", code_verifier),
    ];

    let response = client
        .post(&login_type.token_url())
        .form(&params)
        .send()
        .await
        .map_err(|_| AppError::ConnectionFailed("Failed to connect for token exchange".into()))?;

    if !response.status().is_success() {
        let status = response.status();
        // Try to get error details, but don't expose them in the error message
        let _ = response.text().await;
        error!("Token exchange failed with status: {}", status);
        return Err(AppError::OAuthError("Token exchange failed".into()));
    }

    response
        .json::<TokenResponse>()
        .await
        .map_err(|_| AppError::OAuthError("Invalid token response".into()))
}

/// Fetches user identity information from Salesforce.
async fn fetch_identity(
    id_url: &str,
    access_token: &SecretString,
) -> Result<IdentityResponse, AppError> {
    let client = reqwest::Client::new();

    let response = client
        .get(id_url)
        .bearer_auth(access_token.expose_secret())
        .send()
        .await
        .map_err(|_| AppError::ConnectionFailed("Failed to fetch user identity".into()))?;

    if !response.status().is_success() {
        let status = response.status();
        error!("Identity request failed with status: {}", status);
        return Err(AppError::OAuthError(
            "Failed to verify user identity".into(),
        ));
    }

    response
        .json::<IdentityResponse>()
        .await
        .map_err(|_| AppError::OAuthError("Invalid identity response".into()))
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ─────────────────────────────────────────────────────────────────────────
    // LoginType Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn login_type_production_domain() {
        assert_eq!(LoginType::Production.domain(), "login.salesforce.com");
    }

    #[test]
    fn login_type_sandbox_domain() {
        assert_eq!(LoginType::Sandbox.domain(), "test.salesforce.com");
    }

    #[test]
    fn login_type_custom_domain() {
        let custom = LoginType::Custom("mycompany.my.salesforce.com".to_string());
        assert_eq!(custom.domain(), "mycompany.my.salesforce.com");
    }

    #[test]
    fn login_type_auth_urls() {
        assert_eq!(
            LoginType::Production.auth_url(),
            "https://login.salesforce.com/services/oauth2/authorize"
        );
        assert_eq!(
            LoginType::Sandbox.auth_url(),
            "https://test.salesforce.com/services/oauth2/authorize"
        );
    }

    #[test]
    fn login_type_token_urls() {
        assert_eq!(
            LoginType::Production.token_url(),
            "https://login.salesforce.com/services/oauth2/token"
        );
        assert_eq!(
            LoginType::Sandbox.token_url(),
            "https://test.salesforce.com/services/oauth2/token"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // PKCE Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn pkce_generates_valid_verifier() {
        let pkce = PkceChallenge::generate();

        // Verifier should be base64url encoded 32 bytes = 43 characters
        assert!(pkce.verifier.len() >= 40);
        assert!(pkce.verifier.len() <= 50);

        // Should only contain URL-safe characters
        assert!(pkce
            .verifier
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_'));
    }

    #[test]
    fn pkce_generates_valid_challenge() {
        let pkce = PkceChallenge::generate();

        // Challenge should be base64url encoded SHA256 = 43 characters
        assert!(pkce.challenge.len() >= 40);
        assert!(pkce.challenge.len() <= 50);

        // Should only contain URL-safe characters
        assert!(pkce
            .challenge
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_'));
    }

    #[test]
    fn pkce_challenge_derived_from_verifier() {
        let pkce = PkceChallenge::generate();

        // Manually verify the challenge derivation
        let mut hasher = Sha256::new();
        hasher.update(pkce.verifier.as_bytes());
        let expected_hash = hasher.finalize();
        let expected_challenge = URL_SAFE_NO_PAD.encode(expected_hash);

        assert_eq!(pkce.challenge, expected_challenge);
    }

    #[test]
    fn pkce_generates_unique_values() {
        let pkce1 = PkceChallenge::generate();
        let pkce2 = PkceChallenge::generate();

        assert_ne!(pkce1.verifier, pkce2.verifier);
        assert_ne!(pkce1.challenge, pkce2.challenge);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // State Generation Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn state_generates_valid_token() {
        let state = generate_state();

        // State should be base64url encoded 16 bytes = 22 characters
        assert!(state.len() >= 20);
        assert!(state.len() <= 25);

        // Should only contain URL-safe characters
        assert!(state
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_'));
    }

    #[test]
    fn state_generates_unique_values() {
        let state1 = generate_state();
        let state2 = generate_state();

        assert_ne!(state1, state2);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Callback Parsing Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn parse_callback_extracts_code_and_state() {
        let request = "GET /callback?code=AUTH_CODE_123&state=STATE_TOKEN_456 HTTP/1.1";
        let result = parse_callback_request(request).unwrap();

        assert_eq!(result.code, "AUTH_CODE_123");
        assert_eq!(result.state, "STATE_TOKEN_456");
    }

    #[test]
    fn parse_callback_handles_url_encoded_values() {
        let request = "GET /callback?code=ABC%3D%3D&state=XYZ%2B123 HTTP/1.1";
        let result = parse_callback_request(request).unwrap();

        assert_eq!(result.code, "ABC==");
        assert_eq!(result.state, "XYZ+123");
    }

    #[test]
    fn parse_callback_handles_extra_params() {
        let request = "GET /callback?code=CODE&state=STATE&extra=ignored HTTP/1.1";
        let result = parse_callback_request(request).unwrap();

        assert_eq!(result.code, "CODE");
        assert_eq!(result.state, "STATE");
    }

    #[test]
    fn parse_callback_fails_on_missing_code() {
        let request = "GET /callback?state=STATE HTTP/1.1";
        let result = parse_callback_request(request);

        assert!(result.is_err());
        assert!(matches!(result, Err(AppError::OAuthError(_))));
    }

    #[test]
    fn parse_callback_fails_on_missing_state() {
        let request = "GET /callback?code=CODE HTTP/1.1";
        let result = parse_callback_request(request);

        assert!(result.is_err());
        assert!(matches!(result, Err(AppError::OAuthError(_))));
    }

    #[test]
    fn parse_callback_fails_on_wrong_path() {
        let request = "GET /wrong?code=CODE&state=STATE HTTP/1.1";
        let result = parse_callback_request(request);

        assert!(result.is_err());
        assert!(matches!(result, Err(AppError::OAuthError(_))));
    }

    #[test]
    fn parse_callback_handles_oauth_error() {
        let request =
            "GET /callback?error=access_denied&error_description=User+denied+access HTTP/1.1";
        let result = parse_callback_request(request);

        assert!(result.is_err());
        if let Err(AppError::OAuthError(msg)) = result {
            assert!(msg.contains("denied"));
        } else {
            panic!("Expected OAuthError");
        }
    }

    #[test]
    fn parse_callback_fails_on_invalid_request() {
        let request = "INVALID";
        let result = parse_callback_request(request);

        assert!(result.is_err());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Port Fallback Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn find_available_port_succeeds() {
        let result = find_available_port().await;
        assert!(result.is_ok());

        let (listener, port) = result.unwrap();
        assert!(port > 0);

        // Clean up
        drop(listener);
    }

    #[tokio::test]
    async fn find_available_port_skips_occupied() {
        // Occupy the first port
        let first_port = CALLBACK_PORTS[0];
        let occupied = TcpListener::bind(format!("127.0.0.1:{}", first_port)).await;

        if occupied.is_ok() {
            // First port is now occupied, find_available_port should skip it
            let result = find_available_port().await;
            assert!(result.is_ok());

            let (_listener, port) = result.unwrap();
            // Should not be the first port
            assert_ne!(port, first_port);
        }
        // If we couldn't bind the first port, skip this test
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Auth URL Building Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn build_auth_url_contains_required_params() {
        let url = build_auth_url(
            &LoginType::Production,
            "http://localhost:8477/callback",
            "challenge123",
            "state456",
        )
        .unwrap();

        assert!(url.contains("response_type=code"));
        assert!(url.contains(&format!("client_id={}", CLIENT_ID)));
        assert!(url.contains("redirect_uri="));
        assert!(url.contains("scope="));
        assert!(url.contains("state=state456"));
        assert!(url.contains("code_challenge=challenge123"));
        assert!(url.contains("code_challenge_method=S256"));
    }

    #[test]
    fn build_auth_url_includes_scopes() {
        let url = build_auth_url(
            &LoginType::Production,
            "http://localhost:8477/callback",
            "challenge",
            "state",
        )
        .unwrap();

        // URL-encoded space between scopes
        assert!(url.contains("api"));
        assert!(url.contains("refresh_token"));
        assert!(url.contains("openid"));
    }
}
