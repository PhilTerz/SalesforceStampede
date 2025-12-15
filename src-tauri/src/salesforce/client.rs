//! Salesforce HTTP client with secure credential handling and safe logging.

use std::sync::Arc;
use std::time::{Duration, Instant};

use reqwest::header::{HeaderMap, HeaderValue, USER_AGENT};
use reqwest::Method;
use secrecy::{ExposeSecret, SecretString};
use tokio::sync::{Mutex, RwLock};
use tracing::{info, warn};
use url::Url;

use crate::error::AppError;
use crate::salesforce::refresh::{self, CLIENT_ID};
use crate::storage::credentials as keychain;

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

/// User agent string for all Salesforce API requests.
const CLIENT_USER_AGENT: &str = "SalesforceStampede/1.0.0";

/// Default request timeout in seconds.
const DEFAULT_TIMEOUT_SECS: u64 = 300;

/// Query parameter keys (case-insensitive) that should have their values redacted.
const SENSITIVE_QUERY_PARAMS: &[&str] = &[
    "access_token",
    "refresh_token",
    "client_secret",
    "code",
    "token",
    "sid",
    "session",
    "authorization",
];

// ─────────────────────────────────────────────────────────────────────────────
// LoggingMode
// ─────────────────────────────────────────────────────────────────────────────

/// Controls how URLs are sanitized for logging.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum LoggingMode {
    /// Log only the path component. Strips scheme, host, query, and fragment.
    /// Example: `/services/data/v60.0/query`
    #[default]
    PathOnly,

    /// Log path and query parameters, but redact sensitive values.
    /// Example: `/services/data/v60.0/sobjects?access_token=***&q=SELECT+Id`
    PathAndQueryRedacted,
}

// ─────────────────────────────────────────────────────────────────────────────
// OrgCredentials
// ─────────────────────────────────────────────────────────────────────────────

/// Salesforce organization credentials for API access.
///
/// Sensitive fields (`access_token`, `refresh_token`) are wrapped in `SecretString`
/// to prevent accidental exposure through `Debug` traits or logging.
#[derive(Clone)]
pub struct OrgCredentials {
    /// Salesforce organization ID (e.g., "00D...")
    pub org_id: String,
    /// Salesforce user ID (e.g., "005...")
    pub user_id: String,
    /// Salesforce username (e.g., "user@example.com")
    pub username: String,
    /// Instance URL (e.g., "https://na1.salesforce.com")
    pub instance_url: String,
    /// OAuth access token (wrapped for security)
    pub access_token: SecretString,
    /// OAuth refresh token (wrapped for security, optional for some flows)
    pub refresh_token: Option<SecretString>,
    /// Salesforce API version (e.g., "v60.0")
    pub api_version: String,
}

impl std::fmt::Debug for OrgCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OrgCredentials")
            .field("org_id", &self.org_id)
            .field("user_id", &self.user_id)
            .field("username", &self.username)
            .field("instance_url", &self.instance_url)
            .field("access_token", &"[REDACTED]")
            .field(
                "refresh_token",
                &self.refresh_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("api_version", &self.api_version)
            .finish()
    }
}

impl OrgCredentials {
    /// Creates placeholder credentials for app startup before authentication.
    pub(crate) fn placeholder() -> Self {
        Self {
            org_id: String::new(),
            user_id: String::new(),
            username: String::new(),
            instance_url: String::new(),
            access_token: SecretString::from(String::new()),
            refresh_token: None,
            api_version: "v60.0".to_string(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// URL Sanitization
// ─────────────────────────────────────────────────────────────────────────────

/// Determines if a query parameter key is sensitive and should be redacted.
fn is_sensitive_param(key: &str) -> bool {
    let key_lower = key.to_ascii_lowercase();
    SENSITIVE_QUERY_PARAMS
        .iter()
        .any(|&sensitive| key_lower == sensitive)
}

/// Sanitizes a URL for safe logging based on the specified mode.
///
/// # Security
///
/// This function uses the `url` crate for proper URL parsing rather than
/// regex-based string manipulation, ensuring robust handling of edge cases.
///
/// # Arguments
///
/// * `url` - The URL to sanitize
/// * `mode` - The logging mode determining what parts to include
///
/// # Returns
///
/// A string safe for logging that never contains the scheme, host, or fragment.
pub fn sanitize_url_for_logs(url: &Url, mode: LoggingMode) -> String {
    // Always start with the path
    let path = url.path();

    match mode {
        LoggingMode::PathOnly => path.to_string(),
        LoggingMode::PathAndQueryRedacted => {
            // Check if there are any query parameters
            let query_pairs: Vec<_> = url.query_pairs().collect();
            if query_pairs.is_empty() {
                return path.to_string();
            }

            // Build redacted query string
            let redacted_pairs: Vec<String> = query_pairs
                .into_iter()
                .map(|(key, value)| {
                    if is_sensitive_param(&key) {
                        format!("{}=***", key)
                    } else {
                        format!("{}={}", key, value)
                    }
                })
                .collect();

            format!("{}?{}", path, redacted_pairs.join("&"))
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SalesforceClient
// ─────────────────────────────────────────────────────────────────────────────

/// Thread-safe HTTP client for Salesforce API interactions.
///
/// # Thread Safety
///
/// - `creds`: Protected by `RwLock` allowing concurrent reads (requests)
///   but exclusive writes (credential refresh).
/// - `refresh_lock`: `Mutex` to serialize refresh attempts and prevent
///   thundering herd during token expiration.
#[derive(Clone)]
pub struct SalesforceClient {
    /// The underlying HTTP client.
    http: reqwest::Client,
    /// Thread-safe credentials storage.
    creds: Arc<RwLock<OrgCredentials>>,
    /// Lock to serialize refresh token operations.
    refresh_lock: Arc<Mutex<()>>,
    /// Controls URL sanitization for logging.
    logging_mode: LoggingMode,
}

impl SalesforceClient {
    /// Creates a new Salesforce client with the provided credentials.
    ///
    /// # Arguments
    ///
    /// * `creds` - Valid organization credentials
    ///
    /// # Errors
    ///
    /// Returns `AppError::Internal` if the HTTP client fails to initialize.
    pub fn new(creds: OrgCredentials) -> Result<Self, AppError> {
        let http = build_http_client()?;
        Ok(Self {
            http,
            creds: Arc::new(RwLock::new(creds)),
            refresh_lock: Arc::new(Mutex::new(())),
            logging_mode: LoggingMode::default(),
        })
    }

    /// Creates a client with placeholder credentials for app startup.
    ///
    /// Use this when the app initializes before the user has logged in.
    /// The credentials should be updated via `update_credentials` after
    /// successful OAuth authentication.
    ///
    /// # Errors
    ///
    /// Returns `AppError::Internal` if the HTTP client fails to initialize.
    pub fn new_placeholder() -> Result<Self, AppError> {
        Self::new(OrgCredentials::placeholder())
    }

    /// Updates the logging mode for URL sanitization.
    pub fn with_logging_mode(mut self, mode: LoggingMode) -> Self {
        self.logging_mode = mode;
        self
    }

    /// Updates the stored credentials (e.g., after OAuth or token refresh).
    pub async fn update_credentials(&self, creds: OrgCredentials) {
        let mut guard = self.creds.write().await;
        *guard = creds;
    }

    /// Executes a GET request to an absolute URL with logging.
    ///
    /// # Arguments
    ///
    /// * `url` - The absolute URL to request
    ///
    /// # Errors
    ///
    /// Returns `AppError::ConnectionFailed` for network errors or
    /// `AppError::Internal` for other failures.
    ///
    /// # Security
    ///
    /// This method does NOT automatically inject the Authorization header.
    /// For authenticated requests, use `request_authed` instead.
    pub async fn get_absolute(&self, url: Url) -> Result<reqwest::Response, AppError> {
        let request = self.http.get(url.as_str());
        self.execute_with_logging(request, url).await
    }

    /// Returns a reference to the underlying HTTP client.
    #[allow(dead_code)]
    pub(crate) fn http_client(&self) -> &reqwest::Client {
        &self.http
    }

    /// Builds a full URL by joining the path with the instance URL.
    ///
    /// # Arguments
    ///
    /// * `path` - The API path (e.g., "/services/data/v60.0/query")
    ///
    /// # Errors
    ///
    /// Returns `AppError::NotAuthenticated` if no instance URL is configured.
    /// Returns `AppError::Internal` if the URL cannot be parsed.
    pub async fn build_url(&self, path: &str) -> Result<Url, AppError> {
        let creds = self.creds.read().await;

        if creds.instance_url.is_empty() {
            return Err(AppError::NotAuthenticated);
        }

        let base = Url::parse(&creds.instance_url)
            .map_err(|_| AppError::Internal("Invalid instance URL".to_string()))?;

        base.join(path)
            .map_err(|_| AppError::Internal(format!("Invalid path: {}", path)))
    }

    /// Executes an authenticated request with automatic token refresh.
    ///
    /// This is the primary method for making authenticated Salesforce API calls.
    /// It handles:
    /// - Automatically attaching the Authorization header
    /// - Detecting 401 responses and refreshing the token
    /// - Retrying the request after refresh
    /// - Thread-safe refresh with double-checked locking
    ///
    /// # Arguments
    ///
    /// * `method` - The HTTP method (GET, POST, etc.)
    /// * `path` - The API path (e.g., "/services/data/v60.0/query")
    /// * `body` - Optional request body (as bytes for clonability)
    ///
    /// # Errors
    ///
    /// - `AppError::NotAuthenticated` - No credentials or refresh token available
    /// - `AppError::SessionExpired` - Token refresh failed, user must re-login
    /// - `AppError::ConnectionFailed` - Network error
    pub async fn request_authed(
        &self,
        method: Method,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> Result<reqwest::Response, AppError> {
        // Attempt 1: Try with current credentials
        let (url, original_token) = {
            let creds = self.creds.read().await;
            if creds.instance_url.is_empty() {
                return Err(AppError::NotAuthenticated);
            }

            let base = Url::parse(&creds.instance_url)
                .map_err(|_| AppError::Internal("Invalid instance URL".to_string()))?;

            let url = base
                .join(path)
                .map_err(|_| AppError::Internal(format!("Invalid path: {}", path)))?;

            let token = creds.access_token.expose_secret().to_string();
            (url, token)
        };

        let response = self
            .execute_authed_request(method.clone(), url.clone(), body.clone(), &original_token)
            .await?;

        // Check if we need to refresh
        if response.status() != reqwest::StatusCode::UNAUTHORIZED {
            return Ok(response);
        }

        info!("[SFDC] Received 401, attempting token refresh...");

        // Refresh logic with double-checked locking
        {
            let _refresh_guard = self.refresh_lock.lock().await;

            // Double-check: another thread may have already refreshed
            let current_token = {
                let creds = self.creds.read().await;
                creds.access_token.expose_secret().to_string()
            };

            if current_token != original_token {
                // Token was refreshed by another thread, skip refresh
                info!("[SFDC] Token already refreshed by another thread");
            } else {
                // We need to refresh
                self.do_token_refresh().await?;
            }
        }

        // Attempt 2: Retry with new credentials
        let (new_url, new_token) = {
            let creds = self.creds.read().await;

            // Instance URL might have changed after refresh
            let base = Url::parse(&creds.instance_url)
                .map_err(|_| AppError::Internal("Invalid instance URL".to_string()))?;

            let url = base
                .join(path)
                .map_err(|_| AppError::Internal(format!("Invalid path: {}", path)))?;

            let token = creds.access_token.expose_secret().to_string();
            (url, token)
        };

        let retry_response = self
            .execute_authed_request(method, new_url, body, &new_token)
            .await?;

        // If still 401 after refresh, session is truly expired
        if retry_response.status() == reqwest::StatusCode::UNAUTHORIZED {
            warn!("[SFDC] Still unauthorized after token refresh");
            return Err(AppError::SessionExpired);
        }

        Ok(retry_response)
    }

    /// Executes a single authenticated request (no retry logic).
    async fn execute_authed_request(
        &self,
        method: Method,
        url: Url,
        body: Option<Vec<u8>>,
        access_token: &str,
    ) -> Result<reqwest::Response, AppError> {
        let start = Instant::now();
        let sanitized_url = sanitize_url_for_logs(&url, self.logging_mode);

        let mut request = self.http.request(method.clone(), url.as_str());
        request = request.bearer_auth(access_token);

        if let Some(body_bytes) = body {
            request = request
                .header("Content-Type", "application/json")
                .body(body_bytes);
        }

        let result = request.send().await;
        let duration_ms = start.elapsed().as_millis();

        match result {
            Ok(response) => {
                let status = response.status();
                let x_request_id = response
                    .headers()
                    .get("x-request-id")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("-");

                info!(
                    "[SFDC] {} {} {} {}ms {}",
                    method,
                    sanitized_url,
                    status.as_u16(),
                    duration_ms,
                    x_request_id
                );

                Ok(response)
            }
            Err(_) => {
                info!(
                    "[SFDC] {} {} FAILED {}ms",
                    method, sanitized_url, duration_ms
                );
                Err(AppError::ConnectionFailed(
                    "Connection to Salesforce failed".to_string(),
                ))
            }
        }
    }

    /// Performs the actual token refresh operation.
    async fn do_token_refresh(&self) -> Result<(), AppError> {
        let (org_id, instance_url) = {
            let creds = self.creds.read().await;
            (creds.org_id.clone(), creds.instance_url.clone())
        };

        // Get refresh token from keychain
        let tokens = keychain::get_tokens(&org_id).await.map_err(|e| {
            warn!("[SFDC] Failed to get refresh token from keychain: {:?}", e);
            AppError::NotAuthenticated
        })?;

        let refresh_token_str = tokens.refresh_token.clone();
        let refresh_token = SecretString::from(tokens.refresh_token);
        let login_url = refresh::get_login_url_for_refresh(&instance_url);

        // Perform the refresh
        let token_response =
            refresh::refresh_access_token(&self.http, &login_url, &refresh_token, CLIENT_ID)
                .await?;

        // Update credentials
        {
            let mut creds = self.creds.write().await;
            creds.access_token = SecretString::from(token_response.access_token.clone());
            creds.instance_url = token_response.instance_url.clone();
        }

        // Update keychain with new access token
        keychain::store_tokens(&org_id, &token_response.access_token, &refresh_token_str).await?;

        info!("[SFDC] Token refresh complete, credentials updated");
        Ok(())
    }

    /// Executes a request with timing, logging, and error handling.
    ///
    /// # Security
    ///
    /// - Never logs the Authorization header
    /// - Never logs request/response bodies
    /// - Sanitizes URLs before logging
    /// - Error messages never contain raw URLs or tokens
    async fn execute_with_logging(
        &self,
        request: reqwest::RequestBuilder,
        url: Url,
    ) -> Result<reqwest::Response, AppError> {
        let start = Instant::now();
        let sanitized_url = sanitize_url_for_logs(&url, self.logging_mode);

        // Execute the request
        let result = request.send().await;
        let duration_ms = start.elapsed().as_millis();

        match result {
            Ok(response) => {
                let status = response.status();
                let x_request_id = response
                    .headers()
                    .get("x-request-id")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("-");

                // Log successful request (method is always GET for now)
                info!(
                    "[SFDC] GET {} {} {}ms {}",
                    sanitized_url,
                    status.as_u16(),
                    duration_ms,
                    x_request_id
                );

                Ok(response)
            }
            Err(_) => {
                // Log failed request without exposing the actual error
                // (which may contain the full URL with tokens)
                info!("[SFDC] GET {} FAILED {}ms", sanitized_url, duration_ms);

                // Return a sanitized error message - never expose the raw reqwest error
                Err(AppError::ConnectionFailed(
                    "Connection to Salesforce failed".to_string(),
                ))
            }
        }
    }
}

/// Builds the configured HTTP client.
fn build_http_client() -> Result<reqwest::Client, AppError> {
    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static(CLIENT_USER_AGENT));

    reqwest::Client::builder()
        .default_headers(headers)
        .timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
        .build()
        .map_err(|e| AppError::Internal(format!("Failed to build HTTP client: {}", e)))
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ─────────────────────────────────────────────────────────────────────────
    // URL Sanitization Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn sanitize_strips_scheme_and_host() {
        let url = Url::parse("https://na1.salesforce.com/services/data/v60.0/query").unwrap();

        let result = sanitize_url_for_logs(&url, LoggingMode::PathOnly);

        assert_eq!(result, "/services/data/v60.0/query");
        assert!(!result.contains("https"));
        assert!(!result.contains("na1.salesforce.com"));
    }

    #[test]
    fn sanitize_strips_fragment() {
        let url = Url::parse("https://example.com/path?safe=value#secret-anchor").unwrap();

        // PathOnly mode
        let result = sanitize_url_for_logs(&url, LoggingMode::PathOnly);
        assert!(!result.contains("#"));
        assert!(!result.contains("secret-anchor"));
        assert_eq!(result, "/path");

        // PathAndQueryRedacted mode
        let result = sanitize_url_for_logs(&url, LoggingMode::PathAndQueryRedacted);
        assert!(!result.contains("#"));
        assert!(!result.contains("secret-anchor"));
        assert!(result.contains("safe=value"));
    }

    #[test]
    fn path_only_excludes_query_string() {
        let url =
            Url::parse("https://login.salesforce.com/services/oauth2/token?code=secret&state=abc")
                .unwrap();

        let result = sanitize_url_for_logs(&url, LoggingMode::PathOnly);

        assert_eq!(result, "/services/oauth2/token");
        assert!(!result.contains("?"));
        assert!(!result.contains("code"));
        assert!(!result.contains("secret"));
        assert!(!result.contains("state"));
    }

    #[test]
    fn path_and_query_redacted_preserves_safe_keys() {
        let url = Url::parse(
            "https://na1.salesforce.com/services/data/v60.0/query?q=SELECT+Id+FROM+Account",
        )
        .unwrap();

        let result = sanitize_url_for_logs(&url, LoggingMode::PathAndQueryRedacted);

        // URL crate decodes '+' as space, so we check for the decoded version
        assert!(result.contains("q=SELECT Id FROM Account"));
        assert!(result.starts_with("/services/data/v60.0/query"));
    }

    #[test]
    fn path_and_query_redacted_redacts_sensitive_keys() {
        // Test all sensitive parameters from the deny list
        let test_cases = [
            ("access_token", "abc123"),
            ("Access_Token", "xyz789"),   // Case variation
            ("ACCESS_TOKEN", "TOKEN123"), // Uppercase
            ("refresh_token", "refresh123"),
            ("client_secret", "secret456"),
            ("code", "authcode789"),
            ("token", "sometoken"),
            ("sid", "sessionid123"),
            ("session", "sess456"),
            ("authorization", "bearer123"),
        ];

        for (key, value) in test_cases {
            let url_str = format!("https://example.com/path?{}={}", key, value);
            let url = Url::parse(&url_str).unwrap();

            let result = sanitize_url_for_logs(&url, LoggingMode::PathAndQueryRedacted);

            // Should contain the key but not the value
            assert!(
                result.contains(&format!("{}=***", key)),
                "Expected '{}=***' in result '{}' for key '{}'",
                key,
                result,
                key
            );
            assert!(
                !result.contains(value),
                "Value '{}' should be redacted for key '{}' in result '{}'",
                value,
                key,
                result
            );
        }
    }

    #[test]
    fn path_and_query_redacted_handles_mixed_params() {
        let url = Url::parse(
            "https://login.salesforce.com/oauth?access_token=secret123&q=hello&SID=sess456&format=json",
        )
        .unwrap();

        let result = sanitize_url_for_logs(&url, LoggingMode::PathAndQueryRedacted);

        // Safe params preserved
        assert!(result.contains("q=hello"));
        assert!(result.contains("format=json"));

        // Sensitive params redacted
        assert!(result.contains("access_token=***"));
        assert!(result.contains("SID=***"));
        assert!(!result.contains("secret123"));
        assert!(!result.contains("sess456"));
    }

    #[test]
    fn sanitize_handles_empty_query_string() {
        let url = Url::parse("https://example.com/path").unwrap();

        let result = sanitize_url_for_logs(&url, LoggingMode::PathAndQueryRedacted);

        assert_eq!(result, "/path");
        assert!(!result.contains("?"));
    }

    #[test]
    fn sanitize_handles_path_only_url() {
        let url = Url::parse("https://example.com/").unwrap();

        assert_eq!(sanitize_url_for_logs(&url, LoggingMode::PathOnly), "/");
        assert_eq!(
            sanitize_url_for_logs(&url, LoggingMode::PathAndQueryRedacted),
            "/"
        );
    }

    #[test]
    fn sanitize_handles_deep_paths() {
        let url =
            Url::parse("https://na1.salesforce.com/services/data/v60.0/sobjects/Account/describe")
                .unwrap();

        let result = sanitize_url_for_logs(&url, LoggingMode::PathOnly);

        assert_eq!(result, "/services/data/v60.0/sobjects/Account/describe");
    }

    #[test]
    fn sanitize_handles_url_encoded_values() {
        let url = Url::parse(
            "https://example.com/query?q=SELECT%20Id%20FROM%20Account&access_token=abc%3D%3D",
        )
        .unwrap();

        let result = sanitize_url_for_logs(&url, LoggingMode::PathAndQueryRedacted);

        // The query value should be preserved (URL decoding handled by url crate)
        assert!(result.contains("SELECT"));
        // Token should still be redacted
        assert!(result.contains("access_token=***"));
        assert!(!result.contains("abc"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // OrgCredentials Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn org_credentials_debug_redacts_token() {
        let creds = OrgCredentials {
            org_id: "00Dxx0000001234".to_string(),
            user_id: "005xx0000001234".to_string(),
            username: "test@example.com".to_string(),
            instance_url: "https://na1.salesforce.com".to_string(),
            access_token: SecretString::from("super_secret_token_12345".to_string()),
            refresh_token: Some(SecretString::from("super_secret_refresh_67890".to_string())),
            api_version: "v60.0".to_string(),
        };

        let debug_output = format!("{:?}", creds);

        // Should contain non-sensitive fields
        assert!(debug_output.contains("00Dxx0000001234"));
        assert!(debug_output.contains("005xx0000001234"));
        assert!(debug_output.contains("test@example.com"));
        assert!(debug_output.contains("na1.salesforce.com"));
        assert!(debug_output.contains("v60.0"));

        // Should NOT contain the actual tokens
        assert!(!debug_output.contains("super_secret_token_12345"));
        assert!(!debug_output.contains("super_secret_refresh_67890"));
        assert!(debug_output.contains("[REDACTED]"));
    }

    #[test]
    fn org_credentials_placeholder_has_empty_values() {
        let creds = OrgCredentials::placeholder();

        assert!(creds.org_id.is_empty());
        assert!(creds.user_id.is_empty());
        assert!(creds.username.is_empty());
        assert!(creds.instance_url.is_empty());
        assert!(creds.refresh_token.is_none());
        assert_eq!(creds.api_version, "v60.0");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // SalesforceClient Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn client_new_succeeds_with_valid_creds() {
        let creds = OrgCredentials {
            org_id: "00Dxx0000001234".to_string(),
            user_id: "005xx0000001234".to_string(),
            username: "test@example.com".to_string(),
            instance_url: "https://na1.salesforce.com".to_string(),
            access_token: SecretString::from("token".to_string()),
            refresh_token: Some(SecretString::from("refresh".to_string())),
            api_version: "v60.0".to_string(),
        };

        let result = SalesforceClient::new(creds);

        assert!(result.is_ok());
    }

    #[test]
    fn client_new_placeholder_succeeds() {
        let result = SalesforceClient::new_placeholder();

        assert!(result.is_ok());
    }

    #[test]
    fn client_with_logging_mode_changes_mode() {
        let client = SalesforceClient::new_placeholder().unwrap();
        assert_eq!(client.logging_mode, LoggingMode::PathOnly);

        let client = client.with_logging_mode(LoggingMode::PathAndQueryRedacted);
        assert_eq!(client.logging_mode, LoggingMode::PathAndQueryRedacted);
    }

    #[tokio::test]
    async fn client_update_credentials_works() {
        let client = SalesforceClient::new_placeholder().unwrap();

        // Initial state: empty credentials
        {
            let creds = client.creds.read().await;
            assert!(creds.org_id.is_empty());
        }

        // Update credentials
        let new_creds = OrgCredentials {
            org_id: "00Dxx9999999999".to_string(),
            user_id: "005xx9999999999".to_string(),
            username: "newuser@example.com".to_string(),
            instance_url: "https://na99.salesforce.com".to_string(),
            access_token: SecretString::from("new_token".to_string()),
            refresh_token: Some(SecretString::from("new_refresh".to_string())),
            api_version: "v61.0".to_string(),
        };
        client.update_credentials(new_creds).await;

        // Verify update
        {
            let creds = client.creds.read().await;
            assert_eq!(creds.org_id, "00Dxx9999999999");
            assert_eq!(creds.user_id, "005xx9999999999");
            assert_eq!(creds.username, "newuser@example.com");
            assert_eq!(creds.instance_url, "https://na99.salesforce.com");
            assert_eq!(creds.api_version, "v61.0");
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // is_sensitive_param Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn is_sensitive_param_detects_all_deny_list_items() {
        for param in SENSITIVE_QUERY_PARAMS {
            assert!(
                is_sensitive_param(param),
                "'{}' should be detected as sensitive",
                param
            );
        }
    }

    #[test]
    fn is_sensitive_param_is_case_insensitive() {
        assert!(is_sensitive_param("ACCESS_TOKEN"));
        assert!(is_sensitive_param("Access_Token"));
        assert!(is_sensitive_param("access_token"));
        assert!(is_sensitive_param("REFRESH_TOKEN"));
        assert!(is_sensitive_param("Client_Secret"));
    }

    #[test]
    fn is_sensitive_param_does_not_match_safe_params() {
        let safe_params = ["q", "format", "limit", "offset", "fields", "orderBy"];

        for param in safe_params {
            assert!(
                !is_sensitive_param(param),
                "'{}' should NOT be detected as sensitive",
                param
            );
        }
    }

    #[test]
    fn is_sensitive_param_requires_exact_match() {
        // Should not match partial strings
        assert!(!is_sensitive_param("access_token_id"));
        assert!(!is_sensitive_param("my_access_token"));
        assert!(!is_sensitive_param("tokens"));
        assert!(!is_sensitive_param("sessions"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // HTTP Client Builder Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn build_http_client_succeeds() {
        let result = build_http_client();
        assert!(result.is_ok());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // LoggingMode Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn logging_mode_default_is_path_only() {
        let mode = LoggingMode::default();
        assert_eq!(mode, LoggingMode::PathOnly);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // build_url Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn build_url_returns_not_authenticated_when_no_instance_url() {
        let client = SalesforceClient::new_placeholder().unwrap();

        let result = client.build_url("/services/data/v60.0/query").await;

        assert!(result.is_err());
        assert!(matches!(result, Err(AppError::NotAuthenticated)));
    }

    #[tokio::test]
    async fn build_url_constructs_correct_url() {
        let creds = OrgCredentials {
            org_id: "00Dxx0000001234".to_string(),
            user_id: "005xx0000001234".to_string(),
            username: "test@example.com".to_string(),
            instance_url: "https://na1.salesforce.com".to_string(),
            access_token: SecretString::from("token".to_string()),
            refresh_token: Some(SecretString::from("refresh".to_string())),
            api_version: "v60.0".to_string(),
        };
        let client = SalesforceClient::new(creds).unwrap();

        let result = client.build_url("/services/data/v60.0/query").await;

        assert!(result.is_ok());
        let url = result.unwrap();
        assert_eq!(
            url.as_str(),
            "https://na1.salesforce.com/services/data/v60.0/query"
        );
    }

    #[tokio::test]
    async fn build_url_handles_path_with_query() {
        let creds = OrgCredentials {
            org_id: "00Dxx0000001234".to_string(),
            user_id: "005xx0000001234".to_string(),
            username: "test@example.com".to_string(),
            instance_url: "https://na1.salesforce.com".to_string(),
            access_token: SecretString::from("token".to_string()),
            refresh_token: None,
            api_version: "v60.0".to_string(),
        };
        let client = SalesforceClient::new(creds).unwrap();

        let result = client
            .build_url("/services/data/v60.0/query?q=SELECT+Id")
            .await;

        assert!(result.is_ok());
        let url = result.unwrap();
        assert!(url.as_str().contains("query?q="));
    }
}
