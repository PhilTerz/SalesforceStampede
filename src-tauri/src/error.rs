use serde::Serialize;
use thiserror::Error;

/// Patterns (lowercase) that indicate sensitive data not safe for UI display.
/// Used by `contains_sensitive()` for case-insensitive matching.
pub(crate) const SENSITIVE_PATTERNS: &[&str] = &[
    "bearer ",
    "refresh_token",
    "access_token",
    "client_secret",
    "authorization:",
];

/// Returns true if the message contains any sensitive pattern (case-insensitive).
fn contains_sensitive(msg: &str) -> bool {
    let lower = msg.to_ascii_lowercase();
    SENSITIVE_PATTERNS.iter().any(|p| lower.contains(p))
}

/// Sanitizes a message for UI display.
/// If sensitive content is detected, returns the fallback instead.
fn sanitize_message(msg: &str, fallback: &str) -> String {
    if contains_sensitive(msg) {
        fallback.into()
    } else {
        msg.to_string()
    }
}

/// User-friendly error presentation for the frontend.
#[derive(Debug, Clone, Serialize)]
pub struct ErrorPresentation {
    pub title: String,
    pub message: String,
    pub action: Option<String>,
}

/// Application-wide error type.
#[derive(Debug, Error)]
pub enum AppError {
    // ── Auth ──────────────────────────────────────────────────────────────────
    #[error("Not authenticated")]
    NotAuthenticated,

    #[error("Session expired")]
    SessionExpired,

    #[error("OAuth error: {0}")]
    OAuthError(String),

    #[error("Failed to bind OAuth callback port")]
    PortBindFailed,

    // ── API ───────────────────────────────────────────────────────────────────
    #[error("Salesforce error: {0}")]
    SalesforceError(String),

    #[error("Rate limited")]
    RateLimited { retry_after_secs: Option<u64> },

    #[error("Concurrent API limit exceeded")]
    ConcurrentLimitExceeded,

    #[error("Count query timed out")]
    CountTimeout,

    // ── Bulk Operations ───────────────────────────────────────────────────────
    #[error("Bulk job {job_id} failed: {message}")]
    JobFailed { job_id: String, message: String },

    #[error("Operation cancelled")]
    Cancelled,

    // ── File / CSV ────────────────────────────────────────────────────────────
    #[error("File is not valid UTF-8")]
    NotUtf8,

    #[error("Invalid CSV: {0}")]
    CsvInvalid(String),

    #[error("CSV chunk error: {0}")]
    CsvChunkError(String),

    // ── Network ───────────────────────────────────────────────────────────────
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    // ── Generic fallback ──────────────────────────────────────────────────────
    #[error("Internal error: {0}")]
    Internal(String),
}

impl AppError {
    /// Converts the error into a user-friendly presentation suitable for UI display.
    /// Never leaks secrets, tokens, or sensitive URL parameters.
    pub fn to_presentation(&self) -> ErrorPresentation {
        match self {
            // ── Auth ──────────────────────────────────────────────────────────
            AppError::NotAuthenticated => ErrorPresentation {
                title: "Not Logged In".into(),
                message: "You need to log in to Salesforce to continue.".into(),
                action: Some("Log in to Salesforce".into()),
            },

            AppError::SessionExpired => ErrorPresentation {
                title: "Session Expired".into(),
                message: "Your Salesforce session has expired.".into(),
                action: Some("Log in again".into()),
            },

            AppError::OAuthError(_) => ErrorPresentation {
                title: "Login Failed".into(),
                message: "Could not complete the login process. Please try again.".into(),
                action: Some("Try logging in again".into()),
            },

            AppError::PortBindFailed => ErrorPresentation {
                title: "Login Unavailable".into(),
                message: "Could not start the login process. Another application may be using the required port.".into(),
                action: Some("Close other applications and try again".into()),
            },

            // ── API ───────────────────────────────────────────────────────────
            AppError::SalesforceError(msg) => ErrorPresentation {
                title: "Salesforce Error".into(),
                message: sanitize_message(msg, "A Salesforce error occurred."),
                action: None,
            },

            AppError::RateLimited { retry_after_secs } => {
                let wait_msg = match retry_after_secs {
                    Some(secs) => format!("Please wait {} seconds before trying again.", secs),
                    None => "Please wait a moment before trying again.".into(),
                };
                ErrorPresentation {
                    title: "Too Many Requests".into(),
                    message: format!("Salesforce is limiting requests. {}", wait_msg),
                    action: Some("Wait and retry".into()),
                }
            }

            AppError::ConcurrentLimitExceeded => ErrorPresentation {
                title: "Too Many Operations".into(),
                message: "Salesforce has reached its limit for concurrent operations.".into(),
                action: Some("Wait for current operations to complete".into()),
            },

            AppError::CountTimeout => ErrorPresentation {
                title: "Query Timed Out".into(),
                message: "The record count query took too long. The object may have many records.".into(),
                action: Some("Try again or proceed without count".into()),
            },

            // ── Bulk Operations ───────────────────────────────────────────────
            AppError::JobFailed { job_id: _, message } => ErrorPresentation {
                title: "Bulk Job Failed".into(),
                message: sanitize_message(message, "The bulk operation failed."),
                action: Some("Review the error and try again".into()),
            },

            AppError::Cancelled => ErrorPresentation {
                title: "Cancelled".into(),
                message: "The operation was cancelled.".into(),
                action: None,
            },

            // ── File / CSV ────────────────────────────────────────────────────
            AppError::NotUtf8 => ErrorPresentation {
                title: "Invalid File Encoding".into(),
                message: "The file must be UTF-8 encoded. Please re-save your file with UTF-8 encoding.".into(),
                action: Some("Convert file to UTF-8".into()),
            },

            AppError::CsvInvalid(msg) => ErrorPresentation {
                title: "Invalid CSV".into(),
                message: format!("The CSV file has a formatting problem: {}", msg),
                action: Some("Fix the CSV file and try again".into()),
            },

            AppError::CsvChunkError(msg) => ErrorPresentation {
                title: "CSV Processing Error".into(),
                message: format!("Error while processing CSV: {}", msg),
                action: Some("Check your CSV file format".into()),
            },

            // ── Network ───────────────────────────────────────────────────────
            AppError::ConnectionFailed(_) => ErrorPresentation {
                title: "Connection Failed".into(),
                message: "Could not connect to Salesforce. Please check your internet connection.".into(),
                action: Some("Check network and retry".into()),
            },

            // ── Generic ───────────────────────────────────────────────────────
            AppError::Internal(_) => ErrorPresentation {
                title: "Unexpected Error".into(),
                message: "Something went wrong. Please try again.".into(),
                action: Some("Try again".into()),
            },
        }
    }
}

// Allow AppError to be returned from Tauri commands
impl Serialize for AppError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_presentation().serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Returns all AppError variants for exhaustive testing.
    fn all_variants() -> Vec<AppError> {
        vec![
            // Auth
            AppError::NotAuthenticated,
            AppError::SessionExpired,
            AppError::OAuthError("test oauth error".into()),
            AppError::PortBindFailed,
            // API
            AppError::SalesforceError("test sf error".into()),
            AppError::RateLimited { retry_after_secs: Some(30) },
            AppError::RateLimited { retry_after_secs: None },
            AppError::ConcurrentLimitExceeded,
            AppError::CountTimeout,
            // Bulk
            AppError::JobFailed { job_id: "750xx000000001".into(), message: "test failure".into() },
            AppError::Cancelled,
            // File/CSV
            AppError::NotUtf8,
            AppError::CsvInvalid("missing header".into()),
            AppError::CsvChunkError("chunk parse failed".into()),
            // Network
            AppError::ConnectionFailed("timeout".into()),
            // Generic
            AppError::Internal("something broke".into()),
        ]
    }

    #[test]
    fn all_variants_have_nonempty_title_and_message() {
        for variant in all_variants() {
            let presentation = variant.to_presentation();
            assert!(
                !presentation.title.trim().is_empty(),
                "Empty title for {:?}",
                variant
            );
            assert!(
                !presentation.message.trim().is_empty(),
                "Empty message for {:?}",
                variant
            );
        }
    }

    #[test]
    fn actionable_errors_have_actions() {
        // Errors that should always suggest an action
        let actionable = vec![
            AppError::NotAuthenticated,
            AppError::SessionExpired,
            AppError::RateLimited { retry_after_secs: Some(60) },
            AppError::RateLimited { retry_after_secs: None },
            AppError::ConnectionFailed("network error".into()),
        ];

        for variant in actionable {
            let presentation = variant.to_presentation();
            assert!(
                presentation.action.is_some(),
                "Expected action for {:?}, got None",
                variant
            );
            let action = presentation.action.unwrap();
            assert!(
                !action.trim().is_empty(),
                "Empty action for {:?}",
                variant
            );
        }
    }

    #[test]
    fn auth_errors_suggest_relogin() {
        let auth_errors = vec![AppError::NotAuthenticated, AppError::SessionExpired];

        for variant in auth_errors {
            let presentation = variant.to_presentation();
            let action = presentation.action.expect("auth error should have action");
            let action_lower = action.to_lowercase();
            assert!(
                action_lower.contains("log in") || action_lower.contains("login"),
                "Auth error {:?} action should mention login, got: {}",
                variant,
                action
            );
        }
    }

    #[test]
    fn rate_limited_suggests_wait_retry() {
        let presentation = AppError::RateLimited { retry_after_secs: Some(30) }.to_presentation();
        let action = presentation.action.expect("RateLimited should have action");
        let action_lower = action.to_lowercase();
        assert!(
            action_lower.contains("wait") || action_lower.contains("retry"),
            "RateLimited action should mention wait/retry, got: {}",
            action
        );
        // Message should mention the retry time
        assert!(
            presentation.message.contains("30"),
            "RateLimited message should mention retry_after_secs"
        );
    }

    #[test]
    fn connection_failed_suggests_check_network() {
        let presentation = AppError::ConnectionFailed("timeout".into()).to_presentation();
        let action = presentation.action.expect("ConnectionFailed should have action");
        let action_lower = action.to_lowercase();
        assert!(
            action_lower.contains("network") || action_lower.contains("retry"),
            "ConnectionFailed action should mention network/retry, got: {}",
            action
        );
    }

    #[test]
    fn serialization_produces_valid_json_with_required_fields() {
        for variant in all_variants() {
            let json = serde_json::to_string(&variant)
                .expect(&format!("Failed to serialize {:?}", variant));

            // Parse back to verify structure
            let parsed: serde_json::Value = serde_json::from_str(&json)
                .expect(&format!("Failed to parse JSON for {:?}", variant));

            assert!(
                parsed.get("title").is_some(),
                "Serialized {:?} missing 'title' field",
                variant
            );
            assert!(
                parsed.get("message").is_some(),
                "Serialized {:?} missing 'message' field",
                variant
            );
            // action can be null, but field should exist
            assert!(
                parsed.get("action").is_some(),
                "Serialized {:?} missing 'action' field",
                variant
            );
        }
    }

    #[test]
    fn no_secret_leakage_in_presentation() {
        // Test cases: (variant label, error with sensitive payload)
        let test_cases: Vec<(&str, AppError)> = vec![
            ("OAuthError", AppError::OAuthError("Bearer abc123 refresh_token=secret".into())),
            ("SalesforceError", AppError::SalesforceError("AUTHORIZATION: Bearer token".into())),
            ("ConnectionFailed", AppError::ConnectionFailed("access_token=xyz client_secret=abc".into())),
            ("Internal", AppError::Internal("refresh_token leaked".into())),
            ("JobFailed", AppError::JobFailed {
                job_id: "750xx".into(),
                message: "Bearer token invalid".into(),
            }),
        ];

        for (label, variant) in test_cases {
            let presentation = variant.to_presentation();
            let output_lower = format!(
                "{} {} {}",
                presentation.title,
                presentation.message,
                presentation.action.as_deref().unwrap_or("")
            ).to_ascii_lowercase();

            // Reuse production patterns for consistency
            for pattern in SENSITIVE_PATTERNS {
                assert!(
                    !output_lower.contains(pattern),
                    "{} presentation contains sensitive pattern",
                    label
                );
            }
        }
    }
}
