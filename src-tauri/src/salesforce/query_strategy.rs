//! Query strategy determination for SOQL execution.
//!
//! This module provides functionality to:
//! - Rewrite SOQL queries into COUNT form for fast row counting
//! - Execute COUNT queries with timeout protection
//! - Determine optimal query strategy (REST vs Bulk) based on row count
//!
//! # Security
//!
//! Raw SOQL queries are never logged. Any debug logging must be opt-in and
//! will redact quoted string literals.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::time::timeout;
use tracing::debug;

use crate::error::AppError;
use crate::salesforce::rest::RestQueryClient;

// ─────────────────────────────────────────────────────────────────────────────
// Public Types
// ─────────────────────────────────────────────────────────────────────────────

/// Strategy for executing a SOQL query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryStrategy {
    /// Automatically determine the best strategy based on row count.
    Auto,
    /// Use the REST API (best for smaller result sets, immediate results).
    Rest,
    /// Use the Bulk API (best for large result sets, asynchronous).
    Bulk,
}

/// User preferences for query strategy determination.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPreferences {
    /// Timeout in seconds for COUNT queries. Default: 5 seconds.
    pub count_timeout_secs: u64,
}

impl Default for QueryPreferences {
    fn default() -> Self {
        Self {
            count_timeout_secs: 5,
        }
    }
}

/// Result of attempting to count rows for a SOQL query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CountResult {
    /// Successfully counted rows.
    Count(u64),
    /// The COUNT query timed out.
    TimedOut,
    /// COUNT is not supported for this query shape (e.g., GROUP BY).
    Unsupported,
}

/// Outcome of automatic strategy determination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StrategyDecision {
    /// Strategy was determined automatically.
    Determined(QueryStrategy),
    /// User needs to make a decision.
    NeedUserDecision {
        /// Suggested strategy to use.
        suggested: QueryStrategy,
        /// Short, user-facing explanation of why a decision is needed.
        reason: String,
    },
}

// ─────────────────────────────────────────────────────────────────────────────
// Strategy Thresholds
// ─────────────────────────────────────────────────────────────────────────────

/// Maximum row count for REST API (inclusive).
/// Queries with more rows should use Bulk API.
const BULK_THRESHOLD: u64 = 50_000;

// ─────────────────────────────────────────────────────────────────────────────
// SOQL COUNT Rewriting
// ─────────────────────────────────────────────────────────────────────────────

/// Patterns that indicate a query cannot be reliably converted to COUNT.
/// These are checked case-insensitively.
const UNSUPPORTED_PATTERNS: &[&str] = &["group by", "having", "with rollup", "offset"];

/// Attempt to rewrite a SOQL query into a COUNT form.
///
/// # Arguments
///
/// * `original` - The original SOQL query
///
/// # Returns
///
/// - `Ok(Some(query))` - Successfully rewrote to COUNT form
/// - `Ok(None)` - Query shape is unsupported for COUNT (e.g., GROUP BY)
/// - `Err(AppError)` - Invalid query (e.g., no top-level FROM found)
///
/// # Example
///
/// ```ignore
/// let count_soql = make_count_soql("SELECT Id, Name FROM Account WHERE Active = true")?;
/// assert_eq!(count_soql, Some("SELECT COUNT() FROM Account WHERE Active = true".to_string()));
/// ```
pub fn make_count_soql(original: &str) -> Result<Option<String>, AppError> {
    // Fast-fail on unsupported patterns
    let original_lower = original.to_ascii_lowercase();
    for pattern in UNSUPPORTED_PATTERNS {
        if original_lower.contains(pattern) {
            return Ok(None);
        }
    }

    // Find the top-level FROM keyword
    let from_pos = find_top_level_from(original)?;

    // Build the COUNT query: SELECT COUNT() + everything from FROM onward
    let from_onwards = &original[from_pos..];
    let count_query = format!("SELECT COUNT() {}", from_onwards);

    Ok(Some(count_query))
}

/// Finds the position of the top-level FROM keyword in a SOQL query.
///
/// This handles:
/// - Nested subqueries (parentheses)
/// - String literals (single and double quotes)
/// - Escaped quotes within strings
///
/// # Returns
///
/// The byte position of the 'F' in the top-level FROM keyword.
///
/// # Errors
///
/// Returns `AppError::SalesforceError` if no top-level FROM is found.
fn find_top_level_from(soql: &str) -> Result<usize, AppError> {
    let bytes = soql.as_bytes();
    let len = bytes.len();

    let mut i = 0;
    let mut paren_depth: u32 = 0;
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    while i < len {
        let ch = bytes[i];

        // Handle escape sequences inside quotes
        if (in_single_quote || in_double_quote) && ch == b'\\' {
            // Skip the backslash and the next character
            i += 2;
            continue;
        }

        // Handle quote state transitions
        if ch == b'\'' && !in_double_quote {
            in_single_quote = !in_single_quote;
            i += 1;
            continue;
        }

        if ch == b'"' && !in_single_quote {
            in_double_quote = !in_double_quote;
            i += 1;
            continue;
        }

        // Skip processing if inside quotes
        if in_single_quote || in_double_quote {
            i += 1;
            continue;
        }

        // Track parenthesis depth
        if ch == b'(' {
            paren_depth += 1;
            i += 1;
            continue;
        }

        if ch == b')' {
            paren_depth = paren_depth.saturating_sub(1);
            i += 1;
            continue;
        }

        // Look for "FROM" at top level (paren_depth == 0)
        if paren_depth == 0 && i + 4 <= len {
            // Check if we have "FROM" (case-insensitive)
            let candidate = &soql[i..i + 4];
            if candidate.eq_ignore_ascii_case("from") {
                // Make sure it's a word boundary (not part of a larger identifier)
                let is_start = i == 0 || !is_identifier_char(bytes[i - 1]);
                let is_end = i + 4 >= len || !is_identifier_char(bytes[i + 4]);

                if is_start && is_end {
                    return Ok(i);
                }
            }
        }

        i += 1;
    }

    Err(AppError::SalesforceError(
        "Invalid SOQL: no top-level FROM clause found".to_string(),
    ))
}

/// Checks if a byte is a valid identifier character (letter, digit, or underscore).
fn is_identifier_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

// ─────────────────────────────────────────────────────────────────────────────
// COUNT Query Execution
// ─────────────────────────────────────────────────────────────────────────────

/// Executes a COUNT query with timeout protection.
///
/// # Arguments
///
/// * `rest` - The REST query client to use
/// * `soql` - The original SOQL query (will be rewritten to COUNT form)
/// * `prefs` - Query preferences including timeout
///
/// # Returns
///
/// - `Ok(CountResult::Count(n))` - Successfully counted n rows
/// - `Ok(CountResult::TimedOut)` - Query timed out
/// - `Ok(CountResult::Unsupported)` - Query shape doesn't support COUNT
/// - `Err(AppError)` - Query execution failed
pub async fn count_query(
    rest: &RestQueryClient,
    soql: &str,
    prefs: &QueryPreferences,
) -> Result<CountResult, AppError> {
    // Attempt to rewrite to COUNT form
    let count_soql = match make_count_soql(soql)? {
        Some(q) => q,
        None => return Ok(CountResult::Unsupported),
    };

    // Execute with timeout
    let timeout_duration = Duration::from_secs(prefs.count_timeout_secs);
    let query_future = rest.query(&count_soql);

    match timeout(timeout_duration, query_future).await {
        Ok(Ok(result)) => {
            // Parse the count from the response
            parse_count_result(&result.records)
        }
        Ok(Err(e)) => {
            // Query failed
            Err(e)
        }
        Err(_) => {
            // Timeout elapsed
            debug!("[STRATEGY] COUNT query timed out");
            Ok(CountResult::TimedOut)
        }
    }
}

/// Parses the count value from Salesforce COUNT() response records.
///
/// Salesforce returns COUNT() results as:
/// ```json
/// { "records": [{ "expr0": 123 }] }
/// ```
///
/// The expr0 field can be a number or a string representation.
fn parse_count_result(records: &[serde_json::Value]) -> Result<CountResult, AppError> {
    let record = records
        .first()
        .ok_or_else(|| AppError::SalesforceError("COUNT query returned no records".to_string()))?;

    let expr0 = record.get("expr0").ok_or_else(|| {
        AppError::SalesforceError("COUNT response missing expr0 field".to_string())
    })?;

    // Handle both number and string representations
    let count = if let Some(n) = expr0.as_u64() {
        n
    } else if let Some(n) = expr0.as_i64() {
        n as u64
    } else if let Some(s) = expr0.as_str() {
        s.parse::<u64>()
            .map_err(|_| AppError::SalesforceError(format!("Cannot parse COUNT value: {}", s)))?
    } else if let Some(n) = expr0.as_f64() {
        n as u64
    } else {
        return Err(AppError::SalesforceError(format!(
            "Unexpected COUNT value type: {:?}",
            expr0
        )));
    };

    Ok(CountResult::Count(count))
}

// ─────────────────────────────────────────────────────────────────────────────
// Strategy Determination
// ─────────────────────────────────────────────────────────────────────────────

/// Determines the query strategy based purely on row count.
///
/// This is a pure function for easy unit testing.
///
/// # Strategy Rules
///
/// - `count <= 50,000` → REST (suitable for interactive grid display)
/// - `count > 50,000` → Bulk (for large data exports)
pub fn determine_strategy_from_count(count: u64) -> QueryStrategy {
    if count > BULK_THRESHOLD {
        QueryStrategy::Bulk
    } else {
        QueryStrategy::Rest
    }
}

/// Determines the query strategy based on a COUNT result.
///
/// # Arguments
///
/// * `_soql` - The original SOQL query (reserved for future use)
/// * `count` - The result of the COUNT query
///
/// # Returns
///
/// - `StrategyDecision::Determined(strategy)` - Strategy can be determined automatically
/// - `StrategyDecision::NeedUserDecision { .. }` - User input required
pub fn determine_strategy(_soql: &str, count: CountResult) -> StrategyDecision {
    match count {
        CountResult::Count(n) => StrategyDecision::Determined(determine_strategy_from_count(n)),
        CountResult::TimedOut => {
            // Suggest Bulk because if COUNT timed out, the query is likely
            // touching a large number of records or has complex filters
            StrategyDecision::NeedUserDecision {
                suggested: QueryStrategy::Bulk,
                reason: "Row count timed out. The query may return a large number of records. \
                         Bulk API is recommended for large exports."
                    .to_string(),
            }
        }
        CountResult::Unsupported => StrategyDecision::NeedUserDecision {
            suggested: QueryStrategy::Rest,
            reason: "COUNT is not supported for this query shape (e.g., GROUP BY, HAVING). \
                         Please select a query method."
                .to_string(),
        },
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Debug Utilities (Opt-in)
// ─────────────────────────────────────────────────────────────────────────────

/// Redacts quoted string literals in SOQL for safe debug logging.
///
/// Replaces contents of single-quoted and double-quoted strings with '?'.
/// This is only used for opt-in debug logging.
///
/// # Example
///
/// ```ignore
/// let redacted = redact_soql_literals("SELECT Id FROM Account WHERE Name = 'Secret'");
/// assert_eq!(redacted, "SELECT Id FROM Account WHERE Name = '?'");
/// ```
#[allow(dead_code)]
pub(crate) fn redact_soql_literals(soql: &str) -> String {
    let mut result = String::with_capacity(soql.len());
    let bytes = soql.as_bytes();
    let len = bytes.len();
    let mut i = 0;

    while i < len {
        let ch = bytes[i];

        if ch == b'\'' || ch == b'"' {
            let quote_char = ch;
            result.push(quote_char as char);
            result.push('?');
            i += 1;

            // Skip to the closing quote
            while i < len {
                let inner = bytes[i];
                if inner == b'\\' && i + 1 < len {
                    // Skip escaped character
                    i += 2;
                } else if inner == quote_char {
                    result.push(quote_char as char);
                    i += 1;
                    break;
                } else {
                    i += 1;
                }
            }
        } else {
            result.push(ch as char);
            i += 1;
        }
    }

    result
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ─────────────────────────────────────────────────────────────────────────
    // make_count_soql Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn make_count_soql_simple_query() {
        let result = make_count_soql("SELECT Id, Name FROM Account WHERE Name != ''");
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Some("SELECT COUNT() FROM Account WHERE Name != ''".to_string())
        );
    }

    #[test]
    fn make_count_soql_preserves_case_in_from_onwards() {
        let result = make_count_soql("SELECT Id FROM Account WHERE Name = 'Test'");
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Some("SELECT COUNT() FROM Account WHERE Name = 'Test'".to_string())
        );
    }

    #[test]
    fn make_count_soql_with_nested_subquery() {
        // The subquery has its own FROM, but we should find the top-level FROM
        let result = make_count_soql(
            "SELECT Id, (SELECT Id FROM Contacts) FROM Account WHERE Active__c = true",
        );
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Some("SELECT COUNT() FROM Account WHERE Active__c = true".to_string())
        );
    }

    #[test]
    fn make_count_soql_with_from_in_string_literal() {
        // "FROM" inside a string should not be treated as the FROM clause
        let result = make_count_soql("SELECT Id FROM Account WHERE Description = 'FROM here'");
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Some("SELECT COUNT() FROM Account WHERE Description = 'FROM here'".to_string())
        );
    }

    #[test]
    fn make_count_soql_group_by_returns_none() {
        let result = make_count_soql("SELECT COUNT(Id), Type FROM Account GROUP BY Type");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn make_count_soql_having_returns_none() {
        let result = make_count_soql(
            "SELECT COUNT(Id), Type FROM Account GROUP BY Type HAVING COUNT(Id) > 10",
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn make_count_soql_offset_returns_none() {
        let result = make_count_soql("SELECT Id FROM Account LIMIT 10 OFFSET 20");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn make_count_soql_no_from_returns_error() {
        let result = make_count_soql("SELECT Id, Name");
        assert!(result.is_err());
        match result {
            Err(AppError::SalesforceError(msg)) => {
                assert!(msg.contains("no top-level FROM"));
            }
            _ => panic!("Expected SalesforceError"),
        }
    }

    #[test]
    fn make_count_soql_with_order_by_and_limit() {
        let result = make_count_soql("SELECT Id, Name FROM Account ORDER BY Name LIMIT 100");
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Some("SELECT COUNT() FROM Account ORDER BY Name LIMIT 100".to_string())
        );
    }

    #[test]
    fn make_count_soql_handles_escaped_quotes() {
        let result = make_count_soql(r"SELECT Id FROM Account WHERE Name = 'O\'Brien'");
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Some(r"SELECT COUNT() FROM Account WHERE Name = 'O\'Brien'".to_string())
        );
    }

    #[test]
    fn make_count_soql_case_insensitive_from() {
        // "from" in lowercase
        let result = make_count_soql("SELECT Id from Account");
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Some("SELECT COUNT() from Account".to_string())
        );

        // "FROM" in uppercase
        let result = make_count_soql("SELECT Id FROM Account");
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Some("SELECT COUNT() FROM Account".to_string())
        );

        // "From" in mixed case
        let result = make_count_soql("SELECT Id From Account");
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Some("SELECT COUNT() From Account".to_string())
        );
    }

    #[test]
    fn make_count_soql_from_must_be_word_boundary() {
        // "FROMAGE" should not match as FROM
        let result = make_count_soql("SELECT Id, FROMAGE__c FROM Account");
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Some("SELECT COUNT() FROM Account".to_string())
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // determine_strategy_from_count Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn strategy_threshold_2000() {
        assert_eq!(determine_strategy_from_count(2000), QueryStrategy::Rest);
    }

    #[test]
    fn strategy_threshold_2001() {
        assert_eq!(determine_strategy_from_count(2001), QueryStrategy::Rest);
    }

    #[test]
    fn strategy_threshold_50000() {
        assert_eq!(determine_strategy_from_count(50000), QueryStrategy::Rest);
    }

    #[test]
    fn strategy_threshold_50001() {
        assert_eq!(determine_strategy_from_count(50001), QueryStrategy::Bulk);
    }

    #[test]
    fn strategy_zero_rows() {
        assert_eq!(determine_strategy_from_count(0), QueryStrategy::Rest);
    }

    #[test]
    fn strategy_large_count() {
        assert_eq!(
            determine_strategy_from_count(1_000_000),
            QueryStrategy::Bulk
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // determine_strategy Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn determine_strategy_with_count() {
        let result = determine_strategy("SELECT Id FROM Account", CountResult::Count(100));
        assert_eq!(result, StrategyDecision::Determined(QueryStrategy::Rest));

        let result = determine_strategy("SELECT Id FROM Account", CountResult::Count(100_000));
        assert_eq!(result, StrategyDecision::Determined(QueryStrategy::Bulk));
    }

    #[test]
    fn determine_strategy_timed_out_needs_user_decision() {
        let result = determine_strategy("SELECT Id FROM Account", CountResult::TimedOut);
        match result {
            StrategyDecision::NeedUserDecision { suggested, reason } => {
                assert_eq!(suggested, QueryStrategy::Bulk);
                assert!(!reason.is_empty());
                assert!(reason.contains("timed out") || reason.contains("Timed"));
            }
            _ => panic!("Expected NeedUserDecision for TimedOut"),
        }
    }

    #[test]
    fn determine_strategy_unsupported_needs_user_decision() {
        let result = determine_strategy("SELECT Id FROM Account", CountResult::Unsupported);
        match result {
            StrategyDecision::NeedUserDecision { suggested, reason } => {
                assert_eq!(suggested, QueryStrategy::Rest);
                assert!(!reason.is_empty());
                assert!(
                    reason.contains("not supported") || reason.contains("GROUP BY"),
                    "Reason should explain why: {}",
                    reason
                );
            }
            _ => panic!("Expected NeedUserDecision for Unsupported"),
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // parse_count_result Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn parse_count_from_number() {
        let records = vec![serde_json::json!({ "expr0": 42 })];
        let result = parse_count_result(&records);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CountResult::Count(42));
    }

    #[test]
    fn parse_count_from_string() {
        let records = vec![serde_json::json!({ "expr0": "123" })];
        let result = parse_count_result(&records);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CountResult::Count(123));
    }

    #[test]
    fn parse_count_from_float() {
        // Sometimes Salesforce returns floats for aggregate values
        let records = vec![serde_json::json!({ "expr0": 99.0 })];
        let result = parse_count_result(&records);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CountResult::Count(99));
    }

    #[test]
    fn parse_count_empty_records_error() {
        let records: Vec<serde_json::Value> = vec![];
        let result = parse_count_result(&records);
        assert!(result.is_err());
    }

    #[test]
    fn parse_count_missing_expr0_error() {
        let records = vec![serde_json::json!({ "other_field": 42 })];
        let result = parse_count_result(&records);
        assert!(result.is_err());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // redact_soql_literals Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn redact_single_quoted_string() {
        let result = redact_soql_literals("SELECT Id FROM Account WHERE Name = 'Secret'");
        assert_eq!(result, "SELECT Id FROM Account WHERE Name = '?'");
    }

    #[test]
    fn redact_double_quoted_string() {
        let result = redact_soql_literals("SELECT Id FROM Account WHERE Name = \"Secret\"");
        assert_eq!(result, "SELECT Id FROM Account WHERE Name = \"?\"");
    }

    #[test]
    fn redact_multiple_strings() {
        let result =
            redact_soql_literals("SELECT Id FROM Account WHERE Name = 'First' AND Type = 'Second'");
        assert_eq!(
            result,
            "SELECT Id FROM Account WHERE Name = '?' AND Type = '?'"
        );
    }

    #[test]
    fn redact_escaped_quote() {
        let result = redact_soql_literals(r"SELECT Id FROM Account WHERE Name = 'O\'Brien'");
        assert_eq!(result, "SELECT Id FROM Account WHERE Name = '?'");
    }

    #[test]
    fn redact_no_strings() {
        let result = redact_soql_literals("SELECT Id, Name FROM Account WHERE Active = true");
        assert_eq!(result, "SELECT Id, Name FROM Account WHERE Active = true");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // QueryStrategy Serialization Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn query_strategy_serialization() {
        let auto = serde_json::to_string(&QueryStrategy::Auto).unwrap();
        let rest = serde_json::to_string(&QueryStrategy::Rest).unwrap();
        let bulk = serde_json::to_string(&QueryStrategy::Bulk).unwrap();

        assert_eq!(auto, "\"Auto\"");
        assert_eq!(rest, "\"Rest\"");
        assert_eq!(bulk, "\"Bulk\"");

        // Roundtrip
        let parsed: QueryStrategy = serde_json::from_str(&auto).unwrap();
        assert_eq!(parsed, QueryStrategy::Auto);
    }

    #[test]
    fn query_preferences_default() {
        let prefs = QueryPreferences::default();
        assert_eq!(prefs.count_timeout_secs, 5);
    }
}
