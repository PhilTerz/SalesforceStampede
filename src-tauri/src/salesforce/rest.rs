//! Salesforce REST API client for SOQL query execution with pagination.
//!
//! This module provides a high-level client for executing SOQL queries against
//! the Salesforce REST API. Key features:
//!
//! - **Automatic pagination** - Fetches all records across multiple pages
//! - **Row limiting** - Stop fetching after a specified number of records
//! - **Secure logging** - Never logs raw SOQL queries or sensitive data
//! - **Salesforce error mapping** - Parses Salesforce API errors into `AppError`

use reqwest::Method;
use serde::{Deserialize, Serialize};
use tracing::info;
use url::Url;

use crate::error::AppError;
use crate::salesforce::client::SalesforceClient;
use crate::salesforce::API_VERSION;

// ─────────────────────────────────────────────────────────────────────────────
// Internal Wire Types (match Salesforce JSON exactly)
// ─────────────────────────────────────────────────────────────────────────────

/// Internal struct that mirrors the Salesforce query response JSON exactly.
/// Uses camelCase field names to match the API response.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireQueryResponse {
    /// Total number of records that match the query (not just this page).
    total_size: u64,
    /// Whether this is the last page of results.
    done: bool,
    /// URL to fetch the next page of results (relative to instance URL).
    /// Only present if `done` is false.
    next_records_url: Option<String>,
    /// The actual records returned in this page.
    records: Vec<serde_json::Value>,
}

/// Salesforce API error response format.
/// Salesforce returns errors as an array of error objects.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireSalesforceError {
    message: String,
    error_code: String,
    #[allow(dead_code)]
    fields: Option<Vec<String>>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Public Types
// ─────────────────────────────────────────────────────────────────────────────

/// Result of a SOQL query execution.
///
/// Contains all fetched records and metadata about the query result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    /// The records returned by the query.
    pub records: Vec<serde_json::Value>,
    /// Total number of records that match the query in Salesforce.
    /// Note: This may be larger than `records.len()` if a row limit was applied.
    pub total_size: u64,
    /// Whether all matching records have been fetched.
    /// - `true` if all records were retrieved
    /// - `false` if pagination stopped early (due to row limit or Salesforce pagination)
    pub done: bool,
}

// ─────────────────────────────────────────────────────────────────────────────
// RestQueryClient
// ─────────────────────────────────────────────────────────────────────────────

/// Client for executing SOQL queries against the Salesforce REST API.
///
/// Wraps a `SalesforceClient` to provide high-level query methods with
/// automatic pagination and error handling.
///
/// # Example
///
/// ```ignore
/// let client = RestQueryClient::new(salesforce_client);
/// let result = client.query("SELECT Id, Name FROM Account").await?;
/// println!("Found {} accounts", result.records.len());
/// ```
#[derive(Clone)]
pub struct RestQueryClient {
    /// The underlying Salesforce HTTP client.
    client: SalesforceClient,
}

impl RestQueryClient {
    /// Creates a new REST query client wrapping the given Salesforce client.
    pub fn new(client: SalesforceClient) -> Self {
        Self { client }
    }

    /// Executes a SOQL query and returns all matching records.
    ///
    /// Automatically handles pagination to fetch all records across multiple
    /// API calls if the result set is large.
    ///
    /// # Arguments
    ///
    /// * `soql` - The SOQL query to execute
    ///
    /// # Errors
    ///
    /// - `AppError::NotAuthenticated` - No valid credentials
    /// - `AppError::SessionExpired` - Token refresh failed
    /// - `AppError::SalesforceError` - Query syntax error or API error
    /// - `AppError::ConnectionFailed` - Network error
    ///
    /// # Security
    ///
    /// The SOQL query is never logged to prevent leaking sensitive field names
    /// or filter criteria.
    pub async fn query(&self, soql: &str) -> Result<QueryResult, AppError> {
        self.query_internal(soql, None, None::<fn(u64)>).await
    }

    /// Executes a SOQL query and returns up to `max_rows` records.
    ///
    /// Stops fetching additional pages once the specified number of records
    /// has been reached. Useful for preview queries or limiting memory usage.
    ///
    /// # Arguments
    ///
    /// * `soql` - The SOQL query to execute
    /// * `max_rows` - Maximum number of records to return
    ///
    /// # Returns
    ///
    /// A `QueryResult` where:
    /// - `records.len() <= max_rows`
    /// - `done` is `false` if more records exist but weren't fetched
    /// - `total_size` reflects the true total in Salesforce
    ///
    /// # Errors
    ///
    /// Same as [`query`](Self::query).
    pub async fn query_with_limit(
        &self,
        soql: &str,
        max_rows: u64,
    ) -> Result<QueryResult, AppError> {
        self.query_internal(soql, Some(max_rows), None::<fn(u64)>).await
    }

    /// Executes a SOQL query with an optional progress callback and row limit.
    ///
    /// The progress callback is invoked after each page is fetched with the
    /// current total number of records retrieved.
    ///
    /// # Arguments
    ///
    /// * `soql` - The SOQL query to execute
    /// * `max_rows` - Optional maximum number of records to return
    /// * `on_progress` - Optional callback invoked with current record count after each page
    ///
    /// # Example
    ///
    /// ```ignore
    /// let result = client.query_with_progress(
    ///     "SELECT Id FROM Account",
    ///     Some(1000),
    ///     Some(|count| println!("Fetched {} records", count)),
    /// ).await?;
    /// ```
    pub async fn query_with_progress<F>(
        &self,
        soql: &str,
        max_rows: Option<u64>,
        on_progress: Option<F>,
    ) -> Result<QueryResult, AppError>
    where
        F: Fn(u64),
    {
        self.query_internal(soql, max_rows, on_progress).await
    }

    /// Internal query implementation with optional row limit and progress callback.
    async fn query_internal<F>(
        &self,
        soql: &str,
        max_rows: Option<u64>,
        on_progress: Option<F>,
    ) -> Result<QueryResult, AppError>
    where
        F: Fn(u64),
    {
        // Build initial query URL with properly encoded SOQL
        let base_path = format!("/services/data/{}/query", API_VERSION);
        let initial_url = self.build_query_url(&base_path, soql).await?;

        // Log query start (without the actual SOQL)
        info!("[REST] Starting SOQL query (limit: {:?})", max_rows);

        // Accumulate records across pages
        let mut all_records: Vec<serde_json::Value> = Vec::new();
        let mut total_size: u64 = 0;
        let mut next_url: Option<Url> = Some(initial_url);
        let mut page_count: u32 = 0;

        // Iterative pagination loop
        while let Some(url) = next_url.take() {
            page_count += 1;

            // Execute the request
            let response = self.execute_query_request(&url).await?;

            // Parse the response
            let wire_response: WireQueryResponse = response
                .json()
                .await
                .map_err(|e| AppError::Internal(format!("Failed to parse query response: {}", e)))?;

            // Store total_size from first page
            if page_count == 1 {
                total_size = wire_response.total_size;
            }

            // Append records
            all_records.extend(wire_response.records);

            // Invoke progress callback with current record count
            if let Some(ref callback) = on_progress {
                callback(all_records.len() as u64);
            }

            // Check if we should stop due to row limit
            if let Some(limit) = max_rows {
                let current_count = all_records.len() as u64;
                if current_count >= limit {
                    // Check if we had excess records before truncating
                    let had_excess = current_count > limit;
                    all_records.truncate(limit as usize);

                    // We're truly done only if:
                    // 1. The API says done (no more pages)
                    // 2. We didn't have excess records (we got exactly limit or less)
                    let is_truly_done = wire_response.done && !had_excess;

                    info!(
                        "[REST] Query complete: {} records fetched (limited), {} pages, done={}",
                        all_records.len(),
                        page_count,
                        is_truly_done
                    );
                    return Ok(QueryResult {
                        records: all_records,
                        total_size,
                        done: is_truly_done,
                    });
                }
            }

            // Check if there are more pages
            if wire_response.done {
                break;
            }

            // Prepare next page URL
            if let Some(next_records_url) = wire_response.next_records_url {
                next_url = Some(self.join_url(&next_records_url).await?);
            }
        }

        info!(
            "[REST] Query complete: {} records fetched, {} pages",
            all_records.len(),
            page_count
        );

        Ok(QueryResult {
            records: all_records,
            total_size,
            done: true,
        })
    }

    /// Builds a query URL with the SOQL string properly URL-encoded.
    async fn build_query_url(&self, path: &str, soql: &str) -> Result<Url, AppError> {
        let mut url = self.client.build_url(path).await?;
        url.query_pairs_mut().append_pair("q", soql);
        Ok(url)
    }

    /// Joins a relative URL path with the instance URL.
    async fn join_url(&self, relative_path: &str) -> Result<Url, AppError> {
        // The nextRecordsUrl is relative to the instance URL
        self.client.build_url(relative_path).await
    }

    /// Executes a query request and handles error responses.
    async fn execute_query_request(&self, url: &Url) -> Result<reqwest::Response, AppError> {
        // Use request_authed with the full path including query string
        let path = if let Some(query) = url.query() {
            format!("{}?{}", url.path(), query)
        } else {
            url.path().to_string()
        };

        let response = self.client.request_authed(Method::GET, &path, None).await?;

        // Check for error responses
        let status = response.status();
        if !status.is_success() {
            return self.handle_error_response(response, status).await;
        }

        Ok(response)
    }

    /// Parses error response body and maps to appropriate AppError.
    async fn handle_error_response(
        &self,
        response: reqwest::Response,
        status: reqwest::StatusCode,
    ) -> Result<reqwest::Response, AppError> {
        // Try to read the response body
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| String::from("Unable to read error body"));

        // Attempt to parse as Salesforce error array
        if let Ok(errors) = serde_json::from_str::<Vec<WireSalesforceError>>(&body) {
            if let Some(first_error) = errors.first() {
                return Err(AppError::SalesforceError(format!(
                    "[{}] {}",
                    first_error.error_code, first_error.message
                )));
            }
        }

        // Fallback to generic error with status code
        Err(AppError::SalesforceError(format!(
            "HTTP {} - {}",
            status.as_u16(),
            status.canonical_reason().unwrap_or("Unknown error")
        )))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::salesforce::client::OrgCredentials;
    use secrecy::SecretString;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Helper to create a SalesforceClient with mock server URL.
    fn create_test_client(instance_url: &str) -> SalesforceClient {
        let creds = OrgCredentials {
            org_id: "00Dxx0000001234".to_string(),
            user_id: "005xx0000001234".to_string(),
            username: "test@example.com".to_string(),
            instance_url: instance_url.to_string(),
            access_token: SecretString::from("test_token".to_string()),
            refresh_token: Some(SecretString::from("test_refresh".to_string())),
            api_version: "v60.0".to_string(),
        };
        SalesforceClient::new(creds).unwrap()
    }

    /// Helper to generate mock records.
    fn mock_records(count: usize, start_id: usize) -> Vec<serde_json::Value> {
        (start_id..start_id + count)
            .map(|i| {
                serde_json::json!({
                    "Id": format!("001xx00000{:05}", i),
                    "Name": format!("Account {}", i)
                })
            })
            .collect()
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 1: Pagination Flow
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_pagination_fetches_all_pages() {
        let mock_server = MockServer::start().await;
        let sf_client = create_test_client(&mock_server.uri());
        let client = RestQueryClient::new(sf_client);

        // Page 1: 5 records, not done
        let page1_response = serde_json::json!({
            "totalSize": 10,
            "done": false,
            "nextRecordsUrl": "/services/data/v60.0/query/01gxx000000001-500",
            "records": mock_records(5, 1)
        });

        Mock::given(method("GET"))
            .and(path("/services/data/v60.0/query"))
            .and(query_param("q", "SELECT Id, Name FROM Account"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&page1_response))
            .expect(1)
            .mount(&mock_server)
            .await;

        // Page 2: 5 records, done
        let page2_response = serde_json::json!({
            "totalSize": 10,
            "done": true,
            "records": mock_records(5, 6)
        });

        Mock::given(method("GET"))
            .and(path("/services/data/v60.0/query/01gxx000000001-500"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&page2_response))
            .expect(1)
            .mount(&mock_server)
            .await;

        // Execute query
        let result = client.query("SELECT Id, Name FROM Account").await;

        // Assert
        assert!(result.is_ok(), "Query should succeed");
        let query_result = result.unwrap();
        assert_eq!(query_result.records.len(), 10, "Should have 10 records");
        assert_eq!(query_result.total_size, 10, "Total size should be 10");
        assert!(query_result.done, "Should be done");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 2: Limit Check
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_query_with_limit_stops_at_limit() {
        let mock_server = MockServer::start().await;
        let sf_client = create_test_client(&mock_server.uri());
        let client = RestQueryClient::new(sf_client);

        // Page 1: 8 records, not done (total is 100)
        let page1_response = serde_json::json!({
            "totalSize": 100,
            "done": false,
            "nextRecordsUrl": "/services/data/v60.0/query/01gxx000000002-500",
            "records": mock_records(8, 1)
        });

        Mock::given(method("GET"))
            .and(path("/services/data/v60.0/query"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&page1_response))
            .expect(1)
            .mount(&mock_server)
            .await;

        // Page 2: 8 more records (should not be fetched since limit is 10)
        let page2_response = serde_json::json!({
            "totalSize": 100,
            "done": false,
            "nextRecordsUrl": "/services/data/v60.0/query/01gxx000000003-500",
            "records": mock_records(8, 9)
        });

        Mock::given(method("GET"))
            .and(path("/services/data/v60.0/query/01gxx000000002-500"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&page2_response))
            .expect(1)
            .mount(&mock_server)
            .await;

        // Execute query with limit of 10
        let result = client.query_with_limit("SELECT Id FROM Account", 10).await;

        // Assert
        assert!(result.is_ok(), "Query should succeed");
        let query_result = result.unwrap();
        assert_eq!(
            query_result.records.len(),
            10,
            "Should have exactly 10 records"
        );
        assert_eq!(
            query_result.total_size, 100,
            "Total size should reflect actual total"
        );
        assert!(
            !query_result.done,
            "Should not be done (stopped early due to limit)"
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Test 3: Error Parsing
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_error_response_parsing() {
        let mock_server = MockServer::start().await;
        let sf_client = create_test_client(&mock_server.uri());
        let client = RestQueryClient::new(sf_client);

        // Mock Salesforce error response
        let error_response = serde_json::json!([{
            "message": "INVALID_FIELD: Select Id, InvalidField FROM Account",
            "errorCode": "INVALID_FIELD",
            "fields": ["InvalidField"]
        }]);

        Mock::given(method("GET"))
            .and(path("/services/data/v60.0/query"))
            .respond_with(ResponseTemplate::new(400).set_body_json(&error_response))
            .expect(1)
            .mount(&mock_server)
            .await;

        // Execute query with invalid field
        let result = client.query("SELECT Id, InvalidField FROM Account").await;

        // Assert
        assert!(result.is_err(), "Query should fail");
        let error = result.unwrap_err();
        match error {
            AppError::SalesforceError(msg) => {
                assert!(
                    msg.contains("INVALID_FIELD"),
                    "Error should contain error code: {}",
                    msg
                );
                assert!(
                    msg.contains("Select Id, InvalidField FROM Account"),
                    "Error should contain message: {}",
                    msg
                );
            }
            _ => panic!("Expected SalesforceError, got: {:?}", error),
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Additional Tests
    // ─────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_single_page_query() {
        let mock_server = MockServer::start().await;
        let sf_client = create_test_client(&mock_server.uri());
        let client = RestQueryClient::new(sf_client);

        // Single page response
        let response = serde_json::json!({
            "totalSize": 3,
            "done": true,
            "records": mock_records(3, 1)
        });

        Mock::given(method("GET"))
            .and(path("/services/data/v60.0/query"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&response))
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = client.query("SELECT Id FROM Account").await;

        assert!(result.is_ok());
        let query_result = result.unwrap();
        assert_eq!(query_result.records.len(), 3);
        assert_eq!(query_result.total_size, 3);
        assert!(query_result.done);
    }

    #[tokio::test]
    async fn test_empty_result() {
        let mock_server = MockServer::start().await;
        let sf_client = create_test_client(&mock_server.uri());
        let client = RestQueryClient::new(sf_client);

        // Empty response
        let response = serde_json::json!({
            "totalSize": 0,
            "done": true,
            "records": []
        });

        Mock::given(method("GET"))
            .and(path("/services/data/v60.0/query"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&response))
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = client
            .query("SELECT Id FROM Account WHERE Id = 'nonexistent'")
            .await;

        assert!(result.is_ok());
        let query_result = result.unwrap();
        assert!(query_result.records.is_empty());
        assert_eq!(query_result.total_size, 0);
        assert!(query_result.done);
    }

    #[tokio::test]
    async fn test_generic_http_error() {
        let mock_server = MockServer::start().await;
        let sf_client = create_test_client(&mock_server.uri());
        let client = RestQueryClient::new(sf_client);

        // Non-JSON error response (e.g., HTML error page)
        Mock::given(method("GET"))
            .and(path("/services/data/v60.0/query"))
            .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
            .expect(1)
            .mount(&mock_server)
            .await;

        let result = client.query("SELECT Id FROM Account").await;

        assert!(result.is_err());
        let error = result.unwrap_err();
        match error {
            AppError::SalesforceError(msg) => {
                assert!(
                    msg.contains("500"),
                    "Error should contain status code: {}",
                    msg
                );
            }
            _ => panic!("Expected SalesforceError, got: {:?}", error),
        }
    }

    #[tokio::test]
    async fn test_limit_exact_match() {
        let mock_server = MockServer::start().await;
        let sf_client = create_test_client(&mock_server.uri());
        let client = RestQueryClient::new(sf_client);

        // Response with exactly 5 records
        let response = serde_json::json!({
            "totalSize": 5,
            "done": true,
            "records": mock_records(5, 1)
        });

        Mock::given(method("GET"))
            .and(path("/services/data/v60.0/query"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&response))
            .expect(1)
            .mount(&mock_server)
            .await;

        // Limit equals total records
        let result = client.query_with_limit("SELECT Id FROM Account", 5).await;

        assert!(result.is_ok());
        let query_result = result.unwrap();
        assert_eq!(query_result.records.len(), 5);
        assert!(
            query_result.done,
            "Should be done when limit equals total and page is complete"
        );
    }

    #[test]
    fn test_query_result_serialization() {
        let result = QueryResult {
            records: vec![serde_json::json!({"Id": "001xx"})],
            total_size: 1,
            done: true,
        };

        let json = serde_json::to_string(&result).unwrap();
        let parsed: QueryResult = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.records.len(), 1);
        assert_eq!(parsed.total_size, 1);
        assert!(parsed.done);
    }
}
