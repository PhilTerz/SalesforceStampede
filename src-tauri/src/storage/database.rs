//! SQLite database module with schema migrations.

use std::path::PathBuf;
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension};

use crate::error::AppError;

/// Current schema version. Increment when adding new migrations.
const SCHEMA_VERSION: i32 = 3;

/// V1 schema: creates all initial tables and indexes.
const V1_SCHEMA: &str = r#"
-- Organizations (Salesforce orgs)
CREATE TABLE IF NOT EXISTS orgs (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    org_type TEXT NOT NULL,
    instance_url TEXT NOT NULL,
    login_url TEXT NOT NULL,
    username TEXT NOT NULL,
    api_version TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

-- Job groups (bulk upload groups)
CREATE TABLE IF NOT EXISTS job_groups (
    id TEXT PRIMARY KEY NOT NULL,
    org_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    sobject_name TEXT NOT NULL,
    batch_size INTEGER NOT NULL,
    total_parts INTEGER NOT NULL,
    records_processed INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_job_groups_org_id ON job_groups(org_id);

-- Individual jobs (SF Bulk v2 jobs / parts)
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY NOT NULL,
    group_id TEXT NOT NULL,
    sf_job_id TEXT,
    part_number INTEGER NOT NULL,
    state TEXT NOT NULL,
    records_processed INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jobs_group_id ON jobs(group_id);

-- Saved queries
CREATE TABLE IF NOT EXISTS saved_queries (
    id TEXT PRIMARY KEY NOT NULL,
    org_id TEXT NOT NULL,
    name TEXT NOT NULL,
    soql TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_saved_queries_org_id ON saved_queries(org_id);

-- Query history
CREATE TABLE IF NOT EXISTS query_history (
    id TEXT PRIMARY KEY NOT NULL,
    org_id TEXT NOT NULL,
    soql TEXT NOT NULL,
    executed_at INTEGER NOT NULL,
    row_count INTEGER,
    strategy TEXT,
    duration_ms INTEGER
);

CREATE INDEX IF NOT EXISTS idx_query_history_org_id ON query_history(org_id);
CREATE INDEX IF NOT EXISTS idx_query_history_executed_at ON query_history(executed_at);
"#;

/// V2 migration: adds truncated and export_path columns to query_history.
const V2_MIGRATION: &str = r#"
ALTER TABLE query_history ADD COLUMN truncated INTEGER NOT NULL DEFAULT 0;
ALTER TABLE query_history ADD COLUMN export_path TEXT;
"#;

/// V3 migration: adds bulk_job_groups and bulk_jobs tables for ingest operations.
const V3_MIGRATION: &str = r#"
-- Bulk job groups (user-initiated bulk upload operations)
CREATE TABLE IF NOT EXISTS bulk_job_groups (
    group_id TEXT PRIMARY KEY NOT NULL,
    org_id TEXT NOT NULL,
    object TEXT NOT NULL,
    operation TEXT NOT NULL,
    state TEXT NOT NULL,
    batch_size INTEGER NOT NULL,
    total_parts INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_bulk_job_groups_org_id ON bulk_job_groups(org_id);
CREATE INDEX IF NOT EXISTS idx_bulk_job_groups_state ON bulk_job_groups(state);

-- Bulk jobs (individual Salesforce ingest jobs for each chunk)
CREATE TABLE IF NOT EXISTS bulk_jobs (
    job_id TEXT PRIMARY KEY NOT NULL,
    group_id TEXT NOT NULL,
    part_number INTEGER NOT NULL,
    state TEXT NOT NULL,
    processed_records INTEGER,
    failed_records INTEGER,
    error_message TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_bulk_jobs_group_id ON bulk_jobs(group_id);
CREATE INDEX IF NOT EXISTS idx_bulk_jobs_state ON bulk_jobs(state);
"#;

/// Saved query model.
#[derive(Debug, Clone)]
pub struct SavedQuery {
    pub id: String,
    pub org_id: String,
    pub name: String,
    pub soql: String,
    pub created_at: i64,
}

/// Query history entry model.
#[derive(Debug, Clone)]
pub struct QueryHistoryEntry {
    pub id: String,
    pub org_id: String,
    pub soql: String,
    pub executed_at: i64,
    pub row_count: Option<i64>,
    pub strategy: Option<String>,
    pub duration_ms: Option<i64>,
    pub truncated: bool,
    pub export_path: Option<String>,
}

/// Organization model (minimal for Chunk 1.2).
#[derive(Debug, Clone)]
pub struct Org {
    pub id: String,
    pub name: String,
    pub org_type: String,
    pub instance_url: String,
    pub login_url: String,
    pub username: String,
    pub api_version: String,
    pub created_at: i64,
    pub updated_at: i64,
}

/// SQLite database handle.
#[derive(Debug)]
pub struct Database {
    db_path: PathBuf,
}

impl Database {
    /// Initializes the database at the given path.
    /// Creates parent directories if needed, opens the SQLite file, and runs migrations.
    pub async fn init(db_path: PathBuf) -> Result<Self, AppError> {
        let path = db_path.clone();

        tokio::task::spawn_blocking(move || {
            // Create parent directory if needed
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    AppError::Internal(format!("Failed to create database directory: {e}"))
                })?;
            }

            // Open connection and configure
            let mut conn = Connection::open(&path)
                .map_err(|e| AppError::Internal(format!("Failed to open database: {e}")))?;

            configure_connection(&conn)?;
            run_migrations(&mut conn)?;

            Ok::<_, AppError>(())
        })
        .await
        .map_err(|e| AppError::Internal(format!("Database init task failed: {e}")))??;

        Ok(Self { db_path })
    }

    /// Returns the database path for use by other storage modules.
    pub fn db_path(&self) -> &PathBuf {
        &self.db_path
    }

    /// Simple health check: executes SELECT 1.
    pub async fn health_check(&self) -> Result<(), AppError> {
        let db_path = self.db_path.clone();

        tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&db_path)
                .map_err(|e| AppError::Internal(format!("Failed to open database: {e}")))?;

            configure_connection(&conn)?;

            conn.query_row("SELECT 1", [], |_| Ok(()))
                .map_err(|e| AppError::Internal(format!("Health check failed: {e}")))?;

            Ok::<_, AppError>(())
        })
        .await
        .map_err(|e| AppError::Internal(format!("Health check task failed: {e}")))??;

        Ok(())
    }

    /// Inserts a new organization.
    pub async fn insert_org(&self, org: Org) -> Result<(), AppError> {
        let db_path = self.db_path.clone();

        tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&db_path)
                .map_err(|e| AppError::Internal(format!("Failed to open database: {e}")))?;

            configure_connection(&conn)?;

            conn.execute(
                r#"
                INSERT INTO orgs (id, name, org_type, instance_url, login_url, username, api_version, created_at, updated_at)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                "#,
                rusqlite::params![
                    org.id,
                    org.name,
                    org.org_type,
                    org.instance_url,
                    org.login_url,
                    org.username,
                    org.api_version,
                    org.created_at,
                    org.updated_at,
                ],
            )
            .map_err(|e| AppError::Internal(format!("Failed to insert org: {e}")))?;

            Ok::<_, AppError>(())
        })
        .await
        .map_err(|e| AppError::Internal(format!("Insert org task failed: {e}")))??;

        Ok(())
    }

    /// Lists all organizations.
    pub async fn list_orgs(&self) -> Result<Vec<Org>, AppError> {
        let db_path = self.db_path.clone();

        tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&db_path)
                .map_err(|e| AppError::Internal(format!("Failed to open database: {e}")))?;

            configure_connection(&conn)?;

            let mut stmt = conn
                .prepare(
                    r#"
                    SELECT id, name, org_type, instance_url, login_url, username, api_version, created_at, updated_at
                    FROM orgs
                    ORDER BY name ASC
                    "#,
                )
                .map_err(|e| AppError::Internal(format!("Failed to prepare query: {e}")))?;

            let orgs = stmt
                .query_map([], |row| {
                    Ok(Org {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        org_type: row.get(2)?,
                        instance_url: row.get(3)?,
                        login_url: row.get(4)?,
                        username: row.get(5)?,
                        api_version: row.get(6)?,
                        created_at: row.get(7)?,
                        updated_at: row.get(8)?,
                    })
                })
                .map_err(|e| AppError::Internal(format!("Failed to query orgs: {e}")))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| AppError::Internal(format!("Failed to collect orgs: {e}")))?;

            Ok::<_, AppError>(orgs)
        })
        .await
        .map_err(|e| AppError::Internal(format!("List orgs task failed: {e}")))?
    }

    /// Gets a single organization by ID.
    pub async fn get_org(&self, org_id: &str) -> Result<Option<Org>, AppError> {
        let db_path = self.db_path.clone();
        let org_id = org_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&db_path)
                .map_err(|e| AppError::Internal(format!("Failed to open database: {e}")))?;

            configure_connection(&conn)?;

            let mut stmt = conn
                .prepare(
                    r#"
                    SELECT id, name, org_type, instance_url, login_url, username, api_version, created_at, updated_at
                    FROM orgs
                    WHERE id = ?1
                    "#,
                )
                .map_err(|e| AppError::Internal(format!("Failed to prepare query: {e}")))?;

            let org = stmt
                .query_row([&org_id], |row| {
                    Ok(Org {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        org_type: row.get(2)?,
                        instance_url: row.get(3)?,
                        login_url: row.get(4)?,
                        username: row.get(5)?,
                        api_version: row.get(6)?,
                        created_at: row.get(7)?,
                        updated_at: row.get(8)?,
                    })
                })
                .optional()
                .map_err(|e| AppError::Internal(format!("Failed to query org: {e}")))?;

            Ok::<_, AppError>(org)
        })
        .await
        .map_err(|e| AppError::Internal(format!("Get org task failed: {e}")))?
    }

    /// Inserts or updates an organization (upsert).
    /// Updates `updated_at` on conflict, preserves `created_at`.
    pub async fn upsert_org(&self, org: Org) -> Result<(), AppError> {
        let db_path = self.db_path.clone();

        tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&db_path)
                .map_err(|e| AppError::Internal(format!("Failed to open database: {e}")))?;

            configure_connection(&conn)?;

            conn.execute(
                r#"
                INSERT INTO orgs (id, name, org_type, instance_url, login_url, username, api_version, created_at, updated_at)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    org_type = excluded.org_type,
                    instance_url = excluded.instance_url,
                    login_url = excluded.login_url,
                    username = excluded.username,
                    api_version = excluded.api_version,
                    updated_at = excluded.updated_at
                "#,
                rusqlite::params![
                    org.id,
                    org.name,
                    org.org_type,
                    org.instance_url,
                    org.login_url,
                    org.username,
                    org.api_version,
                    org.created_at,
                    org.updated_at,
                ],
            )
            .map_err(|e| AppError::Internal(format!("Failed to upsert org: {e}")))?;

            Ok::<_, AppError>(())
        })
        .await
        .map_err(|e| AppError::Internal(format!("Upsert org task failed: {e}")))??;

        Ok(())
    }

    /// Deletes an organization by ID.
    pub async fn delete_org(&self, org_id: &str) -> Result<(), AppError> {
        let db_path = self.db_path.clone();
        let org_id = org_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&db_path)
                .map_err(|e| AppError::Internal(format!("Failed to open database: {e}")))?;

            configure_connection(&conn)?;

            conn.execute("DELETE FROM orgs WHERE id = ?1", [&org_id])
                .map_err(|e| AppError::Internal(format!("Failed to delete org: {e}")))?;

            Ok::<_, AppError>(())
        })
        .await
        .map_err(|e| AppError::Internal(format!("Delete org task failed: {e}")))??;

        Ok(())
    }

    // ── Saved Queries ─────────────────────────────────────────────────────────

    /// Inserts a new saved query.
    pub async fn insert_saved_query(&self, query: SavedQuery) -> Result<(), AppError> {
        let db_path = self.db_path.clone();

        tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&db_path)
                .map_err(|e| AppError::Internal(format!("Failed to open database: {e}")))?;

            configure_connection(&conn)?;

            conn.execute(
                r#"
                INSERT INTO saved_queries (id, org_id, name, soql, created_at)
                VALUES (?1, ?2, ?3, ?4, ?5)
                "#,
                rusqlite::params![
                    query.id,
                    query.org_id,
                    query.name,
                    query.soql,
                    query.created_at,
                ],
            )
            .map_err(|e| AppError::Internal(format!("Failed to insert saved query: {e}")))?;

            Ok::<_, AppError>(())
        })
        .await
        .map_err(|e| AppError::Internal(format!("Insert saved query task failed: {e}")))??;

        Ok(())
    }

    /// Lists all saved queries for an org, ordered by name.
    pub async fn list_saved_queries(&self, org_id: &str) -> Result<Vec<SavedQuery>, AppError> {
        let db_path = self.db_path.clone();
        let org_id = org_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&db_path)
                .map_err(|e| AppError::Internal(format!("Failed to open database: {e}")))?;

            configure_connection(&conn)?;

            let mut stmt = conn
                .prepare(
                    r#"
                    SELECT id, org_id, name, soql, created_at
                    FROM saved_queries
                    WHERE org_id = ?1
                    ORDER BY name ASC
                    "#,
                )
                .map_err(|e| AppError::Internal(format!("Failed to prepare query: {e}")))?;

            let queries = stmt
                .query_map([&org_id], |row| {
                    Ok(SavedQuery {
                        id: row.get(0)?,
                        org_id: row.get(1)?,
                        name: row.get(2)?,
                        soql: row.get(3)?,
                        created_at: row.get(4)?,
                    })
                })
                .map_err(|e| AppError::Internal(format!("Failed to query saved queries: {e}")))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| AppError::Internal(format!("Failed to collect saved queries: {e}")))?;

            Ok::<_, AppError>(queries)
        })
        .await
        .map_err(|e| AppError::Internal(format!("List saved queries task failed: {e}")))?
    }

    /// Deletes a saved query by ID.
    pub async fn delete_saved_query(&self, query_id: &str) -> Result<(), AppError> {
        let db_path = self.db_path.clone();
        let query_id = query_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&db_path)
                .map_err(|e| AppError::Internal(format!("Failed to open database: {e}")))?;

            configure_connection(&conn)?;

            conn.execute("DELETE FROM saved_queries WHERE id = ?1", [&query_id])
                .map_err(|e| AppError::Internal(format!("Failed to delete saved query: {e}")))?;

            Ok::<_, AppError>(())
        })
        .await
        .map_err(|e| AppError::Internal(format!("Delete saved query task failed: {e}")))??;

        Ok(())
    }

    // ── Query History ─────────────────────────────────────────────────────────

    /// Inserts a new query history entry.
    pub async fn insert_query_history(&self, entry: QueryHistoryEntry) -> Result<(), AppError> {
        let db_path = self.db_path.clone();

        tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&db_path)
                .map_err(|e| AppError::Internal(format!("Failed to open database: {e}")))?;

            configure_connection(&conn)?;

            conn.execute(
                r#"
                INSERT INTO query_history (id, org_id, soql, executed_at, row_count, strategy, duration_ms, truncated, export_path)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                "#,
                rusqlite::params![
                    entry.id,
                    entry.org_id,
                    entry.soql,
                    entry.executed_at,
                    entry.row_count,
                    entry.strategy,
                    entry.duration_ms,
                    entry.truncated as i32,
                    entry.export_path,
                ],
            )
            .map_err(|e| AppError::Internal(format!("Failed to insert query history: {e}")))?;

            Ok::<_, AppError>(())
        })
        .await
        .map_err(|e| AppError::Internal(format!("Insert query history task failed: {e}")))??;

        Ok(())
    }

    /// Lists query history for an org, ordered by most recent first.
    /// Supports pagination with limit and offset.
    pub async fn list_query_history(
        &self,
        org_id: &str,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<QueryHistoryEntry>, AppError> {
        let db_path = self.db_path.clone();
        let org_id = org_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&db_path)
                .map_err(|e| AppError::Internal(format!("Failed to open database: {e}")))?;

            configure_connection(&conn)?;

            let mut stmt = conn
                .prepare(
                    r#"
                    SELECT id, org_id, soql, executed_at, row_count, strategy, duration_ms, truncated, export_path
                    FROM query_history
                    WHERE org_id = ?1
                    ORDER BY executed_at DESC
                    LIMIT ?2 OFFSET ?3
                    "#,
                )
                .map_err(|e| AppError::Internal(format!("Failed to prepare query: {e}")))?;

            let entries = stmt
                .query_map(rusqlite::params![&org_id, limit, offset], |row| {
                    Ok(QueryHistoryEntry {
                        id: row.get(0)?,
                        org_id: row.get(1)?,
                        soql: row.get(2)?,
                        executed_at: row.get(3)?,
                        row_count: row.get(4)?,
                        strategy: row.get(5)?,
                        duration_ms: row.get(6)?,
                        truncated: row.get::<_, i32>(7)? != 0,
                        export_path: row.get(8)?,
                    })
                })
                .map_err(|e| AppError::Internal(format!("Failed to query history: {e}")))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| AppError::Internal(format!("Failed to collect history: {e}")))?;

            Ok::<_, AppError>(entries)
        })
        .await
        .map_err(|e| AppError::Internal(format!("List query history task failed: {e}")))?
    }
}

/// Configures connection with busy timeout and WAL mode.
fn configure_connection(conn: &Connection) -> Result<(), AppError> {
    conn.busy_timeout(Duration::from_secs(10))
        .map_err(|e| AppError::Internal(format!("Failed to set busy timeout: {e}")))?;

    conn.pragma_update(None, "journal_mode", "WAL")
        .map_err(|e| AppError::Internal(format!("Failed to set WAL mode: {e}")))?;

    Ok(())
}

/// Runs database migrations using PRAGMA user_version.
fn run_migrations(conn: &mut Connection) -> Result<(), AppError> {
    let current_version: i32 = conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(|e| AppError::Internal(format!("Failed to get schema version: {e}")))?;

    if current_version >= SCHEMA_VERSION {
        return Ok(());
    }

    // Run migrations in a transaction
    let tx = conn
        .transaction()
        .map_err(|e| AppError::Internal(format!("Failed to start migration transaction: {e}")))?;

    // V1 migration
    if current_version < 1 {
        tx.execute_batch(V1_SCHEMA)
            .map_err(|e| AppError::Internal(format!("V1 migration failed: {e}")))?;
    }

    // V2 migration: add truncated and export_path to query_history
    if current_version < 2 {
        tx.execute_batch(V2_MIGRATION)
            .map_err(|e| AppError::Internal(format!("V2 migration failed: {e}")))?;
    }

    // V3 migration: add bulk_job_groups and bulk_jobs tables
    if current_version < 3 {
        tx.execute_batch(V3_MIGRATION)
            .map_err(|e| AppError::Internal(format!("V3 migration failed: {e}")))?;
    }

    // Update version
    tx.pragma_update(None, "user_version", SCHEMA_VERSION)
        .map_err(|e| AppError::Internal(format!("Failed to update schema version: {e}")))?;

    tx.commit()
        .map_err(|e| AppError::Internal(format!("Failed to commit migration: {e}")))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_db_path() -> (TempDir, PathBuf) {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("test.db");
        (temp_dir, db_path)
    }

    fn current_timestamp() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as i64
    }

    #[tokio::test]
    async fn init_creates_db_file_and_tables() {
        let (_temp_dir, db_path) = test_db_path();

        // Initialize database
        let db = Database::init(db_path.clone())
            .await
            .expect("Failed to init database");

        // Verify file exists
        assert!(db_path.exists(), "Database file should exist");

        // Verify tables exist by querying them
        let conn = Connection::open(&db_path).expect("Failed to open db");

        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .expect("Failed to prepare")
            .query_map([], |row| row.get(0))
            .expect("Failed to query")
            .collect::<Result<Vec<_>, _>>()
            .expect("Failed to collect");

        assert!(
            tables.contains(&"orgs".to_string()),
            "orgs table should exist"
        );
        assert!(
            tables.contains(&"job_groups".to_string()),
            "job_groups table should exist"
        );
        assert!(
            tables.contains(&"jobs".to_string()),
            "jobs table should exist"
        );
        assert!(
            tables.contains(&"saved_queries".to_string()),
            "saved_queries table should exist"
        );
        assert!(
            tables.contains(&"query_history".to_string()),
            "query_history table should exist"
        );

        // Verify schema version
        let version: i32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("Failed to get version");
        assert_eq!(version, SCHEMA_VERSION, "Schema version should match");

        // Verify WAL mode is set (need to configure connection first since it's per-connection)
        configure_connection(&conn).expect("Failed to configure connection");
        let journal_mode: String = conn
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .expect("Failed to get journal mode");
        assert_eq!(journal_mode.to_lowercase(), "wal", "Should be in WAL mode");

        // Health check should work
        db.health_check().await.expect("Health check should pass");
    }

    #[tokio::test]
    async fn insert_and_list_orgs() {
        let (_temp_dir, db_path) = test_db_path();
        let db = Database::init(db_path)
            .await
            .expect("Failed to init database");

        let now = current_timestamp();

        // Insert an org
        let org = Org {
            id: "00Dxx0000000001".to_string(),
            name: "Acme Corp".to_string(),
            org_type: "Production".to_string(),
            instance_url: "https://acme.my.salesforce.com".to_string(),
            login_url: "https://login.salesforce.com".to_string(),
            username: "admin@acme.com".to_string(),
            api_version: "59.0".to_string(),
            created_at: now,
            updated_at: now,
        };

        db.insert_org(org.clone())
            .await
            .expect("Failed to insert org");

        // List orgs
        let orgs = db.list_orgs().await.expect("Failed to list orgs");

        assert_eq!(orgs.len(), 1, "Should have one org");
        assert_eq!(orgs[0].id, org.id);
        assert_eq!(orgs[0].name, org.name);
        assert_eq!(orgs[0].org_type, org.org_type);
        assert_eq!(orgs[0].instance_url, org.instance_url);
        assert_eq!(orgs[0].login_url, org.login_url);
        assert_eq!(orgs[0].username, org.username);
        assert_eq!(orgs[0].api_version, org.api_version);
    }

    #[tokio::test]
    async fn insert_multiple_orgs() {
        let (_temp_dir, db_path) = test_db_path();
        let db = Database::init(db_path)
            .await
            .expect("Failed to init database");

        let now = current_timestamp();

        // Insert multiple orgs
        let org1 = Org {
            id: "00Dxx0000000001".to_string(),
            name: "Zebra Inc".to_string(),
            org_type: "Production".to_string(),
            instance_url: "https://zebra.my.salesforce.com".to_string(),
            login_url: "https://login.salesforce.com".to_string(),
            username: "admin@zebra.com".to_string(),
            api_version: "59.0".to_string(),
            created_at: now,
            updated_at: now,
        };

        let org2 = Org {
            id: "00Dxx0000000002".to_string(),
            name: "Acme Corp".to_string(),
            org_type: "Sandbox".to_string(),
            instance_url: "https://acme--dev.sandbox.my.salesforce.com".to_string(),
            login_url: "https://test.salesforce.com".to_string(),
            username: "admin@acme.com.dev".to_string(),
            api_version: "58.0".to_string(),
            created_at: now,
            updated_at: now,
        };

        db.insert_org(org1).await.expect("Failed to insert org1");
        db.insert_org(org2).await.expect("Failed to insert org2");

        let orgs = db.list_orgs().await.expect("Failed to list orgs");

        assert_eq!(orgs.len(), 2, "Should have two orgs");
        // Should be ordered by name ASC
        assert_eq!(orgs[0].name, "Acme Corp");
        assert_eq!(orgs[1].name, "Zebra Inc");
    }

    #[tokio::test]
    async fn migrations_are_idempotent() {
        let (_temp_dir, db_path) = test_db_path();

        // Initialize twice - should not fail
        let _db1 = Database::init(db_path.clone())
            .await
            .expect("First init should succeed");

        let db2 = Database::init(db_path.clone())
            .await
            .expect("Second init should succeed");

        db2.health_check().await.expect("Health check should pass");
    }

    #[tokio::test]
    async fn creates_parent_directories() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db_path = temp_dir.path().join("nested").join("dirs").join("test.db");

        let db = Database::init(db_path.clone())
            .await
            .expect("Should create nested directories");

        assert!(
            db_path.exists(),
            "Database file should exist in nested path"
        );
        db.health_check().await.expect("Health check should pass");
    }

    #[tokio::test]
    async fn get_org_returns_none_for_missing() {
        let (_temp_dir, db_path) = test_db_path();
        let db = Database::init(db_path)
            .await
            .expect("Failed to init database");

        let result = db.get_org("nonexistent").await.expect("Should not error");
        assert!(result.is_none(), "Should return None for missing org");
    }

    #[tokio::test]
    async fn get_org_returns_existing() {
        let (_temp_dir, db_path) = test_db_path();
        let db = Database::init(db_path)
            .await
            .expect("Failed to init database");

        let now = current_timestamp();
        let org = Org {
            id: "00Dxx0000000001".to_string(),
            name: "Test Org".to_string(),
            org_type: "Production".to_string(),
            instance_url: "https://test.salesforce.com".to_string(),
            login_url: "https://login.salesforce.com".to_string(),
            username: "test@test.com".to_string(),
            api_version: "60.0".to_string(),
            created_at: now,
            updated_at: now,
        };

        db.insert_org(org.clone()).await.expect("Failed to insert");

        let result = db
            .get_org("00Dxx0000000001")
            .await
            .expect("Should not error");
        assert!(result.is_some(), "Should return the org");
        let fetched = result.unwrap();
        assert_eq!(fetched.id, org.id);
        assert_eq!(fetched.name, org.name);
    }

    #[tokio::test]
    async fn upsert_org_inserts_new() {
        let (_temp_dir, db_path) = test_db_path();
        let db = Database::init(db_path)
            .await
            .expect("Failed to init database");

        let now = current_timestamp();
        let org = Org {
            id: "00Dxx0000000001".to_string(),
            name: "New Org".to_string(),
            org_type: "Production".to_string(),
            instance_url: "https://new.salesforce.com".to_string(),
            login_url: "https://login.salesforce.com".to_string(),
            username: "new@test.com".to_string(),
            api_version: "60.0".to_string(),
            created_at: now,
            updated_at: now,
        };

        db.upsert_org(org.clone()).await.expect("Failed to upsert");

        let orgs = db.list_orgs().await.expect("Failed to list");
        assert_eq!(orgs.len(), 1);
        assert_eq!(orgs[0].name, "New Org");
    }

    #[tokio::test]
    async fn upsert_org_updates_existing() {
        let (_temp_dir, db_path) = test_db_path();
        let db = Database::init(db_path)
            .await
            .expect("Failed to init database");

        let now = current_timestamp();
        let org1 = Org {
            id: "00Dxx0000000001".to_string(),
            name: "Original Name".to_string(),
            org_type: "Production".to_string(),
            instance_url: "https://orig.salesforce.com".to_string(),
            login_url: "https://login.salesforce.com".to_string(),
            username: "orig@test.com".to_string(),
            api_version: "59.0".to_string(),
            created_at: now,
            updated_at: now,
        };

        db.upsert_org(org1)
            .await
            .expect("First upsert should succeed");

        // Update with same ID
        let later = now + 1000;
        let org2 = Org {
            id: "00Dxx0000000001".to_string(),
            name: "Updated Name".to_string(),
            org_type: "Sandbox".to_string(),
            instance_url: "https://updated.salesforce.com".to_string(),
            login_url: "https://test.salesforce.com".to_string(),
            username: "updated@test.com".to_string(),
            api_version: "60.0".to_string(),
            created_at: later,
            updated_at: later,
        };

        db.upsert_org(org2)
            .await
            .expect("Second upsert should succeed");

        let orgs = db.list_orgs().await.expect("Failed to list");
        assert_eq!(orgs.len(), 1, "Should still have one org");
        assert_eq!(orgs[0].name, "Updated Name");
        assert_eq!(orgs[0].org_type, "Sandbox");
        assert_eq!(orgs[0].instance_url, "https://updated.salesforce.com");
    }

    #[tokio::test]
    async fn delete_org_removes_existing() {
        let (_temp_dir, db_path) = test_db_path();
        let db = Database::init(db_path)
            .await
            .expect("Failed to init database");

        let now = current_timestamp();
        let org = Org {
            id: "00Dxx0000000001".to_string(),
            name: "To Delete".to_string(),
            org_type: "Production".to_string(),
            instance_url: "https://delete.salesforce.com".to_string(),
            login_url: "https://login.salesforce.com".to_string(),
            username: "delete@test.com".to_string(),
            api_version: "60.0".to_string(),
            created_at: now,
            updated_at: now,
        };

        db.insert_org(org).await.expect("Failed to insert");

        // Verify it exists
        let before = db.list_orgs().await.expect("Failed to list");
        assert_eq!(before.len(), 1);

        // Delete
        db.delete_org("00Dxx0000000001")
            .await
            .expect("Failed to delete");

        // Verify it's gone
        let after = db.list_orgs().await.expect("Failed to list");
        assert_eq!(after.len(), 0);
    }

    #[tokio::test]
    async fn delete_org_is_idempotent() {
        let (_temp_dir, db_path) = test_db_path();
        let db = Database::init(db_path)
            .await
            .expect("Failed to init database");

        // Delete non-existent org should not error
        db.delete_org("nonexistent")
            .await
            .expect("Delete should succeed for missing org");
    }
}
