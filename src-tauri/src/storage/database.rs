//! SQLite database module with schema migrations.

use std::path::PathBuf;

use rusqlite::Connection;

use crate::error::AppError;

/// Current schema version. Increment when adding new migrations.
const SCHEMA_VERSION: i32 = 1;

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
    object TEXT NOT NULL,
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
                    AppError::Internal(format!("Failed to create database directory: {}", e))
                })?;
            }

            // Open connection and run migrations
            let conn = Connection::open(&path).map_err(|e| {
                AppError::Internal(format!("Failed to open database: {}", e))
            })?;

            run_migrations(&conn)?;

            Ok::<_, AppError>(())
        })
        .await
        .map_err(|e| AppError::Internal(format!("Database init task failed: {}", e)))??;

        Ok(Self { db_path })
    }

    /// Returns a new connection to the database.
    fn connect(&self) -> Result<Connection, AppError> {
        Connection::open(&self.db_path)
            .map_err(|e| AppError::Internal(format!("Failed to open database: {}", e)))
    }

    /// Simple health check: executes SELECT 1.
    pub async fn health_check(&self) -> Result<(), AppError> {
        let db_path = self.db_path.clone();

        tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&db_path)
                .map_err(|e| AppError::Internal(format!("Failed to open database: {}", e)))?;

            conn.execute_batch("SELECT 1")
                .map_err(|e| AppError::Internal(format!("Health check failed: {}", e)))?;

            Ok::<_, AppError>(())
        })
        .await
        .map_err(|e| AppError::Internal(format!("Health check task failed: {}", e)))??;

        Ok(())
    }

    /// Inserts a new organization.
    pub async fn insert_org(&self, org: Org) -> Result<(), AppError> {
        let conn = self.connect()?;

        tokio::task::spawn_blocking(move || {
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
            .map_err(|e| AppError::Internal(format!("Failed to insert org: {}", e)))?;

            Ok::<_, AppError>(())
        })
        .await
        .map_err(|e| AppError::Internal(format!("Insert org task failed: {}", e)))??;

        Ok(())
    }

    /// Lists all organizations.
    pub async fn list_orgs(&self) -> Result<Vec<Org>, AppError> {
        let conn = self.connect()?;

        tokio::task::spawn_blocking(move || {
            let mut stmt = conn
                .prepare(
                    r#"
                    SELECT id, name, org_type, instance_url, login_url, username, api_version, created_at, updated_at
                    FROM orgs
                    ORDER BY name ASC
                    "#,
                )
                .map_err(|e| AppError::Internal(format!("Failed to prepare query: {}", e)))?;

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
                .map_err(|e| AppError::Internal(format!("Failed to query orgs: {}", e)))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| AppError::Internal(format!("Failed to collect orgs: {}", e)))?;

            Ok::<_, AppError>(orgs)
        })
        .await
        .map_err(|e| AppError::Internal(format!("List orgs task failed: {}", e)))?
    }
}

/// Runs database migrations using PRAGMA user_version.
fn run_migrations(conn: &Connection) -> Result<(), AppError> {
    let current_version: i32 = conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(|e| AppError::Internal(format!("Failed to get schema version: {}", e)))?;

    if current_version >= SCHEMA_VERSION {
        return Ok(());
    }

    // Run migrations in a transaction
    let tx = conn
        .unchecked_transaction()
        .map_err(|e| AppError::Internal(format!("Failed to start migration transaction: {}", e)))?;

    // V1 migration
    if current_version < 1 {
        tx.execute_batch(V1_SCHEMA)
            .map_err(|e| AppError::Internal(format!("V1 migration failed: {}", e)))?;
    }

    // Update version
    tx.pragma_update(None, "user_version", SCHEMA_VERSION)
        .map_err(|e| AppError::Internal(format!("Failed to update schema version: {}", e)))?;

    tx.commit()
        .map_err(|e| AppError::Internal(format!("Failed to commit migration: {}", e)))?;

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

        assert!(tables.contains(&"orgs".to_string()), "orgs table should exist");
        assert!(
            tables.contains(&"job_groups".to_string()),
            "job_groups table should exist"
        );
        assert!(tables.contains(&"jobs".to_string()), "jobs table should exist");
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

        // Health check should work
        db.health_check().await.expect("Health check should pass");
    }

    #[tokio::test]
    async fn insert_and_list_orgs() {
        let (_temp_dir, db_path) = test_db_path();
        let db = Database::init(db_path).await.expect("Failed to init database");

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

        db.insert_org(org.clone()).await.expect("Failed to insert org");

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
        let db = Database::init(db_path).await.expect("Failed to init database");

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

        assert!(db_path.exists(), "Database file should exist in nested path");
        db.health_check().await.expect("Health check should pass");
    }
}
