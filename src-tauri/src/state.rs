//! Application state management for Tauri.
//!
//! Provides thread-safe shared state accessible from Tauri commands.

use std::sync::Arc;
use tokio::sync::RwLock;

use crate::storage::Database;

// ─────────────────────────────────────────────────────────────────────────────
// Application State
// ─────────────────────────────────────────────────────────────────────────────

/// Global application state shared across all Tauri commands.
///
/// This state is managed by Tauri and injected into commands via
/// `tauri::State<AppState>`.
pub struct AppState {
    /// Database connection pool for persistent storage.
    pub db: Arc<Database>,
    /// Currently active org ID, if any.
    /// Protected by RwLock for thread-safe read/write access.
    pub active_org_id: RwLock<Option<String>>,
}

impl AppState {
    /// Creates a new AppState with the given database.
    pub fn new(db: Database) -> Self {
        Self {
            db: Arc::new(db),
            active_org_id: RwLock::new(None),
        }
    }

    /// Gets the currently active org ID, if any.
    pub async fn get_active_org_id(&self) -> Option<String> {
        self.active_org_id.read().await.clone()
    }

    /// Sets the active org ID.
    pub async fn set_active_org_id(&self, org_id: Option<String>) {
        *self.active_org_id.write().await = org_id;
    }
}
