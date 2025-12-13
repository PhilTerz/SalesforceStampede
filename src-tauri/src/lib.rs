pub mod commands;
pub mod error;
pub mod salesforce;
pub mod state;
pub mod storage;

use tauri::Manager;

use crate::commands::{get_active_org, list_orgs, login, logout, switch_org};
use crate::state::AppState;
use crate::storage::Database;

// ── Tauri commands ────────────────────────────────────────────────────────────

#[tauri::command]
fn greet(name: &str) -> String {
    format!("Hello, {}! You've been greeted from Rust!", name)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .setup(|app| {
            // Initialize database
            let app_data_dir = app
                .path()
                .app_data_dir()
                .expect("Failed to get app data dir");
            let db_path = app_data_dir.join("stampede.db");

            // Use tauri's async runtime to initialize the database
            let db = tauri::async_runtime::block_on(async {
                Database::init(db_path)
                    .await
                    .expect("Failed to initialize database")
            });

            // Create and manage app state
            let state = AppState::new(db);
            app.manage(state);

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            greet,
            login,
            list_orgs,
            switch_org,
            get_active_org,
            logout,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
