pub mod commands;
pub mod error;
pub mod salesforce;
pub mod state;
pub mod storage;
pub mod streaming;
pub mod validation;

use tauri::Manager;

use crate::commands::{
    // Auth commands
    get_active_org,
    list_orgs,
    login,
    logout,
    switch_org,
    // Query commands
    delete_saved_query,
    execute_query,
    get_query_history,
    get_saved_queries,
    save_query,
    // Bulk commands
    cancel_bulk_upload,
    generate_bulk_results,
    start_bulk_upload,
    validate_csv,
    CancellationTokens,
};
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
        .plugin(tauri_plugin_dialog::init())
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

            // Create and manage cancellation tokens for bulk uploads
            let cancel_tokens = CancellationTokens::new();
            app.manage(cancel_tokens);

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            greet,
            // Auth commands
            login,
            list_orgs,
            switch_org,
            get_active_org,
            logout,
            // Query commands
            execute_query,
            save_query,
            get_saved_queries,
            delete_saved_query,
            get_query_history,
            // Bulk commands
            start_bulk_upload,
            cancel_bulk_upload,
            validate_csv,
            generate_bulk_results,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
