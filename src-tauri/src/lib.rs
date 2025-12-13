pub mod error;
pub mod salesforce;
pub mod storage;

use std::sync::{Arc, RwLock};

/// Application state shared across Tauri commands.
/// This is a minimal shell—real fields will be added in later chunks.
pub struct AppState {
    // Placeholder: will hold auth session, HTTP client, etc.
    _placeholder: (),
}

impl AppState {
    pub fn new() -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(Self { _placeholder: () }))
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self { _placeholder: () }
    }
}

// ── Tauri commands ────────────────────────────────────────────────────────────

#[tauri::command]
fn greet(name: &str) -> String {
    format!("Hello, {}! You've been greeted from Rust!", name)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![greet])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
