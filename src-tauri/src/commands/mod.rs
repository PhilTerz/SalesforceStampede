//! Tauri command handlers.
//!
//! This module contains all commands exposed to the frontend via Tauri's IPC.

pub mod auth;

pub use auth::*;
