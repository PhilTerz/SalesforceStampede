//! Storage module for local SQLite database and secure credential storage.

mod database;
pub mod credentials;

pub use database::{Database, Org};
