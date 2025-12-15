//! Streaming utilities for processing large files.
//!
//! This module provides record-aware CSV chunking that preserves data integrity
//! even when fields contain embedded commas and newlines inside quotes, as well
//! as atomic file writing with automatic cleanup on failure, and result merging
//! for bulk operations.

mod atomic_writer;
mod csv_chunker;
mod result_merger;

pub use atomic_writer::AtomicCsvWriter;
pub use csv_chunker::{split_file, BatchSize, ChunkConfig, ChunkResult};
pub use result_merger::{download_group_results, BulkGroupResults, JobResultProvider};
