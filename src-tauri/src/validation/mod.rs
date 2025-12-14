//! CSV validation module for sample-based file validation.
//!
//! Provides fast validation of CSV files by reading only a fixed-size sample,
//! making it safe for very large files (GBs).

pub mod csv_validator;

pub use csv_validator::{
    validate, CsvValidationError, CsvValidationResult, CsvValidationStats, CsvValidationWarning,
    LineEndings, VALIDATION_SAMPLE_SIZE,
};
