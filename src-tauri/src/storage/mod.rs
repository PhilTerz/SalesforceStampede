//! Storage module for local SQLite database and secure credential storage.

pub mod credentials;
pub mod database;
pub mod jobs;

pub use database::{Database, Org, QueryHistoryEntry, SavedQuery};
pub use jobs::{
    add_job_to_group, cleanup_old_jobs, get_active_groups, get_group, reconcile_jobs,
    save_group, update_group_state, update_job_state, BulkJobGroupRow, BulkJobGroupWithJobs,
    BulkJobRow, GroupState, JobStatusProvider,
};
