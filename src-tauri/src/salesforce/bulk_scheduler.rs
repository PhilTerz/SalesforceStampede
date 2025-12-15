//! Bulk Job Scheduler for concurrency control.
//!
//! Limits the number of concurrent bulk ingest/query operations to prevent
//! overwhelming Salesforce API limits. Default maximum is 3 concurrent jobs.
//!
//! # Usage
//!
//! ```ignore
//! let scheduler = BulkJobScheduler::new(3);
//!
//! // Acquire a permit (waits if all slots are taken)
//! let permit = scheduler.acquire().await;
//!
//! // Do bulk work while holding the permit...
//!
//! // Permit is automatically released when dropped
//! drop(permit);
//! ```

use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

// ─────────────────────────────────────────────────────────────────────────────
// BulkJobScheduler
// ─────────────────────────────────────────────────────────────────────────────

/// Scheduler that limits the number of concurrent bulk jobs.
///
/// Uses a semaphore to enforce the concurrency limit. Permits are automatically
/// released when dropped, ensuring slots are always properly freed.
#[derive(Clone)]
pub struct BulkJobScheduler {
    /// The underlying semaphore for concurrency control.
    sem: Arc<Semaphore>,
    /// Maximum number of concurrent jobs allowed.
    max: usize,
}

impl BulkJobScheduler {
    /// Creates a new scheduler with the specified maximum concurrent jobs.
    ///
    /// # Panics
    ///
    /// Panics if `max_concurrent` is 0.
    pub fn new(max_concurrent: usize) -> Self {
        assert!(max_concurrent > 0, "max_concurrent must be greater than 0");

        Self {
            sem: Arc::new(Semaphore::new(max_concurrent)),
            max: max_concurrent,
        }
    }

    /// Acquires a permit, waiting if all slots are currently in use.
    ///
    /// The permit is automatically released when dropped.
    pub async fn acquire(&self) -> BulkJobPermit {
        // We never close the semaphore, so acquire_owned cannot fail
        let permit = self
            .sem
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore closed unexpectedly");

        BulkJobPermit {
            permit,
            max: self.max,
            sem: self.sem.clone(),
        }
    }

    /// Attempts to acquire a permit without waiting.
    ///
    /// Returns `Some(permit)` if a slot is available, `None` otherwise.
    pub fn try_acquire(&self) -> Option<BulkJobPermit> {
        self.sem
            .clone()
            .try_acquire_owned()
            .ok()
            .map(|permit| BulkJobPermit {
                permit,
                max: self.max,
                sem: self.sem.clone(),
            })
    }

    /// Returns the number of currently active jobs.
    pub fn active_jobs(&self) -> usize {
        self.max - self.sem.available_permits()
    }

    /// Returns the number of available slots for new jobs.
    pub fn available_slots(&self) -> usize {
        self.sem.available_permits()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// BulkJobPermit
// ─────────────────────────────────────────────────────────────────────────────

/// A permit representing an active bulk job slot.
///
/// The slot is automatically released when this permit is dropped.
/// Do NOT implement Drop manually - OwnedSemaphorePermit handles release.
pub struct BulkJobPermit {
    /// The underlying semaphore permit (releases on drop).
    #[allow(dead_code)]
    permit: OwnedSemaphorePermit,
    /// Maximum concurrent jobs for metric calculations.
    max: usize,
    /// Reference to the semaphore for metric queries.
    sem: Arc<Semaphore>,
}

impl BulkJobPermit {
    /// Returns the number of currently active jobs (including this one).
    pub fn active_jobs(&self) -> usize {
        self.max - self.sem.available_permits()
    }

    /// Returns the number of available slots for new jobs.
    pub fn available_slots(&self) -> usize {
        self.sem.available_permits()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    #[test]
    #[should_panic(expected = "max_concurrent must be greater than 0")]
    fn test_new_panics_on_zero() {
        let _ = BulkJobScheduler::new(0);
    }

    #[test]
    fn test_new_creates_scheduler() {
        let scheduler = BulkJobScheduler::new(3);
        assert_eq!(scheduler.active_jobs(), 0);
        assert_eq!(scheduler.available_slots(), 3);
    }

    #[tokio::test]
    async fn test_try_acquire_respects_limits() {
        let scheduler = BulkJobScheduler::new(2);

        // Acquire 2 permits via try_acquire (should succeed)
        let permit1 = scheduler.try_acquire();
        assert!(permit1.is_some(), "First try_acquire should succeed");

        let permit2 = scheduler.try_acquire();
        assert!(permit2.is_some(), "Second try_acquire should succeed");

        // Third try_acquire should fail (no slots available)
        let permit3 = scheduler.try_acquire();
        assert!(permit3.is_none(), "Third try_acquire should fail");

        // Verify metrics
        assert_eq!(scheduler.active_jobs(), 2);
        assert_eq!(scheduler.available_slots(), 0);

        // Drop one permit
        drop(permit1);

        // Now try_acquire should succeed again
        let permit4 = scheduler.try_acquire();
        assert!(permit4.is_some(), "try_acquire after drop should succeed");

        // Verify metrics updated
        assert_eq!(scheduler.active_jobs(), 2);
        assert_eq!(scheduler.available_slots(), 0);
    }

    #[tokio::test]
    async fn test_acquire_blocks_when_full() {
        let scheduler = BulkJobScheduler::new(1);

        // Acquire the only slot
        let permit1 = scheduler.acquire().await;
        assert_eq!(scheduler.active_jobs(), 1);
        assert_eq!(scheduler.available_slots(), 0);

        // Clone scheduler for the spawned task
        let scheduler_clone = scheduler.clone();

        // Spawn a task that attempts to acquire
        let handle = tokio::spawn(async move { scheduler_clone.acquire().await });

        // The task should NOT complete within 50ms (blocked waiting for slot)
        let result = timeout(
            Duration::from_millis(50),
            &mut Box::pin(async {
                // We need to check if handle is done, not await it
                tokio::time::sleep(Duration::from_millis(50)).await;
            }),
        )
        .await;
        assert!(result.is_ok(), "Timeout should not trigger for our sleep");

        // Verify the spawned task is still pending (hasn't completed)
        assert!(
            !handle.is_finished(),
            "Acquire task should still be blocked"
        );

        // Drop the first permit to release the slot
        drop(permit1);

        // Now the task should complete
        let result = timeout(Duration::from_millis(100), handle).await;
        assert!(
            result.is_ok(),
            "Acquire should complete after slot is freed"
        );

        let permit2 = result.unwrap().expect("Task should not panic");
        assert_eq!(permit2.active_jobs(), 1);
    }

    #[tokio::test]
    async fn test_metrics_accuracy() {
        let scheduler = BulkJobScheduler::new(3);

        // Initial state
        assert_eq!(scheduler.active_jobs(), 0);
        assert_eq!(scheduler.available_slots(), 3);

        // Acquire 2 permits
        let permit1 = scheduler.acquire().await;
        let permit2 = scheduler.acquire().await;

        // Verify scheduler metrics
        assert_eq!(scheduler.active_jobs(), 2);
        assert_eq!(scheduler.available_slots(), 1);

        // Verify permit metrics match scheduler metrics
        assert_eq!(permit1.active_jobs(), 2);
        assert_eq!(permit1.available_slots(), 1);
        assert_eq!(permit2.active_jobs(), 2);
        assert_eq!(permit2.available_slots(), 1);

        // Acquire third permit
        let permit3 = scheduler.acquire().await;

        // All slots taken
        assert_eq!(scheduler.active_jobs(), 3);
        assert_eq!(scheduler.available_slots(), 0);
        assert_eq!(permit3.active_jobs(), 3);
        assert_eq!(permit3.available_slots(), 0);

        // Drop permits and verify metrics update
        drop(permit1);
        assert_eq!(scheduler.active_jobs(), 2);
        assert_eq!(scheduler.available_slots(), 1);

        drop(permit2);
        assert_eq!(scheduler.active_jobs(), 1);
        assert_eq!(scheduler.available_slots(), 2);

        drop(permit3);
        assert_eq!(scheduler.active_jobs(), 0);
        assert_eq!(scheduler.available_slots(), 3);
    }

    #[tokio::test]
    async fn test_scheduler_is_clone() {
        let scheduler1 = BulkJobScheduler::new(2);
        let scheduler2 = scheduler1.clone();

        // Acquire from one clone
        let permit = scheduler1.acquire().await;

        // Both clones should show the same state
        assert_eq!(scheduler1.active_jobs(), 1);
        assert_eq!(scheduler2.active_jobs(), 1);
        assert_eq!(scheduler1.available_slots(), 1);
        assert_eq!(scheduler2.available_slots(), 1);

        drop(permit);

        // Both should update
        assert_eq!(scheduler1.active_jobs(), 0);
        assert_eq!(scheduler2.active_jobs(), 0);
    }

    #[tokio::test]
    async fn test_multiple_waiters() {
        let scheduler = BulkJobScheduler::new(1);

        // Take the only slot
        let permit = scheduler.acquire().await;

        let scheduler1 = scheduler.clone();
        let scheduler2 = scheduler.clone();

        // Spawn two waiting tasks
        let handle1 = tokio::spawn(async move { scheduler1.acquire().await });

        let handle2 = tokio::spawn(async move { scheduler2.acquire().await });

        // Give tasks time to start waiting
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Both should be blocked
        assert!(!handle1.is_finished());
        assert!(!handle2.is_finished());

        // Release the slot
        drop(permit);

        // One should complete
        let result1 = timeout(Duration::from_millis(100), handle1).await;
        assert!(result1.is_ok());
        let permit1 = result1.unwrap().unwrap();

        // The other should still be waiting
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!handle2.is_finished());

        // Release again
        drop(permit1);

        // Now the second should complete
        let result2 = timeout(Duration::from_millis(100), handle2).await;
        assert!(result2.is_ok());
    }
}
