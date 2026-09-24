use async_trait::async_trait;
use std::time::Duration;
use thiserror::Error;

use crate::core::{CognitiveTrace, CurrentState, ExperienceEvent};

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("storage backend error: {0}")]
    Backend(String),
    #[error("event integrity verification failed for {event_id}")]
    Integrity { event_id: String },
    #[error("state revision conflict: expected {expected}, received {received}")]
    RevisionConflict { expected: u64, received: u64 },
    #[error("stale thought context: expected base revision {expected}, actual {actual}")]
    StaleContext { expected: u64, actual: u64 },
    #[error("stored state is invalid: {0}")]
    InvalidState(String),
}

#[async_trait]
pub trait Storage: Send + Sync {
    async fn load_state(&self) -> Result<CurrentState, StorageError>;
    async fn load_events(&self) -> Result<Vec<ExperienceEvent>, StorageError>;
    async fn commit(
        &self,
        event: &ExperienceEvent,
        state: &CurrentState,
    ) -> Result<(), StorageError> {
        self.commit_batch(std::slice::from_ref(event), state, None, None)
            .await
    }
    async fn commit_batch(
        &self,
        events: &[ExperienceEvent],
        state: &CurrentState,
        expected_base_revision: Option<u64>,
        cognitive_trace: Option<&CognitiveTrace>,
    ) -> Result<(), StorageError>;
    async fn record_cognitive_trace(&self, trace: &CognitiveTrace) -> Result<(), StorageError>;
    async fn acquire_foreground_lease(
        &self,
        owner: &str,
        ttl: Duration,
    ) -> Result<bool, StorageError>;
    async fn renew_foreground_lease(
        &self,
        owner: &str,
        ttl: Duration,
    ) -> Result<bool, StorageError>;
    async fn release_foreground_lease(&self, owner: &str) -> Result<(), StorageError>;
    async fn has_active_foreground_lease(&self) -> Result<bool, StorageError>;
    async fn shutdown(&self) -> Result<(), StorageError> {
        Ok(())
    }
}
