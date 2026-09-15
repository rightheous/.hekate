use serde::Serialize;
use thiserror::Error;

use crate::core::{
    ApprovalId, ApprovalStatus, AttemptId, AttemptStatus, OperationId, OperationStatus, RunId,
    RunStatus,
};
use crate::ports::{Storage, StorageError};
use crate::runtime::projector::{ProjectionError, Projector};

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RecoveryReport {
    pub revision: u64,
    pub active_runs: Vec<RunId>,
    pub pending_approvals: Vec<ApprovalId>,
    pub incomplete_attempts: Vec<AttemptId>,
    pub unknown_operations: Vec<OperationId>,
    pub projection_verified: bool,
}

#[derive(Debug, Error)]
pub enum RecoveryError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Projection(#[from] ProjectionError),
    #[error("stored projection revision {stored} does not match event ledger revision {ledger}")]
    RevisionMismatch { stored: u64, ledger: u64 },
    #[error("stored projection does not match replayed event ledger")]
    ProjectionMismatch,
}

pub async fn recover(storage: &dyn Storage) -> Result<RecoveryReport, RecoveryError> {
    let state = storage.load_state().await?;
    let events = storage.load_events().await?;
    let replayed = Projector::replay(&events)?;
    if replayed.revision != state.revision {
        return Err(RecoveryError::RevisionMismatch {
            stored: state.revision,
            ledger: replayed.revision,
        });
    }
    if state != replayed {
        return Err(RecoveryError::ProjectionMismatch);
    }
    let active_runs = state
        .runs
        .values()
        .filter(|run| {
            matches!(
                run.status,
                RunStatus::Pending
                    | RunStatus::Running
                    | RunStatus::Suspended
                    | RunStatus::NeedsAttention
            )
        })
        .map(|run| run.id)
        .collect();
    let unknown_operations = state
        .operations
        .values()
        .filter(|operation| matches!(operation.status, OperationStatus::Started))
        .map(|operation| operation.id)
        .collect();
    let pending_approvals = state
        .approvals
        .values()
        .filter(|approval| matches!(approval.status, ApprovalStatus::Pending))
        .map(|approval| approval.id)
        .collect();
    let incomplete_attempts = state
        .attempts
        .values()
        .filter(|attempt| matches!(attempt.status, AttemptStatus::Started))
        .map(|attempt| attempt.id)
        .collect();
    Ok(RecoveryReport {
        revision: state.revision,
        active_runs,
        pending_approvals,
        incomplete_attempts,
        unknown_operations,
        projection_verified: true,
    })
}
