use std::sync::Arc;

use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::core::{
    ActiveMemory, Approval, Attempt, CognitiveTrace, Commitment, Conflict, ConflictStatus,
    CurrentState, Decision, EventKind, ExperienceEvent, Goal, IdentityVersion,
    IntegrationCandidate, MemoryCandidate, Observation, Operation, Position, PositionStatus,
    Principal, Receipt, Relationship, Run, SleepRun, SleepRunStatus, Task, Verification,
    VerificationStatus, WorkingState,
};
use crate::ports::{Storage, StorageError};

#[derive(Debug, Error)]
pub enum ProjectionError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("invalid payload for {event_kind}: {message}")]
    InvalidPayload { event_kind: String, message: String },
    #[error("event {0} has already been projected")]
    Duplicate(String),
    #[error("stale thought context: expected base revision {expected}, actual {actual}")]
    StaleContext { expected: u64, actual: u64 },
}

pub struct Projector {
    storage: Arc<dyn Storage>,
}

impl Projector {
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self { storage }
    }

    pub async fn record(&self, event: ExperienceEvent) -> Result<CurrentState, ProjectionError> {
        self.record_batch(std::slice::from_ref(&event), None, None)
            .await
    }

    pub async fn record_batch(
        &self,
        events: &[ExperienceEvent],
        expected_base_revision: Option<u64>,
        cognitive_trace: Option<&CognitiveTrace>,
    ) -> Result<CurrentState, ProjectionError> {
        let mut state = self.storage.load_state().await?;
        if let Some(expected) = expected_base_revision {
            if state.revision != expected {
                return Err(ProjectionError::StaleContext {
                    expected,
                    actual: state.revision,
                });
            }
        }
        for event in events {
            Self::apply(&mut state, event)?;
        }
        self.storage
            .commit_batch(events, &state, expected_base_revision, cognitive_trace)
            .await?;
        Ok(state)
    }

    pub fn replay(events: &[ExperienceEvent]) -> Result<CurrentState, ProjectionError> {
        let mut state = CurrentState::default();
        for event in events {
            Self::apply(&mut state, event)?;
        }
        Ok(state)
    }

    fn apply(state: &mut CurrentState, event: &ExperienceEvent) -> Result<(), ProjectionError> {
        if state.applied_events.contains(&event.event_id) {
            return Err(ProjectionError::Duplicate(event.event_id.to_string()));
        }

        match &event.event_kind {
            EventKind::PrincipalCreated => {
                insert(&event.event_kind, &event.payload, &mut state.principals)?
            }
            EventKind::IdentityVersionCreated => {
                let identity: IdentityVersion = payload(event)?;
                if let Some(principal) = state.principals.get_mut(&identity.principal_id) {
                    principal.identity_version_id = Some(identity.id);
                }
                state.identity_versions.insert(identity.id, identity);
            }
            EventKind::RelationshipCreated => {
                insert(&event.event_kind, &event.payload, &mut state.relationships)?
            }
            EventKind::ObservationRecorded | EventKind::UserMessageReceived => {
                insert(&event.event_kind, &event.payload, &mut state.observations)?
            }
            EventKind::GoalCreated => insert(&event.event_kind, &event.payload, &mut state.goals)?,
            EventKind::TaskCreated => insert(&event.event_kind, &event.payload, &mut state.tasks)?,
            EventKind::RunStarted | EventKind::RunSuspended | EventKind::RunCompleted => {
                insert(&event.event_kind, &event.payload, &mut state.runs)?
            }
            EventKind::AttemptStarted | EventKind::AttemptCompleted => {
                insert(&event.event_kind, &event.payload, &mut state.attempts)?
            }
            EventKind::WorkingStateUpdated => {
                let working_state: WorkingState = payload(event)?;
                state
                    .working_states
                    .insert(working_state.run_id, working_state);
            }
            EventKind::DecisionCreated => {
                insert(&event.event_kind, &event.payload, &mut state.decisions)?
            }
            EventKind::PositionEstablished
            | EventKind::PositionMaintained
            | EventKind::PositionRecorded => {
                insert(&event.event_kind, &event.payload, &mut state.positions)?
            }
            EventKind::PositionRevised => {
                let position: Position = payload(event)?;
                if let Some(previous_id) = position.supersedes {
                    if let Some(previous) = state.positions.get_mut(&previous_id) {
                        previous.status = PositionStatus::Superseded;
                    }
                }
                state.positions.insert(position.id, position);
            }
            EventKind::PositionRetracted => {
                let position: Position = payload(event)?;
                state.positions.insert(position.id, position);
            }
            EventKind::ConflictOpened | EventKind::ConflictRecorded => {
                let conflict: Conflict = payload(event)?;
                store_conflict(state, conflict);
            }
            EventKind::ConflictUpdated | EventKind::ConflictResolved => {
                let conflict: Conflict = payload(event)?;
                store_conflict(state, conflict);
            }
            EventKind::CommitmentCreated | EventKind::CommitmentFulfilled => {
                insert(&event.event_kind, &event.payload, &mut state.commitments)?
            }
            EventKind::ActionIntentCreated => {
                insert(&event.event_kind, &event.payload, &mut state.action_intents)?
            }
            EventKind::OperationPlanned
            | EventKind::OperationAuthorized
            | EventKind::OperationStarted
            | EventKind::OperationSucceeded
            | EventKind::OperationFailed
            | EventKind::OperationStateUnknown => {
                insert(&event.event_kind, &event.payload, &mut state.operations)?
            }
            EventKind::ArtifactCreated => {
                insert(&event.event_kind, &event.payload, &mut state.artifacts)?
            }
            EventKind::ApprovalRequested | EventKind::ApprovalResolved => {
                insert(&event.event_kind, &event.payload, &mut state.approvals)?
            }
            EventKind::ReceiptRecorded => {
                insert(&event.event_kind, &event.payload, &mut state.receipts)?
            }
            EventKind::VerificationRecorded => {
                let verification: Verification = payload(event)?;
                if let Some(operation) = state.operations.get_mut(&verification.operation_id) {
                    operation.status = match verification.status {
                        VerificationStatus::Verified => crate::core::OperationStatus::Verified,
                        VerificationStatus::Disputed => crate::core::OperationStatus::Disputed,
                        VerificationStatus::Failed => crate::core::OperationStatus::Failed,
                    };
                }
                state.verifications.insert(verification.id, verification);
            }
            EventKind::MemoryCandidateCreated | EventKind::MemoryRejected => insert(
                &event.event_kind,
                &event.payload,
                &mut state.memory_candidates,
            )?,
            EventKind::MemoryPromoted | EventKind::MemorySuperseded | EventKind::MemoryExpired => {
                let memory: ActiveMemory = payload(event)?;
                if let Some(candidate) = state.memory_candidates.get_mut(&memory.candidate_id) {
                    candidate.status = match memory.status {
                        crate::core::ActiveMemoryStatus::Active => {
                            crate::core::MemoryCandidateStatus::Promoted
                        }
                        crate::core::ActiveMemoryStatus::Superseded => {
                            crate::core::MemoryCandidateStatus::Superseded
                        }
                        crate::core::ActiveMemoryStatus::Expired => {
                            crate::core::MemoryCandidateStatus::Expired
                        }
                    };
                }
                state.active_memories.insert(memory.id, memory);
            }
            EventKind::SleepRunStarted
            | EventKind::SleepRunCompleted
            | EventKind::SleepRunInterrupted
            | EventKind::SleepRunFailed => apply_sleep_run(state, event)?,
            EventKind::IntegrationCandidateCreated => {
                let mut candidate: IntegrationCandidate = payload(event)?;
                if !matches!(
                    candidate.disposition,
                    crate::core::VerificationDisposition::NeedsValidation
                ) || candidate.content.trim().is_empty()
                    || candidate.rationale.trim().is_empty()
                    || candidate.source_event_ids.is_empty()
                    || candidate.source_event_ids.len() > crate::core::MAX_CANDIDATE_SOURCES
                    || candidate.counterevidence_event_ids.len()
                        > crate::core::MAX_CANDIDATE_SOURCES
                    || candidate.confidence > 100
                {
                    return Err(ProjectionError::InvalidPayload {
                        event_kind: format!("{:?}", event.event_kind),
                        message: "invalid integration candidate".to_owned(),
                    });
                }
                if candidate
                    .source_event_ids
                    .iter()
                    .any(|event_id| !state.applied_events.contains(event_id))
                    || candidate
                        .counterevidence_event_ids
                        .iter()
                        .any(|event_id| !state.applied_events.contains(event_id))
                    || candidate
                        .source_event_ids
                        .iter()
                        .any(|event_id| candidate.counterevidence_event_ids.contains(event_id))
                {
                    return Err(ProjectionError::InvalidPayload {
                        event_kind: format!("{:?}", event.event_kind),
                        message: "candidate provenance is not in the applied event ledger"
                            .to_owned(),
                    });
                }
                let Some(run) = state.sleep_runs.get(&candidate.sleep_run_id) else {
                    return Err(ProjectionError::InvalidPayload {
                        event_kind: format!("{:?}", event.event_kind),
                        message: "candidate does not belong to a sleep run".to_owned(),
                    });
                };
                if !matches!(run.status, SleepRunStatus::Running) {
                    return Err(ProjectionError::InvalidPayload {
                        event_kind: format!("{:?}", event.event_kind),
                        message: "candidate does not belong to a running sleep run".to_owned(),
                    });
                }
                if candidate.as_of_revision == 0 && event.payload.get("as_of_revision").is_none() {
                    candidate.as_of_revision = run.high_water_revision;
                }
                if candidate.as_of_revision != run.high_water_revision {
                    return Err(ProjectionError::InvalidPayload {
                        event_kind: format!("{:?}", event.event_kind),
                        message: "candidate revision does not match its sleep run".to_owned(),
                    });
                }
                let fingerprint = crate::core::integration_candidate_fingerprint(
                    &candidate.kind,
                    &candidate.content,
                    &candidate.source_event_ids,
                    &candidate.counterevidence_event_ids,
                );
                if fingerprint != candidate.fingerprint
                    || state
                        .integration_candidates
                        .values()
                        .any(|item| item.fingerprint == candidate.fingerprint)
                {
                    return Err(ProjectionError::InvalidPayload {
                        event_kind: format!("{:?}", event.event_kind),
                        message: "duplicate or invalid candidate fingerprint".to_owned(),
                    });
                }
                state.integration_candidates.insert(candidate.id, candidate);
            }
            EventKind::ResponseProduced | EventKind::StateChanged => {}
        }
        state.revision += 1;
        state.applied_events.push(event.event_id);
        Ok(())
    }
}

fn apply_sleep_run(
    state: &mut CurrentState,
    event: &ExperienceEvent,
) -> Result<(), ProjectionError> {
    let run: SleepRun = payload(event)?;
    let error = |message: &str| ProjectionError::InvalidPayload {
        event_kind: format!("{:?}", event.event_kind),
        message: message.to_owned(),
    };
    match event.event_kind {
        EventKind::SleepRunStarted => {
            if state.sleep_runs.contains_key(&run.id)
                || run.status != SleepRunStatus::Running
                || run.cursor_before != state.sleep_cursor
                || run.high_water_revision > state.revision
                || run.cursor_after.is_some()
            {
                return Err(error("invalid sleep run start"));
            }
        }
        EventKind::SleepRunCompleted
        | EventKind::SleepRunInterrupted
        | EventKind::SleepRunFailed => {
            let Some(previous) = state.sleep_runs.get(&run.id) else {
                return Err(error("sleep run terminal event has no start"));
            };
            if previous.status != SleepRunStatus::Running
                || run.cursor_before != previous.cursor_before
                || run.high_water_revision != previous.high_water_revision
                || run.seed_event_ids != previous.seed_event_ids
            {
                return Err(error("invalid sleep run transition"));
            }
            match event.event_kind {
                EventKind::SleepRunCompleted => {
                    let Some(cursor_after) = run.cursor_after else {
                        return Err(error("completed sleep run has no cursor"));
                    };
                    if run.status != SleepRunStatus::Completed
                        || cursor_after < run.cursor_before
                        || cursor_after > run.high_water_revision
                    {
                        return Err(error("invalid completed sleep cursor"));
                    }
                    state.sleep_cursor = cursor_after;
                }
                EventKind::SleepRunInterrupted => {
                    if run.status != SleepRunStatus::Interrupted || run.cursor_after.is_some() {
                        return Err(error("invalid interrupted sleep run"));
                    }
                }
                EventKind::SleepRunFailed => {
                    if run.status != SleepRunStatus::Failed || run.cursor_after.is_some() {
                        return Err(error("invalid failed sleep run"));
                    }
                }
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    }
    state.sleep_runs.insert(run.id, run);
    Ok(())
}

fn payload<T: DeserializeOwned>(event: &ExperienceEvent) -> Result<T, ProjectionError> {
    serde_json::from_value(event.payload.clone()).map_err(|error| ProjectionError::InvalidPayload {
        event_kind: format!("{:?}", event.event_kind),
        message: error.to_string(),
    })
}

fn insert<T, K>(
    event_kind: &EventKind,
    value: &serde_json::Value,
    target: &mut std::collections::BTreeMap<K, T>,
) -> Result<(), ProjectionError>
where
    T: DeserializeOwned + Keyed<K>,
    K: Ord,
{
    let item: T =
        serde_json::from_value(value.clone()).map_err(|error| ProjectionError::InvalidPayload {
            event_kind: format!("{event_kind:?}"),
            message: error.to_string(),
        })?;
    let key = item.key();
    target.insert(key, item);
    Ok(())
}

fn store_conflict(state: &mut CurrentState, conflict: Conflict) {
    let participant_ids = conflict
        .participant_positions
        .iter()
        .filter_map(|position_id| state.positions.get(position_id))
        .map(|position| position.principal_id)
        .collect::<Vec<_>>();
    let unresolved = matches!(
        conflict.status,
        ConflictStatus::Open | ConflictStatus::Negotiating
    );
    for relationship in state.relationships.values_mut() {
        if !participant_ids
            .iter()
            .any(|principal_id| relationship.participants.contains(principal_id))
        {
            continue;
        }
        if unresolved {
            if !relationship.unresolved_conflicts.contains(&conflict.id) {
                relationship.unresolved_conflicts.push(conflict.id);
            }
        } else {
            relationship
                .unresolved_conflicts
                .retain(|id| id != &conflict.id);
        }
    }
    state.conflicts.insert(conflict.id, conflict);
}

trait Keyed<K> {
    fn key(&self) -> K;
}

macro_rules! keyed {
    ($type:ty, $key:ty, $field:ident) => {
        impl Keyed<$key> for $type {
            fn key(&self) -> $key {
                self.$field
            }
        }
    };
}

keyed!(Principal, crate::core::PrincipalId, id);
keyed!(IdentityVersion, crate::core::IdentityVersionId, id);
keyed!(Relationship, crate::core::RelationshipId, id);
keyed!(Observation, crate::core::ObservationId, id);
keyed!(Goal, crate::core::GoalId, id);
keyed!(Task, crate::core::TaskId, id);
keyed!(Run, crate::core::RunId, id);
keyed!(Attempt, crate::core::AttemptId, id);
keyed!(Decision, crate::core::DecisionId, id);
keyed!(Position, crate::core::PositionId, id);
keyed!(Conflict, crate::core::ConflictId, id);
keyed!(Commitment, crate::core::CommitmentId, id);
keyed!(crate::core::ActionIntent, crate::core::ActionIntentId, id);
keyed!(Operation, crate::core::OperationId, id);
keyed!(Approval, crate::core::ApprovalId, id);
keyed!(Receipt, crate::core::ReceiptId, id);
keyed!(Verification, crate::core::VerificationId, id);
keyed!(MemoryCandidate, crate::core::MemoryCandidateId, id);
keyed!(ActiveMemory, crate::core::MemoryId, id);
keyed!(crate::core::Artifact, crate::core::ArtifactId, id);
