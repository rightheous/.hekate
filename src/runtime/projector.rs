use std::sync::Arc;

use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::core::transition::transition_task;
use crate::core::{
    ActiveMemory, Approval, Attempt, CognitiveTrace, Commitment, CompletionClaim,
    CompletionClaimTransition, CompletionCriterion, Conflict, ConflictStatus, CurrentState,
    Decision, EventKind, EvidenceRef, ExperienceEvent, Goal, IdentityVersion, IntegrationCandidate,
    IntegrationMaterialization, IntegrationVerification, MemoryCandidate, Observation, Operation,
    Position, PositionIntegrationActionKind, PositionIntegrationEventPayload,
    PositionIntegrationMaterialization, PositionIntegrationOperation, PositionIntegrationProposal,
    PositionStatus, Principal, Receipt, Relationship, Run, SleepRun, SleepRunStatus, Task,
    TaskStatus, Verification, VerificationDisposition, VerificationStatus, WorkingState,
};
use crate::ports::{Storage, StorageError};
use crate::runtime::deliberation::validate_position_revision;

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
            EventKind::TaskCompleted => apply_task_completion(state, event)?,
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
            | EventKind::PositionRecorded
            | EventKind::PositionRevised
            | EventKind::PositionRetracted => apply_position_event(state, event)?,
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
            EventKind::CompletionCriterionDefined => {
                let mut criterion: CompletionCriterion = payload(event)?;
                if criterion.description.trim().is_empty() {
                    return invalid(&event.event_kind, "criterion description cannot be empty");
                }
                if !state.tasks.contains_key(&criterion.task_id) {
                    return invalid(&event.event_kind, "criterion references an unknown task");
                }
                if state.completion_criteria.contains_key(&criterion.id) {
                    return invalid(&event.event_kind, "criterion ID already exists");
                }
                let normalized = crate::core::normalize_description(&criterion.description);
                if state.completion_criteria.values().any(|existing| {
                    existing.task_id == criterion.task_id
                        && crate::core::normalize_description(&existing.description) == normalized
                }) {
                    return invalid(
                        &event.event_kind,
                        "criterion description already exists for task",
                    );
                }
                criterion.description = criterion.description.trim().to_owned();
                state.completion_criteria.insert(criterion.id, criterion);
            }
            EventKind::CompletionClaimCreated => {
                if is_model_actor(state, event.actor_id) {
                    return invalid(&event.event_kind, "model cannot create completion claims");
                }
                let claim: CompletionClaim = payload(event)?;
                apply_claim_created(state, claim, event.event_kind.clone())?;
            }
            EventKind::CompletionClaimVerified => {
                if is_model_actor(state, event.actor_id) {
                    return invalid(&event.event_kind, "model cannot verify completion claims");
                }
                let transition: CompletionClaimTransition = payload(event)?;
                apply_claim_transition(
                    state,
                    transition,
                    VerificationDisposition::Verified,
                    event.event_kind.clone(),
                )?;
            }
            EventKind::CompletionClaimRejected => {
                if is_model_actor(state, event.actor_id) {
                    return invalid(&event.event_kind, "model cannot reject completion claims");
                }
                let transition: CompletionClaimTransition = payload(event)?;
                apply_claim_transition(
                    state,
                    transition,
                    VerificationDisposition::Rejected,
                    event.event_kind.clone(),
                )?;
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
            EventKind::IntegrationCandidateVerified => {
                apply_integration_transition(state, event, VerificationDisposition::Verified)?
            }
            EventKind::IntegrationCandidateRejected => {
                apply_integration_transition(state, event, VerificationDisposition::Rejected)?
            }
            EventKind::IntegrationCandidateMaterialized => {
                apply_integration_materialization(state, event)?
            }
            EventKind::ResponseProduced | EventKind::StateChanged => {}
        }
        state.revision += 1;
        state.applied_events.push(event.event_id);
        Ok(())
    }
}

fn apply_task_completion(
    state: &mut CurrentState,
    event: &ExperienceEvent,
) -> Result<(), ProjectionError> {
    let completed: Task = payload(event)?;
    if event.subject.as_ref().map(|subject| {
        subject.kind == crate::core::EntityKind::Task && subject.id == completed.id.uuid()
    }) != Some(true)
    {
        return invalid(
            &event.event_kind,
            "task completion subject does not match payload",
        );
    }
    let Some(current) = state.tasks.get(&completed.id) else {
        return invalid(&event.event_kind, "completion references an unknown task");
    };
    let mut expected = current.clone();
    if transition_task(&mut expected, TaskStatus::Completed).is_err() || expected != completed {
        return invalid(&event.event_kind, "invalid task completion transition");
    }
    state.tasks.insert(completed.id, completed);
    Ok(())
}

fn apply_position_event(
    state: &mut CurrentState,
    event: &ExperienceEvent,
) -> Result<(), ProjectionError> {
    if event.payload.get("position_integration").is_some() {
        let integration_event: PositionIntegrationEventPayload = payload(event)?;
        return apply_position_integration(state, event, integration_event);
    }
    let position: Position = payload(event)?;
    match event.event_kind {
        EventKind::PositionEstablished
        | EventKind::PositionMaintained
        | EventKind::PositionRecorded => {
            state.positions.insert(position.id, position);
            Ok(())
        }
        EventKind::PositionRevised => {
            if let Some(previous_id) = position.supersedes {
                if let Some(previous) = state.positions.get_mut(&previous_id) {
                    previous.status = PositionStatus::Superseded;
                }
            }
            state.positions.insert(position.id, position);
            Ok(())
        }
        EventKind::PositionRetracted => {
            state.positions.insert(position.id, position);
            Ok(())
        }
        _ => invalid(&event.event_kind, "event is not a Position lifecycle event"),
    }
}

fn apply_position_integration(
    state: &mut CurrentState,
    event: &ExperienceEvent,
    integration_event: PositionIntegrationEventPayload,
) -> Result<(), ProjectionError> {
    let error = |message: &str| ProjectionError::InvalidPayload {
        event_kind: format!("{:?}", event.event_kind),
        message: message.to_owned(),
    };
    let position = integration_event.position;
    let materialization = integration_event.position_integration;
    let candidate_id = materialization.candidate_id.to_string();
    if event.subject.as_ref().map(|subject| {
        subject.kind != crate::core::EntityKind::Position || subject.id != position.id.uuid()
    }) != Some(false)
        || materialization.position_id != position.id
        || materialization.position_event_id != event.event_id
    {
        return Err(error(
            "Position integration event subject does not match payload",
        ));
    }
    if event.source.source_type != "position_integration"
        || event.source.source_ref.as_deref() != Some(candidate_id.as_str())
        || event.correlation_id.as_deref() != Some(candidate_id.as_str())
        || event.causation_id != Some(materialization.verification_event_id)
    {
        return Err(error(
            "Position integration event provenance does not match candidate",
        ));
    }
    if !state.principals.contains_key(&event.actor_id) || is_model_actor(state, event.actor_id) {
        return Err(error(
            "model or unknown actor cannot materialize a Position",
        ));
    }
    let Some(principal) = state.principals.get(&position.principal_id) else {
        return Err(error("Position references an unknown principal"));
    };
    if !matches!(principal.kind, crate::core::PrincipalKind::Hekate) {
        return Err(error(
            "Position integration can only change HEKATE's Position",
        ));
    }
    let Some(candidate) = state
        .integration_candidates
        .get(&materialization.candidate_id)
        .cloned()
    else {
        return Err(error(
            "Position materialization references an unknown candidate",
        ));
    };
    if !matches!(
        candidate.kind,
        crate::core::IntegrationCandidateKind::Position
    ) || candidate.disposition != VerificationDisposition::Verified
    {
        return Err(error(
            "only a verified Position candidate can be materialized",
        ));
    }
    if state
        .position_integration_materializations
        .contains_key(&materialization.candidate_id)
    {
        return Err(error("Position candidate has already been materialized"));
    }
    let Some(verification) = state
        .integration_verifications
        .get(&materialization.candidate_id)
    else {
        return Err(error("Position materialization has no verification record"));
    };
    let evidence_refs = crate::core::normalize_evidence_refs(materialization.evidence_refs.clone());
    if verification.new_disposition != VerificationDisposition::Verified
        || evidence_refs != verification.evidence_refs
        || !state
            .applied_events
            .contains(&materialization.verification_event_id)
    {
        return Err(error(
            "Position materialization does not match verification",
        ));
    }
    if materialization.fingerprint != candidate.fingerprint
        || crate::core::integration_candidate_fingerprint(
            &candidate.kind,
            &candidate.content,
            &candidate.source_event_ids,
            &candidate.counterevidence_event_ids,
        ) != candidate.fingerprint
        || materialization.source_event_ids != candidate.source_event_ids
        || materialization.counterevidence_event_ids != candidate.counterevidence_event_ids
        || materialization.as_of_revision != candidate.as_of_revision
        || candidate.as_of_revision > state.revision
        || candidate
            .source_event_ids
            .iter()
            .chain(candidate.counterevidence_event_ids.iter())
            .any(|id| !state.applied_events.contains(id))
    {
        return Err(error(
            "Position materialization provenance does not match candidate",
        ));
    }
    validate_integration_evidence(
        state,
        &candidate,
        &evidence_refs,
        materialization.as_of_revision,
        &error,
    )?;
    if !position_matches_proposal(&candidate, &position, &materialization) {
        return Err(error("Position does not match its typed proposal"));
    }
    if position.confidence != candidate.confidence {
        return Err(error("Position confidence does not match candidate"));
    }
    let prior_position = materialization.prior_position.as_ref();
    match materialization.action {
        PositionIntegrationActionKind::Establish => {
            if prior_position.is_some()
                || position.status != PositionStatus::Active
                || position.version != 1
                || position.supersedes.is_some()
            {
                return Err(error("invalid Position establishment"));
            }
        }
        PositionIntegrationActionKind::Revise => {
            let Some(prior) = prior_position else {
                return Err(error("Position revision has no prior Position"));
            };
            if state.positions.get(&prior.id) != Some(prior)
                || prior.status != PositionStatus::Active
                || position.id == prior.id
                || position.supersedes != Some(prior.id)
                || position.principal_id != prior.principal_id
                || position.subject != prior.subject
                || prior.version.checked_add(1) != Some(position.version)
                || position.status != PositionStatus::Active
            {
                return Err(error("Position changed or revision is invalid"));
            }
        }
        PositionIntegrationActionKind::Withdraw => {
            let Some(prior) = prior_position else {
                return Err(error("Position withdrawal has no prior Position"));
            };
            if state.positions.get(&prior.id) != Some(prior)
                || prior.status != PositionStatus::Active
                || position.id != prior.id
                || position.supersedes.is_some()
                || position.status != PositionStatus::Retracted
                || position.version != prior.version
                || position.principal_id != prior.principal_id
                || position.subject != prior.subject
                || position.stance != prior.stance
            {
                return Err(error("Position changed or withdrawal is invalid"));
            }
        }
    }
    let expected_kind = match materialization.action {
        PositionIntegrationActionKind::Establish => EventKind::PositionEstablished,
        PositionIntegrationActionKind::Revise => EventKind::PositionRevised,
        PositionIntegrationActionKind::Withdraw => EventKind::PositionRetracted,
    };
    if event.event_kind != expected_kind {
        return Err(error(
            "Position event kind does not match integration operation",
        ));
    }
    let target_id = prior_position.map(|prior| prior.id);
    if crate::core::position_integration_blocked(
        state,
        position.principal_id,
        &position.subject,
        target_id,
    ) {
        return Err(error("an active Position conflict blocks materialization"));
    }
    validate_position_revision(state, &position)
        .map_err(|_| error("Position transition violates its lifecycle"))?;
    if materialization.action != PositionIntegrationActionKind::Withdraw
        && position.evidence_refs != candidate.source_event_ids
    {
        return Err(error("Position evidence does not match candidate sources"));
    }
    if materialization.action == PositionIntegrationActionKind::Withdraw
        && candidate
            .source_event_ids
            .iter()
            .any(|id| !position.evidence_refs.contains(id))
    {
        return Err(error("Position evidence omits candidate sources"));
    }

    if materialization.action == PositionIntegrationActionKind::Revise {
        if let Some(prior_id) = position.supersedes {
            if let Some(previous) = state.positions.get_mut(&prior_id) {
                previous.status = PositionStatus::Superseded;
            }
        }
    }
    state.positions.insert(position.id, position);
    state
        .position_integration_materializations
        .insert(materialization.candidate_id, materialization);
    Ok(())
}

fn position_matches_proposal(
    candidate: &IntegrationCandidate,
    position: &Position,
    materialization: &PositionIntegrationMaterialization,
) -> bool {
    let Ok(proposal) = PositionIntegrationProposal::parse(&candidate.content) else {
        return false;
    };
    let trim = |values: &[String]| {
        values
            .iter()
            .map(|value| value.trim().to_owned())
            .collect::<Vec<_>>()
    };
    match (
        &proposal.operation,
        materialization.action,
        materialization.prior_position.as_ref(),
    ) {
        (
            PositionIntegrationOperation::Establish {
                subject,
                stance,
                reasons,
                reconsideration_conditions,
            },
            PositionIntegrationActionKind::Establish,
            None,
        ) => {
            position.subject == subject.trim()
                && position.stance == *stance
                && position.reasons == trim(reasons)
                && position.reconsideration_conditions == trim(reconsideration_conditions)
        }
        (
            PositionIntegrationOperation::Revise {
                position_id,
                expected_version,
                stance,
                reasons,
                reconsideration_conditions,
            },
            PositionIntegrationActionKind::Revise,
            Some(prior),
        ) => {
            *position_id == prior.id
                && *expected_version == prior.version
                && position.subject == prior.subject
                && position.stance == *stance
                && position.reasons == trim(reasons)
                && position.reconsideration_conditions == trim(reconsideration_conditions)
        }
        (
            PositionIntegrationOperation::Withdraw {
                position_id,
                expected_version,
                reason,
            },
            PositionIntegrationActionKind::Withdraw,
            Some(prior),
        ) => {
            let mut expected = prior.clone();
            expected.status = PositionStatus::Retracted;
            expected.reasons.push(reason.trim().to_owned());
            expected
                .evidence_refs
                .extend(materialization.source_event_ids.iter().copied());
            expected.evidence_refs.sort();
            expected.evidence_refs.dedup();
            *position_id == prior.id && *expected_version == prior.version && *position == expected
        }
        _ => false,
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
keyed!(CompletionCriterion, crate::core::CompletionCriterionId, id);
keyed!(CompletionClaim, crate::core::CompletionClaimId, id);

fn invalid<T>(event_kind: &EventKind, message: impl Into<String>) -> Result<T, ProjectionError> {
    Err(ProjectionError::InvalidPayload {
        event_kind: format!("{event_kind:?}"),
        message: message.into(),
    })
}

fn is_model_actor(state: &CurrentState, actor_id: crate::core::PrincipalId) -> bool {
    state
        .principals
        .get(&actor_id)
        .is_some_and(|principal| matches!(principal.kind, crate::core::PrincipalKind::Hekate))
}

fn apply_claim_created(
    state: &mut CurrentState,
    mut claim: CompletionClaim,
    event_kind: EventKind,
) -> Result<(), ProjectionError> {
    if !state.tasks.contains_key(&claim.task_id) {
        return invalid(&event_kind, "claim references an unknown task");
    }
    let Some(criterion) = state.completion_criteria.get(&claim.criterion_id) else {
        return invalid(&event_kind, "claim references an unknown criterion");
    };
    if criterion.task_id != claim.task_id {
        return invalid(&event_kind, "criterion does not belong to claim task");
    }
    if !matches!(claim.disposition, VerificationDisposition::NeedsValidation) {
        return invalid(&event_kind, "new claims must require validation");
    }
    if claim.confidence > 100 {
        return invalid(&event_kind, "claim confidence must be between 0 and 100");
    }
    if claim.as_of_sequence > state.revision {
        return invalid(&event_kind, "claim as-of sequence is in the future");
    }
    claim.evidence_refs = crate::core::normalize_evidence_refs(claim.evidence_refs);
    if claim.fingerprint
        != crate::core::completion_fingerprint(
            claim.task_id,
            claim.criterion_id,
            &claim.evidence_refs,
        )
    {
        return invalid(&event_kind, "claim fingerprint does not match its evidence");
    }
    for evidence in &claim.evidence_refs {
        validate_evidence_ref(state, evidence, claim.as_of_sequence, event_kind.clone())?;
    }
    if state.completion_claims.contains_key(&claim.id) {
        return invalid(&event_kind, "claim ID already exists");
    }
    if state
        .completion_claims
        .values()
        .any(|existing| existing.fingerprint == claim.fingerprint)
    {
        return invalid(&event_kind, "claim fingerprint already exists");
    }
    if let Some(previous_id) = claim.supersedes {
        let Some(previous) = state.completion_claims.get(&previous_id) else {
            return invalid(&event_kind, "claim supersedes an unknown claim");
        };
        if previous.task_id != claim.task_id || previous.criterion_id != claim.criterion_id {
            return invalid(&event_kind, "claim supersedes a different task criterion");
        }
    }
    state.completion_claims.insert(claim.id, claim);
    Ok(())
}

fn apply_claim_transition(
    state: &mut CurrentState,
    transition: CompletionClaimTransition,
    expected: VerificationDisposition,
    event_kind: EventKind,
) -> Result<(), ProjectionError> {
    let Some(claim) = state.completion_claims.get_mut(&transition.claim_id) else {
        return invalid(&event_kind, "transition references an unknown claim");
    };
    if claim.task_id != transition.task_id || claim.criterion_id != transition.criterion_id {
        return invalid(
            &event_kind,
            "claim transition identity does not match claim",
        );
    }
    if claim.disposition != transition.previous_disposition
        || transition.previous_disposition != VerificationDisposition::NeedsValidation
        || transition.disposition != expected
    {
        return invalid(&event_kind, "invalid claim disposition transition");
    }
    if transition.reason.trim().is_empty() {
        return invalid(&event_kind, "claim transition reason cannot be empty");
    }
    let evidence_refs = crate::core::normalize_evidence_refs(transition.evidence_refs);
    if evidence_refs != claim.evidence_refs {
        return invalid(
            &event_kind,
            "claim transition evidence does not match claim",
        );
    }
    if matches!(expected, VerificationDisposition::Verified) {
        if claim.evidence_refs.is_empty() {
            return invalid(&event_kind, "a claim needs evidence before verification");
        }
        if claim
            .blocker
            .as_deref()
            .is_some_and(|blocker| !blocker.trim().is_empty())
        {
            return invalid(&event_kind, "a blocked claim cannot be verified");
        }
    }
    claim.disposition = expected;
    Ok(())
}

fn validate_evidence_ref(
    state: &CurrentState,
    evidence: &EvidenceRef,
    claim_as_of_sequence: u64,
    event_kind: EventKind,
) -> Result<(), ProjectionError> {
    if !crate::core::valid_sha256_hex(&evidence.source_hash) {
        return invalid(
            &event_kind,
            "evidence source hash is not lowercase SHA-256 hex",
        );
    }
    if evidence.as_of_sequence > claim_as_of_sequence {
        return invalid(
            &event_kind,
            "evidence is newer than the claim as-of sequence",
        );
    }
    let Some(event_sequence) = state
        .applied_events
        .iter()
        .position(|event_id| event_id == &evidence.event_id)
        .map(|index| index as u64 + 1)
    else {
        return invalid(&event_kind, "evidence references an unknown event");
    };
    if event_sequence > claim_as_of_sequence {
        return invalid(
            &event_kind,
            "evidence event is newer than the claim as-of sequence",
        );
    }
    if let Some(artifact_id) = evidence.artifact_id {
        let Some(artifact) = state.artifacts.get(&artifact_id) else {
            return invalid(&event_kind, "evidence references an unknown artifact");
        };
        if artifact.provenance_event_id != evidence.event_id {
            return invalid(
                &event_kind,
                "artifact provenance event does not match evidence",
            );
        }
        if artifact.content_hash != evidence.source_hash {
            return invalid(&event_kind, "artifact source hash does not match evidence");
        }
    }
    Ok(())
}

fn apply_integration_transition(
    state: &mut CurrentState,
    event: &ExperienceEvent,
    expected: VerificationDisposition,
) -> Result<(), ProjectionError> {
    let verification: IntegrationVerification = payload(event)?;
    let error = |message: &str| ProjectionError::InvalidPayload {
        event_kind: format!("{:?}", event.event_kind),
        message: message.to_owned(),
    };
    if event.subject.as_ref().map(|subject| {
        subject.kind != crate::core::EntityKind::IntegrationCandidate
            || subject.id != verification.candidate_id.uuid()
    }) != Some(false)
    {
        return Err(error(
            "integration verification subject does not match candidate",
        ));
    }
    if event.actor_id != verification.actor_id {
        return Err(error(
            "integration verification actor does not match event actor",
        ));
    }
    if state
        .principals
        .get(&verification.actor_id)
        .map(|principal| matches!(principal.kind, crate::core::PrincipalKind::Hekate))
        .unwrap_or(true)
    {
        return Err(error(
            "integration verification actor is not an eligible principal",
        ));
    }
    let Some(candidate) = state
        .integration_candidates
        .get(&verification.candidate_id)
        .cloned()
    else {
        return Err(error(
            "integration verification references an unknown candidate",
        ));
    };
    if candidate.disposition != verification.previous_disposition
        || verification.previous_disposition != VerificationDisposition::NeedsValidation
        || verification.new_disposition != expected
    {
        return Err(error(
            "invalid integration candidate disposition transition",
        ));
    }
    if verification.reason.trim().is_empty() {
        return Err(error("integration verification reason cannot be empty"));
    }
    if verification.as_of_revision != candidate.as_of_revision
        || verification.as_of_revision > state.revision
    {
        return Err(error(
            "integration verification has an invalid as-of revision",
        ));
    }
    if crate::core::integration_candidate_fingerprint(
        &candidate.kind,
        &candidate.content,
        &candidate.source_event_ids,
        &candidate.counterevidence_event_ids,
    ) != candidate.fingerprint
    {
        return Err(error("candidate fingerprint does not match its projection"));
    }
    if expected == VerificationDisposition::Verified
        && matches!(
            &candidate.kind,
            crate::core::IntegrationCandidateKind::Position
        )
    {
        PositionIntegrationProposal::parse(&candidate.content)
            .map_err(|_| error("Position candidate has an invalid typed proposal"))?;
    }
    let evidence_refs = crate::core::normalize_evidence_refs(verification.evidence_refs.clone());
    if evidence_refs.is_empty() {
        return Err(error("integration verification needs evidence"));
    }
    validate_integration_evidence(
        state,
        &candidate,
        &evidence_refs,
        verification.as_of_revision,
        &error,
    )?;
    let candidate_id = candidate.id;
    let Some(candidate) = state.integration_candidates.get_mut(&candidate_id) else {
        return Err(error("integration candidate disappeared during transition"));
    };
    candidate.disposition = expected;
    let mut stored = verification;
    stored.reason = stored.reason.trim().to_owned();
    stored.evidence_refs = evidence_refs;
    state
        .integration_verifications
        .insert(stored.candidate_id, stored);
    Ok(())
}

fn apply_integration_materialization(
    state: &mut CurrentState,
    event: &ExperienceEvent,
) -> Result<(), ProjectionError> {
    let materialization: IntegrationMaterialization = payload(event)?;
    let error = |message: &str| ProjectionError::InvalidPayload {
        event_kind: format!("{:?}", event.event_kind),
        message: message.to_owned(),
    };
    if event.subject.as_ref().map(|subject| {
        subject.kind != crate::core::EntityKind::IntegrationCandidate
            || subject.id != materialization.candidate_id.uuid()
    }) != Some(false)
    {
        return Err(error("materialization subject does not match candidate"));
    }
    let Some(candidate) = state
        .integration_candidates
        .get(&materialization.candidate_id)
    else {
        return Err(error("materialization references an unknown candidate"));
    };
    if candidate.disposition != VerificationDisposition::Verified {
        return Err(error("only verified candidates can be materialized"));
    }
    if state
        .integration_materializations
        .contains_key(&materialization.candidate_id)
    {
        return Err(error("candidate has already been materialized"));
    }
    if crate::core::integration_candidate_fingerprint(
        &candidate.kind,
        &candidate.content,
        &candidate.source_event_ids,
        &candidate.counterevidence_event_ids,
    ) != materialization.fingerprint
    {
        return Err(error(
            "materialization fingerprint does not match candidate",
        ));
    }
    if !matches!(
        &candidate.kind,
        crate::core::IntegrationCandidateKind::Memory
    ) {
        return Err(error("candidate kind cannot be materialized as Memory"));
    }
    if materialization.source_event_ids != candidate.source_event_ids
        || materialization.counterevidence_event_ids != candidate.counterevidence_event_ids
        || materialization.as_of_revision != candidate.as_of_revision
    {
        return Err(error("materialization provenance does not match candidate"));
    }
    let Some(verification) = state
        .integration_verifications
        .get(&materialization.candidate_id)
    else {
        return Err(error("materialization has no verification record"));
    };
    if verification.new_disposition != VerificationDisposition::Verified
        || crate::core::normalize_evidence_refs(materialization.evidence_refs.clone())
            != verification.evidence_refs
    {
        return Err(error(
            "materialization evidence does not match verification",
        ));
    }
    let Some(memory_candidate) = state
        .memory_candidates
        .get(&materialization.memory_candidate_id)
    else {
        return Err(error(
            "materialization references an unknown Memory candidate",
        ));
    };
    if memory_candidate.status != crate::core::MemoryCandidateStatus::Promoted
        || memory_candidate.kind != crate::core::MemoryKind::Lesson
        || memory_candidate.content != candidate.content
        || memory_candidate.source_event_ids != candidate.source_event_ids
    {
        return Err(error("Memory candidate does not match materialization"));
    }
    let Some(memory) = state.active_memories.get(&materialization.memory_id) else {
        return Err(error("materialization references an unknown Memory"));
    };
    if memory.status != crate::core::ActiveMemoryStatus::Active
        || memory.candidate_id != materialization.memory_candidate_id
    {
        return Err(error("Memory does not match materialization"));
    }
    let evidence_refs = crate::core::normalize_evidence_refs(materialization.evidence_refs.clone());
    validate_integration_evidence(
        state,
        candidate,
        &evidence_refs,
        materialization.as_of_revision,
        &error,
    )?;
    let candidate_id = materialization.candidate_id;
    let mut stored = materialization;
    stored.evidence_refs = evidence_refs;
    state
        .integration_materializations
        .insert(candidate_id, stored);
    Ok(())
}

fn validate_integration_evidence(
    state: &CurrentState,
    candidate: &crate::core::IntegrationCandidate,
    evidence_refs: &[EvidenceRef],
    as_of_revision: u64,
    error: &impl Fn(&str) -> ProjectionError,
) -> Result<(), ProjectionError> {
    for evidence in evidence_refs {
        if !crate::core::valid_sha256_hex(&evidence.source_hash) {
            return Err(error("evidence source hash is invalid"));
        }
        if evidence.as_of_sequence > as_of_revision {
            return Err(error("evidence is newer than candidate as-of revision"));
        }
        if !candidate
            .source_event_ids
            .iter()
            .chain(candidate.counterevidence_event_ids.iter())
            .any(|event_id| event_id == &evidence.event_id)
        {
            return Err(error("evidence is outside candidate provenance"));
        }
        let Some(event_sequence) = state
            .applied_events
            .iter()
            .position(|event_id| event_id == &evidence.event_id)
            .map(|index| index as u64 + 1)
        else {
            return Err(error("evidence references an unknown event"));
        };
        if event_sequence > evidence.as_of_sequence || event_sequence > as_of_revision {
            return Err(error("evidence event is newer than its as-of revision"));
        }
        if let Some(artifact_id) = evidence.artifact_id {
            let Some(artifact) = state.artifacts.get(&artifact_id) else {
                return Err(error("evidence references an unknown artifact"));
            };
            if artifact.provenance_event_id != evidence.event_id
                || artifact.content_hash != evidence.source_hash
            {
                return Err(error("artifact evidence provenance does not match"));
            }
        }
    }
    Ok(())
}
