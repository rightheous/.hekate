use crate::core::{
    ConflictStatus, CurrentState, Decision, EventId, Position, PositionStatus, PrincipalId,
    ThoughtContext, ThoughtCycle,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum JudgmentValidationError {
    #[error("judgment target must be the observed user")]
    InvalidTarget,
    #[error("judgment context hash does not match the thought context")]
    ContextHashMismatch,
    #[error("judgment references unknown event {0}")]
    UnknownEvent(EventId),
    #[error("judgment references unknown position {0}")]
    UnknownPosition(crate::core::PositionId),
    #[error("HEKATE cannot record another principal's position")]
    WrongPositionPrincipal,
    #[error("position revision is invalid")]
    InvalidPositionRevision,
    #[error("conflict references an unknown position {0}")]
    ConflictUnknownPosition(crate::core::PositionId),
    #[error("conflict must include both the user and HEKATE")]
    ConflictParticipants,
    #[error("conflict revision or subject is invalid")]
    InvalidConflictRevision,
    #[error("conflict transition is invalid")]
    InvalidConflictTransition,
    #[error("open conflict needs reasons, reconsideration conditions, and an unresolved question")]
    IncompleteConflict,
    #[error("resolved conflict needs a resolution")]
    IncompleteResolution,
    #[error("action proposal is incomplete")]
    IncompleteAction,
    #[error("confidence must be between 0 and 100")]
    ConfidenceOutOfRange,
    #[error("final response cannot be empty")]
    EmptyResponse,
}

pub fn decision_from_cycle(context: &ThoughtContext, cycle: &ThoughtCycle) -> Decision {
    let commitment = &cycle.commitment;
    let mut decision = Decision::respond(commitment.response.clone());
    decision.kind = commitment.final_act.clone();
    decision.target_principal_id = Some(context.observation.actor_id);
    decision.request = context.observation.content.clone();
    decision.context_hash = context.snapshot_hash.clone();
    decision.confidence = commitment.confidence;
    decision.reasons = commitment.reasons.clone();
    decision.evidence_refs = commitment.evidence_refs.clone();
    decision.alternatives = commitment.alternatives.clone();
    decision.reconsideration_conditions = commitment.reconsideration_conditions.clone();
    decision.unresolved_questions = commitment.unresolved_questions.clone();
    decision.related_position_ids = commitment.related_position_ids.clone();
    decision.user_position = commitment.user_position.clone();
    decision.cognitive_trace_id = Some(cycle.trace.trace_id.clone());
    decision.position = commitment.position.clone();
    decision.conflict = commitment.conflict.clone();
    decision.action = commitment
        .action
        .clone()
        .map(|action| crate::core::ActionIntent {
            id: crate::core::ActionIntentId::new(),
            capability: action.capability,
            operation: action.operation,
            target: action.target,
            arguments: action.arguments,
            expected_effect: action.expected_effect,
            preconditions: action.preconditions,
            proposed_by_event: None,
        });
    decision
}

pub fn validate_judgment(
    context: &ThoughtContext,
    state: &CurrentState,
    event_ids: &[EventId],
    decision: &Decision,
    hekate_id: PrincipalId,
) -> Result<(), JudgmentValidationError> {
    if decision.target_principal_id != Some(context.observation.actor_id) {
        return Err(JudgmentValidationError::InvalidTarget);
    }
    if decision.context_hash != context.snapshot_hash {
        return Err(JudgmentValidationError::ContextHashMismatch);
    }
    if decision.message.trim().is_empty() {
        return Err(JudgmentValidationError::EmptyResponse);
    }
    if decision.confidence > 100 {
        return Err(JudgmentValidationError::ConfidenceOutOfRange);
    }
    for event_id in &decision.evidence_refs {
        if !event_ids.contains(event_id) {
            return Err(JudgmentValidationError::UnknownEvent(*event_id));
        }
    }

    let proposed_position_ids = [
        decision.position.as_ref().map(|position| position.id),
        decision.user_position.as_ref().map(|position| position.id),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    for position_id in &decision.related_position_ids {
        if !state.positions.contains_key(position_id)
            && !proposed_position_ids.contains(position_id)
        {
            return Err(JudgmentValidationError::UnknownPosition(*position_id));
        }
    }
    if let Some(position) = &decision.position {
        validate_position(state, position, hekate_id)?;
        if position.confidence > 100 {
            return Err(JudgmentValidationError::ConfidenceOutOfRange);
        }
        validate_evidence(&position.evidence_refs, event_ids)?;
    }
    if let Some(position) = &decision.user_position {
        if position.principal_id != context.observation.actor_id {
            return Err(JudgmentValidationError::WrongPositionPrincipal);
        }
        if position.confidence > 100 {
            return Err(JudgmentValidationError::ConfidenceOutOfRange);
        }
        validate_position_revision(state, position)?;
        validate_evidence(&position.evidence_refs, event_ids)?;
    }
    if let Some(conflict) = &decision.conflict {
        validate_conflict(context, state, event_ids, decision, conflict, hekate_id)?;
    }
    if let Some(action) = &decision.action {
        if action.capability.trim().is_empty()
            || action.operation.trim().is_empty()
            || action.target.trim().is_empty()
        {
            return Err(JudgmentValidationError::IncompleteAction);
        }
    }
    Ok(())
}

fn validate_position(
    state: &CurrentState,
    position: &Position,
    hekate_id: PrincipalId,
) -> Result<(), JudgmentValidationError> {
    if position.principal_id != hekate_id {
        return Err(JudgmentValidationError::WrongPositionPrincipal);
    }
    validate_position_revision(state, position)
}

pub(crate) fn validate_position_revision(
    state: &CurrentState,
    position: &Position,
) -> Result<(), JudgmentValidationError> {
    match (state.positions.get(&position.id), position.supersedes) {
        (Some(previous), None)
            if position.version == previous.version
                && position.principal_id == previous.principal_id
                && position.subject == previous.subject
                && position.stance == previous.stance
                && matches!(
                    position.status,
                    PositionStatus::Active | PositionStatus::Retracted
                ) =>
        {
            Ok(())
        }
        (None, None) if position.version == 1 && position.status == PositionStatus::Active => {
            Ok(())
        }
        (None, Some(previous_id)) => {
            let Some(previous) = state.positions.get(&previous_id) else {
                return Err(JudgmentValidationError::InvalidPositionRevision);
            };
            if previous.status != PositionStatus::Active
                || previous.principal_id != position.principal_id
                || previous.subject != position.subject
                || position.version != previous.version.saturating_add(1)
                || position.status != PositionStatus::Active
            {
                return Err(JudgmentValidationError::InvalidPositionRevision);
            }
            Ok(())
        }
        _ => Err(JudgmentValidationError::InvalidPositionRevision),
    }
}

fn validate_conflict(
    context: &ThoughtContext,
    state: &CurrentState,
    event_ids: &[EventId],
    decision: &Decision,
    conflict: &crate::core::Conflict,
    hekate_id: PrincipalId,
) -> Result<(), JudgmentValidationError> {
    let mut participants = Vec::new();
    for position_id in &conflict.participant_positions {
        let position = state
            .positions
            .get(position_id)
            .or_else(|| {
                decision
                    .position
                    .as_ref()
                    .filter(|item| item.id == *position_id)
            })
            .or_else(|| {
                decision
                    .user_position
                    .as_ref()
                    .filter(|item| item.id == *position_id)
            })
            .ok_or(JudgmentValidationError::ConflictUnknownPosition(
                *position_id,
            ))?;
        participants.push(position.principal_id);
    }
    if !participants.contains(&hekate_id) || !participants.contains(&context.observation.actor_id) {
        return Err(JudgmentValidationError::ConflictParticipants);
    }
    for event_id in &conflict.evidence_refs {
        if !event_ids.contains(event_id) {
            return Err(JudgmentValidationError::UnknownEvent(*event_id));
        }
    }
    let existing = state.conflicts.get(&conflict.id);
    match existing {
        Some(previous) => {
            if previous.subject != conflict.subject
                || conflict.revision != previous.revision.saturating_add(1)
            {
                return Err(JudgmentValidationError::InvalidConflictRevision);
            }
            if !valid_conflict_transition(&previous.status, &conflict.status) {
                return Err(JudgmentValidationError::InvalidConflictTransition);
            }
        }
        None if conflict.revision != 1 || matches!(conflict.status, ConflictStatus::Resolved) => {
            return Err(JudgmentValidationError::InvalidConflictRevision)
        }
        None => {}
    }
    if matches!(
        conflict.status,
        ConflictStatus::Open | ConflictStatus::Negotiating
    ) && (conflict.reasons.is_empty()
        || conflict.reconsideration_conditions.is_empty()
        || conflict.unresolved_questions.is_empty())
    {
        return Err(JudgmentValidationError::IncompleteConflict);
    }
    if matches!(
        conflict.status,
        ConflictStatus::Resolved | ConflictStatus::AcceptedDisagreement
    ) && conflict.resolution.is_none()
    {
        return Err(JudgmentValidationError::IncompleteResolution);
    }
    Ok(())
}

fn validate_evidence(
    evidence_refs: &[EventId],
    event_ids: &[EventId],
) -> Result<(), JudgmentValidationError> {
    for event_id in evidence_refs {
        if !event_ids.contains(event_id) {
            return Err(JudgmentValidationError::UnknownEvent(*event_id));
        }
    }
    Ok(())
}

fn valid_conflict_transition(previous: &ConflictStatus, next: &ConflictStatus) -> bool {
    matches!(
        (previous, next),
        (ConflictStatus::Open, ConflictStatus::Open)
            | (ConflictStatus::Open, ConflictStatus::Negotiating)
            | (ConflictStatus::Open, ConflictStatus::Resolved)
            | (ConflictStatus::Open, ConflictStatus::AcceptedDisagreement)
            | (ConflictStatus::Negotiating, ConflictStatus::Negotiating)
            | (ConflictStatus::Negotiating, ConflictStatus::Resolved)
            | (
                ConflictStatus::Negotiating,
                ConflictStatus::AcceptedDisagreement
            )
    )
}
