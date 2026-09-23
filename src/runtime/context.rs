use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::core::{
    ActiveMemoryStatus, ConflictStatus, CurrentState, ExperienceEvent, Focus,
    MemoryCandidateStatus, Observation, PositionStatus, PrincipalKind, RecallBundle,
    ThoughtContext,
};

pub fn build_context(
    state: &CurrentState,
    observation: &Observation,
    focus: &Focus,
    events: &[ExperienceEvent],
    available_capabilities: Vec<String>,
) -> ThoughtContext {
    build_context_with_recall(
        state,
        observation,
        focus,
        events,
        available_capabilities,
        RecallBundle::empty_for(&observation.content, ""),
    )
}

pub fn build_context_with_recall(
    state: &CurrentState,
    observation: &Observation,
    focus: &Focus,
    events: &[ExperienceEvent],
    available_capabilities: Vec<String>,
    recall: RecallBundle,
) -> ThoughtContext {
    build_context_with_recall_and_snapshot(
        state,
        observation,
        focus,
        events,
        available_capabilities,
        recall,
        None,
    )
}

pub fn build_context_with_recall_and_snapshot(
    state: &CurrentState,
    observation: &Observation,
    focus: &Focus,
    events: &[ExperienceEvent],
    available_capabilities: Vec<String>,
    recall: RecallBundle,
    context_snapshot: Option<serde_json::Value>,
) -> ThoughtContext {
    let goal = focus.goal_id.and_then(|id| state.goals.get(&id)).cloned();
    let task = focus.task_id.and_then(|id| state.tasks.get(&id)).cloned();
    let run = focus.run_id.and_then(|id| state.runs.get(&id)).cloned();
    let working_state = focus
        .run_id
        .and_then(|id| state.working_states.get(&id))
        .cloned();
    let hekate_id = state
        .principals
        .values()
        .find(|principal| matches!(&principal.kind, PrincipalKind::Hekate))
        .map(|principal| principal.id);
    let relationship = state.relationships.values().find(|relationship| {
        relationship.participants.contains(&observation.actor_id)
            && hekate_id
                .map(|id| relationship.participants.contains(&id))
                .unwrap_or(false)
    });
    let positions = hekate_id
        .map(|id| {
            state
                .active_positions(id)
                .into_iter()
                .cloned()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let conflicts = state
        .conflicts
        .values()
        .filter(|conflict| {
            matches!(
                conflict.status,
                ConflictStatus::Open | ConflictStatus::Negotiating
            )
        })
        .filter(|conflict| {
            relationship
                .map(|item| item.unresolved_conflicts.contains(&conflict.id))
                .unwrap_or(true)
        })
        .cloned()
        .collect::<Vec<_>>();
    let conflict_position_ids = conflicts
        .iter()
        .flat_map(|conflict| conflict.participant_positions.iter().copied())
        .collect::<Vec<_>>();
    let request = observation.content.to_ascii_lowercase();
    let user_positions = state
        .positions
        .values()
        .filter(|position| {
            position.principal_id == observation.actor_id
                && matches!(position.status, PositionStatus::Active)
                && (conflict_position_ids.contains(&position.id)
                    || relevant_subject(&position.subject, &request))
        })
        .cloned()
        .collect::<Vec<_>>();
    let relevant_events = select_events(
        state,
        observation,
        focus,
        &positions,
        &user_positions,
        &conflicts,
        events,
    );
    let mut recent_event_ids = relevant_events
        .iter()
        .map(|event| event.event_id)
        .collect::<Vec<_>>();
    if let Some(event_id) = events.iter().find_map(|event| {
        event
            .subject
            .as_ref()
            .filter(|subject| {
                subject.kind == crate::core::EntityKind::Observation
                    && subject.id == observation.id.uuid()
            })
            .map(|_| event.event_id)
    }) {
        if !recent_event_ids.contains(&event_id) {
            recent_event_ids.push(event_id);
        }
    }
    let mut snapshot = ThoughtContext {
        event_sequence: state.revision,
        observation: observation.clone(),
        focus: focus.clone(),
        identity: state.hekate_identity().cloned(),
        relationship: relationship.cloned(),
        goal,
        task,
        run,
        working_state,
        positions,
        user_positions,
        conflicts,
        commitments: state
            .commitments
            .values()
            .filter(|commitment| {
                matches!(commitment.status, crate::core::CommitmentStatus::Open)
            })
            .cloned()
            .collect(),
        memories: state
            .active_memories
            .values()
            .filter(|memory| matches!(memory.status, ActiveMemoryStatus::Active))
            .cloned()
            .collect(),
        memory_candidates: state
            .memory_candidates
            .values()
            .filter(|candidate| matches!(candidate.status, MemoryCandidateStatus::Candidate))
            .cloned()
            .collect(),
        pending_approvals: state
            .approvals
            .values()
            .filter(|approval| matches!(approval.status, crate::core::ApprovalStatus::Pending))
            .cloned()
            .collect(),
        artifacts: state.artifacts.values().cloned().collect(),
        recall,
        context_snapshot,
        recent_event_ids,
        relevant_events,
        available_capabilities,
        output_schema: "thought_cycle(draft, review, commitment(final_act, reasons, response, confidence, evidence_refs, position, user_position, conflict, action))".to_owned(),
        snapshot_hash: String::new(),
    };
    let bytes = serde_json::to_vec(&snapshot).unwrap_or_default();
    let digest = Sha256::digest(bytes);
    snapshot.snapshot_hash = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    snapshot
}

fn select_events(
    state: &CurrentState,
    observation: &Observation,
    focus: &Focus,
    positions: &[crate::core::Position],
    user_positions: &[crate::core::Position],
    conflicts: &[crate::core::Conflict],
    events: &[ExperienceEvent],
) -> Vec<ExperienceEvent> {
    let mut subjects = positions
        .iter()
        .map(|position| position.id.uuid())
        .chain(user_positions.iter().map(|position| position.id.uuid()))
        .chain(conflicts.iter().map(|conflict| conflict.id.uuid()))
        .collect::<Vec<Uuid>>();
    if let Some(id) = focus.goal_id.and_then(|id| state.goals.get(&id)) {
        subjects.push(id.id.uuid());
    }
    if let Some(id) = focus.task_id.and_then(|id| state.tasks.get(&id)) {
        subjects.push(id.id.uuid());
    }
    if let Some(id) = focus.run_id.and_then(|id| state.runs.get(&id)) {
        subjects.push(id.id.uuid());
    }
    let observation_id = observation.id.to_string();
    let recent_start = events.len().saturating_sub(20);
    events
        .iter()
        .enumerate()
        .filter(|(index, event)| {
            !event.subject.as_ref().is_some_and(|subject| {
                subject.kind == crate::core::EntityKind::Observation
                    && subject.id == observation.id.uuid()
            }) && (*index >= recent_start
                || event.correlation_id.as_deref() == Some(observation_id.as_str())
                || event
                    .subject
                    .as_ref()
                    .map(|subject| subjects.contains(&subject.id))
                    .unwrap_or(false))
        })
        .map(|(_, event)| event.clone())
        .collect()
}

fn relevant_subject(subject: &str, request: &str) -> bool {
    let subject = subject.to_ascii_lowercase();
    subject.contains(request) || request.contains(&subject)
}
