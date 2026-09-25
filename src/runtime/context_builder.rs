use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use crate::core::context_snapshot::ContextBudgetReport;
use crate::core::{
    ActiveMemoryStatus, ConflictStatus, ContextBuildRequest, ContextItem, ContextItemKind,
    ContextSnapshot, ContextSourceRef, CurrentState, EntityKind, EntityRef, EventId, EventKind,
    ExperienceEvent, MemoryKind, PositionStatus, RecalledItem, Relationship, ResponseProfile,
};
use crate::runtime::response_profile::resolve_response_profile;

#[derive(Debug, Error, Eq, PartialEq)]
pub enum ContextBuilderError {
    #[error("anchor budget exceeded: {bytes} bytes > {limit} bytes")]
    AnchorBudgetExceeded { bytes: usize, limit: usize },
    #[error("projection revision {actual} is newer than requested as-of revision {requested}")]
    FutureProjection { requested: u64, actual: u64 },
    #[error(
        "event {event_id} at sequence {sequence} is newer than requested as-of revision {as_of}"
    )]
    FutureEvent {
        event_id: EventId,
        sequence: u64,
        as_of: u64,
    },
    #[error(
        "recall source event {event_id} at sequence {sequence} is newer than requested as-of revision {as_of}"
    )]
    FutureRecallSource {
        event_id: EventId,
        sequence: u64,
        as_of: u64,
    },
    #[error("recall for event {event_id} has invalid as-of revision {item_as_of}")]
    InvalidRecallAsOf { event_id: EventId, item_as_of: u64 },
    #[error("response profile as-of revision {profile_revision} is newer than requested {request_revision}")]
    FutureResponseProfile {
        profile_revision: u64,
        request_revision: u64,
    },
    #[error("source event {event_id} is missing")]
    MissingSourceEvent { event_id: EventId },
    #[error("source event {event_id} hash mismatch")]
    SourceHashMismatch { event_id: EventId },
    #[error("source event {event_id} is invalid: {message}")]
    InvalidSourceEvent { event_id: EventId, message: String },
    #[error("requested {kind} {id} is missing")]
    MissingRequestedEntity { kind: &'static str, id: String },
    #[error("serialization failed: {0}")]
    Serialization(String),
}

pub type ContextBuildError = ContextBuilderError;

type EventIndex<'a> = BTreeMap<EventId, (u64, &'a ExperienceEvent)>;

const RECENT_EVENT_WINDOW: u64 = 20;

pub fn build_context_snapshot<R>(
    state: &CurrentState,
    events: &[ExperienceEvent],
    request: R,
) -> Result<ContextSnapshot, ContextBuilderError>
where
    R: Borrow<ContextBuildRequest>,
{
    let request = request.borrow();
    let profile = resolve_response_profile(
        state,
        events,
        request.principal_id,
        request.relationship_id,
        request.task_id,
        request.as_of_revision,
    );
    build_with_profile(state, events, request, &profile)
}

pub fn build_context_snapshot_with_profile<R>(
    state: &CurrentState,
    events: &[ExperienceEvent],
    request: R,
    profile: &ResponseProfile,
) -> Result<ContextSnapshot, ContextBuilderError>
where
    R: Borrow<ContextBuildRequest>,
{
    build_with_profile(state, events, request.borrow(), profile)
}

fn build_with_profile(
    state: &CurrentState,
    events: &[ExperienceEvent],
    request: &ContextBuildRequest,
    profile: &ResponseProfile,
) -> Result<ContextSnapshot, ContextBuilderError> {
    if state.revision > request.as_of_revision {
        return Err(ContextBuilderError::FutureProjection {
            requested: request.as_of_revision,
            actual: state.revision,
        });
    }

    let index = event_index(events);
    validate_profile(profile, request.as_of_revision, &index)?;

    let anchors = build_anchors(state, request, request.as_of_revision, &index)?;
    let anchor_bytes = serialized_len(&anchors)?;
    if anchor_bytes > request.budget.anchor_bytes {
        return Err(ContextBuilderError::AnchorBudgetExceeded {
            bytes: anchor_bytes,
            limit: request.budget.anchor_bytes,
        });
    }

    let middle_candidates = build_middle(state, request, request.as_of_revision, &index)?;
    let (mut middle, mut dropped_middle_items) = select_by_budget(
        middle_candidates
            .into_iter()
            .map(|candidate| candidate.item),
        request.budget.middle_bytes,
    )?;
    let (mut active_recent, mut dropped_active_items) = select_by_budget(
        build_active_recent(request, request.as_of_revision, &index)?,
        request.budget.active_bytes,
    )?;

    let mut total_hint = 0;
    for _ in 0..8 {
        let source_refs = source_refs(
            &anchors,
            &middle,
            &active_recent,
            profile,
            request.as_of_revision,
            &index,
        )?;
        let report = budget_report(
            &anchors,
            &middle,
            &active_recent,
            total_hint,
            dropped_middle_items,
            dropped_active_items,
            request.budget.hard_limit_bytes,
        )?;
        let snapshot_hash = snapshot_hash(
            request.as_of_revision,
            &anchors,
            &middle,
            &active_recent,
            profile,
            &report,
            &source_refs,
        )?;
        let snapshot = ContextSnapshot {
            as_of_revision: request.as_of_revision,
            anchors: anchors.clone(),
            compressed_middle: middle.clone(),
            active_recent: active_recent.clone(),
            response_profile: profile.clone(),
            budget_report: report,
            source_refs,
            snapshot_hash,
        };
        let total_bytes = serialized_len(&snapshot)?;
        if total_bytes > request.budget.hard_limit_bytes {
            if drop_for_hard_limit(
                &mut middle,
                &mut active_recent,
                &mut dropped_middle_items,
                &mut dropped_active_items,
            ) {
                total_hint = 0;
                continue;
            }
            return Err(ContextBuilderError::AnchorBudgetExceeded {
                bytes: total_bytes,
                limit: request.budget.hard_limit_bytes,
            });
        }
        if snapshot.budget_report.total_bytes == total_bytes {
            return Ok(snapshot);
        }
        total_hint = total_bytes;
    }

    Err(ContextBuilderError::Serialization(
        "snapshot size did not stabilize".to_owned(),
    ))
}

fn event_index(events: &[ExperienceEvent]) -> EventIndex<'_> {
    events
        .iter()
        .enumerate()
        .map(|(index, event)| (event.event_id, (index as u64 + 1, event)))
        .collect()
}

fn build_anchors(
    state: &CurrentState,
    request: &ContextBuildRequest,
    as_of: u64,
    index: &EventIndex<'_>,
) -> Result<Vec<ContextItem>, ContextBuilderError> {
    let mut anchors = Vec::new();
    let mut seen = BTreeSet::new();
    let identity = state.hekate_identity().or_else(|| {
        state
            .principals
            .get(&request.principal_id)
            .and_then(|principal| principal.identity_version_id)
            .and_then(|id| state.identity_versions.get(&id))
    });

    if let Some(identity) = identity {
        let entity = EntityRef::new(EntityKind::IdentityVersion, identity.id.uuid());
        let sources = validated_sources(
            subject_sources(index, &entity.kind, entity.id, as_of),
            as_of,
            index,
        )?;
        push_anchor(
            &mut anchors,
            &mut seen,
            ContextItem {
                kind: ContextItemKind::Identity,
                text: json_text(identity)?,
                source_event_ids: sources.clone(),
                entity: Some(entity.clone()),
                as_of_revision: as_of,
            },
        );
        push_anchor(
            &mut anchors,
            &mut seen,
            ContextItem {
                kind: ContextItemKind::Policy,
                text: json_text(&serde_json::json!({
                    "values": identity.values,
                    "boundaries": identity.boundaries,
                }))?,
                source_event_ids: sources,
                entity: None,
                as_of_revision: as_of,
            },
        );
    }

    let relationship = requested_relationship(state, request)?;
    if let Some(relationship) = relationship {
        let entity = EntityRef::new(EntityKind::Relationship, relationship.id.uuid());
        let sources = validated_sources(
            subject_sources(index, &entity.kind, entity.id, as_of),
            as_of,
            index,
        )?;
        push_anchor(
            &mut anchors,
            &mut seen,
            ContextItem {
                kind: ContextItemKind::Relationship,
                text: json_text(relationship)?,
                source_event_ids: sources,
                entity: Some(entity),
                as_of_revision: as_of,
            },
        );
    }

    let run = requested_run(state, request)?;
    let task_id = request.task_id.or_else(|| run.and_then(|run| run.task_id));
    let task = task_id
        .map(|id| {
            state
                .tasks
                .get(&id)
                .ok_or(ContextBuilderError::MissingRequestedEntity {
                    kind: "task",
                    id: id.to_string(),
                })
        })
        .transpose()?;
    let goal = task
        .and_then(|task| task.goal_id)
        .and_then(|id| state.goals.get(&id));
    let working_state = run.and_then(|run| state.working_states.get(&run.id));

    let mut focus_sources = Vec::new();
    if let Some(goal) = goal {
        let entity = EntityRef::new(EntityKind::Goal, goal.id.uuid());
        let sources = validated_sources(
            subject_sources(index, &entity.kind, entity.id, as_of),
            as_of,
            index,
        )?;
        focus_sources.extend(sources.iter().copied());
        push_anchor(
            &mut anchors,
            &mut seen,
            ContextItem {
                kind: ContextItemKind::Goal,
                text: json_text(goal)?,
                source_event_ids: sources,
                entity: Some(entity),
                as_of_revision: as_of,
            },
        );
    }
    if let Some(task) = task {
        let entity = EntityRef::new(EntityKind::Task, task.id.uuid());
        let sources = validated_sources(
            subject_sources(index, &entity.kind, entity.id, as_of),
            as_of,
            index,
        )?;
        focus_sources.extend(sources.iter().copied());
        push_anchor(
            &mut anchors,
            &mut seen,
            ContextItem {
                kind: ContextItemKind::Task,
                text: json_text(task)?,
                source_event_ids: sources,
                entity: Some(entity),
                as_of_revision: as_of,
            },
        );
    }
    if let Some(run) = run {
        let entity = EntityRef::new(EntityKind::Run, run.id.uuid());
        let sources = validated_sources(
            subject_sources(index, &entity.kind, entity.id, as_of),
            as_of,
            index,
        )?;
        focus_sources.extend(sources.iter().copied());
        push_anchor(
            &mut anchors,
            &mut seen,
            ContextItem {
                kind: ContextItemKind::Run,
                text: json_text(run)?,
                source_event_ids: sources,
                entity: Some(entity),
                as_of_revision: as_of,
            },
        );
        if let Some(working_state) = working_state {
            let entity = EntityRef::new(EntityKind::WorkingState, working_state.id.uuid());
            let sources = validated_sources(
                subject_sources(index, &entity.kind, entity.id, as_of),
                as_of,
                index,
            )?;
            focus_sources.extend(sources.iter().copied());
            push_anchor(
                &mut anchors,
                &mut seen,
                ContextItem {
                    kind: ContextItemKind::WorkingState,
                    text: json_text(working_state)?,
                    source_event_ids: sources,
                    entity: Some(entity),
                    as_of_revision: as_of,
                },
            );
        }
    }

    if goal.is_some() || task.is_some() || run.is_some() {
        let focus = serde_json::json!({
            "goal_id": goal.map(|item| item.id.to_string()),
            "task_id": task.map(|item| item.id.to_string()),
            "run_id": run.map(|item| item.id.to_string()),
        });
        let sources = validated_sources(focus_sources, as_of, index)?;
        push_anchor(
            &mut anchors,
            &mut seen,
            ContextItem {
                kind: ContextItemKind::Focus,
                text: json_text(&focus)?,
                source_event_ids: sources,
                entity: None,
                as_of_revision: as_of,
            },
        );
    }

    let principal_ids = relevant_principal_ids(state, request.principal_id);
    let active_positions = state
        .positions
        .values()
        .filter(|position| {
            principal_ids.contains(&position.principal_id)
                && matches!(position.status, PositionStatus::Active)
        })
        .collect::<Vec<_>>();
    let active_position_ids = active_positions
        .iter()
        .map(|position| position.id)
        .collect::<BTreeSet<_>>();
    for position in active_positions {
        let entity = EntityRef::new(EntityKind::Position, position.id.uuid());
        let mut source_ids = position.evidence_refs.clone();
        source_ids.extend(subject_sources(index, &entity.kind, entity.id, as_of));
        let sources = validated_sources(source_ids, as_of, index)?;
        push_anchor(
            &mut anchors,
            &mut seen,
            ContextItem {
                kind: ContextItemKind::Position,
                text: crate::runtime::embedding_indexer::position_text(position),
                source_event_ids: sources,
                entity: Some(entity),
                as_of_revision: as_of,
            },
        );
    }

    let relationship_conflicts = relationship.map(|item| &item.unresolved_conflicts);
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
            relationship_conflicts
                .map(|ids| ids.contains(&conflict.id))
                .unwrap_or(false)
                || conflict
                    .participant_positions
                    .iter()
                    .any(|id| active_position_ids.contains(id))
                || relationship.is_none()
        })
        .collect::<Vec<_>>();
    for conflict in conflicts {
        let entity = EntityRef::new(EntityKind::Conflict, conflict.id.uuid());
        let mut source_ids = conflict.evidence_refs.clone();
        source_ids.extend(subject_sources(index, &entity.kind, entity.id, as_of));
        let sources = validated_sources(source_ids, as_of, index)?;
        push_anchor(
            &mut anchors,
            &mut seen,
            ContextItem {
                kind: ContextItemKind::Conflict,
                text: json_text(conflict)?,
                source_event_ids: sources,
                entity: Some(entity),
                as_of_revision: as_of,
            },
        );
    }

    Ok(anchors)
}

fn build_middle(
    state: &CurrentState,
    request: &ContextBuildRequest,
    as_of: u64,
    index: &EventIndex<'_>,
) -> Result<Vec<MiddleCandidate>, ContextBuilderError> {
    let principal_ids = relevant_principal_ids(state, request.principal_id);
    let focus_ids = focus_ids(state, request);
    let mut candidates = Vec::new();

    for memory in state.active_memories.values() {
        if !matches!(memory.status, ActiveMemoryStatus::Active)
            || memory
                .subject_principal_id
                .is_some_and(|id| !principal_ids.contains(&id))
            || memory.source_event_ids.is_empty()
        {
            continue;
        }
        let sources = validated_sources(memory.source_event_ids.clone(), as_of, index)?;
        candidates.push(MiddleCandidate {
            item: ContextItem {
                kind: ContextItemKind::Memory,
                text: memory.content.clone(),
                source_event_ids: sources.clone(),
                entity: Some(EntityRef::new(EntityKind::Memory, memory.id.uuid())),
                as_of_revision: as_of,
            },
            authority: memory_authority(&memory.kind),
            scope: memory
                .subject_principal_id
                .is_some_and(|id| id == request.principal_id) as u8,
            revision: source_revision(&sources, index),
            relevance: 0,
            key: memory.id.to_string(),
        });
    }

    for position in state.positions.values() {
        if principal_ids.contains(&position.principal_id)
            && !matches!(position.status, PositionStatus::Active)
        {
            let entity = EntityRef::new(EntityKind::Position, position.id.uuid());
            let mut source_ids = position.evidence_refs.clone();
            source_ids.extend(subject_sources(index, &entity.kind, entity.id, as_of));
            let sources = validated_sources(source_ids, as_of, index)?;
            if sources.is_empty() {
                continue;
            }
            candidates.push(MiddleCandidate {
                item: ContextItem {
                    kind: ContextItemKind::Position,
                    text: crate::runtime::embedding_indexer::position_text(position),
                    source_event_ids: sources.clone(),
                    entity: Some(entity),
                    as_of_revision: as_of,
                },
                authority: 3,
                scope: 1,
                revision: u64::from(position.version),
                relevance: relevance(position.subject.as_str(), &focus_ids),
                key: position.id.to_string(),
            });
        }
    }

    for artifact in state.artifacts.values() {
        let Some((_, event)) = index.get(&artifact.provenance_event_id) else {
            continue;
        };
        if !event_matches_focus(event, &focus_ids) {
            continue;
        }
        let sources = validated_sources(vec![artifact.provenance_event_id], as_of, index)?;
        candidates.push(MiddleCandidate {
            item: ContextItem {
                kind: ContextItemKind::Artifact,
                text: json_text(artifact)?,
                source_event_ids: sources.clone(),
                entity: Some(EntityRef::new(EntityKind::Artifact, artifact.id.uuid())),
                as_of_revision: as_of,
            },
            authority: 2,
            scope: 1,
            revision: source_revision(&sources, index),
            relevance: 2,
            key: artifact.id.to_string(),
        });
    }

    candidates.sort_by(|left, right| {
        right
            .authority
            .cmp(&left.authority)
            .then_with(|| right.scope.cmp(&left.scope))
            .then_with(|| right.revision.cmp(&left.revision))
            .then_with(|| right.relevance.cmp(&left.relevance))
            .then_with(|| left.key.cmp(&right.key))
    });
    Ok(candidates)
}

fn build_active_recent(
    request: &ContextBuildRequest,
    as_of: u64,
    index: &EventIndex<'_>,
) -> Result<Vec<ContextItem>, ContextBuilderError> {
    let focus_ids = request_focus_ids(request);
    let focus_strings = focus_ids
        .iter()
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();
    let recent_start = as_of.saturating_sub(RECENT_EVENT_WINDOW - 1).max(1);
    let mut latest_changes = BTreeMap::<String, u64>::new();
    for (sequence, event) in index.values() {
        if *sequence > as_of || !is_change_event(&event.event_kind) {
            continue;
        }
        latest_changes
            .entry(change_key(event))
            .and_modify(|latest| *latest = (*latest).max(*sequence))
            .or_insert(*sequence);
    }

    let mut selected_events = index
        .values()
        .filter(|(sequence, event)| {
            *sequence <= as_of
                && is_recent_event(&event.event_kind)
                && (*sequence >= recent_start
                    || latest_changes.get(&change_key(event)) == Some(sequence)
                    || event_matches_focus(event, &focus_ids)
                    || event
                        .correlation_id
                        .as_ref()
                        .is_some_and(|id| focus_strings.contains(id))
                    || event
                        .subject
                        .as_ref()
                        .is_some_and(|subject| focus_strings.contains(&subject.id.to_string())))
        })
        .filter(|(_, event)| {
            !request
                .current_observation_id
                .is_some_and(|observation_id| {
                    event.subject.as_ref().is_some_and(|subject| {
                        subject.kind == EntityKind::Observation
                            && subject.id == observation_id.uuid()
                    })
                })
        })
        .map(|(sequence, event)| (*sequence, event))
        .collect::<Vec<_>>();
    selected_events.sort_by(|left, right| {
        left.0.cmp(&right.0).then_with(|| {
            left.1
                .event_id
                .to_string()
                .cmp(&right.1.event_id.to_string())
        })
    });

    let mut active = Vec::new();
    let mut current_sources = BTreeSet::new();
    for (_, event) in selected_events {
        let sources = validated_sources(vec![event.event_id], as_of, index)?;
        current_sources.insert(event.event_id);
        let (kind, entity, text) = recent_event_fields(event)?;
        active.push(ContextItem {
            kind,
            text,
            source_event_ids: sources,
            entity,
            as_of_revision: as_of,
        });
    }

    let mut recalled = BTreeMap::<EventId, &RecalledItem>::new();
    for item in &request.recalled.items {
        let (sequence, event) = index.get(&item.source_event_id).copied().ok_or(
            ContextBuilderError::MissingSourceEvent {
                event_id: item.source_event_id,
            },
        )?;
        if sequence > as_of {
            return Err(ContextBuilderError::FutureRecallSource {
                event_id: item.source_event_id,
                sequence,
                as_of,
            });
        }
        validate_event_hash(item.source_event_id, event)?;
        if item.as_of_sequence < sequence || item.as_of_sequence > as_of {
            return Err(ContextBuilderError::InvalidRecallAsOf {
                event_id: item.source_event_id,
                item_as_of: item.as_of_sequence,
            });
        }
        let expected_hash =
            event
                .canonical_hash()
                .map_err(|error| ContextBuilderError::InvalidSourceEvent {
                    event_id: item.source_event_id,
                    message: error.to_string(),
                })?;
        if item.source_hash != expected_hash {
            return Err(ContextBuilderError::SourceHashMismatch {
                event_id: item.source_event_id,
            });
        }
        if current_sources.contains(&item.source_event_id) || !item.score.is_finite() {
            continue;
        }
        let replace = recalled
            .get(&item.source_event_id)
            .map(|existing| recall_order(item, existing) == Ordering::Greater)
            .unwrap_or(true);
        if replace {
            recalled.insert(item.source_event_id, item);
        }
    }
    let mut recalled = recalled.into_values().collect::<Vec<_>>();
    recalled.sort_by(|left, right| recall_order(left, right).reverse());
    for item in recalled {
        let sources = validated_sources(vec![item.source_event_id], as_of, index)?;
        active.push(ContextItem {
            kind: ContextItemKind::Recall,
            text: format!("[untrusted historical evidence] {}", item.text),
            source_event_ids: sources,
            entity: Some(item.entity.clone()),
            as_of_revision: as_of,
        });
    }
    Ok(active)
}

fn requested_relationship<'a>(
    state: &'a CurrentState,
    request: &ContextBuildRequest,
) -> Result<Option<&'a Relationship>, ContextBuilderError> {
    if let Some(id) = request.relationship_id {
        return state
            .relationships
            .get(&id)
            .ok_or(ContextBuilderError::MissingRequestedEntity {
                kind: "relationship",
                id: id.to_string(),
            })
            .map(Some);
    }
    let hekate_id = state
        .principals
        .values()
        .find(|principal| matches!(principal.kind, crate::core::PrincipalKind::Hekate))
        .map(|principal| principal.id);
    Ok(state.relationships.values().find(|relationship| {
        relationship.participants.contains(&request.principal_id)
            && hekate_id
                .map(|id| relationship.participants.contains(&id))
                .unwrap_or(true)
    }))
}

fn requested_run<'a>(
    state: &'a CurrentState,
    request: &ContextBuildRequest,
) -> Result<Option<&'a crate::core::Run>, ContextBuilderError> {
    request
        .run_id
        .map(|id| {
            state
                .runs
                .get(&id)
                .ok_or(ContextBuilderError::MissingRequestedEntity {
                    kind: "run",
                    id: id.to_string(),
                })
        })
        .transpose()
}

fn relevant_principal_ids(
    state: &CurrentState,
    principal_id: crate::core::PrincipalId,
) -> BTreeSet<crate::core::PrincipalId> {
    let mut ids = BTreeSet::from([principal_id]);
    if let Some(id) = state
        .principals
        .values()
        .find(|principal| matches!(principal.kind, crate::core::PrincipalKind::Hekate))
        .map(|principal| principal.id)
    {
        ids.insert(id);
    }
    ids
}

fn focus_ids(state: &CurrentState, request: &ContextBuildRequest) -> BTreeSet<Uuid> {
    let mut ids = request_focus_ids(request);
    if let Some(task) = request.task_id.and_then(|id| state.tasks.get(&id)) {
        ids.insert(task.id.uuid());
        if let Some(goal_id) = task.goal_id {
            ids.insert(goal_id.uuid());
        }
    }
    if let Some(run) = request.run_id.and_then(|id| state.runs.get(&id)) {
        ids.insert(run.id.uuid());
        if let Some(task_id) = run.task_id {
            ids.insert(task_id.uuid());
        }
    }
    ids
}

fn request_focus_ids(request: &ContextBuildRequest) -> BTreeSet<Uuid> {
    [
        request.relationship_id.map(|id| id.uuid()),
        request.task_id.map(|id| id.uuid()),
        request.run_id.map(|id| id.uuid()),
    ]
    .into_iter()
    .flatten()
    .collect()
}

fn subject_sources(
    index: &EventIndex<'_>,
    kind: &EntityKind,
    id: Uuid,
    as_of: u64,
) -> Vec<EventId> {
    let mut sources = index
        .values()
        .filter(|(sequence, event)| {
            *sequence <= as_of
                && event
                    .subject
                    .as_ref()
                    .is_some_and(|subject| subject.kind == *kind && subject.id == id)
        })
        .map(|(sequence, event)| (*sequence, event.event_id))
        .collect::<Vec<_>>();
    sources.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.to_string().cmp(&right.1.to_string()))
    });
    sources.into_iter().map(|(_, event_id)| event_id).collect()
}

fn validated_sources(
    ids: Vec<EventId>,
    as_of: u64,
    index: &EventIndex<'_>,
) -> Result<Vec<EventId>, ContextBuilderError> {
    let mut sources = ids
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|event_id| {
            let (sequence, event) = index
                .get(&event_id)
                .copied()
                .ok_or(ContextBuilderError::MissingSourceEvent { event_id })?;
            if sequence > as_of {
                return Err(ContextBuilderError::FutureEvent {
                    event_id,
                    sequence,
                    as_of,
                });
            }
            validate_event_hash(event_id, event)?;
            Ok((sequence, event_id))
        })
        .collect::<Result<Vec<_>, ContextBuilderError>>()?;
    sources.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.to_string().cmp(&right.1.to_string()))
    });
    Ok(sources.into_iter().map(|(_, event_id)| event_id).collect())
}

fn validate_event_hash(
    event_id: EventId,
    event: &ExperienceEvent,
) -> Result<(), ContextBuilderError> {
    match event.verify_integrity() {
        Ok(true) => Ok(()),
        Ok(false) => Err(ContextBuilderError::SourceHashMismatch { event_id }),
        Err(error) => Err(ContextBuilderError::InvalidSourceEvent {
            event_id,
            message: error.to_string(),
        }),
    }
}

fn validate_profile(
    profile: &ResponseProfile,
    as_of: u64,
    index: &EventIndex<'_>,
) -> Result<(), ContextBuilderError> {
    if profile.as_of_revision > as_of {
        return Err(ContextBuilderError::FutureResponseProfile {
            profile_revision: profile.as_of_revision,
            request_revision: as_of,
        });
    }
    for evidence in &profile.evidence {
        validated_sources(vec![evidence.source_event_id], as_of, index)?;
    }
    Ok(())
}

fn source_refs(
    anchors: &[ContextItem],
    middle: &[ContextItem],
    active_recent: &[ContextItem],
    profile: &ResponseProfile,
    as_of: u64,
    index: &EventIndex<'_>,
) -> Result<Vec<ContextSourceRef>, ContextBuilderError> {
    let mut refs = BTreeMap::<String, ContextSourceRef>::new();
    for item in anchors.iter().chain(middle).chain(active_recent) {
        for event_id in &item.source_event_ids {
            let source_hash = event_hash(*event_id, as_of, index)?;
            refs.entry(event_id.to_string())
                .or_insert_with(|| ContextSourceRef {
                    event_id: *event_id,
                    entity: item.entity.clone(),
                    source_hash,
                    as_of_revision: as_of,
                });
        }
    }
    for evidence in &profile.evidence {
        let event_id = evidence.source_event_id;
        let source_hash = event_hash(event_id, as_of, index)?;
        let entity = Some(EntityRef::new(
            EntityKind::Memory,
            evidence.memory_id.uuid(),
        ));
        refs.entry(event_id.to_string())
            .or_insert(ContextSourceRef {
                event_id,
                entity,
                source_hash,
                as_of_revision: as_of,
            });
    }
    Ok(refs.into_values().collect())
}

fn event_hash(
    event_id: EventId,
    as_of: u64,
    index: &EventIndex<'_>,
) -> Result<String, ContextBuilderError> {
    let (sequence, event) = index
        .get(&event_id)
        .copied()
        .ok_or(ContextBuilderError::MissingSourceEvent { event_id })?;
    if sequence > as_of {
        return Err(ContextBuilderError::FutureEvent {
            event_id,
            sequence,
            as_of,
        });
    }
    validate_event_hash(event_id, event)?;
    event
        .canonical_hash()
        .map_err(|error| ContextBuilderError::InvalidSourceEvent {
            event_id,
            message: error.to_string(),
        })
}

fn budget_report(
    anchors: &[ContextItem],
    middle: &[ContextItem],
    active_recent: &[ContextItem],
    total_bytes: usize,
    dropped_middle_items: usize,
    dropped_active_items: usize,
    hard_limit_bytes: usize,
) -> Result<ContextBudgetReport, ContextBuilderError> {
    Ok(ContextBudgetReport {
        anchor_bytes: serialized_len(anchors)?,
        middle_bytes: serialized_len(middle)?,
        active_bytes: serialized_len(active_recent)?,
        total_bytes,
        included_items: anchors.len() + middle.len() + active_recent.len(),
        dropped_middle_items,
        dropped_active_items,
        hard_limit_bytes,
    })
}

fn snapshot_hash(
    as_of: u64,
    anchors: &[ContextItem],
    middle: &[ContextItem],
    active_recent: &[ContextItem],
    profile: &ResponseProfile,
    report: &ContextBudgetReport,
    source_refs: &[ContextSourceRef],
) -> Result<String, ContextBuilderError> {
    #[derive(Serialize)]
    struct HashInput<'a> {
        as_of_revision: u64,
        anchors: &'a [ContextItem],
        compressed_middle: &'a [ContextItem],
        active_recent: &'a [ContextItem],
        response_profile_hash: &'a str,
        budget_report: &'a ContextBudgetReport,
        source_refs: &'a [ContextSourceRef],
    }
    let bytes = serde_json::to_vec(&HashInput {
        as_of_revision: as_of,
        anchors,
        compressed_middle: middle,
        active_recent,
        response_profile_hash: &profile.profile_hash,
        budget_report: report,
        source_refs,
    })
    .map_err(|error| ContextBuilderError::Serialization(error.to_string()))?;
    let digest = Sha256::digest(bytes);
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn select_by_budget<I>(
    candidates: I,
    budget: usize,
) -> Result<(Vec<ContextItem>, usize), ContextBuilderError>
where
    I: IntoIterator<Item = ContextItem>,
{
    let mut selected = Vec::new();
    let mut dropped = 0;
    for item in candidates {
        selected.push(item);
        if serialized_len(&selected)? > budget {
            selected.pop();
            dropped += 1;
        }
    }
    Ok((selected, dropped))
}

fn drop_for_hard_limit(
    middle: &mut Vec<ContextItem>,
    active_recent: &mut Vec<ContextItem>,
    dropped_middle_items: &mut usize,
    dropped_active_items: &mut usize,
) -> bool {
    if let Some(index) = active_recent
        .iter()
        .rposition(|item| matches!(item.kind, ContextItemKind::Recall))
    {
        active_recent.remove(index);
        *dropped_active_items += 1;
        return true;
    }
    if middle.pop().is_some() {
        *dropped_middle_items += 1;
        return true;
    }
    if !active_recent.is_empty() {
        active_recent.remove(0);
        *dropped_active_items += 1;
        return true;
    }
    false
}

fn push_anchor(anchors: &mut Vec<ContextItem>, seen: &mut BTreeSet<String>, item: ContextItem) {
    let key = format!(
        "{:?}:{}",
        item.kind,
        item.entity
            .as_ref()
            .map(|entity| format!("{:?}:{}", entity.kind, entity.id))
            .unwrap_or_default()
    );
    if seen.insert(key) {
        anchors.push(item);
    }
}

fn recent_event_fields(
    event: &ExperienceEvent,
) -> Result<(ContextItemKind, Option<EntityRef>, String), ContextBuilderError> {
    let observation = matches!(
        event.event_kind,
        EventKind::ObservationRecorded | EventKind::UserMessageReceived
    );
    if observation {
        if let Ok(value) = serde_json::from_value::<crate::core::Observation>(event.payload.clone())
        {
            return Ok((
                ContextItemKind::RecentObservation,
                event
                    .subject
                    .clone()
                    .or_else(|| Some(EntityRef::new(EntityKind::Observation, value.id.uuid()))),
                value.content,
            ));
        }
    }
    let kind = if observation {
        ContextItemKind::RecentObservation
    } else {
        ContextItemKind::RecentEvent
    };
    let text = format!(
        "event={} payload={}",
        serde_json::to_string(&event.event_kind)
            .map_err(|error| ContextBuilderError::Serialization(error.to_string()))?,
        serde_json::to_string(&event.payload)
            .map_err(|error| ContextBuilderError::Serialization(error.to_string()))?
    );
    Ok((kind, event.subject.clone(), text))
}

fn is_recent_event(kind: &EventKind) -> bool {
    matches!(
        kind,
        EventKind::ObservationRecorded
            | EventKind::UserMessageReceived
            | EventKind::FocusResolved
            | EventKind::DecisionCreated
            | EventKind::PositionEstablished
            | EventKind::PositionMaintained
            | EventKind::PositionRevised
            | EventKind::PositionRetracted
            | EventKind::PositionRecorded
            | EventKind::ConflictOpened
            | EventKind::ConflictUpdated
            | EventKind::ConflictResolved
            | EventKind::ConflictRecorded
            | EventKind::RunStarted
            | EventKind::RunSuspended
            | EventKind::RunCompleted
            | EventKind::WorkingStateUpdated
            | EventKind::ArtifactCreated
            | EventKind::ResponseProduced
    )
}

fn is_change_event(kind: &EventKind) -> bool {
    matches!(
        kind,
        EventKind::DecisionCreated
            | EventKind::PositionEstablished
            | EventKind::PositionMaintained
            | EventKind::PositionRevised
            | EventKind::PositionRetracted
            | EventKind::PositionRecorded
            | EventKind::ConflictOpened
            | EventKind::ConflictUpdated
            | EventKind::ConflictResolved
            | EventKind::ConflictRecorded
    )
}

fn change_key(event: &ExperienceEvent) -> String {
    event
        .subject
        .as_ref()
        .map(|subject| format!("{:?}:{}", subject.kind, subject.id))
        .unwrap_or_else(|| event.event_id.to_string())
}

fn event_matches_focus(event: &ExperienceEvent, focus_ids: &BTreeSet<Uuid>) -> bool {
    event
        .subject
        .as_ref()
        .is_some_and(|subject| focus_ids.contains(&subject.id))
        || event
            .correlation_id
            .as_ref()
            .and_then(|value| Uuid::parse_str(value).ok())
            .is_some_and(|id| focus_ids.contains(&id))
}

fn recall_order(left: &RecalledItem, right: &RecalledItem) -> Ordering {
    left.score.total_cmp(&right.score).then_with(|| {
        right
            .source_event_id
            .to_string()
            .cmp(&left.source_event_id.to_string())
    })
}

fn memory_authority(kind: &MemoryKind) -> u8 {
    match kind {
        MemoryKind::ExplicitPreference => 5,
        MemoryKind::VerifiedFact => 4,
        MemoryKind::Lesson => 3,
        MemoryKind::Episode => 2,
        MemoryKind::InferredPreference => 1,
    }
}

fn relevance(text: &str, focus_ids: &BTreeSet<Uuid>) -> u8 {
    if focus_ids.iter().any(|id| text.contains(&id.to_string())) {
        1
    } else {
        0
    }
}

fn source_revision(ids: &[EventId], index: &EventIndex<'_>) -> u64 {
    ids.iter()
        .filter_map(|id| index.get(id).map(|(sequence, _)| *sequence))
        .max()
        .unwrap_or_default()
}

fn json_text<T: Serialize>(value: &T) -> Result<String, ContextBuilderError> {
    serde_json::to_string(value)
        .map_err(|error| ContextBuilderError::Serialization(error.to_string()))
}

fn serialized_len<T: Serialize + ?Sized>(value: &T) -> Result<usize, ContextBuilderError> {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len())
        .map_err(|error| ContextBuilderError::Serialization(error.to_string()))
}

struct MiddleCandidate {
    item: ContextItem,
    authority: u8,
    scope: u8,
    revision: u64,
    relevance: u8,
    key: String,
}
