use std::collections::{BTreeMap, BTreeSet};

use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::core::{
    initiative_fingerprint, AgendaCandidate, Commitment, CommitmentStatus, Conflict,
    ConflictStatus, CurrentState, EntityKind, EntityRef, EventId, EventKind, ExperienceEvent,
    InitiativeKind, Observation, Position, PositionStatus, PrincipalId, PrincipalKind,
    MAX_CANDIDATE_SOURCES, MAX_SLEEP_CANDIDATES,
};

const MAX_TEXT_BYTES: usize = 320;
const NON_TOPICAL_WORDS: &[&str] = &[
    "about", "after", "also", "and", "are", "could", "does", "for", "from", "have", "into", "is",
    "its", "more", "should", "that", "the", "their", "this", "through", "with", "would",
];

type VerifiedLedger<'a> = BTreeMap<EventId, (&'a ExperienceEvent, u64)>;

/// Select evidence-backed candidates from the current projection and ledger.
pub fn select(
    state: &CurrentState,
    events: &[ExperienceEvent],
    hekate_id: PrincipalId,
    user_id: PrincipalId,
) -> Vec<AgendaCandidate> {
    if hekate_id == user_id
        || state.principals.get(&hekate_id).map_or(true, |principal| {
            !matches!(&principal.kind, PrincipalKind::Hekate)
        })
        || state.principals.get(&user_id).map_or(true, |principal| {
            !matches!(&principal.kind, PrincipalKind::User)
        })
    {
        return Vec::new();
    }

    let Some(ledger) = verified_ledger(state, events) else {
        return Vec::new();
    };
    let mut candidates = Vec::new();
    add_conflict_candidates(state, &ledger, hekate_id, user_id, &mut candidates);
    add_commitment_candidates(state, &ledger, hekate_id, user_id, &mut candidates);
    add_position_review_candidates(state, &ledger, hekate_id, user_id, &mut candidates);

    // Conflict questions come first, then commitments, then position reviews.
    candidates.sort_by(|left, right| {
        priority(left.kind)
            .cmp(&priority(right.kind))
            .then_with(|| left.source_entity.id.cmp(&right.source_entity.id))
            .then_with(|| left.source_version.cmp(&right.source_version))
            .then_with(|| left.fingerprint.cmp(&right.fingerprint))
    });
    candidates.dedup_by(|left, right| left.fingerprint == right.fingerprint);
    candidates.truncate(MAX_SLEEP_CANDIDATES);
    candidates
}

fn verified_ledger<'a>(
    state: &CurrentState,
    events: &'a [ExperienceEvent],
) -> Option<VerifiedLedger<'a>> {
    let revision = u64::try_from(state.applied_events.len()).ok()?;
    if revision != state.revision {
        return None;
    }

    let mut sequence_by_id = BTreeMap::new();
    for (index, event_id) in state.applied_events.iter().enumerate() {
        sequence_by_id.insert(*event_id, u64::try_from(index).ok()?.checked_add(1)?);
    }
    if sequence_by_id.len() != state.applied_events.len() {
        return None;
    }

    let mut ledger = BTreeMap::new();
    for event in events {
        let Some(sequence) = sequence_by_id.get(&event.event_id).copied() else {
            continue;
        };
        if event.verify_integrity().ok() == Some(true) {
            ledger.entry(event.event_id).or_insert((event, sequence));
        }
    }
    if ledger.len() != sequence_by_id.len() {
        return None;
    }
    Some(ledger)
}

fn latest_entity_payload<'events, 'ledger, T>(
    ledger: &'ledger VerifiedLedger<'events>,
    entity_kind: &EntityKind,
    entity_id: Uuid,
    lifecycle_event: impl Fn(&EventKind) -> bool,
    payload_id: impl Fn(&T) -> Uuid,
) -> Option<(u64, &'events ExperienceEvent, T)>
where
    T: DeserializeOwned,
{
    let mut latest: Option<(u64, &'events ExperienceEvent, T)> = None;
    for (event, sequence) in ledger.values() {
        if !lifecycle_event(&event.event_kind)
            || !event
                .subject
                .as_ref()
                .is_some_and(|subject| &subject.kind == entity_kind && subject.id == entity_id)
        {
            continue;
        }
        let Ok(payload) = serde_json::from_value::<T>(event.payload.clone()) else {
            continue;
        };
        if payload_id(&payload) != entity_id {
            continue;
        }
        if latest
            .as_ref()
            .map_or(true, |(latest_sequence, _, _)| sequence > latest_sequence)
        {
            latest = Some((*sequence, event, payload));
        }
    }
    latest
}

fn current_position_sequence(position: &Position, ledger: &VerifiedLedger<'_>) -> Option<u64> {
    let (sequence, _, projected) = latest_entity_payload(
        ledger,
        &EntityKind::Position,
        position.id.uuid(),
        |kind| {
            matches!(
                kind,
                EventKind::PositionEstablished
                    | EventKind::PositionMaintained
                    | EventKind::PositionRecorded
                    | EventKind::PositionRevised
                    | EventKind::PositionRetracted
            )
        },
        |payload: &Position| payload.id.uuid(),
    )?;
    (projected == *position).then_some(sequence)
}

fn current_conflict_sequence(conflict: &Conflict, ledger: &VerifiedLedger<'_>) -> Option<u64> {
    let (sequence, _, projected) = latest_entity_payload(
        ledger,
        &EntityKind::Conflict,
        conflict.id.uuid(),
        |kind| {
            matches!(
                kind,
                EventKind::ConflictOpened
                    | EventKind::ConflictUpdated
                    | EventKind::ConflictResolved
                    | EventKind::ConflictRecorded
            )
        },
        |payload: &Conflict| payload.id.uuid(),
    )?;
    (projected == *conflict).then_some(sequence)
}

fn current_commitment(
    commitment: &Commitment,
    ledger: &VerifiedLedger<'_>,
) -> Option<(u64, EventKind)> {
    let (sequence, event, projected) = latest_entity_payload(
        ledger,
        &EntityKind::Commitment,
        commitment.id.uuid(),
        |kind| {
            matches!(
                kind,
                EventKind::CommitmentCreated | EventKind::CommitmentFulfilled
            )
        },
        |payload: &Commitment| payload.id.uuid(),
    )?;
    (projected == *commitment).then_some((sequence, event.event_kind.clone()))
}

fn add_conflict_candidates(
    state: &CurrentState,
    ledger: &VerifiedLedger<'_>,
    hekate_id: PrincipalId,
    user_id: PrincipalId,
    candidates: &mut Vec<AgendaCandidate>,
) {
    for conflict in state.conflicts.values() {
        if !matches!(
            &conflict.status,
            ConflictStatus::Open | ConflictStatus::Negotiating
        ) {
            continue;
        }
        let Some(question) = conflict
            .unresolved_questions
            .iter()
            .map(|question| question.trim())
            .find(|question| !question.is_empty())
        else {
            continue;
        };
        let Some(conflict_sequence) = current_conflict_sequence(conflict, ledger) else {
            continue;
        };
        let hekate_participates = conflict.participant_positions.iter().any(|position_id| {
            let Some(position) = state.positions.get(position_id) else {
                return false;
            };
            position.principal_id == hekate_id
                && matches!(&position.status, PositionStatus::Active)
                && current_position_sequence(position, ledger)
                    .is_some_and(|sequence| sequence < conflict_sequence)
        });
        let user_participates = conflict.participant_positions.iter().any(|position_id| {
            let Some(position) = state.positions.get(position_id) else {
                return false;
            };
            position.principal_id == user_id
                && current_position_sequence(position, ledger)
                    .is_some_and(|sequence| sequence < conflict_sequence)
        });
        if !hekate_participates || !user_participates {
            continue;
        }
        let sources = bounded_valid_event_ids(
            conflict.evidence_refs.iter().copied(),
            ledger,
            Some(conflict_sequence),
        );
        if sources.is_empty() {
            continue;
        }
        let content = question.to_owned();
        let rationale = format!(
            "An open conflict about {} has an unresolved question backed by verified events.",
            conflict.subject.trim()
        );
        if let Some(candidate) = make_candidate(
            InitiativeKind::Question,
            content,
            rationale,
            EntityRef::new(EntityKind::Conflict, conflict.id.uuid()),
            u64::from(conflict.revision),
            sources,
            user_id,
            state.revision,
        ) {
            candidates.push(candidate);
        }
    }
}

fn add_commitment_candidates(
    state: &CurrentState,
    ledger: &VerifiedLedger<'_>,
    hekate_id: PrincipalId,
    user_id: PrincipalId,
    candidates: &mut Vec<AgendaCandidate>,
) {
    for commitment in state.commitments.values() {
        if !matches!(&commitment.status, CommitmentStatus::Open)
            || commitment.debtor_principal_id != hekate_id
            || commitment.creditor_principal_id != user_id
        {
            continue;
        }
        let Some((source_version, event_kind)) = current_commitment(commitment, ledger) else {
            continue;
        };
        if event_kind != EventKind::CommitmentCreated {
            continue;
        }
        let Some(source_event_id) = commitment.source_event_id else {
            continue;
        };
        let sources = bounded_valid_event_ids([source_event_id], ledger, Some(source_version));
        if sources.is_empty() {
            continue;
        }
        let content = format!("Can I follow up on my commitment: {}?", commitment.promise);
        let rationale =
            "HEKATE has an open commitment to this principal with a verified source event.";
        // Commitment has no version counter; its verified creation sequence is stable.
        if let Some(candidate) = make_candidate(
            InitiativeKind::FollowUp,
            content,
            rationale.to_owned(),
            EntityRef::new(EntityKind::Commitment, commitment.id.uuid()),
            source_version,
            sources,
            user_id,
            state.revision,
        ) {
            candidates.push(candidate);
        }
    }
}

fn add_position_review_candidates(
    state: &CurrentState,
    ledger: &VerifiedLedger<'_>,
    hekate_id: PrincipalId,
    user_id: PrincipalId,
    candidates: &mut Vec<AgendaCandidate>,
) {
    for position in state.positions.values() {
        if position.principal_id != hekate_id || !matches!(&position.status, PositionStatus::Active)
        {
            continue;
        }
        let Some(position_sequence) = current_position_sequence(position, ledger) else {
            continue;
        };
        let topic_terms = position_terms(position);
        if topic_terms.is_empty() {
            continue;
        }

        // ponytail: lexical overlap is the offline relevance ceiling; use typed evidence links if the event contract adds them.
        let mut related = ledger
            .values()
            .filter_map(|(event, sequence)| {
                if *sequence <= position_sequence
                    || !matches!(
                        &event.event_kind,
                        EventKind::ObservationRecorded | EventKind::UserMessageReceived
                    )
                {
                    return None;
                }
                let observation =
                    serde_json::from_value::<Observation>(event.payload.clone()).ok()?;
                let has_observation_subject = event.subject.as_ref().is_some_and(|subject| {
                    matches!(&subject.kind, EntityKind::Observation)
                        && subject.id == observation.id.uuid()
                });
                if !has_observation_subject
                    || event.actor_id != observation.actor_id
                    || state.observations.get(&observation.id) != Some(&observation)
                    || !observation_related(&topic_terms, &observation.content)
                {
                    return None;
                }
                Some((*sequence, event.event_id))
            })
            .collect::<Vec<_>>();
        related.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
        related.dedup_by_key(|(_, event_id)| *event_id);
        related.truncate(MAX_CANDIDATE_SOURCES);
        let mut sources = related
            .into_iter()
            .map(|(_, event_id)| event_id)
            .collect::<Vec<_>>();
        sources.sort_unstable();
        if sources.is_empty() {
            continue;
        }

        let content = format!(
            "Could new evidence change HEKATE's position on {}?",
            position.subject.trim()
        );
        let rationale =
            "A related observation arrived after this active HEKATE position was recorded.";
        if let Some(candidate) = make_candidate(
            InitiativeKind::Challenge,
            content,
            rationale.to_owned(),
            EntityRef::new(EntityKind::Position, position.id.uuid()),
            u64::from(position.version),
            sources,
            user_id,
            state.revision,
        ) {
            candidates.push(candidate);
        }
    }
}

fn bounded_valid_event_ids(
    event_ids: impl IntoIterator<Item = EventId>,
    ledger: &VerifiedLedger<'_>,
    before_sequence: Option<u64>,
) -> Vec<EventId> {
    let mut valid = event_ids
        .into_iter()
        .filter_map(|event_id| {
            let (_, sequence) = ledger.get(&event_id)?;
            if before_sequence.is_some_and(|before| *sequence >= before) {
                return None;
            }
            Some((event_id, *sequence))
        })
        .collect::<Vec<_>>();
    valid.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    valid.dedup_by_key(|(event_id, _)| *event_id);
    valid.truncate(MAX_CANDIDATE_SOURCES);
    let mut event_ids = valid
        .into_iter()
        .map(|(event_id, _)| event_id)
        .collect::<Vec<_>>();
    event_ids.sort_unstable();
    event_ids
}

fn position_terms(position: &Position) -> BTreeSet<String> {
    std::iter::once(position.subject.as_str())
        .chain(position.reasons.iter().map(String::as_str))
        .chain(
            position
                .reconsideration_conditions
                .iter()
                .map(String::as_str),
        )
        .flat_map(tokens)
        .filter(|token| token.chars().count() >= 4 && !NON_TOPICAL_WORDS.contains(&token.as_str()))
        .collect()
}

fn tokens(text: &str) -> impl Iterator<Item = String> + '_ {
    text.split(|character: char| !character.is_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(str::to_lowercase)
}

fn observation_related(topic_terms: &BTreeSet<String>, content: &str) -> bool {
    let content_terms = tokens(content).collect::<BTreeSet<_>>();
    let matched = topic_terms.intersection(&content_terms).count();
    matched >= topic_terms.len().min(2)
}

fn make_candidate(
    kind: InitiativeKind,
    content: String,
    rationale: String,
    source_entity: EntityRef,
    source_version: u64,
    mut source_event_ids: Vec<EventId>,
    target_principal_id: PrincipalId,
    as_of_revision: u64,
) -> Option<AgendaCandidate> {
    source_event_ids.sort_unstable();
    source_event_ids.dedup();
    source_event_ids.truncate(MAX_CANDIDATE_SOURCES);
    if source_event_ids.is_empty() {
        return None;
    }
    let content = bounded_text(&content);
    let rationale = bounded_text(&rationale);
    if content.is_empty() || rationale.is_empty() {
        return None;
    }
    let fingerprint = initiative_fingerprint(
        kind,
        &source_entity,
        source_version,
        target_principal_id,
        &source_event_ids,
    );
    Some(AgendaCandidate {
        kind,
        content,
        rationale,
        source_entity,
        source_version,
        source_event_ids,
        target_principal_id,
        as_of_revision,
        fingerprint,
    })
}

fn bounded_text(value: &str) -> String {
    let value = value.trim();
    let mut end = value.len().min(MAX_TEXT_BYTES);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].trim_end().to_owned()
}

fn priority(kind: InitiativeKind) -> u8 {
    match kind {
        InitiativeKind::Question => 0,
        InitiativeKind::FollowUp => 1,
        InitiativeKind::Challenge => 2,
    }
}
