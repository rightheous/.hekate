use std::collections::BTreeMap;

use hekate::core::{
    initiative_fingerprint, Commitment, CommitmentId, CommitmentStatus, Conflict, ConflictId,
    ConflictStatus, EntityKind, EntityRef, EventKind, EventSource, ExperienceEvent, Goal, GoalId,
    GoalStatus, InitiativeKind, Observation, ObservationId, Position, PositionId, PositionStatus,
    Principal, PrincipalId, PrincipalKind, Run, RunId, RunStatus, Stance, Task, TaskId, TaskStatus,
    MAX_SLEEP_CANDIDATES,
};
use hekate::runtime::{initiative::agenda::select, Projector};
use uuid::Uuid;

fn id(value: u128) -> Uuid {
    Uuid::from_u128(value)
}

fn principal(id_value: u128, kind: PrincipalKind, name: &str) -> Principal {
    Principal {
        id: PrincipalId::from_uuid(id(id_value)),
        kind,
        name: name.to_owned(),
        identity_version_id: None,
    }
}

fn event<T: serde::Serialize>(
    actor_id: PrincipalId,
    event_kind: EventKind,
    entity_kind: EntityKind,
    entity_id: Uuid,
    payload: &T,
) -> ExperienceEvent {
    ExperienceEvent::new(
        actor_id,
        event_kind,
        Some(EntityRef::new(entity_kind, entity_id)),
        serde_json::to_value(payload).expect("serialize event payload"),
        EventSource::new("initiative-agenda-test", None),
        None,
        None,
        Some(1.0),
    )
    .expect("create event")
}

fn base() -> (PrincipalId, PrincipalId, Vec<ExperienceEvent>) {
    let hekate = principal(1, PrincipalKind::Hekate, "HEKATE");
    let user = principal(2, PrincipalKind::User, "User");
    let events = vec![
        event(
            hekate.id,
            EventKind::PrincipalCreated,
            EntityKind::Principal,
            hekate.id.uuid(),
            &hekate,
        ),
        event(
            user.id,
            EventKind::PrincipalCreated,
            EntityKind::Principal,
            user.id.uuid(),
            &user,
        ),
    ];
    (hekate.id, user.id, events)
}

fn observation_event(
    events: &mut Vec<ExperienceEvent>,
    actor_id: PrincipalId,
    observation_id: u128,
    content: &str,
) -> hekate::core::EventId {
    let observation = Observation {
        id: ObservationId::from_uuid(id(observation_id)),
        actor_id,
        content: content.to_owned(),
        source_type: "test".to_owned(),
        source_ref: None,
        thread_id: None,
        message_id: None,
        received_at: "2026-01-01T00:00:00Z".to_owned(),
    };
    let event = event(
        actor_id,
        EventKind::ObservationRecorded,
        EntityKind::Observation,
        observation.id.uuid(),
        &observation,
    );
    let event_id = event.event_id;
    events.push(event);
    event_id
}

fn position(
    id_value: u128,
    principal_id: PrincipalId,
    subject: &str,
    evidence_refs: Vec<hekate::core::EventId>,
) -> Position {
    Position {
        id: PositionId::from_uuid(id(id_value)),
        principal_id,
        subject: subject.to_owned(),
        stance: Stance::Support,
        version: 1,
        status: PositionStatus::Active,
        confidence: 80,
        supersedes: None,
        reasons: vec!["support this position based on prior evidence".to_owned()],
        evidence_refs,
        reconsideration_conditions: vec!["new workspace retention evidence".to_owned()],
        created_at: "2026-01-01T00:00:00Z".to_owned(),
    }
}

fn add_position(events: &mut Vec<ExperienceEvent>, actor_id: PrincipalId, value: &Position) {
    events.push(event(
        actor_id,
        EventKind::PositionEstablished,
        EntityKind::Position,
        value.id.uuid(),
        value,
    ));
}

fn conflict(
    id_value: u128,
    positions: Vec<PositionId>,
    evidence_refs: Vec<hekate::core::EventId>,
    question: &str,
) -> Conflict {
    Conflict {
        id: ConflictId::from_uuid(id(id_value)),
        subject: "workspace retention".to_owned(),
        participant_positions: positions,
        status: ConflictStatus::Open,
        revision: 1,
        reasons: vec!["the participants hold different positions".to_owned()],
        evidence_refs,
        alternatives: Vec::new(),
        reconsideration_conditions: vec!["new retention evidence".to_owned()],
        unresolved_questions: vec![question.to_owned()],
        resolution: None,
        resolved_at: None,
        created_at: "2026-01-01T00:00:00Z".to_owned(),
    }
}

fn add_conflict(events: &mut Vec<ExperienceEvent>, actor: PrincipalId, value: &Conflict) {
    events.push(event(
        actor,
        EventKind::ConflictOpened,
        EntityKind::Conflict,
        value.id.uuid(),
        value,
    ));
}

fn replay(events: &[ExperienceEvent]) -> hekate::core::CurrentState {
    Projector::replay(events).expect("replay test events")
}

fn conflict_fixture() -> (
    PrincipalId,
    PrincipalId,
    Position,
    Conflict,
    hekate::core::EventId,
    Vec<ExperienceEvent>,
) {
    let (hekate_id, user_id, mut events) = base();
    let source = observation_event(&mut events, user_id, 10, "workspace retention evidence");
    let hekate_position = position(20, hekate_id, "workspace retention", vec![source]);
    let user_position = position(21, user_id, "workspace retention", vec![source]);
    add_position(&mut events, hekate_id, &hekate_position);
    add_position(&mut events, user_id, &user_position);
    let conflict = conflict(
        30,
        vec![hekate_position.id, user_position.id],
        vec![source],
        "Should we retain the current workspace?",
    );
    add_conflict(&mut events, hekate_id, &conflict);
    (
        hekate_id,
        user_id,
        hekate_position,
        conflict,
        source,
        events,
    )
}

#[test]
fn open_conflict_with_hekate_position_and_verified_evidence_yields_question() {
    let (hekate_id, user_id, position, conflict, source, events) = conflict_fixture();
    let state = replay(&events);
    let candidates = select(&state, &events, hekate_id, user_id);

    assert_eq!(candidates.len(), 1);
    let candidate = &candidates[0];
    assert_eq!(candidate.kind, InitiativeKind::Question);
    assert_eq!(candidate.source_entity.kind, EntityKind::Conflict);
    assert_eq!(candidate.source_entity.id, conflict.id.uuid());
    assert_eq!(candidate.source_version, u64::from(conflict.revision));
    assert_eq!(candidate.source_event_ids, vec![source]);
    assert_eq!(candidate.target_principal_id, user_id);
    assert_eq!(candidate.as_of_revision, state.revision);
    assert_eq!(
        candidate.fingerprint,
        initiative_fingerprint(
            candidate.kind,
            &candidate.source_entity,
            candidate.source_version,
            candidate.target_principal_id,
            &candidate.source_event_ids,
        )
    );
    assert_eq!(
        state
            .positions
            .get(&position.id)
            .map(|stored| &stored.status),
        Some(&PositionStatus::Active)
    );
}

#[test]
fn missing_or_invalid_evidence_and_user_positions_do_not_create_conflict_candidates() {
    let (hekate_id, user_id, hekate_position, _, source, mut events) = conflict_fixture();
    let no_evidence = conflict(31, vec![hekate_position.id], Vec::new(), "What changed?");
    add_conflict(&mut events, hekate_id, &no_evidence);
    let unrelated_conflict = conflict(
        32,
        vec![hekate_position.id],
        vec![source],
        "Does this unrelated conflict affect the user?",
    );
    add_conflict(&mut events, hekate_id, &unrelated_conflict);
    let state = replay(&events);
    assert!(select(&state, &events, hekate_id, user_id)
        .iter()
        .all(|candidate| candidate.source_entity.id != no_evidence.id.uuid()));
    assert!(select(&state, &events, hekate_id, user_id)
        .iter()
        .all(|candidate| candidate.source_entity.id != unrelated_conflict.id.uuid()));

    let tampered = events
        .iter()
        .cloned()
        .map(|mut event| {
            if event.event_id == source {
                event.payload["content"] =
                    serde_json::Value::String("changed after signing".into());
            }
            event
        })
        .collect::<Vec<_>>();
    assert!(select(&state, &tampered, hekate_id, user_id).is_empty());

    let (hekate_id, user_id, mut user_position_events) = base();
    let source = observation_event(
        &mut user_position_events,
        user_id,
        40,
        "workspace retention evidence",
    );
    let user_position = position(41, user_id, "workspace retention", vec![source]);
    user_position_events.push(event(
        user_id,
        EventKind::PositionEstablished,
        EntityKind::Position,
        user_position.id.uuid(),
        &user_position,
    ));
    let user_conflict = conflict(
        42,
        vec![user_position.id],
        vec![source],
        "Should this position change?",
    );
    add_conflict(&mut user_position_events, hekate_id, &user_conflict);
    let state = replay(&user_position_events);
    assert!(select(&state, &user_position_events, hekate_id, user_id).is_empty());
}

#[test]
fn goals_tasks_and_old_incomplete_runs_are_not_agenda_sources() {
    let (hekate_id, user_id, mut events) = base();
    let stale_goal = Goal {
        id: GoalId::from_uuid(id(50)),
        owner_principal_id: hekate_id,
        participants: vec![user_id],
        title: "Delete the entire workspace".to_owned(),
        description: "Old workspace-wide deletion request".to_owned(),
        status: GoalStatus::Active,
        created_at: "2020-01-01T00:00:00Z".to_owned(),
    };
    events.push(event(
        hekate_id,
        EventKind::GoalCreated,
        EntityKind::Goal,
        stale_goal.id.uuid(),
        &stale_goal,
    ));
    let user_goal = Goal {
        id: GoalId::from_uuid(id(51)),
        owner_principal_id: user_id,
        participants: vec![user_id],
        title: "User-owned project".to_owned(),
        description: "A user goal is not an initiative source".to_owned(),
        status: GoalStatus::Active,
        created_at: "2026-01-01T00:00:00Z".to_owned(),
    };
    events.push(event(
        user_id,
        EventKind::GoalCreated,
        EntityKind::Goal,
        user_goal.id.uuid(),
        &user_goal,
    ));
    let task = Task {
        id: TaskId::from_uuid(id(52)),
        goal_id: Some(user_goal.id),
        title: "Old unfinished task".to_owned(),
        status: TaskStatus::Blocked,
        created_at: "2020-01-01T00:00:00Z".to_owned(),
    };
    events.push(event(
        user_id,
        EventKind::TaskCreated,
        EntityKind::Task,
        task.id.uuid(),
        &task,
    ));
    let old_run = Run {
        id: RunId::from_uuid(id(53)),
        task_id: Some(task.id),
        status: RunStatus::NeedsAttention,
        started_at: "2020-01-01T00:00:00Z".to_owned(),
        completed_at: None,
    };
    events.push(event(
        hekate_id,
        EventKind::RunStarted,
        EntityKind::Run,
        old_run.id.uuid(),
        &old_run,
    ));

    let state = replay(&events);
    assert!(state.goals.contains_key(&stale_goal.id));
    assert!(state.tasks.contains_key(&task.id));
    assert!(state.runs.contains_key(&old_run.id));
    assert!(select(&state, &events, hekate_id, user_id).is_empty());
}

#[test]
fn open_hekate_commitment_requires_a_verified_origin_event() {
    let (hekate_id, user_id, mut events) = base();
    let source = observation_event(&mut events, user_id, 60, "I asked for a promised report");
    let owned = Commitment {
        id: CommitmentId::from_uuid(id(61)),
        debtor_principal_id: hekate_id,
        creditor_principal_id: user_id,
        promise: "send the report".to_owned(),
        status: CommitmentStatus::Open,
        source_event_id: Some(source),
        created_at: "2026-01-01T00:00:00Z".to_owned(),
    };
    events.push(event(
        hekate_id,
        EventKind::CommitmentCreated,
        EntityKind::Commitment,
        owned.id.uuid(),
        &owned,
    ));
    let user_owned = Commitment {
        id: CommitmentId::from_uuid(id(62)),
        debtor_principal_id: user_id,
        creditor_principal_id: hekate_id,
        promise: "user's task".to_owned(),
        status: CommitmentStatus::Open,
        source_event_id: Some(source),
        created_at: "2026-01-01T00:00:00Z".to_owned(),
    };
    events.push(event(
        user_id,
        EventKind::CommitmentCreated,
        EntityKind::Commitment,
        user_owned.id.uuid(),
        &user_owned,
    ));
    let no_source = Commitment {
        id: CommitmentId::from_uuid(id(63)),
        debtor_principal_id: hekate_id,
        creditor_principal_id: user_id,
        promise: "missing origin".to_owned(),
        status: CommitmentStatus::Open,
        source_event_id: None,
        created_at: "2026-01-01T00:00:00Z".to_owned(),
    };
    events.push(event(
        hekate_id,
        EventKind::CommitmentCreated,
        EntityKind::Commitment,
        no_source.id.uuid(),
        &no_source,
    ));

    let state = replay(&events);
    let candidates = select(&state, &events, hekate_id, user_id);
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].kind, InitiativeKind::FollowUp);
    assert_eq!(candidates[0].source_entity.id, owned.id.uuid());
    assert_eq!(candidates[0].source_event_ids, vec![source]);
    assert_eq!(candidates[0].source_version, 4);
}

#[test]
fn position_review_requires_relevant_observation_after_position() {
    let (hekate_id, user_id, mut events) = base();
    let old_evidence = observation_event(
        &mut events,
        user_id,
        70,
        "workspace retention scope should be reviewed",
    );
    let position = position(
        71,
        hekate_id,
        "workspace retention scope",
        vec![old_evidence],
    );
    add_position(&mut events, hekate_id, &position);
    observation_event(
        &mut events,
        user_id,
        72,
        "The weather forecast is clear tomorrow",
    );

    let before_related_evidence = replay(&events);
    assert!(select(&before_related_evidence, &events, hekate_id, user_id).is_empty());

    let new_evidence = observation_event(
        &mut events,
        user_id,
        73,
        "New evidence addresses workspace retention scope and deletion",
    );
    let state = replay(&events);
    let candidate = select(&state, &events, hekate_id, user_id)
        .into_iter()
        .next()
        .expect("related newer evidence creates a review candidate");
    assert_eq!(candidate.kind, InitiativeKind::Challenge);
    assert_eq!(candidate.source_entity.id, position.id.uuid());
    assert_eq!(candidate.source_version, u64::from(position.version));
    assert_eq!(candidate.source_event_ids, vec![new_evidence]);
    assert_eq!(candidate.as_of_revision, state.revision);
}

#[test]
fn ordering_is_stable_prioritized_capped_and_fingerprint_ignores_revision() {
    let (hekate_id, user_id, mut events) = base();
    let source = observation_event(&mut events, user_id, 80, "workspace retention source");
    let hekate_position = position(81, hekate_id, "position unrelated", vec![source]);
    let user_position = position(82, user_id, "position unrelated", vec![source]);
    add_position(&mut events, hekate_id, &hekate_position);
    add_position(&mut events, user_id, &user_position);

    for conflict_id in 100..106 {
        let value = conflict(
            conflict_id,
            vec![hekate_position.id, user_position.id],
            vec![source],
            "Should the workspace be retained?",
        );
        add_conflict(&mut events, hekate_id, &value);
    }
    let commitment = Commitment {
        id: CommitmentId::from_uuid(id(90)),
        debtor_principal_id: hekate_id,
        creditor_principal_id: user_id,
        promise: "send the retention report".to_owned(),
        status: CommitmentStatus::Open,
        source_event_id: Some(source),
        created_at: "2026-01-01T00:00:00Z".to_owned(),
    };
    events.push(event(
        hekate_id,
        EventKind::CommitmentCreated,
        EntityKind::Commitment,
        commitment.id.uuid(),
        &commitment,
    ));
    let review_position = position(91, hekate_id, "workspace retention", vec![source]);
    add_position(&mut events, hekate_id, &review_position);
    observation_event(
        &mut events,
        user_id,
        92,
        "New workspace retention evidence changes the scope",
    );

    let state = replay(&events);
    let candidates = select(&state, &events, hekate_id, user_id);
    assert_eq!(candidates.len(), 8);
    assert_eq!(
        candidates
            .iter()
            .map(|candidate| candidate.kind)
            .collect::<Vec<_>>(),
        [
            InitiativeKind::Question,
            InitiativeKind::Question,
            InitiativeKind::Question,
            InitiativeKind::Question,
            InitiativeKind::Question,
            InitiativeKind::Question,
            InitiativeKind::FollowUp,
            InitiativeKind::Challenge,
        ]
    );
    assert!(candidates
        .iter()
        .all(|candidate| candidate.content.len() <= 320 && candidate.rationale.len() <= 320));
    let mut reversed_events = events.clone();
    reversed_events.reverse();
    assert_eq!(
        candidates,
        select(&state, &reversed_events, hekate_id, user_id)
    );

    let first_fingerprints = candidates
        .iter()
        .filter(|candidate| candidate.kind == InitiativeKind::Question)
        .map(|candidate| (candidate.source_entity.id, candidate.fingerprint.clone()))
        .collect::<BTreeMap<_, _>>();
    for conflict_id in 106..110 {
        let value = conflict(
            conflict_id,
            vec![hekate_position.id, user_position.id],
            vec![source],
            "Should the workspace be retained?",
        );
        add_conflict(&mut events, hekate_id, &value);
    }
    let expanded_state = replay(&events);
    let expanded = select(&expanded_state, &events, hekate_id, user_id);
    assert_eq!(expanded.len(), MAX_SLEEP_CANDIDATES);
    assert!(expanded
        .iter()
        .all(|candidate| candidate.kind == InitiativeKind::Question));
    assert!(first_fingerprints.iter().all(|(source_id, fingerprint)| {
        expanded.iter().any(|candidate| {
            candidate.source_entity.id == *source_id && candidate.fingerprint == *fingerprint
        })
    }));
}
