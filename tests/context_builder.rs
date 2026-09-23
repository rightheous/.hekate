use hekate::core::ResponseProfile;
use hekate::core::{
    ActiveMemory, ActiveMemoryStatus, ContextBudget, ContextBuildRequest, ContextItemKind,
    CurrentState, EntityKind, EntityRef, EventKind, EventSource, ExperienceEvent, IdentityVersion,
    IdentityVersionId, MemoryCandidateId, MemoryId, MemoryKind, Observation, ObservationId,
    Principal, PrincipalId, PrincipalKind, RecallBundle, RecalledItem,
};
use hekate::runtime::context_builder::{
    build_context_snapshot, build_context_snapshot_with_profile, ContextBuilderError,
};

fn event(
    actor_id: PrincipalId,
    kind: EventKind,
    subject: Option<EntityRef>,
    payload: serde_json::Value,
) -> ExperienceEvent {
    ExperienceEvent::new(
        actor_id,
        kind,
        subject,
        payload,
        EventSource::new("test", None),
        None,
        None,
        Some(1.0),
    )
    .expect("event")
}

fn observation(actor_id: PrincipalId, content: &str) -> (Observation, ExperienceEvent) {
    let observation = Observation {
        id: ObservationId::new(),
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
        Some(EntityRef::new(
            EntityKind::Observation,
            observation.id.uuid(),
        )),
        serde_json::to_value(&observation).expect("observation payload"),
    );
    (observation, event)
}

fn request(
    principal_id: PrincipalId,
    as_of_revision: u64,
    recalled: RecallBundle,
    budget: ContextBudget,
) -> ContextBuildRequest {
    ContextBuildRequest {
        principal_id,
        relationship_id: None,
        task_id: None,
        run_id: None,
        as_of_revision,
        recalled,
        budget,
    }
}

fn identity_state(principal_id: PrincipalId) -> (CurrentState, IdentityVersion, ExperienceEvent) {
    let identity = IdentityVersion {
        id: IdentityVersionId::new(),
        principal_id,
        version: 1,
        name: "Hekate".to_owned(),
        values: vec!["care".to_owned(), "truth".to_owned()],
        boundaries: vec!["do not erase evidence".to_owned()],
        created_at: "2026-01-01T00:00:00Z".to_owned(),
        supersedes: None,
    };
    let mut state = CurrentState::default();
    state.principals.insert(
        principal_id,
        Principal {
            id: principal_id,
            kind: PrincipalKind::Hekate,
            name: "Hekate".to_owned(),
            identity_version_id: Some(identity.id),
        },
    );
    state
        .identity_versions
        .insert(identity.id, identity.clone());
    let identity_event = event(
        principal_id,
        EventKind::IdentityVersionCreated,
        Some(EntityRef::new(
            EntityKind::IdentityVersion,
            identity.id.uuid(),
        )),
        serde_json::to_value(&identity).expect("identity payload"),
    );
    (state, identity, identity_event)
}

#[test]
fn selection_is_deterministic_and_drops_whole_low_priority_items() {
    let principal_id = PrincipalId::new();
    let (mut state, _identity, identity_event) = identity_state(principal_id);
    let source_event = event(
        principal_id,
        EventKind::StateChanged,
        None,
        serde_json::json!({"source": true}),
    );
    let (old_observation, old_observation_event) = observation(principal_id, "old source");
    let (_, recent_observation_event) = observation(principal_id, "current turn");
    let memory_one = ActiveMemory {
        id: MemoryId::new(),
        candidate_id: MemoryCandidateId::new(),
        kind: MemoryKind::VerifiedFact,
        content: "middle-one ".repeat(120),
        subject_principal_id: None,
        status: ActiveMemoryStatus::Active,
        confidence: 90,
        source_event_ids: vec![source_event.event_id],
        valid_from: None,
        valid_until: None,
        supersedes: None,
        last_verified_at: None,
        created_at: "2026-01-01T00:00:00Z".to_owned(),
    };
    let mut memory_two = memory_one.clone();
    memory_two.id = MemoryId::new();
    memory_two.candidate_id = MemoryCandidateId::new();
    memory_two.content = "middle-two ".repeat(120);
    state
        .active_memories
        .insert(memory_one.id, memory_one.clone());
    state.active_memories.insert(memory_two.id, memory_two);

    let mut events = vec![identity_event, source_event, old_observation_event];
    for _ in 0..22 {
        events.push(event(
            principal_id,
            EventKind::StateChanged,
            None,
            serde_json::json!({"filler": true}),
        ));
    }
    events.push(recent_observation_event);
    state.revision = events.len() as u64;
    state.applied_events = events.iter().map(|item| item.event_id).collect();

    let recalled = RecallBundle {
        query_hash: "query".to_owned(),
        embedding_space_id: "test".to_owned(),
        items: vec![RecalledItem {
            entity: EntityRef::new(EntityKind::Observation, old_observation.id.uuid()),
            source_event_id: events[2].event_id,
            text: "historical recall ".repeat(800),
            score: 0.9,
            created_at: "2026-01-01T00:00:00Z".to_owned(),
        }],
    };
    let budget = ContextBudget {
        anchor_bytes: 8 * 1024,
        middle_bytes: 1_700,
        active_bytes: 2_000,
        hard_limit_bytes: 20 * 1024,
    };
    let request = request(principal_id, state.revision, recalled, budget.clone());
    let first = build_context_snapshot(&state, &events, &request).expect("snapshot");
    let second = build_context_snapshot(&state, &events, &request).expect("snapshot");

    assert_eq!(first, second);
    assert_eq!(first.snapshot_hash, second.snapshot_hash);
    assert!(first
        .anchors
        .iter()
        .any(|item| item.kind == ContextItemKind::Identity));
    assert!(first
        .anchors
        .iter()
        .any(|item| item.kind == ContextItemKind::Policy));
    assert!(first
        .compressed_middle
        .iter()
        .any(|item| { item.text == memory_one.content || item.text == "middle-two ".repeat(120) }));
    assert!(first
        .active_recent
        .iter()
        .any(|item| item.text == "current turn"));
    assert!(first
        .active_recent
        .iter()
        .all(|item| !item.text.contains("historical recall")));
    assert!(first.budget_report.dropped_middle_items > 0);
    assert!(first.budget_report.dropped_active_items > 0);
    assert!(first
        .compressed_middle
        .iter()
        .all(|item| item.text.len() == item.text.as_bytes().len()));
    assert!(first.budget_report.total_bytes <= budget.hard_limit_bytes);
    assert!(
        serde_json::to_vec(&first)
            .expect("serialized snapshot")
            .len()
            <= budget.hard_limit_bytes
    );
    assert!(first
        .source_refs
        .iter()
        .any(|source| source.event_id == events[1].event_id && !source.source_hash.is_empty()));
}

#[test]
fn invalid_revision_provenance_and_anchor_budget_fail_closed_without_mutation() {
    let principal_id = PrincipalId::new();
    let (mut state, identity, identity_event) = identity_state(principal_id);
    state.revision = 1;
    state.applied_events = vec![identity_event.event_id];
    let before_state = state.clone();
    let before_event = identity_event.clone();
    let oversized_request = request(
        principal_id,
        1,
        RecallBundle::default(),
        ContextBudget {
            anchor_bytes: 1,
            ..ContextBudget::default()
        },
    );
    assert!(matches!(
        build_context_snapshot(&state, &[identity_event.clone()], &oversized_request),
        Err(ContextBuilderError::AnchorBudgetExceeded { .. })
    ));
    assert_eq!(state, before_state);
    assert_eq!(identity_event, before_event);

    let projection_request = request(
        principal_id,
        0,
        RecallBundle::default(),
        ContextBudget::default(),
    );
    assert!(matches!(
        build_context_snapshot(&state, &[identity_event.clone()], &projection_request),
        Err(ContextBuilderError::FutureProjection { .. })
    ));

    let mut future_profile = ResponseProfile::default();
    future_profile.as_of_revision = 2;
    let profile_request = request(
        principal_id,
        1,
        RecallBundle::default(),
        ContextBudget::default(),
    );
    assert!(matches!(
        build_context_snapshot_with_profile(
            &state,
            &[identity_event.clone()],
            &profile_request,
            &future_profile,
        ),
        Err(ContextBuilderError::FutureResponseProfile { .. })
    ));

    let future_event = event(
        principal_id,
        EventKind::StateChanged,
        None,
        serde_json::json!({}),
    );
    let future_request = request(
        principal_id,
        1,
        RecallBundle {
            query_hash: String::new(),
            embedding_space_id: String::new(),
            items: vec![RecalledItem {
                entity: EntityRef::new(EntityKind::IdentityVersion, identity.id.uuid()),
                source_event_id: future_event.event_id,
                text: "future".to_owned(),
                score: 1.0,
                created_at: String::new(),
            }],
        },
        ContextBudget::default(),
    );
    assert!(matches!(
        build_context_snapshot(
            &state,
            &[identity_event.clone(), future_event],
            &future_request
        ),
        Err(ContextBuilderError::FutureRecallSource { .. })
    ));

    let mut corrupted = identity_event.clone();
    corrupted.payload = serde_json::json!({"changed": true});
    let hash_request = request(
        principal_id,
        1,
        RecallBundle {
            query_hash: String::new(),
            embedding_space_id: String::new(),
            items: vec![RecalledItem {
                entity: EntityRef::new(EntityKind::IdentityVersion, identity.id.uuid()),
                source_event_id: corrupted.event_id,
                text: "corrupted".to_owned(),
                score: 1.0,
                created_at: String::new(),
            }],
        },
        ContextBudget::default(),
    );
    assert!(matches!(
        build_context_snapshot(&state, &[corrupted], &hash_request),
        Err(ContextBuilderError::SourceHashMismatch { .. })
    ));
    assert_eq!(state, before_state);
}
