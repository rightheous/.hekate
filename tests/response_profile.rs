use hekate::bootstrap::build_response_profile;
use hekate::core::{
    ActiveMemory, ActiveMemoryStatus, EntityKind, EntityRef, EventId, EventKind, EventSource,
    ExperienceEvent, MemoryCandidate, MemoryCandidateId, MemoryCandidateStatus, MemoryId,
    MemoryKind, PrincipalId, ResponsePreferenceKey, ResponseVerbosity,
};
use hekate::runtime::response_profile::resolve_response_profile_with_report;
use hekate::runtime::Projector;

fn event(
    actor: PrincipalId,
    kind: EventKind,
    subject: Option<EntityRef>,
    payload: serde_json::Value,
) -> ExperienceEvent {
    ExperienceEvent::new(
        actor,
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

fn preference_events(
    principal_id: PrincipalId,
    kind: MemoryKind,
    content: &str,
) -> (Vec<ExperienceEvent>, ActiveMemory, EventId) {
    let source = event(
        principal_id,
        EventKind::StateChanged,
        None,
        serde_json::Value::Null,
    );
    let candidate = MemoryCandidate {
        id: MemoryCandidateId::new(),
        kind: kind.clone(),
        content: content.to_owned(),
        subject_principal_id: Some(principal_id),
        status: MemoryCandidateStatus::Candidate,
        confidence: 100,
        source_event_ids: vec![source.event_id],
        valid_from: None,
        valid_until: None,
        supersedes: None,
        created_at: "2026-01-01T00:00:00Z".to_owned(),
    };
    let memory = ActiveMemory {
        id: MemoryId::new(),
        candidate_id: candidate.id,
        kind,
        content: candidate.content.clone(),
        subject_principal_id: candidate.subject_principal_id,
        status: ActiveMemoryStatus::Active,
        confidence: candidate.confidence,
        source_event_ids: candidate.source_event_ids.clone(),
        valid_from: None,
        valid_until: None,
        supersedes: None,
        last_verified_at: None,
        created_at: "2026-01-01T00:00:00Z".to_owned(),
    };
    let candidate_event = event(
        principal_id,
        EventKind::MemoryCandidateCreated,
        Some(EntityRef::new(
            EntityKind::MemoryCandidate,
            candidate.id.uuid(),
        )),
        serde_json::to_value(&candidate).expect("candidate payload"),
    );
    let promotion_event = event(
        principal_id,
        EventKind::MemoryPromoted,
        Some(EntityRef::new(EntityKind::Memory, memory.id.uuid())),
        serde_json::to_value(&memory).expect("memory payload"),
    );
    (
        vec![source, candidate_event, promotion_event],
        memory,
        source_id(&candidate),
    )
}

fn source_id(candidate: &MemoryCandidate) -> EventId {
    candidate.source_event_ids[0]
}

#[test]
fn precedence_supersedes_provenance_and_replay_are_deterministic() {
    let principal_id = PrincipalId::new();
    let other_principal_id = PrincipalId::new();
    let mut events = Vec::new();

    let (mut old_language_events, old_language, old_language_source) = preference_events(
        principal_id,
        MemoryKind::ExplicitPreference,
        r#"{"schema":"hekate.response_preference.v1","key":"language","value":"en-US"}"#,
    );
    events.append(&mut old_language_events);
    let mut superseded_language = old_language.clone();
    superseded_language.status = ActiveMemoryStatus::Superseded;
    events.push(event(
        principal_id,
        EventKind::MemorySuperseded,
        Some(EntityRef::new(
            EntityKind::Memory,
            superseded_language.id.uuid(),
        )),
        serde_json::to_value(&superseded_language).expect("superseded payload"),
    ));

    let (mut new_language_events, new_language, new_language_source) = preference_events(
        principal_id,
        MemoryKind::ExplicitPreference,
        r#"{"schema":"hekate.response_preference.v1","key":"language","value":"ko-KR"}"#,
    );
    events.append(&mut new_language_events);
    let (mut old_verbosity_events, _, _) = preference_events(
        principal_id,
        MemoryKind::ExplicitPreference,
        r#"{"schema":"hekate.response_preference.v1","key":"verbosity","value":"compact"}"#,
    );
    events.append(&mut old_verbosity_events);
    let (mut new_verbosity_events, new_verbosity, new_verbosity_source) = preference_events(
        principal_id,
        MemoryKind::ExplicitPreference,
        r#"{"schema":"hekate.response_preference.v1","key":"verbosity","value":"detailed"}"#,
    );
    events.append(&mut new_verbosity_events);
    let (mut inferred_events, _, _) = preference_events(
        principal_id,
        MemoryKind::InferredPreference,
        r#"{"schema":"hekate.response_preference.v1","key":"step_size","value":"small"}"#,
    );
    events.append(&mut inferred_events);
    let (mut other_events, _, _) = preference_events(
        other_principal_id,
        MemoryKind::ExplicitPreference,
        r#"{"schema":"hekate.response_preference.v1","key":"language","value":"fr-FR"}"#,
    );
    events.append(&mut other_events);

    let state = Projector::replay(&events).expect("replay");
    let first = resolve_response_profile_with_report(
        &state,
        &events,
        principal_id,
        None,
        None,
        state.revision,
    );
    let replayed_state = Projector::replay(&events).expect("replay again");
    let second = resolve_response_profile_with_report(
        &replayed_state,
        &events,
        principal_id,
        None,
        None,
        replayed_state.revision,
    );

    assert_eq!(first, second);
    assert_eq!(first.profile.language.as_deref(), Some("ko-KR"));
    assert_eq!(first.profile.verbosity, ResponseVerbosity::Detailed);
    assert_eq!(first.report.applied_preferences, 2);
    assert_eq!(first.report.ignored_non_explicit, 1);
    assert_eq!(first.report.ignored_scope_mismatch, 1);
    assert_eq!(first.report.conflicting_keys, 1);
    assert_eq!(first.profile.evidence.len(), 2);
    let language_evidence = first
        .profile
        .evidence
        .iter()
        .find(|evidence| evidence.key == ResponsePreferenceKey::Language)
        .expect("language evidence");
    assert_eq!(language_evidence.memory_id, new_language.id);
    assert_eq!(language_evidence.source_event_id, new_language_source);
    assert_ne!(language_evidence.source_event_id, old_language_source);
    let verbosity_evidence = first
        .profile
        .evidence
        .iter()
        .find(|evidence| evidence.key == ResponsePreferenceKey::Verbosity)
        .expect("verbosity evidence");
    assert_eq!(verbosity_evidence.memory_id, new_verbosity.id);
    assert_eq!(verbosity_evidence.source_event_id, new_verbosity_source);
}

#[tokio::test]
async fn malformed_preferences_are_ignored_and_empty_bootstrap_is_default() {
    let principal_id = PrincipalId::new();
    let contents = [
        "{not json",
        r#"{"schema":"hekate.response_preference.v2","key":"language","value":"ko"}"#,
        r#"{"schema":"hekate.response_preference.v1","key":"unknown","value":"x"}"#,
        r#"{"schema":"hekate.response_preference.v1","key":"verbosity","value":"verbose"}"#,
        "I prefer concise responses",
    ];
    let mut events = Vec::new();
    for content in contents {
        let (mut memory_events, _, _) =
            preference_events(principal_id, MemoryKind::ExplicitPreference, content);
        events.append(&mut memory_events);
    }
    let state = Projector::replay(&events).expect("replay");
    let resolution = resolve_response_profile_with_report(
        &state,
        &events,
        principal_id,
        None,
        None,
        state.revision,
    );

    assert_eq!(resolution.report.considered_memories, contents.len());
    assert_eq!(resolution.report.ignored_malformed, contents.len());
    assert_eq!(resolution.report.applied_preferences, 0);
    assert_eq!(resolution.profile.language, None);
    assert_eq!(resolution.profile.verbosity, ResponseVerbosity::Balanced);
    assert!(resolution.profile.evidence.is_empty());
    assert_eq!(resolution.profile.profile_hash.len(), 64);

    let config = hekate::config::Config {
        database_url: "sqlite::memory:".to_owned(),
        ..hekate::config::Config::default()
    };
    let bootstrapped = build_response_profile(&config).await.expect("bootstrap");
    assert_eq!(bootstrapped.profile.language, None);
    assert_eq!(bootstrapped.profile.verbosity, ResponseVerbosity::Balanced);
    assert!(bootstrapped.profile.evidence.is_empty());
}
