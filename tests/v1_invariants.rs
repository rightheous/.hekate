use hekate::core::{
    ActiveMemory, ActiveMemoryStatus, EntityKind, EntityRef, EventKind, EventSource,
    ExperienceEvent, MemoryCandidate, MemoryCandidateId, MemoryCandidateStatus, MemoryId,
    MemoryKind, PrincipalId,
};
use hekate::runtime::Projector;

#[test]
fn replay_keeps_memory_candidate_and_activation_separate() {
    let actor = PrincipalId::new();
    let candidate = MemoryCandidate {
        id: MemoryCandidateId::new(),
        kind: MemoryKind::ExplicitPreference,
        content: "prefers concise status updates".to_owned(),
        subject_principal_id: Some(actor),
        status: MemoryCandidateStatus::Candidate,
        confidence: 100,
        source_event_ids: Vec::new(),
        valid_from: None,
        valid_until: None,
        supersedes: None,
        created_at: hekate::core::model::now(),
    };
    let memory = ActiveMemory {
        id: MemoryId::new(),
        candidate_id: candidate.id,
        kind: candidate.kind.clone(),
        content: candidate.content.clone(),
        subject_principal_id: candidate.subject_principal_id,
        status: ActiveMemoryStatus::Active,
        confidence: candidate.confidence,
        source_event_ids: Vec::new(),
        valid_from: None,
        valid_until: None,
        supersedes: None,
        last_verified_at: None,
        created_at: hekate::core::model::now(),
    };
    let first = match ExperienceEvent::new(
        actor,
        EventKind::MemoryCandidateCreated,
        Some(EntityRef::new(
            EntityKind::MemoryCandidate,
            candidate.id.uuid(),
        )),
        serde_json::to_value(&candidate)
            .ok()
            .unwrap_or(serde_json::Value::Null),
        EventSource::new("test", None),
        None,
        None,
        Some(1.0),
    ) {
        Ok(event) => event,
        Err(error) => {
            assert!(false, "event creation failed: {error}");
            return;
        }
    };
    let second = match ExperienceEvent::new(
        actor,
        EventKind::MemoryPromoted,
        Some(EntityRef::new(EntityKind::Memory, memory.id.uuid())),
        serde_json::to_value(&memory)
            .ok()
            .unwrap_or(serde_json::Value::Null),
        EventSource::new("test", None),
        Some(first.event_id),
        None,
        Some(1.0),
    ) {
        Ok(event) => event,
        Err(error) => {
            assert!(false, "event creation failed: {error}");
            return;
        }
    };
    let state = match Projector::replay(&[first, second]) {
        Ok(state) => state,
        Err(error) => {
            assert!(false, "replay failed: {error}");
            return;
        }
    };
    assert_eq!(state.active_memories.len(), 1);
    assert_eq!(
        state.memory_candidates[&candidate.id].status,
        MemoryCandidateStatus::Promoted
    );
}
