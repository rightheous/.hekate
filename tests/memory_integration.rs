use std::sync::Arc;

use async_trait::async_trait;
use hekate::adapters::local_policy::LocalPolicy;
use hekate::adapters::sqlite::{SqliteEmbeddingStore, SqliteStore};
use hekate::config::Config;
use hekate::core::{
    now, CognitiveTrace, EmbeddingDocument, EmbeddingSpace, EntityKind, EntityRef, EventKind,
    EventSource, EvidenceRef, ExperienceEvent, IntegrationCandidate, IntegrationCandidateId,
    IntegrationCandidateKind, Observation, ObservationId, PositionStatus, Principal, PrincipalKind,
    SleepRun, SleepRunId, SleepRunStatus, VerificationDisposition,
};
use hekate::ports::{
    CapabilityCatalog, CapabilityError, CapabilityResult, CognitiveError, CognitiveModel,
    EmbeddingProvider, EmbeddingProviderError,
};
use hekate::runtime::embedding_indexer::EmbeddingIndexer;
use hekate::runtime::memory_integration::IntegrationError;
use hekate::runtime::recall::SemanticRecall;
use hekate::runtime::{Engine, EngineError, Projector};
use uuid::Uuid;

struct NoCapabilities;

#[async_trait]
impl CapabilityCatalog for NoCapabilities {
    fn names(&self) -> Vec<String> {
        Vec::new()
    }

    async fn execute(
        &self,
        _: &str,
        _: serde_json::Value,
    ) -> Result<CapabilityResult, CapabilityError> {
        Err(CapabilityError::Execution("unused".to_owned()))
    }
}

struct NoModel;

#[async_trait]
impl CognitiveModel for NoModel {
    async fn think(
        &self,
        _: &hekate::core::ThoughtContext,
    ) -> Result<hekate::core::ThoughtCycle, CognitiveError> {
        Err(CognitiveError::Provider {
            message: "unused".to_owned(),
            trace: trace(),
        })
    }
}

struct FailingEmbeddingProvider {
    space: EmbeddingSpace,
}

#[async_trait]
impl EmbeddingProvider for FailingEmbeddingProvider {
    fn space(&self) -> &EmbeddingSpace {
        &self.space
    }

    async fn embed_query(
        &self,
        _: &str,
    ) -> Result<hekate::core::EmbeddingVector, EmbeddingProviderError> {
        Err(EmbeddingProviderError::Request)
    }

    async fn embed_documents(
        &self,
        _: &[EmbeddingDocument],
    ) -> Result<Vec<hekate::core::EmbeddingVector>, EmbeddingProviderError> {
        Err(EmbeddingProviderError::Request)
    }
}

fn trace() -> CognitiveTrace {
    CognitiveTrace {
        trace_id: Uuid::new_v4().to_string(),
        outcome: "failed".to_owned(),
        provider: "test".to_owned(),
        model: "test".to_owned(),
        schema_version: "test".to_owned(),
        context_sequence: 0,
        context_hash: String::new(),
        referenced_event_ids: Vec::new(),
        draft: None,
        review: None,
        commitment: None,
        parse_errors: Vec::new(),
        retries: 0,
        elapsed_ms: 0,
        raw_response_hash: None,
        error_kind: None,
        created_at: now(),
    }
}

fn database_url(label: &str) -> String {
    format!(
        "sqlite://{}",
        std::env::temp_dir()
            .join(format!(
                "hekate-memory-integration-{label}-{}.db",
                Uuid::new_v4()
            ))
            .display()
    )
}

fn event<T: serde::Serialize>(
    actor_id: hekate::core::PrincipalId,
    kind: EventKind,
    entity_kind: EntityKind,
    entity_id: Uuid,
    payload: &T,
) -> ExperienceEvent {
    ExperienceEvent::new(
        actor_id,
        kind,
        Some(EntityRef::new(entity_kind, entity_id)),
        serde_json::to_value(payload).expect("payload"),
        EventSource::new("test", None),
        None,
        None,
        Some(1.0),
    )
    .expect("event")
}

async fn fixture() -> Result<
    (
        Arc<SqliteStore>,
        IntegrationCandidateId,
        IntegrationCandidateId,
        hekate::core::EventId,
    ),
    Box<dyn std::error::Error>,
> {
    let store = Arc::new(SqliteStore::open(&database_url("fixture")).await?);
    let config = Config::default();
    let projector = Projector::new(store.clone());
    let hekate = Principal {
        id: config.hekate_principal_id,
        kind: PrincipalKind::Hekate,
        name: "HEKATE".to_owned(),
        identity_version_id: None,
    };
    let user = Principal {
        id: config.user_principal_id,
        kind: PrincipalKind::User,
        name: "User".to_owned(),
        identity_version_id: None,
    };
    projector
        .record(event(
            hekate.id,
            EventKind::PrincipalCreated,
            EntityKind::Principal,
            hekate.id.uuid(),
            &hekate,
        ))
        .await?;
    projector
        .record(event(
            user.id,
            EventKind::PrincipalCreated,
            EntityKind::Principal,
            user.id.uuid(),
            &user,
        ))
        .await?;
    let observation = Observation {
        id: ObservationId::new(),
        actor_id: user.id,
        content: "a durable memory source".to_owned(),
        source_type: "test".to_owned(),
        source_ref: None,
        thread_id: None,
        message_id: None,
        received_at: now(),
    };
    let observation_event = event(
        user.id,
        EventKind::ObservationRecorded,
        EntityKind::Observation,
        observation.id.uuid(),
        &observation,
    );
    let high_water = projector.record(observation_event.clone()).await?.revision;
    let run = SleepRun {
        id: SleepRunId::new(),
        status: SleepRunStatus::Running,
        high_water_revision: high_water,
        cursor_before: 0,
        cursor_after: None,
        seed_event_ids: vec![observation_event.event_id],
        processed_observation_count: 1,
        created_candidate_count: 0,
        started_at: now(),
        finished_at: None,
        error_kind: None,
        context_budget_report: None,
    };
    projector
        .record(event(
            hekate.id,
            EventKind::SleepRunStarted,
            EntityKind::SleepRun,
            run.id.uuid(),
            &run,
        ))
        .await?;

    let make_candidate = |kind: IntegrationCandidateKind, content: &str| {
        let source_event_ids = vec![observation_event.event_id];
        IntegrationCandidate {
            id: IntegrationCandidateId::new(),
            sleep_run_id: run.id,
            kind: kind.clone(),
            disposition: VerificationDisposition::NeedsValidation,
            as_of_revision: high_water,
            content: content.to_owned(),
            rationale: "the supplied source supports this candidate".to_owned(),
            source_event_ids: source_event_ids.clone(),
            counterevidence_event_ids: Vec::new(),
            confidence: 100,
            fingerprint: hekate::core::integration_candidate_fingerprint(
                &kind,
                content,
                &source_event_ids,
                &[],
            ),
            created_at: now(),
        }
    };
    let memory_candidate = make_candidate(IntegrationCandidateKind::Memory, "durable memory");
    let position_candidate = make_candidate(
        IntegrationCandidateKind::Position,
        r#"{"schema":"hekate.position_integration.v1","operation":{"action":"establish","subject":"durable position","stance":"support","reasons":["supported by source"],"reconsideration_conditions":["new contrary evidence"]}}"#,
    );
    for candidate in [&memory_candidate, &position_candidate] {
        projector
            .record(event(
                hekate.id,
                EventKind::IntegrationCandidateCreated,
                EntityKind::IntegrationCandidate,
                candidate.id.uuid(),
                candidate,
            ))
            .await?;
    }
    let mut completed = run.clone();
    completed.status = SleepRunStatus::Completed;
    completed.cursor_after = Some(high_water);
    completed.created_candidate_count = 2;
    completed.finished_at = Some(now());
    projector
        .record(event(
            hekate.id,
            EventKind::SleepRunCompleted,
            EntityKind::SleepRun,
            run.id.uuid(),
            &completed,
        ))
        .await?;
    Ok((
        store,
        memory_candidate.id,
        position_candidate.id,
        observation_event.event_id,
    ))
}

async fn position_candidate(
    store: &SqliteStore,
    actor_id: hekate::core::PrincipalId,
    source_event_id: hekate::core::EventId,
    content: String,
) -> Result<IntegrationCandidateId, Box<dyn std::error::Error>> {
    let projector = Projector::new(Arc::new(store.clone()));
    let state = store.state().await?;
    let high_water_revision = state.revision;
    let run = SleepRun {
        id: SleepRunId::new(),
        status: SleepRunStatus::Running,
        high_water_revision,
        cursor_before: state.sleep_cursor,
        cursor_after: None,
        seed_event_ids: vec![source_event_id],
        processed_observation_count: 1,
        created_candidate_count: 0,
        started_at: now(),
        finished_at: None,
        error_kind: None,
        context_budget_report: None,
    };
    projector
        .record(event(
            actor_id,
            EventKind::SleepRunStarted,
            EntityKind::SleepRun,
            run.id.uuid(),
            &run,
        ))
        .await?;
    let kind = IntegrationCandidateKind::Position;
    let source_event_ids = vec![source_event_id];
    let candidate = IntegrationCandidate {
        id: IntegrationCandidateId::new(),
        sleep_run_id: run.id,
        kind: kind.clone(),
        disposition: VerificationDisposition::NeedsValidation,
        as_of_revision: high_water_revision,
        fingerprint: hekate::core::integration_candidate_fingerprint(
            &kind,
            &content,
            &source_event_ids,
            &[],
        ),
        content,
        rationale: "source supports this revision".to_owned(),
        source_event_ids,
        counterevidence_event_ids: Vec::new(),
        confidence: 100,
        created_at: now(),
    };
    projector
        .record(event(
            actor_id,
            EventKind::IntegrationCandidateCreated,
            EntityKind::IntegrationCandidate,
            candidate.id.uuid(),
            &candidate,
        ))
        .await?;
    let mut completed = run;
    completed.status = SleepRunStatus::Completed;
    completed.cursor_after = Some(high_water_revision);
    completed.created_candidate_count = 1;
    completed.finished_at = Some(now());
    projector
        .record(event(
            actor_id,
            EventKind::SleepRunCompleted,
            EntityKind::SleepRun,
            completed.id.uuid(),
            &completed,
        ))
        .await?;
    Ok(candidate.id)
}

fn engine(store: Arc<SqliteStore>, recall: Option<Arc<SemanticRecall>>) -> Engine {
    let config = Config::default();
    let engine = Engine::new(
        store,
        Arc::new(NoModel),
        Arc::new(LocalPolicy),
        Arc::new(NoCapabilities),
        config.hekate_principal_id,
        config.user_principal_id,
    );
    match recall {
        Some(recall) => engine.with_semantic_recall(recall),
        None => engine,
    }
}

#[tokio::test]
async fn verifies_materializes_replays_and_survives_embedding_failure(
) -> Result<(), Box<dyn std::error::Error>> {
    let (store, candidate_id, _, _) = fixture().await?;
    let config = Config::default();
    let before = store.state().await?;
    assert_eq!(before.integration_candidates[&candidate_id].confidence, 100);
    assert_eq!(
        before.integration_candidates[&candidate_id].disposition,
        VerificationDisposition::NeedsValidation
    );

    let space = EmbeddingSpace::new("test", "failing", "v1", 2, true, "", "");
    let embedding_store = Arc::new(SqliteEmbeddingStore::open(&database_url("embedding")).await?);
    let indexer = Arc::new(EmbeddingIndexer::new(
        Arc::new(FailingEmbeddingProvider { space }),
        embedding_store,
        4,
    ));
    let engine = engine(store.clone(), Some(Arc::new(SemanticRecall::new(indexer))));
    let verified = engine
        .verify_integration_candidate(candidate_id, config.user_principal_id, "human checked")
        .await?;
    assert_eq!(verified.disposition, VerificationDisposition::Verified);
    let result = engine.integrate_memory(candidate_id).await?;
    assert_eq!(result.memory.kind, hekate::core::MemoryKind::Lesson);
    assert_eq!(
        result.memory_candidate.status,
        hekate::core::MemoryCandidateStatus::Promoted
    );
    assert_eq!(result.materialization.memory_id, result.memory.id);
    let state = store.state().await?;
    assert_eq!(state.active_memories.len(), 1);
    assert_eq!(state.memory_candidates.len(), 1);
    assert_eq!(
        state.integration_materializations[&candidate_id].memory_id,
        result.memory.id
    );
    assert_eq!(
        state.integration_verifications[&candidate_id].evidence_refs,
        state.integration_materializations[&candidate_id].evidence_refs
    );
    assert!(matches!(
        engine.integrate_memory(candidate_id).await,
        Err(EngineError::Integration(IntegrationError::AlreadyMaterialized(id))) if id == candidate_id
    ));
    let events = store.events().await?;
    assert_eq!(Projector::replay(&events)?, state);
    assert!(
        hekate::runtime::recovery::recover(store.as_ref())
            .await?
            .projection_verified
    );
    engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn rejects_bad_evidence_and_never_materializes_failed_transitions(
) -> Result<(), Box<dyn std::error::Error>> {
    let (store, memory_id, position_id, source_event_id) = fixture().await?;
    let config = Config::default();
    let engine = engine(store.clone(), None);
    let state = store.state().await?;
    let source_hash = store
        .events()
        .await?
        .into_iter()
        .find(|event| event.event_id == source_event_id)
        .expect("source event")
        .integrity_hash;
    let candidate = state.integration_candidates[&memory_id].clone();
    let outside = EvidenceRef {
        event_id: hekate::core::EventId::new(),
        artifact_id: None,
        source_hash: source_hash.clone(),
        as_of_sequence: candidate.as_of_revision,
    };
    assert!(matches!(
        engine
            .verify_integration_candidate_with_evidence(
                memory_id,
                config.user_principal_id,
                "human checked",
                vec![outside],
            )
            .await,
        Err(EngineError::Integration(
            IntegrationError::EvidenceOutsideCandidate(_)
        ))
    ));
    let future = EvidenceRef {
        event_id: source_event_id,
        artifact_id: None,
        source_hash,
        as_of_sequence: candidate.as_of_revision + 1,
    };
    assert!(matches!(
        engine
            .verify_integration_candidate_with_evidence(
                memory_id,
                config.user_principal_id,
                "human checked",
                vec![future],
            )
            .await,
        Err(EngineError::Integration(
            IntegrationError::FutureEvidenceSequence { .. }
        ))
    ));
    assert!(matches!(
        engine
            .verify_integration_candidate(memory_id, config.user_principal_id, "  ")
            .await,
        Err(EngineError::Integration(IntegrationError::InvalidReason))
    ));
    let rejected = engine
        .reject_integration_candidate(memory_id, config.user_principal_id, "not supported")
        .await?;
    assert_eq!(rejected.disposition, VerificationDisposition::Rejected);
    assert!(matches!(
        engine.integrate_memory(memory_id).await,
        Err(EngineError::Integration(IntegrationError::CandidateNotVerified(id))) if id == memory_id
    ));

    let verified_position = engine
        .verify_integration_candidate(position_id, config.user_principal_id, "human checked")
        .await?;
    assert_eq!(
        verified_position.disposition,
        VerificationDisposition::Verified
    );
    assert!(matches!(
        engine.integrate_memory(position_id).await,
        Err(EngineError::Integration(
            IntegrationError::UnsupportedCandidateKind(IntegrationCandidateKind::Position)
        ))
    ));
    let state = store.state().await?;
    assert!(state.memory_candidates.is_empty());
    assert!(state.active_memories.is_empty());
    assert_eq!(Projector::replay(&store.events().await?)?, state);
    engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn position_integration_establishes_revises_withdraws_and_replays(
) -> Result<(), Box<dyn std::error::Error>> {
    let (store, _, candidate_id, source_event_id) = fixture().await?;
    let config = Config::default();
    let engine = engine(store.clone(), None);

    assert!(matches!(
        engine.integrate_position(candidate_id).await,
        Err(EngineError::Integration(
            IntegrationError::CandidateNotVerified(id)
        )) if id == candidate_id
    ));
    let candidate = store.state().await?.integration_candidates[&candidate_id].clone();
    let source_hash = store
        .events()
        .await?
        .into_iter()
        .find(|event| event.event_id == source_event_id)
        .expect("Position source event")
        .integrity_hash;
    assert!(matches!(
        engine
            .verify_integration_candidate_with_evidence(
                candidate_id,
                config.user_principal_id,
                "human checked",
                vec![EvidenceRef {
                    event_id: hekate::core::EventId::new(),
                    artifact_id: None,
                    source_hash,
                    as_of_sequence: candidate.as_of_revision,
                }],
            )
            .await,
        Err(EngineError::Integration(
            IntegrationError::EvidenceOutsideCandidate(_)
        ))
    ));
    engine
        .verify_integration_candidate(candidate_id, config.user_principal_id, "human checked")
        .await?;
    let established = engine.integrate_position(candidate_id).await?;
    assert_eq!(
        established.position.principal_id,
        config.hekate_principal_id
    );
    assert_eq!(established.position.version, 1);
    assert_eq!(established.position.status, PositionStatus::Active);
    assert_eq!(
        established.materialization.source_event_ids,
        vec![source_event_id]
    );
    assert!(matches!(
        engine.integrate_position(candidate_id).await,
        Err(EngineError::Integration(
            IntegrationError::PositionAlreadyMaterialized(id)
        )) if id == candidate_id
    ));

    let revision_candidate = |stance: &str| {
        serde_json::json!({
            "schema": "hekate.position_integration.v1",
            "operation": {
                "action": "revise",
                "position_id": established.position.id.to_string(),
                "expected_version": 1,
                "stance": stance,
                "reasons": ["new evidence changed the assessment"],
                "reconsideration_conditions": ["further source confirmation"]
            }
        })
        .to_string()
    };
    let revised_id = position_candidate(
        &store,
        config.hekate_principal_id,
        source_event_id,
        revision_candidate("oppose"),
    )
    .await?;
    let competing_revised_id = position_candidate(
        &store,
        config.hekate_principal_id,
        source_event_id,
        revision_candidate("uncertain"),
    )
    .await?;
    engine
        .verify_integration_candidate(revised_id, config.user_principal_id, "human checked")
        .await?;
    engine
        .verify_integration_candidate(
            competing_revised_id,
            config.user_principal_id,
            "human checked",
        )
        .await?;
    let revised = engine.integrate_position(revised_id).await?;
    assert!(matches!(
        engine.integrate_position(competing_revised_id).await,
        Err(EngineError::Integration(IntegrationError::PositionChanged(id)))
            if id == established.position.id
    ));
    assert_eq!(revised.position.version, 2);
    assert_eq!(revised.position.supersedes, Some(established.position.id));
    assert_eq!(
        store.state().await?.positions[&established.position.id].status,
        PositionStatus::Superseded
    );

    let unsupported_withdraw_id = position_candidate(
        &store,
        config.hekate_principal_id,
        source_event_id,
        serde_json::json!({
            "schema": "hekate.position_integration.v1",
            "operation": {
                "action": "withdraw",
                "position_id": revised.position.id.to_string(),
                "expected_version": 2,
                "reason": "source was withdrawn"
            }
        })
        .to_string(),
    )
    .await?;
    assert!(matches!(
        engine
            .verify_integration_candidate(
                unsupported_withdraw_id,
                config.user_principal_id,
                "human checked"
            )
            .await,
        Err(EngineError::Integration(
            IntegrationError::InvalidPositionTransition
        ))
    ));

    let second_candidate_id = position_candidate(
        &store,
        config.hekate_principal_id,
        source_event_id,
        serde_json::json!({
            "schema": "hekate.position_integration.v1",
            "operation": {
                "action": "establish",
                "subject": "independent position",
                "stance": "support",
                "reasons": ["supported by source"],
                "reconsideration_conditions": ["new contrary evidence"]
            }
        })
        .to_string(),
    )
    .await?;
    engine
        .verify_integration_candidate(
            second_candidate_id,
            config.user_principal_id,
            "human checked",
        )
        .await?;
    let second = engine.integrate_position(second_candidate_id).await?;
    let withdrawn_id = position_candidate(
        &store,
        config.hekate_principal_id,
        source_event_id,
        serde_json::json!({
            "schema": "hekate.position_integration.v1",
            "operation": {
                "action": "withdraw",
                "position_id": second.position.id.to_string(),
                "expected_version": 1,
                "reason": "source was withdrawn"
            }
        })
        .to_string(),
    )
    .await?;
    engine
        .verify_integration_candidate(withdrawn_id, config.user_principal_id, "human checked")
        .await?;
    let withdrawn = engine.integrate_position(withdrawn_id).await?;
    assert_eq!(withdrawn.position.id, second.position.id);
    assert_eq!(withdrawn.position.version, 1);
    assert_eq!(withdrawn.position.status, PositionStatus::Retracted);

    let state = store.state().await?;
    assert_eq!(Projector::replay(&store.events().await?)?, state);
    assert_eq!(state.position_integration_materializations.len(), 4);
    engine.shutdown().await?;
    Ok(())
}
