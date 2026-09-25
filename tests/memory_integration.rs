use std::fs;
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use hekate::adapters::local_policy::LocalPolicy;
use hekate::adapters::sqlite::{SqliteEmbeddingStore, SqliteStore};
use hekate::config::Config;
use hekate::core::{
    now, ActiveMemoryStatus, CognitiveTrace, CommittedJudgment, DecisionKind, EmbeddingDocument,
    EmbeddingEntityKind, EmbeddingSpace, EntityKind, EntityRef, EventKind, EventSource,
    EvidenceRef, ExperienceEvent, IntegrationCandidate, IntegrationCandidateId,
    IntegrationCandidateKind, MemoryCandidateStatus, MemoryId, MemoryKind, MemoryRevisionOperation,
    Observation, ObservationId, PositionStatus, Principal, PrincipalKind, RecallQuery, SleepRun,
    SleepRunId, SleepRunStatus, Stance, VerificationDisposition, MEMORY_REVISION_SCHEMA,
};
use hekate::ports::{
    CapabilityCatalog, CapabilityError, CapabilityResult, CognitiveError, CognitiveModel,
    EmbeddingProvider, EmbeddingProviderError,
};
use hekate::runtime::embedding_indexer::EmbeddingIndexer;
use hekate::runtime::memory_integration::IntegrationError;
use hekate::runtime::recall::{recall_local, SemanticRecall};
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

struct ContextCaptureModel {
    seen: Arc<Mutex<Vec<hekate::core::ThoughtContext>>>,
}

#[async_trait]
impl CognitiveModel for ContextCaptureModel {
    async fn think(
        &self,
        context: &hekate::core::ThoughtContext,
    ) -> Result<hekate::core::ThoughtCycle, CognitiveError> {
        self.seen
            .lock()
            .expect("capture lock")
            .push(context.clone());
        let memory = context
            .recall
            .items
            .iter()
            .find(|item| item.entity.kind == EntityKind::Memory);
        let response = memory
            .map(|item| format!("active memory: {}", item.text))
            .unwrap_or_else(|| "no active memory in recall".to_owned());
        let evidence_refs = memory
            .map(|item| vec![item.source_event_id])
            .unwrap_or_default();
        let commitment = CommittedJudgment {
            final_act: DecisionKind::Agree,
            reasons: vec!["inspect supplied test context".to_owned()],
            response,
            confidence: 90,
            unresolved_questions: Vec::new(),
            alternatives: Vec::new(),
            reconsideration_conditions: Vec::new(),
            evidence_refs: evidence_refs.clone(),
            related_position_ids: Vec::new(),
            position: None,
            user_position: None,
            conflict: None,
            action: None,
        };
        let trace = CognitiveTrace {
            trace_id: Uuid::new_v4().to_string(),
            outcome: "succeeded".to_owned(),
            provider: "test".to_owned(),
            model: "context-capture".to_owned(),
            schema_version: "test".to_owned(),
            context_sequence: context.event_sequence,
            context_hash: context.snapshot_hash.clone(),
            referenced_event_ids: evidence_refs,
            draft: None,
            review: None,
            commitment: Some(commitment.clone()),
            parse_errors: Vec::new(),
            retries: 0,
            elapsed_ms: 0,
            raw_response_hash: None,
            error_kind: None,
            created_at: now(),
        };
        Ok(hekate::core::ThoughtCycle {
            draft: hekate::core::ThoughtDraft {
                interpretation: "use current test context".to_owned(),
                initial_judgment: DecisionKind::Agree,
                reasons: vec!["inspect supplied test context".to_owned()],
                uncertainties: Vec::new(),
                initial_intent: "respond".to_owned(),
            },
            review: hekate::core::SelfReview {
                strongest_counterargument: "none".to_owned(),
                value_conflicts: Vec::new(),
                unsupported_claims: Vec::new(),
                revision_direction: None,
            },
            commitment,
            trace,
        })
    }
}

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

async fn memory_revision_candidate(
    store: &SqliteStore,
    actor_id: hekate::core::PrincipalId,
    target_memory_id: MemoryId,
    expected_event_id: hekate::core::EventId,
    expected_event_hash: String,
    replacement_content: Option<&str>,
) -> Result<(IntegrationCandidateId, hekate::core::EventId), Box<dyn std::error::Error>> {
    let observation = Observation {
        id: ObservationId::new(),
        actor_id,
        content: "new evidence changes the durable memory claim".to_owned(),
        source_type: "test".to_owned(),
        source_ref: None,
        thread_id: None,
        message_id: None,
        received_at: now(),
    };
    let source_event = event(
        actor_id,
        EventKind::ObservationRecorded,
        EntityKind::Observation,
        observation.id.uuid(),
        &observation,
    );
    let projector = Projector::new(Arc::new(store.clone()));
    let high_water_revision = projector.record(source_event.clone()).await?.revision;
    let state = store.state().await?;
    let run = SleepRun {
        id: SleepRunId::new(),
        status: SleepRunStatus::Running,
        high_water_revision,
        cursor_before: state.sleep_cursor,
        cursor_after: None,
        seed_event_ids: vec![source_event.event_id],
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
    let operation = match replacement_content {
        Some(replacement_content) => MemoryRevisionOperation::Replace {
            target_memory_id,
            expected_event_id,
            expected_event_hash,
            replacement_content: replacement_content.to_owned(),
        },
        None => MemoryRevisionOperation::Expire {
            target_memory_id,
            expected_event_id,
            expected_event_hash,
        },
    };
    let content = serde_json::json!({
        "schema": MEMORY_REVISION_SCHEMA,
        "operation": operation,
    })
    .to_string();
    let kind = IntegrationCandidateKind::MemoryRevision;
    let source_event_ids = vec![source_event.event_id];
    let counterevidence_event_ids = vec![expected_event_id];
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
            &counterevidence_event_ids,
        ),
        content,
        rationale: "new evidence conflicts with the active Memory".to_owned(),
        source_event_ids,
        counterevidence_event_ids,
        confidence: 85,
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
    Ok((candidate.id, source_event.event_id))
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

fn context_engine(
    store: Arc<SqliteStore>,
    seen: Arc<Mutex<Vec<hekate::core::ThoughtContext>>>,
) -> Engine {
    let config = Config::default();
    Engine::new(
        store,
        Arc::new(ContextCaptureModel { seen }),
        Arc::new(LocalPolicy),
        Arc::new(NoCapabilities),
        config.hekate_principal_id,
        config.user_principal_id,
    )
}

fn write_eval_rows(
    filename: &str,
    records: &[serde_json::Value],
) -> Result<(), Box<dyn std::error::Error>> {
    let path = std::path::Path::new("target/hekate-evals").join(filename);
    fs::create_dir_all(path.parent().expect("eval parent"))?;
    let jsonl = records
        .iter()
        .map(serde_json::Value::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(path, format!("{jsonl}\n"))?;
    Ok(())
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
    let hekate::runtime::MemoryIntegrationResult::Created {
        memory_candidate,
        memory,
        materialization,
    } = result
    else {
        panic!("plain Memory candidate should create a Lesson")
    };
    assert_eq!(memory.kind, hekate::core::MemoryKind::Lesson);
    assert_eq!(
        memory_candidate.status,
        hekate::core::MemoryCandidateStatus::Promoted
    );
    assert_eq!(materialization.memory_id, memory.id);
    let state = store.state().await?;
    assert_eq!(state.active_memories.len(), 1);
    assert_eq!(state.memory_candidates.len(), 1);
    assert_eq!(
        state.integration_materializations[&candidate_id].memory_id,
        memory.id
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
    let mut eval_rows = Vec::new();
    for iteration in 1..=10 {
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
        let events = store.events().await?;
        let replay_verified = Projector::replay(&events)? == state;
        assert!(replay_verified);
        eval_rows.push(serde_json::json!({
            "scenario": "rejected_or_invalid_candidate_stays_inactive",
            "iteration": iteration,
            "input": "candidate evidence is outside, future, unsupported, or rejected",
            "as_of_revision": candidate.as_of_revision,
            "source_event_ids": candidate.source_event_ids,
            "counterevidence_event_ids": candidate.counterevidence_event_ids,
            "active_before": [],
            "active_after": state.active_memories.values()
                .filter(|item| item.status == ActiveMemoryStatus::Active)
                .map(|item| serde_json::json!({"id": item.id, "content": item.content}))
                .collect::<Vec<_>>(),
            "cursor_before": state.sleep_cursor,
            "cursor_after": state.sleep_cursor,
            "result": "passed",
            "projection_verified": replay_verified,
            "external_capability_executed": false,
        }));
        engine.shutdown().await?;
    }
    write_eval_rows("consolidation-rejection.jsonl", &eval_rows)?;
    assert_eq!(eval_rows.len(), 10);
    assert!(eval_rows.iter().all(|record| {
        record["result"] == "passed"
            && record["projection_verified"] == true
            && record["external_capability_executed"] == false
    }));
    Ok(())
}

#[tokio::test]
async fn verified_memory_revision_replaces_expires_and_replays(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut eval_rows = Vec::new();
    for scenario in [
        "observation_source_provenance_retained",
        "revision_candidate_waits_for_nonmodel_verification",
        "verified_replace_expire_preserves_history",
        "foreground_context_reflects_materialized_memory",
    ] {
        for iteration in 1..=10 {
            let (store, initial_id, _, original_source_id) = fixture().await?;
            let config = Config::default();
            let engine = engine(store.clone(), None);
            engine
                .verify_integration_candidate(initial_id, config.user_principal_id, "human checked")
                .await?;
            let hekate::runtime::MemoryIntegrationResult::Created {
                memory_candidate: original_candidate,
                memory: original_memory,
                ..
            } = engine.integrate_memory(initial_id).await?
            else {
                panic!("plain Memory must retain the legacy creation path")
            };
            let follow_up = "What does the durable memory source say?";
            let before_seen = Arc::new(Mutex::new(Vec::new()));
            let before_result = context_engine(store.clone(), before_seen.clone())
                .handle(Observation {
                    id: ObservationId::new(),
                    actor_id: config.user_principal_id,
                    content: follow_up.to_owned(),
                    source_type: "test".to_owned(),
                    source_ref: None,
                    thread_id: None,
                    message_id: None,
                    received_at: now(),
                })
                .await?;
            let before_context = before_seen
                .lock()
                .expect("before context capture")
                .last()
                .cloned()
                .expect("before context");
            let before_memory = before_context
                .recall
                .items
                .iter()
                .find(|item| item.entity.kind == EntityKind::Memory)
                .expect("original Memory in model recall");
            assert_eq!(before_memory.entity.id, original_memory.id.uuid());
            let before_middle = before_context
                .context_snapshot
                .as_ref()
                .expect("before context snapshot")["compressed_middle"]
                .as_array()
                .expect("middle items");
            assert!(before_middle
                .iter()
                .any(|item| item["entity"]["id"] == original_memory.id.to_string()));
            let original_events = store.events().await?;
            let original_memory_event = original_events
                .iter()
                .find(|event| {
                    event.event_kind == EventKind::MemoryPromoted
                        && event.subject.as_ref().is_some_and(|subject| {
                            subject.kind == EntityKind::Memory
                                && subject.id == original_memory.id.uuid()
                        })
                })
                .expect("original Memory source event")
                .clone();
            let (replace_id, replace_source_id) = memory_revision_candidate(
                &store,
                config.user_principal_id,
                original_memory.id,
                original_memory_event.event_id,
                original_memory_event.canonical_hash()?,
                Some("new memory claim after contradictory evidence"),
            )
            .await?;
            let pending = store.state().await?;
            assert_eq!(
                pending.active_memories[&original_memory.id].status,
                ActiveMemoryStatus::Active
            );
            assert_eq!(
                pending.integration_candidates[&replace_id].disposition,
                VerificationDisposition::NeedsValidation
            );
            assert!(pending.memory_revision_materializations.is_empty());
            assert!(pending.integration_candidates[&replace_id]
                .counterevidence_event_ids
                .contains(&original_memory_event.event_id));
            let pending_events = store.events().await?;
            assert_eq!(Projector::replay(&pending_events)?, pending);
            engine
                .verify_integration_candidate(replace_id, config.user_principal_id, "human checked")
                .await?;
            let hekate::runtime::MemoryIntegrationResult::Replaced {
                memory_candidate: replacement_candidate,
                memory: replacement,
                previous_memory,
                materialization: replacement_materialization,
            } = engine.integrate_memory(replace_id).await?
            else {
                panic!("typed replace proposal should replace the Memory")
            };
            assert_eq!(previous_memory.status, ActiveMemoryStatus::Superseded);
            assert_eq!(replacement.supersedes, Some(original_memory.id));
            assert_eq!(
                replacement_candidate.supersedes,
                Some(original_candidate.id)
            );
            assert_eq!(
                replacement_materialization.source_event_ids,
                vec![replace_source_id]
            );
            let after_seen = Arc::new(Mutex::new(Vec::new()));
            let after_result = context_engine(store.clone(), after_seen.clone())
                .handle(Observation {
                    id: ObservationId::new(),
                    actor_id: config.user_principal_id,
                    content: follow_up.to_owned(),
                    source_type: "test".to_owned(),
                    source_ref: None,
                    thread_id: None,
                    message_id: None,
                    received_at: now(),
                })
                .await?;
            let after_context = after_seen
                .lock()
                .expect("after context capture")
                .last()
                .cloned()
                .expect("after context");
            let after_memory = after_context
                .recall
                .items
                .iter()
                .find(|item| item.entity.kind == EntityKind::Memory)
                .expect("replacement Memory in model recall");
            assert_eq!(after_memory.entity.id, replacement.id.uuid());
            assert_eq!(after_memory.text, replacement.content);
            assert_ne!(
                before_result.decision.message,
                after_result.decision.message
            );
            let after_middle = after_context
                .context_snapshot
                .as_ref()
                .expect("after context snapshot")["compressed_middle"]
                .as_array()
                .expect("middle items");
            assert!(after_middle
                .iter()
                .any(|item| item["entity"]["id"] == replacement.id.to_string()));
            assert!(!after_middle
                .iter()
                .any(|item| item["entity"]["id"] == original_memory.id.to_string()));
            let event_count_after_replace = store.events().await?.len();
            assert!(matches!(
                engine.integrate_memory(replace_id).await,
                Err(EngineError::Integration(IntegrationError::AlreadyMaterialized(id))) if id == replace_id
            ));
            assert_eq!(store.events().await?.len(), event_count_after_replace);

            let replacement_event = store
                .events()
                .await?
                .into_iter()
                .find(|event| {
                    event.event_kind == EventKind::MemoryPromoted
                        && event.subject.as_ref().is_some_and(|subject| {
                            subject.kind == EntityKind::Memory
                                && subject.id == replacement.id.uuid()
                        })
                })
                .expect("replacement Memory source event");
            let (expire_id, expire_source_id) = memory_revision_candidate(
                &store,
                config.user_principal_id,
                replacement.id,
                replacement_event.event_id,
                replacement_event.canonical_hash()?,
                None,
            )
            .await?;
            engine
                .verify_integration_candidate(expire_id, config.user_principal_id, "human checked")
                .await?;
            let hekate::runtime::MemoryIntegrationResult::Expired {
                memory: expired,
                materialization: expiry_materialization,
            } = engine.integrate_memory(expire_id).await?
            else {
                panic!("typed expire proposal should expire the Memory")
            };
            assert_eq!(expired.status, ActiveMemoryStatus::Expired);
            assert_eq!(
                expiry_materialization.source_event_ids,
                vec![expire_source_id]
            );
            assert_eq!(
                store.state().await?.memory_revision_materializations.len(),
                2
            );
            let state = store.state().await?;
            assert!(state
                .active_memories
                .values()
                .all(|memory| memory.status != ActiveMemoryStatus::Active));
            assert!(state
                .observations
                .values()
                .any(|item| item.content == "a durable memory source"));
            let events = store.events().await?;
            let original_source = events
                .iter()
                .find(|event| event.event_id == original_source_id)
                .expect("original source event retained");
            assert!(original_source.verify_integrity()?);
            assert_eq!(
                original_source.payload["content"],
                "a durable memory source"
            );
            let recalled = recall_local(
                &RecallQuery {
                    text: "new memory claim after contradictory evidence".to_owned(),
                    limit: 6,
                    exclude_event_ids: Vec::new(),
                    as_of_sequence: Some(state.revision),
                },
                &state,
                &events,
            );
            assert!(recalled
                .items
                .iter()
                .all(|item| item.entity.kind != EntityKind::Memory));
            assert!(
                hekate::runtime::embedding_indexer::embedding_documents(&state, &events)?
                    .iter()
                    .all(|document| document.entity_kind != EmbeddingEntityKind::Memory)
            );
            assert_eq!(Projector::replay(&events)?, state);
            eval_rows.push(serde_json::json!({
        "scenario": scenario,
        "iteration": iteration,
        "input": "new evidence changes the durable memory claim",
        "as_of_revision": pending.integration_candidates[&replace_id].as_of_revision,
        "source_event_ids": pending.integration_candidates[&replace_id].source_event_ids,
        "counterevidence_event_ids": pending.integration_candidates[&replace_id].counterevidence_event_ids,
        "active_before": pending.active_memories.values()
            .filter(|item| item.status == ActiveMemoryStatus::Active)
            .map(|item| serde_json::json!({"id": item.id, "content": item.content}))
            .collect::<Vec<_>>(),
        "active_after": state.active_memories.values()
            .filter(|item| item.status == ActiveMemoryStatus::Active)
            .map(|item| serde_json::json!({"id": item.id, "content": item.content}))
            .collect::<Vec<_>>(),
        "context_before": before_result.decision.message,
        "context_after": after_result.decision.message,
        "cursor_before": pending.sleep_cursor,
        "cursor_after": state.sleep_cursor,
        "result": "passed",
        "projection_verified": Projector::replay(&events)? == state,
        "external_capability_executed": false,
    }));
            assert!(
                hekate::runtime::recovery::recover(store.as_ref())
                    .await?
                    .projection_verified
            );
            engine.shutdown().await?;
        }
    }
    write_eval_rows("consolidation-memory-revision.jsonl", &eval_rows)?;
    assert_eq!(eval_rows.len(), 40);
    for scenario in [
        "observation_source_provenance_retained",
        "revision_candidate_waits_for_nonmodel_verification",
        "verified_replace_expire_preserves_history",
        "foreground_context_reflects_materialized_memory",
    ] {
        assert_eq!(
            eval_rows
                .iter()
                .filter(|record| record["scenario"] == scenario)
                .count(),
            10,
            "{scenario} repetitions"
        );
    }
    assert!(eval_rows.iter().all(|record| record["result"] == "passed"
        && record["projection_verified"] == true
        && record["external_capability_executed"] == false));
    Ok(())
}

#[tokio::test]
async fn sleep_memory_revision_cannot_overwrite_explicit_user_preference(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut eval_rows = Vec::new();
    for iteration in 1..=10 {
        let (store, _, _, source_event_id) = fixture().await?;
        let config = Config::default();
        let projector = Projector::new(store.clone());
        let preference_candidate = hekate::core::MemoryCandidate {
            id: hekate::core::MemoryCandidateId::new(),
            kind: MemoryKind::ExplicitPreference,
            content: "explicit preference: keep answers concise".to_owned(),
            subject_principal_id: Some(config.user_principal_id),
            status: MemoryCandidateStatus::Candidate,
            confidence: 100,
            source_event_ids: vec![source_event_id],
            valid_from: None,
            valid_until: None,
            supersedes: None,
            created_at: now(),
        };
        let preference = hekate::core::ActiveMemory {
            id: MemoryId::new(),
            candidate_id: preference_candidate.id,
            kind: MemoryKind::ExplicitPreference,
            content: preference_candidate.content.clone(),
            subject_principal_id: preference_candidate.subject_principal_id,
            status: ActiveMemoryStatus::Active,
            confidence: 100,
            source_event_ids: vec![source_event_id],
            valid_from: None,
            valid_until: None,
            supersedes: None,
            last_verified_at: None,
            created_at: now(),
        };
        projector
            .record(event(
                config.user_principal_id,
                EventKind::MemoryCandidateCreated,
                EntityKind::MemoryCandidate,
                preference_candidate.id.uuid(),
                &preference_candidate,
            ))
            .await?;
        let preference_event = event(
            config.user_principal_id,
            EventKind::MemoryPromoted,
            EntityKind::Memory,
            preference.id.uuid(),
            &preference,
        );
        projector.record(preference_event.clone()).await?;
        let (candidate_id, _) = memory_revision_candidate(
            &store,
            config.user_principal_id,
            preference.id,
            preference_event.event_id,
            preference_event.canonical_hash()?,
            Some("Sleep inferred a different answer style"),
        )
        .await?;
        let engine = engine(store.clone(), None);
        assert!(matches!(
            engine
                .verify_integration_candidate(
                    candidate_id,
                    config.user_principal_id,
                    "human checked"
                )
                .await,
            Err(EngineError::Integration(
                IntegrationError::ExplicitPreferenceProtected
            ))
        ));
        let state = store.state().await?;
        assert_eq!(state.active_memories[&preference.id], preference);
        assert_eq!(
            state.integration_candidates[&candidate_id].disposition,
            VerificationDisposition::NeedsValidation
        );
        assert!(!state.integration_verifications.contains_key(&candidate_id));
        let replay_verified = Projector::replay(&store.events().await?)? == state;
        assert!(replay_verified);
        eval_rows.push(serde_json::json!({
        "scenario": "explicit_preference_is_protected",
        "iteration": iteration,
        "input": "Sleep proposes a contradictory inferred preference",
        "as_of_revision": state.integration_candidates[&candidate_id].as_of_revision,
        "source_event_ids": state.integration_candidates[&candidate_id].source_event_ids,
        "counterevidence_event_ids": state.integration_candidates[&candidate_id].counterevidence_event_ids,
        "active_before": [{"id": preference.id, "content": preference.content}],
        "active_after": [{"id": state.active_memories[&preference.id].id, "content": state.active_memories[&preference.id].content}],
        "cursor_before": state.sleep_cursor,
        "cursor_after": state.sleep_cursor,
        "result": "passed",
        "projection_verified": replay_verified,
        "external_capability_executed": false,
    }));
        engine.shutdown().await?;
    }
    write_eval_rows("consolidation-preference.jsonl", &eval_rows)?;
    assert_eq!(eval_rows.len(), 10);
    assert!(eval_rows.iter().all(|record| {
        record["result"] == "passed"
            && record["projection_verified"] == true
            && record["external_capability_executed"] == false
    }));
    Ok(())
}

#[tokio::test]
async fn memory_revision_rejects_target_changed_after_as_of(
) -> Result<(), Box<dyn std::error::Error>> {
    let (store, initial_id, _, _) = fixture().await?;
    let config = Config::default();
    let engine = engine(store.clone(), None);
    engine
        .verify_integration_candidate(initial_id, config.user_principal_id, "human checked")
        .await?;
    let hekate::runtime::MemoryIntegrationResult::Created { memory, .. } =
        engine.integrate_memory(initial_id).await?
    else {
        panic!("plain Memory must be created")
    };
    let memory_event = store
        .events()
        .await?
        .into_iter()
        .find(|event| {
            event.event_kind == EventKind::MemoryPromoted
                && event.subject.as_ref().is_some_and(|subject| {
                    subject.kind == EntityKind::Memory && subject.id == memory.id.uuid()
                })
        })
        .expect("Memory promotion event");
    let (candidate_id, _) = memory_revision_candidate(
        &store,
        config.user_principal_id,
        memory.id,
        memory_event.event_id,
        memory_event.canonical_hash()?,
        Some("replacement after newer evidence"),
    )
    .await?;
    engine
        .verify_integration_candidate(candidate_id, config.user_principal_id, "human checked")
        .await?;
    engine.expire_memory(memory.id).await?;
    assert!(matches!(
        engine.integrate_memory(candidate_id).await,
        Err(EngineError::Integration(IntegrationError::MemoryChanged(id))) if id == memory.id
    ));
    let state = store.state().await?;
    assert_eq!(
        state.active_memories[&memory.id].status,
        ActiveMemoryStatus::Expired
    );
    assert_eq!(
        state.integration_candidates[&candidate_id].disposition,
        VerificationDisposition::Verified
    );
    assert!(!state
        .memory_revision_materializations
        .contains_key(&candidate_id));
    assert_eq!(Projector::replay(&store.events().await?)?, state);
    engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn position_integration_keeps_user_owned_position_unchanged(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut eval_rows = Vec::new();
    for iteration in 1..=10 {
        let (store, _, _, source_event_id) = fixture().await?;
        let config = Config::default();
        let user_position = hekate::core::Position {
            id: hekate::core::PositionId::new(),
            principal_id: config.user_principal_id,
            subject: "user-owned position".to_owned(),
            stance: Stance::Support,
            version: 1,
            status: PositionStatus::Active,
            confidence: 100,
            supersedes: None,
            reasons: vec!["recorded by the user".to_owned()],
            evidence_refs: vec![source_event_id],
            reconsideration_conditions: vec!["new evidence".to_owned()],
            created_at: now(),
        };
        Projector::new(store.clone())
            .record(event(
                config.user_principal_id,
                EventKind::PositionEstablished,
                EntityKind::Position,
                user_position.id.uuid(),
                &user_position,
            ))
            .await?;
        let candidate_id = position_candidate(
            &store,
            config.user_principal_id,
            source_event_id,
            serde_json::json!({
                "schema": "hekate.position_integration.v1",
                "operation": {
                    "action": "revise",
                    "position_id": user_position.id.to_string(),
                    "expected_version": 1,
                    "stance": "oppose",
                    "reasons": ["Sleep inferred a different stance"],
                    "reconsideration_conditions": ["more evidence"]
                }
            })
            .to_string(),
        )
        .await?;
        let engine = engine(store.clone(), None);
        assert!(matches!(
            engine
                .verify_integration_candidate(candidate_id, config.user_principal_id, "human checked")
                .await,
            Err(EngineError::Integration(
                IntegrationError::WrongPositionPrincipal(id)
            )) if id == user_position.id
        ));
        let state = store.state().await?;
        assert_eq!(state.positions[&user_position.id], user_position);
        assert_eq!(
            state.integration_candidates[&candidate_id].disposition,
            VerificationDisposition::NeedsValidation
        );
        let replay_verified = Projector::replay(&store.events().await?)? == state;
        assert!(replay_verified);
        eval_rows.push(serde_json::json!({
        "scenario": "user_owned_position_is_unchanged",
        "iteration": iteration,
        "input": "Sleep proposes a revised stance for a user-owned Position",
        "as_of_revision": state.integration_candidates[&candidate_id].as_of_revision,
        "source_event_ids": state.integration_candidates[&candidate_id].source_event_ids,
        "counterevidence_event_ids": state.integration_candidates[&candidate_id].counterevidence_event_ids,
        "active_before": [{"id": user_position.id, "version": user_position.version, "status": user_position.status}],
        "active_after": [{"id": state.positions[&user_position.id].id, "version": state.positions[&user_position.id].version, "status": state.positions[&user_position.id].status}],
        "cursor_before": state.sleep_cursor,
        "cursor_after": state.sleep_cursor,
        "result": "passed",
        "projection_verified": replay_verified,
        "external_capability_executed": false,
    }));
        engine.shutdown().await?;
    }
    write_eval_rows("consolidation-position.jsonl", &eval_rows)?;
    assert_eq!(eval_rows.len(), 10);
    assert!(eval_rows.iter().all(|record| {
        record["result"] == "passed"
            && record["projection_verified"] == true
            && record["external_capability_executed"] == false
    }));
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
