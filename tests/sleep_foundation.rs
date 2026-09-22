use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use async_trait::async_trait;
use hekate::adapters::local_policy::LocalPolicy;
use hekate::adapters::sqlite::{SqliteEmbeddingStore, SqliteStore};
use hekate::config::Config;
use hekate::core::{
    integration_candidate_fingerprint, now, CognitiveTrace, EntityKind, EntityRef, EventKind,
    EventSource, ExperienceEvent, IntegrationCandidate, IntegrationCandidateId,
    IntegrationCandidateKind, Observation, ObservationId, SleepContext, SleepDeliberation,
    SleepRun, SleepRunId, SleepRunStatus, SleepSelfReview, VerificationDisposition,
};
use hekate::core::{EmbeddingDocument, EmbeddingSpace, EmbeddingVector};
use hekate::ports::{
    CapabilityCatalog, CapabilityError, CapabilityResult, CognitiveError, CognitiveModel,
    EmbeddingProvider, EmbeddingProviderError, SleepCognitiveError, SleepCognitiveModel,
};
use hekate::runtime::embedding_indexer::EmbeddingIndexer;
use hekate::runtime::recall::SemanticRecall;
use hekate::runtime::{Engine, Projector};
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
        Err(CapabilityError::Execution(
            "unused in sleep test".to_owned(),
        ))
    }
}

struct NoForegroundModel;

#[async_trait]
impl CognitiveModel for NoForegroundModel {
    async fn think(
        &self,
        _: &hekate::core::ThoughtContext,
    ) -> Result<hekate::core::ThoughtCycle, CognitiveError> {
        Err(CognitiveError::Provider {
            message: "unused in sleep test".to_owned(),
            trace: empty_trace(),
        })
    }
}

struct FakeSleepModel {
    calls: Arc<AtomicUsize>,
    contexts: Arc<Mutex<Vec<SleepContext>>>,
    invalid_source: bool,
    confidence: u8,
}

#[async_trait]
impl SleepCognitiveModel for FakeSleepModel {
    async fn deliberate_sleep(
        &self,
        context: &SleepContext,
    ) -> Result<SleepDeliberation, SleepCognitiveError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.contexts
            .lock()
            .expect("sleep context lock")
            .push(context.clone());
        let source_event_id = if self.invalid_source {
            hekate::core::EventId::new()
        } else {
            context
                .seed_observations
                .first()
                .expect("sleep seed")
                .event_id
        };
        Ok(SleepDeliberation {
            draft_summary: "bounded test summary".to_owned(),
            self_review: SleepSelfReview {
                weak_points: Vec::new(),
                possible_counterevidence_event_ids: Vec::new(),
                revised: false,
            },
            candidates: vec![hekate::core::IntegrationCandidateDraft {
                kind: IntegrationCandidateKind::Memory,
                content: "a durable test association".to_owned(),
                rationale: "the same pattern appeared in supplied history".to_owned(),
                source_event_ids: vec![source_event_id],
                counterevidence_event_ids: Vec::new(),
                confidence: self.confidence,
            }],
            trace: None,
        })
    }
}

#[derive(Clone)]
struct FixedProvider {
    space: EmbeddingSpace,
    vector: EmbeddingVector,
}

#[async_trait]
impl EmbeddingProvider for FixedProvider {
    fn space(&self) -> &EmbeddingSpace {
        &self.space
    }

    async fn embed_query(&self, _: &str) -> Result<EmbeddingVector, EmbeddingProviderError> {
        Ok(self.vector.clone())
    }

    async fn embed_documents(
        &self,
        documents: &[EmbeddingDocument],
    ) -> Result<Vec<EmbeddingVector>, EmbeddingProviderError> {
        Ok(documents.iter().map(|_| self.vector.clone()).collect())
    }
}

fn empty_trace() -> CognitiveTrace {
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
            .join(format!("hekate-sleep-{label}-{}.db", Uuid::new_v4()))
            .display()
    )
}

fn observation(content: &str) -> Observation {
    Observation {
        id: ObservationId::new(),
        actor_id: Config::default().user_principal_id,
        content: content.to_owned(),
        source_type: "test".to_owned(),
        source_ref: None,
        thread_id: None,
        message_id: None,
        received_at: now(),
    }
}

fn observation_event(observation: &Observation) -> ExperienceEvent {
    ExperienceEvent::new(
        observation.actor_id,
        EventKind::ObservationRecorded,
        Some(EntityRef::new(
            EntityKind::Observation,
            observation.id.uuid(),
        )),
        serde_json::to_value(observation).expect("observation payload"),
        EventSource::new("test", None),
        None,
        None,
        Some(1.0),
    )
    .expect("observation event")
}

fn engine(
    store: Arc<SqliteStore>,
    sleep_model: Arc<FakeSleepModel>,
    recall: Option<Arc<SemanticRecall>>,
) -> Engine {
    let config = Config::default();
    let engine = Engine::new(
        store,
        Arc::new(NoForegroundModel),
        Arc::new(LocalPolicy),
        Arc::new(NoCapabilities),
        config.hekate_principal_id,
        config.user_principal_id,
    )
    .with_sleep_model(sleep_model);
    match recall {
        Some(recall) => engine.with_semantic_recall(recall),
        None => engine,
    }
}

async fn fixed_recall(
    url: &str,
    state: &hekate::core::CurrentState,
    events: &[ExperienceEvent],
) -> Arc<SemanticRecall> {
    let space = EmbeddingSpace::new("test", "test", "v1", 2, true, "query: ", "document: ");
    let vector = EmbeddingVector::new(vec![1.0, 0.0], &space).expect("vector");
    let store = Arc::new(
        SqliteEmbeddingStore::open(url)
            .await
            .expect("embedding store"),
    );
    let indexer = Arc::new(EmbeddingIndexer::new(
        Arc::new(FixedProvider { space, vector }),
        store,
        16,
    ));
    indexer.index_once(state, events).await.expect("index");
    Arc::new(SemanticRecall::new(indexer))
}

#[tokio::test]
async fn sleep_lifecycle_replays_and_advances_only_observation_cursor() {
    let url = database_url("lifecycle");
    let store = Arc::new(SqliteStore::open(&url).await.expect("store"));
    for index in 0..13 {
        Projector::new(store.clone())
            .record(observation_event(&observation(&format!(
                "observation {index}"
            ))))
            .await
            .expect("observation");
    }
    let state_before = store.state().await.expect("state");
    let events_before = store.events().await.expect("events");
    let recall = fixed_recall(&url, &state_before, &events_before).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let model = Arc::new(FakeSleepModel {
        calls: calls.clone(),
        contexts: contexts.clone(),
        invalid_source: false,
        confidence: 80,
    });
    let engine = engine(store.clone(), model, Some(recall));

    let first = engine.sleep_once().await.expect("first sleep");
    assert!(matches!(
        first.status,
        hekate::runtime::SleepOnceStatus::Completed
    ));
    assert_eq!(first.processed_observations, 12);
    assert_eq!(first.created_candidates, 1);
    assert_eq!(first.cursor_after, Some(12));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        contexts
            .lock()
            .expect("contexts")
            .first()
            .expect("first context")
            .seed_observations
            .len(),
        12
    );
    assert!(!contexts
        .lock()
        .expect("contexts")
        .first()
        .expect("first context")
        .recalled_experiences
        .is_empty());

    let second = engine.sleep_once().await.expect("second sleep");
    assert!(matches!(
        second.status,
        hekate::runtime::SleepOnceStatus::Completed
    ));
    assert_eq!(second.cursor_after, Some(13));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let third = engine.sleep_once().await.expect("idle sleep");
    assert!(matches!(
        third.status,
        hekate::runtime::SleepOnceStatus::Idle
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    let state = store.state().await.expect("final state");
    assert_eq!(state.sleep_runs.len(), 2);
    assert_eq!(state.integration_candidates.len(), 2);
    assert!(state.active_memories.is_empty());
    assert!(state.positions.is_empty());
    assert!(state.identity_versions.is_empty());
    assert_eq!(state.sleep_cursor, 13);
    let events = store.events().await.expect("final events");
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_kind == EventKind::IntegrationCandidateCreated)
            .count(),
        2
    );
    assert_eq!(Projector::replay(&events).expect("replay"), state);
    engine.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn running_sleep_resumes_and_existing_fingerprint_is_not_duplicated() {
    let url = database_url("resume");
    let store = Arc::new(SqliteStore::open(&url).await.expect("store"));
    let observation = observation("resume this observation");
    let observation_event = observation_event(&observation);
    let projector = Projector::new(store.clone());
    projector
        .record(observation_event.clone())
        .await
        .expect("observation");
    let run = SleepRun {
        id: SleepRunId::new(),
        status: SleepRunStatus::Running,
        high_water_revision: 1,
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
    let start = ExperienceEvent::new(
        Config::default().hekate_principal_id,
        EventKind::SleepRunStarted,
        Some(EntityRef::new(EntityKind::SleepRun, run.id.uuid())),
        serde_json::to_value(&run).expect("run payload"),
        EventSource::new("test", None),
        None,
        Some(run.id.to_string()),
        Some(1.0),
    )
    .expect("start event");
    projector.record(start).await.expect("start");
    let source = vec![observation_event.event_id];
    let fingerprint = integration_candidate_fingerprint(
        &IntegrationCandidateKind::Memory,
        "a durable test association",
        &source,
        &[],
    );
    let existing = IntegrationCandidate {
        id: IntegrationCandidateId::new(),
        sleep_run_id: run.id,
        kind: IntegrationCandidateKind::Memory,
        disposition: VerificationDisposition::NeedsValidation,
        as_of_revision: run.high_water_revision,
        content: "a durable test association".to_owned(),
        rationale: "the same pattern appeared in supplied history".to_owned(),
        source_event_ids: source,
        counterevidence_event_ids: Vec::new(),
        confidence: 80,
        fingerprint,
        created_at: now(),
    };
    let candidate_event = ExperienceEvent::new(
        Config::default().hekate_principal_id,
        EventKind::IntegrationCandidateCreated,
        Some(EntityRef::new(
            EntityKind::IntegrationCandidate,
            existing.id.uuid(),
        )),
        serde_json::to_value(&existing).expect("candidate payload"),
        EventSource::new("test", None),
        None,
        Some(run.id.to_string()),
        Some(1.0),
    )
    .expect("candidate event");
    projector.record(candidate_event).await.expect("candidate");

    let calls = Arc::new(AtomicUsize::new(0));
    let resume_engine = engine(
        store.clone(),
        Arc::new(FakeSleepModel {
            calls: calls.clone(),
            contexts: Arc::new(Mutex::new(Vec::new())),
            invalid_source: false,
            confidence: 80,
        }),
        None,
    );
    let result = resume_engine.sleep_once().await.expect("resumed sleep");
    assert!(matches!(
        result.status,
        hekate::runtime::SleepOnceStatus::RunningResumed
    ));
    assert!(result.resumed);
    assert_eq!(result.run_id, Some(run.id));
    assert_eq!(result.created_candidates, 0);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let state = store.state().await.expect("state");
    assert_eq!(state.sleep_runs.len(), 1);
    assert_eq!(state.integration_candidates.len(), 1);
    assert_eq!(state.sleep_cursor, 1);
    let events = store.events().await.expect("events");
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_kind == EventKind::SleepRunStarted)
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_kind == EventKind::IntegrationCandidateCreated)
            .count(),
        1
    );
    assert_eq!(Projector::replay(&events).expect("replay"), state);
    resume_engine.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn invalid_provenance_fails_without_cursor_and_foreground_defers() {
    let url = database_url("invalid");
    let store = Arc::new(SqliteStore::open(&url).await.expect("store"));
    Projector::new(store.clone())
        .record(observation_event(&observation("reject invalid provenance")))
        .await
        .expect("observation");
    let calls = Arc::new(AtomicUsize::new(0));
    let invalid_engine = engine(
        store.clone(),
        Arc::new(FakeSleepModel {
            calls: calls.clone(),
            contexts: Arc::new(Mutex::new(Vec::new())),
            invalid_source: true,
            confidence: 80,
        }),
        None,
    );
    let result = invalid_engine.sleep_once().await.expect("failed sleep");
    assert!(matches!(
        result.status,
        hekate::runtime::SleepOnceStatus::Failed
    ));
    assert_eq!(result.cursor_after, None);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let state = store.state().await.expect("failed state");
    assert_eq!(state.sleep_cursor, 0);
    assert!(state.integration_candidates.is_empty());
    assert_eq!(
        state
            .sleep_runs
            .values()
            .filter(|run| matches!(run.status, SleepRunStatus::Failed))
            .count(),
        1
    );
    invalid_engine.shutdown().await.expect("shutdown");

    let url = database_url("deferred");
    let store = Arc::new(SqliteStore::open(&url).await.expect("store"));
    let run = hekate::core::Run {
        id: hekate::core::RunId::new(),
        task_id: None,
        status: hekate::core::RunStatus::Running,
        started_at: now(),
        completed_at: None,
    };
    let attempt = hekate::core::Attempt {
        id: hekate::core::AttemptId::new(),
        run_id: run.id,
        status: hekate::core::AttemptStatus::Started,
        started_at: now(),
        finished_at: None,
    };
    let projector = Projector::new(store.clone());
    projector
        .record(event_for(
            EventKind::RunStarted,
            EntityKind::Run,
            run.id.uuid(),
            &run,
        ))
        .await
        .expect("run");
    projector
        .record(event_for(
            EventKind::AttemptStarted,
            EntityKind::Attempt,
            attempt.id.uuid(),
            &attempt,
        ))
        .await
        .expect("attempt");
    let calls = Arc::new(AtomicUsize::new(0));
    let deferred_engine = engine(
        store.clone(),
        Arc::new(FakeSleepModel {
            calls: calls.clone(),
            contexts: Arc::new(Mutex::new(Vec::new())),
            invalid_source: false,
            confidence: 80,
        }),
        None,
    );
    let result = deferred_engine.sleep_once().await.expect("deferred sleep");
    assert!(matches!(
        result.status,
        hekate::runtime::SleepOnceStatus::Deferred
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(store.state().await.expect("state").sleep_runs.is_empty());
    deferred_engine.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn confidence_does_not_verify_candidate() {
    let url = database_url("verification-disposition");
    let store = Arc::new(SqliteStore::open(&url).await.expect("store"));
    Projector::new(store.clone())
        .record(observation_event(&observation(
            "confidence is not verification",
        )))
        .await
        .expect("observation");
    let calls = Arc::new(AtomicUsize::new(0));
    let model = Arc::new(FakeSleepModel {
        calls: calls.clone(),
        contexts: Arc::new(Mutex::new(Vec::new())),
        invalid_source: false,
        confidence: 100,
    });
    let sleep = engine(store.clone(), model, None);
    let result = sleep.sleep_once().await.expect("sleep");
    assert!(matches!(
        result.status,
        hekate::runtime::SleepOnceStatus::Completed
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let state = store.state().await.expect("state");
    let candidate = state
        .integration_candidates
        .values()
        .next()
        .expect("candidate");
    assert_eq!(candidate.confidence, 100);
    assert_eq!(
        candidate.disposition,
        VerificationDisposition::NeedsValidation
    );
    assert_eq!(
        candidate.as_of_revision,
        result.high_water_revision.unwrap()
    );
    assert!(state
        .sleep_runs
        .get(&result.run_id.unwrap())
        .and_then(|run| run.context_budget_report.as_ref())
        .is_some());
    assert_eq!(
        Projector::replay(&store.events().await.expect("events")).expect("replay"),
        state
    );
    sleep.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn seed_budget_defers_observations_and_advances_only_included_cursor() {
    let url = database_url("seed-budget");
    let store = Arc::new(SqliteStore::open(&url).await.expect("store"));
    let projector = Projector::new(store.clone());
    for content in ["a".repeat(22_000), "b".repeat(22_000)] {
        projector
            .record(observation_event(&observation(&content)))
            .await
            .expect("observation");
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let sleep = engine(
        store.clone(),
        Arc::new(FakeSleepModel {
            calls: calls.clone(),
            contexts: contexts.clone(),
            invalid_source: false,
            confidence: 80,
        }),
        None,
    );

    let first = sleep.sleep_once().await.expect("first sleep");
    assert_eq!(first.processed_observations, 1);
    assert_eq!(first.cursor_after, Some(1));
    let first_context = contexts.lock().expect("contexts")[0].clone();
    assert_eq!(first_context.seed_observations.len(), 1);
    let first_run = store
        .state()
        .await
        .expect("state")
        .sleep_runs
        .get(&first.run_id.expect("run"))
        .cloned()
        .expect("first run");
    let report = first_run.context_budget_report.expect("budget report");
    assert_eq!(report.included_seed_count, 1);
    assert_eq!(report.deferred_seed_count, 1);
    assert!(report.seed_bytes <= 32 * 1024);
    assert!(report.total_bytes <= 80 * 1024);

    let second = sleep.sleep_once().await.expect("second sleep");
    assert_eq!(second.processed_observations, 1);
    assert_eq!(second.cursor_after, Some(2));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(store.state().await.expect("state").sleep_cursor, 2);
    sleep.shutdown().await.expect("shutdown");
}

fn event_for<T: serde::Serialize>(
    kind: EventKind,
    entity_kind: EntityKind,
    id: Uuid,
    payload: &T,
) -> ExperienceEvent {
    ExperienceEvent::new(
        Config::default().hekate_principal_id,
        kind,
        Some(EntityRef::new(entity_kind, id)),
        serde_json::to_value(payload).expect("payload"),
        EventSource::new("test", None),
        None,
        None,
        Some(1.0),
    )
    .expect("event")
}
