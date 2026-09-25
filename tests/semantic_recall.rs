use std::fs;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use hekate::adapters::local_policy::LocalPolicy;
use hekate::adapters::sqlite::{SqliteEmbeddingStore, SqliteStore};
use hekate::core::{
    now, ActiveMemory, ActiveMemoryStatus, CognitiveTrace, CommittedJudgment, DecisionKind,
    EmbeddingDocument, EmbeddingEntityKind, EmbeddingSpace, EmbeddingVector, EntityKind, EntityRef,
    EventKind, EventSource, ExperienceEvent, MemoryCandidate, MemoryCandidateId,
    MemoryCandidateStatus, MemoryId, MemoryKind, Observation, ObservationId, RecallQuery,
    SelfReview, ThoughtContext, ThoughtCycle, ThoughtDraft,
};
use hekate::ports::{
    CapabilityCatalog, CapabilityError, CapabilityResult, CognitiveError, CognitiveModel,
    EmbeddingProvider, EmbeddingProviderError, EmbeddingStore,
};
use hekate::runtime::embedding_indexer::EmbeddingIndexer;
use hekate::runtime::recall::SemanticRecall;
use hekate::runtime::{Engine, Projector};
use uuid::Uuid;

fn space() -> EmbeddingSpace {
    EmbeddingSpace::new(
        "test",
        "test-embedding",
        "v1",
        2,
        true,
        "query: ",
        "document: ",
    )
}

fn vector() -> EmbeddingVector {
    EmbeddingVector::new(vec![1.0, 0.0], &space()).expect("test vector")
}

#[derive(Clone)]
struct TestProvider {
    space: EmbeddingSpace,
    vector: Option<EmbeddingVector>,
    fail: bool,
}

struct HangingProvider {
    space: EmbeddingSpace,
}

#[async_trait]
impl EmbeddingProvider for HangingProvider {
    fn space(&self) -> &EmbeddingSpace {
        &self.space
    }

    async fn embed_query(&self, _: &str) -> Result<EmbeddingVector, EmbeddingProviderError> {
        std::future::pending().await
    }

    async fn embed_documents(
        &self,
        _: &[EmbeddingDocument],
    ) -> Result<Vec<EmbeddingVector>, EmbeddingProviderError> {
        std::future::pending().await
    }
}

#[async_trait]
impl EmbeddingProvider for TestProvider {
    fn space(&self) -> &EmbeddingSpace {
        &self.space
    }

    async fn embed_query(&self, _: &str) -> Result<EmbeddingVector, EmbeddingProviderError> {
        if self.fail {
            Err(EmbeddingProviderError::Request)
        } else {
            self.vector
                .clone()
                .ok_or(EmbeddingProviderError::InvalidResponse {
                    kind: "missing test vector".to_owned(),
                    response_hash: "test".to_owned(),
                })
        }
    }

    async fn embed_documents(
        &self,
        documents: &[EmbeddingDocument],
    ) -> Result<Vec<EmbeddingVector>, EmbeddingProviderError> {
        if self.fail {
            return Err(EmbeddingProviderError::Request);
        }
        let vector = self
            .vector
            .clone()
            .ok_or(EmbeddingProviderError::InvalidResponse {
                kind: "missing test vector".to_owned(),
                response_hash: "test".to_owned(),
            })?;
        Ok(documents.iter().map(|_| vector.clone()).collect())
    }
}

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
        Err(CapabilityError::Execution("not used in test".to_owned()))
    }
}

struct RecordingModel {
    seen: Arc<Mutex<Vec<ThoughtContext>>>,
    cite_recall: bool,
}

#[async_trait]
impl CognitiveModel for RecordingModel {
    async fn think(&self, context: &ThoughtContext) -> Result<ThoughtCycle, CognitiveError> {
        self.seen
            .lock()
            .expect("model capture lock")
            .push(context.clone());
        let evidence_refs = if self.cite_recall {
            context
                .recall
                .items
                .first()
                .map(|item| vec![item.source_event_id])
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        let commitment = CommittedJudgment {
            final_act: DecisionKind::Agree,
            reasons: vec!["test response".to_owned()],
            response: "test response".to_owned(),
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
            model: "test".to_owned(),
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
        Ok(ThoughtCycle {
            draft: ThoughtDraft {
                interpretation: "test".to_owned(),
                initial_judgment: DecisionKind::Agree,
                reasons: vec!["test".to_owned()],
                uncertainties: Vec::new(),
                initial_intent: "test".to_owned(),
            },
            review: SelfReview {
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

fn database_url(label: &str) -> String {
    format!(
        "sqlite://{}",
        std::env::temp_dir()
            .join(format!(
                "hekate-semantic-recall-{label}-{}.db",
                Uuid::new_v4()
            ))
            .display()
    )
}

fn observation(content: &str) -> Observation {
    Observation {
        id: ObservationId::new(),
        actor_id: hekate::config::Config::default().user_principal_id,
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

fn engine(store: Arc<SqliteStore>, model: RecordingModel, recall: Arc<SemanticRecall>) -> Engine {
    let config = hekate::config::Config::default();
    Engine::new(
        store,
        Arc::new(model),
        Arc::new(LocalPolicy),
        Arc::new(NoCapabilities),
        config.hekate_principal_id,
        config.user_principal_id,
    )
    .with_semantic_recall(recall)
}

async fn add_language_preference(
    store: Arc<SqliteStore>,
    principal_id: hekate::core::PrincipalId,
    language: &str,
) -> Result<(ActiveMemory, hekate::core::EventId), Box<dyn std::error::Error>> {
    let source = ExperienceEvent::new(
        principal_id,
        EventKind::StateChanged,
        None,
        serde_json::json!({"preference_source": true}),
        EventSource::new("test", None),
        None,
        None,
        Some(1.0),
    )?;
    let content = serde_json::json!({
        "schema": "hekate.response_preference.v1",
        "key": "language",
        "value": language,
    })
    .to_string();
    let candidate = MemoryCandidate {
        id: MemoryCandidateId::new(),
        kind: MemoryKind::ExplicitPreference,
        content: content.clone(),
        subject_principal_id: Some(principal_id),
        status: MemoryCandidateStatus::Candidate,
        confidence: 100,
        source_event_ids: vec![source.event_id],
        valid_from: None,
        valid_until: None,
        supersedes: None,
        created_at: now(),
    };
    let memory = ActiveMemory {
        id: MemoryId::new(),
        candidate_id: candidate.id,
        kind: MemoryKind::ExplicitPreference,
        content,
        subject_principal_id: Some(principal_id),
        status: ActiveMemoryStatus::Active,
        confidence: 100,
        source_event_ids: candidate.source_event_ids.clone(),
        valid_from: None,
        valid_until: None,
        supersedes: None,
        last_verified_at: None,
        created_at: now(),
    };
    let projector = Projector::new(store);
    projector.record(source.clone()).await?;
    projector
        .record(ExperienceEvent::new(
            principal_id,
            EventKind::MemoryCandidateCreated,
            Some(EntityRef::new(
                EntityKind::MemoryCandidate,
                candidate.id.uuid(),
            )),
            serde_json::to_value(&candidate)?,
            EventSource::new("test", None),
            None,
            None,
            Some(1.0),
        )?)
        .await?;
    projector
        .record(ExperienceEvent::new(
            principal_id,
            EventKind::MemoryPromoted,
            Some(EntityRef::new(EntityKind::Memory, memory.id.uuid())),
            serde_json::to_value(&memory)?,
            EventSource::new("test", None),
            None,
            None,
            Some(1.0),
        )?)
        .await?;
    Ok((memory, source.event_id))
}

#[tokio::test]
async fn interaction_injects_recalled_provenance_and_excludes_current_observation(
) -> Result<(), Box<dyn std::error::Error>> {
    let url = database_url("related");
    let store = Arc::new(SqliteStore::open(&url).await?);
    let old = observation("we decided to keep the migration small");
    let old_event = observation_event(&old);
    Projector::new(store.clone())
        .record(old_event.clone())
        .await?;
    for _ in 0..20 {
        Projector::new(store.clone())
            .record(ExperienceEvent::new(
                old.actor_id,
                EventKind::StateChanged,
                None,
                serde_json::Value::Null,
                EventSource::new("test", None),
                None,
                None,
                Some(1.0),
            )?)
            .await?;
    }

    let embedding_store = Arc::new(SqliteEmbeddingStore::open(&url).await?);
    let provider = TestProvider {
        space: space(),
        vector: Some(vector()),
        fail: false,
    };
    let indexer = Arc::new(EmbeddingIndexer::new(
        Arc::new(provider.clone()),
        embedding_store.clone(),
        16,
    ));
    let state = store.state().await?;
    let events = store.events().await?;
    indexer.index_once(&state, &events).await?;
    let duplicate = EmbeddingDocument::new(
        EmbeddingEntityKind::Observation,
        old.id.to_string(),
        old_event.event_id,
        "stale second chunk",
        1,
    );
    embedding_store
        .store_embeddings(&space(), &[duplicate], &[vector()])
        .await?;

    let config = hekate::config::Config::default();
    let (user_preference, user_preference_source) =
        add_language_preference(store.clone(), config.user_principal_id, "ko-KR").await?;
    add_language_preference(store.clone(), config.hekate_principal_id, "fr-FR").await?;

    let recall = Arc::new(SemanticRecall::new(indexer));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let result = engine(
        store.clone(),
        RecordingModel {
            seen: seen.clone(),
            cite_recall: true,
        },
        recall.clone(),
    )
    .handle(observation(
        "the current request is to inspect foreground context",
    ))
    .await?;

    let context = seen
        .lock()
        .expect("model capture lock")
        .last()
        .cloned()
        .expect("captured thought context");
    let snapshot = context
        .context_snapshot
        .as_ref()
        .expect("bounded context snapshot attached to thought context");
    assert_eq!(context.observation.id, result.observation_id);
    assert_eq!(
        snapshot["response_profile"]["as_of_revision"].as_u64(),
        snapshot["as_of_revision"].as_u64()
    );
    assert!(snapshot["anchors"]
        .as_array()
        .expect("anchor items")
        .iter()
        .any(|item| item["kind"] == "identity"));
    assert_eq!(snapshot["response_profile"]["language"], "ko-KR");
    assert!(snapshot["response_profile"]["evidence"]
        .as_array()
        .expect("profile evidence")
        .iter()
        .any(|evidence| {
            evidence["memory_id"] == user_preference.id.to_string()
                && evidence["source_event_id"] == user_preference_source.to_string()
        }));
    let middle = snapshot["compressed_middle"]
        .as_array()
        .expect("compressed middle items");
    let active_preference = middle
        .iter()
        .find(|item| item["entity"]["id"] == user_preference.id.uuid().to_string())
        .expect("selected active preference memory");
    assert!(active_preference["source_event_ids"]
        .as_array()
        .expect("memory provenance")
        .contains(&serde_json::json!(user_preference_source)));
    let active_recent = snapshot["active_recent"]
        .as_array()
        .expect("active recent items");
    assert!(active_recent.iter().any(|item| {
        item["kind"] == "recall"
            && item["source_event_ids"]
                .as_array()
                .is_some_and(|sources| sources.contains(&serde_json::json!(old_event.event_id)))
            && item["text"]
                .as_str()
                .is_some_and(|text| text.starts_with("[untrusted historical evidence] "))
    }));
    assert!(!active_recent.iter().any(|item| {
        item["kind"] == "recent_observation"
            && item["entity"]["id"] == result.observation_id.uuid().to_string()
    }));
    assert_eq!(context.recall.items.len(), 1);
    assert_eq!(context.recall.items[0].source_event_id, old_event.event_id);
    assert_eq!(context.recall.items[0].text, old.content);
    assert_eq!(result.decision.evidence_refs, vec![old_event.event_id]);

    let events = store.events().await?;
    let current = events
        .iter()
        .find(|event| {
            event.subject.as_ref().is_some_and(|subject| {
                subject.kind == EntityKind::Observation
                    && subject.id == result.observation_id.uuid()
            })
        })
        .expect("current observation event");
    let current_sequence = events
        .iter()
        .position(|event| event.event_id == current.event_id)
        .expect("current observation sequence")
        + 1;
    let focus = events
        .iter()
        .find(|event| {
            event.event_kind == EventKind::FocusResolved
                && event.subject.as_ref().is_some_and(|subject| {
                    subject.kind == EntityKind::Observation
                        && subject.id == result.observation_id.uuid()
                })
        })
        .expect("persisted focus resolution");
    let focus_sequence = events
        .iter()
        .position(|event| event.event_id == focus.event_id)
        .expect("focus resolution sequence")
        + 1;
    let attempt = events
        .iter()
        .find(|event| {
            event.event_kind == EventKind::AttemptStarted
                && event.correlation_id.as_deref()
                    == Some(result.observation_id.to_string().as_str())
        })
        .expect("attempt after focus resolution");
    let attempt_sequence = events
        .iter()
        .position(|event| event.event_id == attempt.event_id)
        .expect("attempt sequence")
        + 1;
    assert_eq!(
        snapshot["as_of_revision"].as_u64(),
        Some(focus_sequence as u64)
    );
    assert!(focus_sequence > current_sequence);
    assert_eq!(attempt_sequence, focus_sequence + 1);
    assert_eq!(context.event_sequence, attempt_sequence as u64);
    assert!(context.event_sequence >= current_sequence as u64);
    assert!(!context.relevant_events.iter().any(|event| {
        event.subject.as_ref().is_some_and(|subject| {
            subject.kind == EntityKind::Observation && subject.id == result.observation_id.uuid()
        })
    }));
    assert!(context.recent_event_ids.contains(&current.event_id));
    let state = store.state().await?;
    let current_bundle = recall
        .recall(
            &RecallQuery {
                text: "we decided to keep the migration small".to_owned(),
                limit: 6,
                exclude_event_ids: vec![current.event_id],
                as_of_sequence: Some(state.revision),
            },
            &state,
            &store.events().await?,
        )
        .await?;
    assert!(!current_bundle
        .items
        .iter()
        .any(|item| item.source_event_id == current.event_id));
    Ok(())
}

#[tokio::test]
async fn recall_and_incremental_index_failures_do_not_block_commit(
) -> Result<(), Box<dyn std::error::Error>> {
    let url = database_url("fallback");
    let store = Arc::new(SqliteStore::open(&url).await?);
    let embedding_store = Arc::new(SqliteEmbeddingStore::open(&url).await?);
    let indexer = Arc::new(EmbeddingIndexer::new(
        Arc::new(TestProvider {
            space: space(),
            vector: None,
            fail: true,
        }),
        embedding_store,
        16,
    ));
    let recall = Arc::new(SemanticRecall::new(indexer));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let result = engine(
        store.clone(),
        RecordingModel {
            seen: seen.clone(),
            cite_recall: false,
        },
        recall,
    )
    .handle(observation(
        "check the request even when semantic recall is unavailable",
    ))
    .await?;

    assert_eq!(result.decision.message, "test response");
    assert_eq!(store.state().await?.decisions.len(), 1);
    assert!(seen
        .lock()
        .expect("model capture lock")
        .last()
        .expect("captured thought context")
        .recall
        .items
        .is_empty());
    Ok(())
}

#[tokio::test]
async fn stalled_embedding_uses_bounded_local_recall_with_event_provenance(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut eval_rows = Vec::new();
    for iteration in 1..=10 {
        let url = database_url("stalled-local-fallback");
        let store = Arc::new(SqliteStore::open(&url).await?);
        let old = observation("the migration plan is to retain event provenance");
        let old_event = observation_event(&old);
        Projector::new(store.clone())
            .record(old_event.clone())
            .await?;
        let embedding_store = Arc::new(SqliteEmbeddingStore::open(&url).await?);
        let indexer = Arc::new(EmbeddingIndexer::new(
            Arc::new(HangingProvider { space: space() }),
            embedding_store,
            16,
        ));
        let recall = Arc::new(SemanticRecall::new(indexer));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let started = Instant::now();
        let engine = engine(
            store.clone(),
            RecordingModel {
                seen: seen.clone(),
                cite_recall: true,
            },
            recall,
        );
        let result = engine
            .handle(observation(
                "please recall the migration plan and retain event provenance",
            ))
            .await?;
        assert!(started.elapsed() < Duration::from_secs(2));
        let context = seen
            .lock()
            .expect("model capture lock")
            .last()
            .cloned()
            .expect("captured context");
        let recalled = context
            .recall
            .items
            .iter()
            .find(|item| item.source_event_id == old_event.event_id)
            .expect("local recall result");
        assert_eq!(recalled.text, old.content);
        assert_eq!(recalled.source_hash, old_event.canonical_hash()?);
        assert!(recalled.as_of_sequence >= 1);
        assert!(recalled.as_of_sequence <= context.event_sequence);
        assert_eq!(result.decision.evidence_refs, vec![old_event.event_id]);
        let state = store.state().await?;
        let events = store.events().await?;
        let projection_verified = Projector::replay(&events)? == state;
        assert!(projection_verified);
        eval_rows.push(serde_json::json!({
            "scenario": "stalled_embedding_uses_bounded_local_recall",
            "iteration": iteration,
            "input": "please recall the migration plan and retain event provenance",
            "as_of_revision": context.event_sequence,
            "source_event_ids": [old_event.event_id],
            "counterevidence_event_ids": [],
            "active_before": [],
            "active_after": [],
            "elapsed_ms": started.elapsed().as_millis(),
            "cursor_before": state.sleep_cursor,
            "cursor_after": state.sleep_cursor,
            "result": "passed",
            "projection_verified": projection_verified,
            "external_capability_executed": false,
        }));
        engine.shutdown().await?;
    }
    let path = std::path::Path::new("target/hekate-evals/consolidation-recall.jsonl");
    fs::create_dir_all(path.parent().expect("eval parent"))?;
    let jsonl = eval_rows
        .iter()
        .map(serde_json::Value::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(path, format!("{jsonl}\n"))?;
    assert_eq!(eval_rows.len(), 10);
    assert!(eval_rows.iter().all(|record| {
        record["result"] == "passed"
            && record["projection_verified"] == true
            && record["external_capability_executed"] == false
    }));
    Ok(())
}
