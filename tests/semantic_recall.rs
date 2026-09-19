use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use hekate::adapters::local_policy::LocalPolicy;
use hekate::adapters::sqlite::{SqliteEmbeddingStore, SqliteStore};
use hekate::core::{
    now, CognitiveTrace, CommittedJudgment, DecisionKind, EmbeddingDocument, EmbeddingEntityKind,
    EmbeddingSpace, EmbeddingVector, EntityKind, EntityRef, EventKind, EventSource,
    ExperienceEvent, Observation, ObservationId, RecallQuery, SelfReview, ThoughtContext,
    ThoughtCycle, ThoughtDraft,
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
    .handle(observation("we decided to keep the migration small"))
    .await?;

    let context = seen
        .lock()
        .expect("model capture lock")
        .last()
        .cloned()
        .expect("captured thought context");
    assert_eq!(context.recall.items.len(), 1);
    assert_eq!(context.recall.items[0].source_event_id, old_event.event_id);
    assert_eq!(context.recall.items[0].text, old.content);
    assert_eq!(result.decision.evidence_refs, vec![old_event.event_id]);

    let current = store
        .events()
        .await?
        .into_iter()
        .find(|event| {
            event.subject.as_ref().is_some_and(|subject| {
                subject.kind == EntityKind::Observation
                    && subject.id == result.observation_id.uuid()
            })
        })
        .expect("current observation event");
    let current_bundle = recall
        .recall(
            &RecallQuery {
                text: "we decided to keep the migration small".to_owned(),
                limit: 6,
                exclude_event_ids: vec![current.event_id],
            },
            &store.state().await?,
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
        "continue even when semantic recall is unavailable",
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
