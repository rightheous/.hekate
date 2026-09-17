use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;

use async_trait::async_trait;
use hekate::adapters::embedding::OpenAiEmbeddingAdapter;
use hekate::adapters::sqlite::{SqliteEmbeddingStore, SqliteStore};
use hekate::bootstrap::build_engine;
use hekate::config::Config;
use hekate::core::{
    now, EmbeddingDocument, EmbeddingEntityKind, EmbeddingSpace, EmbeddingValidationError,
    EmbeddingVector, EntityKind, EntityRef, EventKind, EventSource, ExperienceEvent, Observation,
    ObservationId, DEFAULT_DOCUMENT_PREFIX, DEFAULT_QUERY_PREFIX,
};
use hekate::ports::{EmbeddingProvider, EmbeddingProviderError};
use hekate::runtime::embedding_indexer::EmbeddingIndexer;
use hekate::runtime::Projector;

fn space() -> EmbeddingSpace {
    EmbeddingSpace::new(
        "openai-compatible",
        "embeddinggemma",
        "test-768",
        768,
        true,
        DEFAULT_QUERY_PREFIX,
        DEFAULT_DOCUMENT_PREFIX,
    )
}

fn unit_vector(index: usize) -> Vec<f32> {
    let mut vector = vec![0.0; 768];
    vector[index] = 1.0;
    vector
}

fn server(body: String) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
    let address = listener.local_addr().expect("address");
    std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("request");
        let mut request = [0_u8; 8192];
        let _ = stream.read(&mut request).expect("read request");
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream
            .write_all(response.as_bytes())
            .expect("write response");
    });
    format!("http://{address}/v1/embeddings")
}

#[tokio::test]
async fn provider_orders_valid_768_vectors_and_rejects_invalid_vectors() {
    let body = serde_json::json!({
        "data": [
            {"index": 1, "embedding": unit_vector(1)},
            {"index": 0, "embedding": unit_vector(0)}
        ]
    })
    .to_string();
    let adapter = OpenAiEmbeddingAdapter::new(server(body), space(), 5).expect("adapter");
    let documents = vec![
        EmbeddingDocument::new(
            EmbeddingEntityKind::Observation,
            "one",
            hekate::core::EventId::new(),
            "first",
            0,
        ),
        EmbeddingDocument::new(
            EmbeddingEntityKind::Observation,
            "two",
            hekate::core::EventId::new(),
            "second",
            0,
        ),
    ];
    let vectors = adapter.embed_documents(&documents).await.expect("vectors");
    assert_eq!(vectors.len(), 2);
    assert_eq!(vectors[0].values()[0], 1.0);
    assert_eq!(vectors[1].values()[1], 1.0);
    assert!(matches!(
        EmbeddingVector::new(vec![1.0], &space()),
        Err(EmbeddingValidationError::Dimensions { .. })
    ));
    assert!(matches!(
        EmbeddingVector::new(vec![f32::NAN; 768], &space()),
        Err(EmbeddingValidationError::NonFinite)
    ));
    assert!(matches!(
        EmbeddingVector::new(vec![0.0; 768], &space()),
        Err(EmbeddingValidationError::Zero)
    ));
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

fn temp_database(label: &str) -> String {
    let name = format!("hekate-embedding-{label}-{}.db", uuid::Uuid::new_v4());
    format!("sqlite://{}", std::env::temp_dir().join(name).display())
}

#[tokio::test]
async fn indexes_once_searches_after_restart_and_isolates_unavailable_endpoint() {
    let url = temp_database("lifecycle");
    let canonical = Arc::new(SqliteStore::open(&url).await.expect("canonical store"));
    let observation = Observation {
        id: ObservationId::new(),
        actor_id: Config::default().user_principal_id,
        content: "a durable observation for semantic search".to_owned(),
        source_type: "test".to_owned(),
        source_ref: None,
        thread_id: None,
        message_id: None,
        received_at: now(),
    };
    let event = ExperienceEvent::new(
        observation.actor_id,
        EventKind::ObservationRecorded,
        Some(EntityRef::new(
            EntityKind::Observation,
            observation.id.uuid(),
        )),
        serde_json::to_value(&observation).expect("payload"),
        EventSource::new("test", None),
        None,
        None,
        None,
    )
    .expect("event");
    Projector::new(canonical.clone())
        .record(event.clone())
        .await
        .expect("record observation");
    let state = canonical.state().await.expect("state");
    let events = canonical.events().await.expect("events");
    let vector = EmbeddingVector::new(unit_vector(0), &space()).expect("vector");
    let provider = FixedProvider {
        space: space(),
        vector,
    };
    let store = Arc::new(
        SqliteEmbeddingStore::open(&url)
            .await
            .expect("embedding store"),
    );
    let indexer = EmbeddingIndexer::new(Arc::new(provider.clone()), store.clone(), 16);
    let first = indexer
        .index_once(&state, &events)
        .await
        .expect("first index");
    assert_eq!(first.discovered, 1);
    assert_eq!(first.embedded, 1);
    let second = indexer
        .index_once(&state, &events)
        .await
        .expect("second index");
    assert_eq!(second.embedded, 0);
    assert_eq!(second.skipped, 1);
    drop(indexer);
    drop(store);

    let restarted = Arc::new(
        SqliteEmbeddingStore::open(&url)
            .await
            .expect("restarted store"),
    );
    let indexer = EmbeddingIndexer::new(Arc::new(provider), restarted, 16);
    let matches = indexer
        .search("durable observation", 10)
        .await
        .expect("search");
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].source_event_id, event.event_id);

    let mut config = Config::default();
    config.database_url = temp_database("unavailable");
    config.workspace_root = std::env::current_dir().expect("workspace root");
    config.embedding_url = "http://127.0.0.1:1/v1/embeddings".to_owned();
    let engine = build_engine(&config)
        .await
        .expect("bootstrap without endpoint");
    assert!(
        engine
            .recovery_report()
            .await
            .expect("recovery without endpoint")
            .projection_verified
    );
    engine.shutdown().await.expect("shutdown");
}
