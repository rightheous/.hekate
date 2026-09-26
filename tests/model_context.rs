use async_trait::async_trait;
use hekate::{
    adapters::{
        model_transport::{self, ollama, openai_compatible},
        primary_model::PrimaryModel,
        sqlite::SqliteStore,
    },
    config::Config,
    core::{model_io::*, *},
    ports::*,
    runtime::{context::build_context, prompt_budget, recovery::recover, Engine},
};
use serde_json::{json, Value};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc,
    },
    thread,
};

fn mock(responses: Vec<String>) -> (String, mpsc::Receiver<Value>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        for response in responses {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            let mut bytes = Vec::new();
            let mut chunk = [0u8; 4096];
            let (start, length) = loop {
                let n = stream.read(&mut chunk).unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&chunk[..n]);
                if let Some(end) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..end]);
                    let length: usize = headers
                        .lines()
                        .find_map(|s| {
                            s.split_once(':')
                                .filter(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                                .map(|(_, v)| v.trim().parse().unwrap())
                        })
                        .unwrap();
                    if bytes.len() >= end + 4 + length {
                        break (end + 4, length);
                    }
                }
            };
            tx.send(serde_json::from_slice(&bytes[start..start + length]).unwrap())
                .unwrap();
            write!(stream,"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",response.len(),response).unwrap();
        }
    });
    (format!("http://{address}/v1"), rx)
}
fn config(url: String) -> Config {
    Config {
        model_base_url: url,
        model_timeout_seconds: 5,
        model_io: ModelIoConfig {
            context_tokens: Some(8192),
            foreground_max_tokens: 2048,
            sleep_max_tokens: 2048,
            ..Default::default()
        },
        ..Default::default()
    }
}
fn observation(config: &Config, text: &str, message: &str) -> Observation {
    Observation {
        id: ObservationId::new(),
        actor_id: config.user_principal_id,
        content: text.into(),
        source_type: "test".into(),
        source_ref: None,
        thread_id: Some("context-test".into()),
        message_id: Some(message.into()),
        received_at: now(),
    }
}
fn context() -> ThoughtContext {
    let c = Config::default();
    build_context(
        &CurrentState::default(),
        &observation(&c, "한국어 질문 전체", "one"),
        &Focus::unattached(),
        &[],
        vec![],
    )
}
fn cycle(evidence: Vec<EventId>) -> String {
    json!({"draft_interpretation":"질문을 이해했습니다", "draft_initial_judgment":"agree", "draft_reasons":["이유"],"draft_doubts":[],
    "review_strongest_objection":"추가 근거가 필요할 수 있습니다", "review_identity_conflicts":[],"review_unsupported_claims":[],"review_suggested_revision":null,
    "act":"agree","rationale":"검토 결과", "response":"확인했습니다", "confidence":70,"evidence_refs":evidence,"position_change":null,"conflict_change":null}).to_string()
}
fn openai(content: &str, finish: &str) -> String {
    json!({"choices":[{"finish_reason":finish,"message":{"content":content}}],"usage":{"prompt_tokens":123,"completion_tokens":45}}).to_string()
}
fn native(content: &str, reason: &str) -> String {
    json!({"done":true,"done_reason":reason,"message":{"content":content,"thinking":"PRIVATE_THINKING"},"prompt_eval_count":123,"eval_count":45}).to_string()
}

#[test]
fn request_formats_and_lazy_configuration() {
    let mut context = context();
    let current_event = EventId::new();
    context.current_observation_event_id = Some(current_event);
    let mut c = config("http://localhost:9/v1/".into());
    c.model_io.foreground_think = Some(Think::Level("low".into()));
    let model = PrimaryModel::from_config(&c).unwrap();
    let request = model.prepare_foreground(&context, None).unwrap();
    assert!(request.evidence_ids.contains(&current_event));
    assert!(request.messages[1]
        .content
        .contains(&current_event.to_string()));
    let value = openai_compatible::body("test", &request);
    assert_eq!(value["reasoning_effort"], "low");
    assert!(value.get("model_options").is_none());
    assert_eq!(value["max_tokens"], 2048);
    let value = ollama::body("test", &request);
    assert_eq!(value["options"]["num_ctx"], 8192);
    assert_eq!(value["options"]["num_predict"], 2048);
    assert_eq!(value["think"], "low");
    assert_eq!(value["stream"], false);
    let sleep = SleepContext {
        sleep_run_id: SleepRunId::new(),
        high_water_revision: 1,
        seed_observations: vec![],
        recalled_experiences: vec![],
        identity: None,
        active_positions: vec![],
        active_conflicts: vec![],
        relationship: None,
        snapshot_hash: "local-hash".into(),
    };
    let request = model.prepare_sleep(&sleep, None).unwrap();
    let value = openai_compatible::body("test", &request);
    assert_eq!(value["model_options"]["reasoning_effort"], "none");
    assert!(value.get("reasoning_effort").is_none());
    assert!(ollama::body("test", &request).get("think").is_none());
    c.model_io.sleep_think = Some(Think::Enabled(false));
    let request = PrimaryModel::from_config(&c)
        .unwrap()
        .prepare_sleep(&sleep, None)
        .unwrap();
    assert_eq!(ollama::body("test", &request)["think"], false);
    for suffix in ["", "/", "/v1", "/v1/", "/api"] {
        assert_eq!(
            model_transport::endpoint(
                &format!("http://localhost:9{suffix}"),
                TransportKind::Ollama
            )
            .unwrap()
            .path(),
            "/api/chat"
        );
    }
    for url in [
        "ftp://host",
        "http://host/bad",
        "http://user:secret@host",
        "http://host/v1?x=y",
    ] {
        assert!(model_transport::endpoint(url, TransportKind::Ollama).is_err());
    }
    let c: Config = toml::from_str("model_name='old-config'").unwrap();
    let model = PrimaryModel::from_config(&c).unwrap();
    assert!(matches!(
        model.prepare_foreground(&context, None),
        Err(PreparationError::Configuration(_))
    ));
    let c: Config = toml::from_str("[model_io]\ntransport='ollama'\ncontext_tokens=8192\nforeground_think='low'\nsleep_think=false").unwrap();
    assert_eq!(c.model_io.transport, TransportKind::Ollama);
    assert_eq!(c.model_io.sleep_think, Some(Think::Enabled(false)));
}

#[tokio::test]
async fn whole_item_budget_counts_corrections_and_never_sends_overflow() {
    let mut context = context();
    let id = EventId::new();
    context.recall.items.push(RecalledItem {
        entity: EntityRef::new(EntityKind::Observation, uuid::Uuid::new_v4()),
        source_event_id: id,
        source_hash: "not-for-the-model".into(),
        as_of_sequence: 1,
        text: "긴 과거 인용문".repeat(1500),
        score: 1.0,
        created_at: now(),
    });
    let c = config("http://127.0.0.1:9/v1".into());
    let model = PrimaryModel::from_config(&c).unwrap();
    let prepared = model.prepare_foreground(&context, None).unwrap();
    assert_eq!(prepared.budget.excluded_items, 1);
    assert!(!prepared.evidence_ids.contains(&id));
    assert!(prepared.messages[1].content.contains("한국어 질문 전체"));
    assert!(!prepared.messages[1].content.contains("긴 과거 인용문"));
    assert!(prompt_budget::check_prepared(&prepared));
    let with_correction = model
        .prepare_foreground(&context, Some("교정문 전체"))
        .unwrap();
    assert!(with_correction.budget.estimated_input_tokens > prepared.budget.estimated_input_tokens);
    assert_eq!(
        with_correction.messages.last().unwrap().content,
        "교정문 전체"
    );
    context.observation.content = "현재 입력 절대로 자르지 마세요".repeat(2000);
    let error = model.think(&context).await.unwrap_err();
    assert_eq!(
        error.trace().error_kind.as_deref(),
        Some("context_budget_exceeded")
    );
    assert!(error.trace().model_io[0].request_hash.is_empty());
    assert_eq!(error.trace().retries, 0);
    assert!(error.trace().raw_response_hash.is_none());
    assert_eq!(
        context.observation.content,
        "현재 입력 절대로 자르지 마세요".repeat(2000)
    );
}

#[tokio::test]
async fn excluded_evidence_is_rejected_on_both_attempts() {
    let mut context = context();
    let included = EventId::new();
    let excluded = EventId::new();
    for (id, text) in [
        (included, "보존된 인용문".into()),
        (excluded, "누락되어야 하는 긴 인용문".repeat(2000)),
    ] {
        context.recall.items.push(RecalledItem {
            entity: EntityRef::new(EntityKind::Observation, uuid::Uuid::new_v4()),
            source_event_id: id,
            source_hash: String::new(),
            as_of_sequence: 0,
            text,
            score: 1.0,
            created_at: now(),
        });
    }
    context.recent_event_ids.push(excluded);
    let (url, rx) = mock(vec![
        openai(&cycle(vec![excluded]), "stop"),
        openai(&cycle(vec![excluded]), "stop"),
    ]);
    let model = PrimaryModel::from_config(&config(url)).unwrap();
    let prepared = model.prepare_foreground(&context, None).unwrap();
    assert!(prepared.evidence_ids.contains(&included));
    assert!(!prepared.evidence_ids.contains(&excluded));
    let error = model.think(&context).await.unwrap_err();
    assert_eq!(
        error.trace().error_kind.as_deref(),
        Some("invalid_judgment")
    );
    assert_eq!(error.trace().retries, 1);
    assert_eq!(error.trace().model_io.len(), 2);
    for _ in 0..2 {
        let request = rx.recv().unwrap();
        let user: Value =
            serde_json::from_str(request["messages"][1]["content"].as_str().unwrap()).unwrap();
        assert!(!user["allowed_evidence_event_ids"]
            .as_array()
            .unwrap()
            .contains(&json!(excluded)));
    }
    let (url, rx) = mock(vec![openai(&cycle(vec![included]), "stop")]);
    let success = PrimaryModel::from_config(&config(url))
        .unwrap()
        .think(&context)
        .await
        .unwrap();
    assert_eq!(success.commitment.evidence_refs, vec![included]);
    rx.recv().unwrap();
    assert_eq!(success.trace.context_hash, context.snapshot_hash);
    assert_eq!(success.trace.context_sequence, context.event_sequence);
}

#[tokio::test]
async fn response_failures_are_distinct_and_never_use_thinking() {
    for (transport, response, expected) in [
        (TransportKind::OpenaiCompatible, openai(&cycle(vec![]),"length"),"truncated_response"),
        (TransportKind::Ollama, native(&cycle(vec![]),"length"),"truncated_response"),
        (TransportKind::Ollama, native("","stop"),"empty_content"),
        (TransportKind::OpenaiCompatible,json!({"choices":[{"finish_reason":"stop","message":{"content":null,"reasoning_content":cycle(vec![])}}]}).to_string(),"empty_content"),
        (TransportKind::Ollama, "{invalid".into(),"malformed_response"),
        (TransportKind::Ollama, json!({"done":false,"message":{"content":cycle(vec![])}}).to_string(),"incomplete_response"),
        (TransportKind::OpenaiCompatible,openai(&cycle(vec![]),"unknown"),"incomplete_response"),
    ] {
        let (url,rx) = mock(vec![response]); let mut c = config(url); c.model_io.transport=transport;
        let error = PrimaryModel::from_config(&c).unwrap().think(&context()).await.unwrap_err(); rx.recv().unwrap();
        assert_eq!(error.trace().error_kind.as_deref(),Some(expected)); assert_eq!(error.trace().retries,0);
        assert!(error.trace().commitment.is_none());
        let diagnostics = serde_json::to_string(&error.trace().model_io).unwrap();
        assert!(!diagnostics.contains("PRIVATE_THINKING")); assert!(!diagnostics.contains("한국어 질문"));
        for message in &error.trace().parse_errors {assert!(message.len()<=512);}
    }
    let (url, rx) = mock(vec![
        openai("not JSON", "stop"),
        openai("still not JSON", "stop"),
    ]);
    let error = PrimaryModel::from_config(&config(url))
        .unwrap()
        .think(&context())
        .await
        .unwrap_err();
    assert_eq!(
        error.trace().error_kind.as_deref(),
        Some("thought_cycle_json_error")
    );
    assert_eq!(error.trace().retries, 1);
    rx.recv().unwrap();
    rx.recv().unwrap();
    let mut old = serde_json::to_value(error.trace()).unwrap();
    old.as_object_mut().unwrap().remove("model_io");
    assert!(serde_json::from_value::<CognitiveTrace>(old)
        .unwrap()
        .model_io
        .is_empty());
}

struct Denied(Arc<AtomicUsize>);
#[async_trait]
impl CapabilityCatalog for Denied {
    fn names(&self) -> Vec<String> {
        vec![]
    }
    async fn execute(&self, _: &str, _: Value) -> Result<CapabilityResult, CapabilityError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(CapabilityError::Execution("blocked".into()))
    }
}
struct DenyPolicy;
impl Policy for DenyPolicy {
    fn evaluate(&self, _: PrincipalId, _: &ActionIntent) -> Result<PolicyDecision, PolicyError> {
        Ok(PolicyDecision {
            allowed: false,
            requires_approval: true,
            reason: "blocked".into(),
        })
    }
}

#[tokio::test]
async fn engine_native_failure_retry_replay_and_sleep() {
    let sleep = json!({"draft_summary":"검토 완료", "self_review":{"weak_points":[],"possible_counterevidence":[],"revised":false},"candidates":[]}).to_string();
    let (url, rx) = mock(vec![
        native(&cycle(vec![]), "length"),
        native(&cycle(vec![]), "stop"),
        native(&sleep, "stop"),
    ]);
    let mut c = config(url);
    c.model_io.transport = TransportKind::Ollama;
    let store = Arc::new(SqliteStore::open("sqlite::memory:").await.unwrap());
    let calls = Arc::new(AtomicUsize::new(0));
    let make_engine = || {
        let model = Arc::new(PrimaryModel::from_config(&c).unwrap());
        Engine::new(
            store.clone(),
            model.clone(),
            Arc::new(DenyPolicy),
            Arc::new(Denied(calls.clone())),
            c.hekate_principal_id,
            c.user_principal_id,
        )
        .with_sleep_model(model)
    };
    let engine = make_engine();
    let obs = observation(&c, "짧은 질문", "same-message");
    let error = engine.handle(obs.clone()).await.unwrap_err();
    assert!(error.to_string().contains("truncated_response"));
    rx.recv().unwrap();
    let state = store.state().await.unwrap();
    assert!(state.decisions.is_empty());
    assert!(state.positions.is_empty());
    assert!(state.conflicts.is_empty());
    assert!(state.active_memories.is_empty());
    assert!(state
        .attempts
        .values()
        .all(|a| a.status == AttemptStatus::Failed));
    assert!(!store.has_active_foreground_lease().await.unwrap());
    drop(engine);
    recover(store.as_ref()).await.unwrap();
    let engine = make_engine();
    let success = engine.handle(obs.clone()).await.unwrap();
    rx.recv().unwrap();
    assert!(!store.has_active_foreground_lease().await.unwrap());
    assert!(store
        .state()
        .await
        .unwrap()
        .attempts
        .values()
        .any(|a| a.status == AttemptStatus::Succeeded));
    let replay = engine.handle(obs).await.unwrap();
    assert_eq!(success.decision.id, replay.decision.id);
    assert_eq!(store.state().await.unwrap().decisions.len(), 1);
    let slept = engine.sleep_once().await.unwrap();
    assert!(matches!(
        slept.status,
        hekate::runtime::SleepOnceStatus::Completed
    ));
    let sleep_request = rx.recv().unwrap();
    assert_eq!(sleep_request["options"]["num_predict"], 2048);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    recover(store.as_ref()).await.unwrap();
    assert_eq!(store.state().await.unwrap().decisions.len(), 1);
}
