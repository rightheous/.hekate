use std::fs;
use std::process::Command;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use hekate::adapters::local_policy::LocalPolicy;
use hekate::adapters::sqlite::SqliteStore;
use hekate::config::Config;
use hekate::core::{
    now, Approval, ApprovalId, ApprovalStatus, Attempt, AttemptId, AttemptStatus, CognitiveTrace,
    CommittedJudgment, DecisionKind, EntityKind, EntityRef, EventKind, EventSource,
    ExperienceEvent, IntegrationCandidateKind, Observation, ObservationId, Operation, OperationId,
    OperationStatus, Run, RunId, RunStatus, SelfReview, SleepContext, SleepDeliberation,
    SleepSelfReview, ThoughtContext, ThoughtCycle, ThoughtDraft,
};
use hekate::ports::{
    CapabilityCatalog, CapabilityError, CapabilityResult, CognitiveError, CognitiveModel,
    SleepCognitiveError, SleepCognitiveModel, Storage,
};
use hekate::runtime::{Engine, EngineError, Projector, SleepOnceStatus};
use tokio::sync::Notify;
use uuid::Uuid;

struct NoCapabilities {
    calls: Arc<AtomicUsize>,
}

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
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(CapabilityError::Execution(
            "unused test capability".to_owned(),
        ))
    }
}

struct ForegroundModel {
    calls: Arc<AtomicUsize>,
    timeout_first: bool,
    started: Option<Arc<Notify>>,
    release: Option<Arc<Notify>>,
}

impl ForegroundModel {
    fn normal() -> (Arc<Self>, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        (
            Arc::new(Self {
                calls: calls.clone(),
                timeout_first: false,
                started: None,
                release: None,
            }),
            calls,
        )
    }

    fn timeout_first() -> (Arc<Self>, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        (
            Arc::new(Self {
                calls: calls.clone(),
                timeout_first: true,
                started: None,
                release: None,
            }),
            calls,
        )
    }

    fn blocked(started: Arc<Notify>, release: Arc<Notify>) -> (Arc<Self>, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        (
            Arc::new(Self {
                calls: calls.clone(),
                timeout_first: false,
                started: Some(started),
                release: Some(release),
            }),
            calls,
        )
    }
}

#[async_trait]
impl CognitiveModel for ForegroundModel {
    async fn think(&self, context: &ThoughtContext) -> Result<ThoughtCycle, CognitiveError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some(started) = &self.started {
            started.notify_one();
        }
        if let Some(release) = &self.release {
            release.notified().await;
        }
        if self.timeout_first && call == 1 {
            return Err(CognitiveError::Timeout {
                trace: trace(context.event_sequence, &context.snapshot_hash, "timeout"),
            });
        }
        let commitment = CommittedJudgment {
            final_act: DecisionKind::Agree,
            reasons: vec!["test decision".to_owned()],
            response: "test response".to_owned(),
            confidence: 90,
            unresolved_questions: Vec::new(),
            alternatives: Vec::new(),
            reconsideration_conditions: Vec::new(),
            evidence_refs: Vec::new(),
            related_position_ids: Vec::new(),
            position: None,
            user_position: None,
            conflict: None,
            action: None,
        };
        Ok(ThoughtCycle {
            draft: ThoughtDraft {
                interpretation: "test interpretation".to_owned(),
                initial_judgment: DecisionKind::Agree,
                reasons: vec!["test reason".to_owned()],
                uncertainties: Vec::new(),
                initial_intent: "respond".to_owned(),
            },
            review: SelfReview {
                strongest_counterargument: "none".to_owned(),
                value_conflicts: Vec::new(),
                unsupported_claims: Vec::new(),
                revision_direction: None,
            },
            trace: CognitiveTrace {
                commitment: Some(commitment.clone()),
                ..trace(context.event_sequence, &context.snapshot_hash, "succeeded")
            },
            commitment,
        })
    }
}

struct SleepModel {
    calls: Arc<AtomicUsize>,
    started: Option<Arc<Notify>>,
    release: Option<Arc<Notify>>,
}

impl SleepModel {
    fn normal() -> (Arc<Self>, Arc<AtomicUsize>) {
        Self::new(None, None)
    }

    fn blocked(started: Arc<Notify>, release: Arc<Notify>) -> (Arc<Self>, Arc<AtomicUsize>) {
        Self::new(Some(started), Some(release))
    }

    fn new(
        started: Option<Arc<Notify>>,
        release: Option<Arc<Notify>>,
    ) -> (Arc<Self>, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        (
            Arc::new(Self {
                calls: calls.clone(),
                started,
                release,
            }),
            calls,
        )
    }
}

#[async_trait]
impl SleepCognitiveModel for SleepModel {
    async fn deliberate_sleep(
        &self,
        context: &SleepContext,
    ) -> Result<SleepDeliberation, SleepCognitiveError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(started) = &self.started {
            started.notify_one();
        }
        if let Some(release) = &self.release {
            release.notified().await;
        }
        let source_event_id = context
            .seed_observations
            .first()
            .expect("sleep seed")
            .event_id;
        Ok(SleepDeliberation {
            draft_summary: "test summary".to_owned(),
            self_review: SleepSelfReview {
                weak_points: Vec::new(),
                possible_counterevidence_event_ids: Vec::new(),
                revised: false,
            },
            candidates: vec![hekate::core::IntegrationCandidateDraft {
                kind: IntegrationCandidateKind::Memory,
                content: "durable test association".to_owned(),
                rationale: "repeated supported pattern".to_owned(),
                source_event_ids: vec![source_event_id],
                counterevidence_event_ids: Vec::new(),
                confidence: 80,
            }],
            trace: None,
        })
    }
}

fn trace(sequence: u64, hash: &str, outcome: &str) -> CognitiveTrace {
    CognitiveTrace {
        trace_id: Uuid::new_v4().to_string(),
        outcome: outcome.to_owned(),
        provider: "test".to_owned(),
        model: "fake".to_owned(),
        schema_version: "test".to_owned(),
        context_sequence: sequence,
        context_hash: hash.to_owned(),
        referenced_event_ids: Vec::new(),
        draft: None,
        review: None,
        commitment: None,
        parse_errors: Vec::new(),
        retries: 0,
        elapsed_ms: 0,
        raw_response_hash: None,
        error_kind: (outcome == "timeout").then(|| "timeout".to_owned()),
        created_at: now(),
    }
}

fn database_url(label: &str) -> String {
    format!(
        "sqlite://{}",
        std::env::temp_dir()
            .join(format!(
                "hekate-foreground-sleep-{label}-{}.db",
                Uuid::new_v4()
            ))
            .display()
    )
}

fn observation(content: &str) -> Observation {
    Observation {
        id: ObservationId::new(),
        actor_id: Config::default().user_principal_id,
        content: content.to_owned(),
        source_type: "test-chat".to_owned(),
        source_ref: Some("thread-1".to_owned()),
        thread_id: Some("thread-1".to_owned()),
        message_id: None,
        received_at: now(),
    }
}

fn message_observation(content: &str, message_id: &str) -> Observation {
    Observation {
        message_id: Some(message_id.to_owned()),
        ..observation(content)
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
    foreground: Arc<dyn CognitiveModel>,
    sleep: Arc<dyn SleepCognitiveModel>,
    capability_calls: Arc<AtomicUsize>,
) -> Engine {
    let config = Config::default();
    Engine::new(
        store,
        foreground,
        Arc::new(LocalPolicy),
        Arc::new(NoCapabilities {
            calls: capability_calls,
        }),
        config.hekate_principal_id,
        config.user_principal_id,
    )
    .with_sleep_model(sleep)
}

fn unused_calls() -> Arc<AtomicUsize> {
    Arc::new(AtomicUsize::new(0))
}

#[tokio::test]
async fn normal_decision_closes_attempt_and_sleep_runs_with_open_run() {
    let url = database_url("success");
    let store = Arc::new(SqliteStore::open(&url).await.expect("store"));
    let (foreground, _) = ForegroundModel::normal();
    let (sleep, sleep_calls) = SleepModel::normal();
    let engine = engine(store.clone(), foreground, sleep, unused_calls());

    let result = engine
        .handle(observation("keep the foreground run open"))
        .await
        .expect("foreground response");
    assert!(matches!(result.decision.kind, DecisionKind::Agree));
    let state = store.state().await.expect("state");
    assert!(matches!(
        state.runs.values().next().expect("run").status,
        RunStatus::Running
    ));
    assert_eq!(state.attempts.len(), 1);
    assert!(matches!(
        state.attempts.values().next().expect("attempt").status,
        AttemptStatus::Succeeded
    ));

    let slept = engine.sleep_once().await.expect("sleep");
    assert!(matches!(slept.status, SleepOnceStatus::Completed));
    assert_eq!(sleep_calls.load(Ordering::SeqCst), 1);
    assert!(
        engine
            .recovery_report()
            .await
            .expect("recovery")
            .projection_verified
    );
}

#[tokio::test]
async fn foreground_timeout_fails_attempt_restart_sleep_and_message_retry_reuses_events() {
    let url = database_url("retry");
    let first_store = Arc::new(SqliteStore::open(&url).await.expect("store"));
    let (foreground, model_calls) = ForegroundModel::timeout_first();
    let (first_sleep, _) = SleepModel::normal();
    let first_engine = engine(
        first_store.clone(),
        foreground.clone(),
        first_sleep,
        unused_calls(),
    );
    let observation = message_observation("retry this message", "message-17");
    assert!(matches!(
        first_engine.handle(observation.clone()).await,
        Err(EngineError::Cognitive(CognitiveError::Timeout { .. }))
    ));
    let after_timeout = first_store.state().await.expect("failed state");
    assert_eq!(after_timeout.observations.len(), 1);
    assert!(matches!(
        after_timeout
            .attempts
            .values()
            .next()
            .expect("failed attempt")
            .status,
        AttemptStatus::Failed
    ));
    drop(first_engine);
    drop(first_store);

    let restarted_store = Arc::new(SqliteStore::open(&url).await.expect("reopened store"));
    let (sleep, sleep_calls) = SleepModel::normal();
    let restarted = engine(restarted_store.clone(), foreground, sleep, unused_calls());
    let recovery = restarted.recovery_report().await.expect("replay recovery");
    assert!(recovery.projection_verified);
    assert!(recovery.incomplete_attempts.is_empty());
    let sleep_result = restarted.sleep_once().await.expect("sleep after restart");
    assert!(matches!(sleep_result.status, SleepOnceStatus::Completed));
    assert_eq!(sleep_calls.load(Ordering::SeqCst), 1);

    let retried = restarted
        .handle(observation.clone())
        .await
        .expect("retry without a committed decision");
    let repeated = restarted
        .handle(observation)
        .await
        .expect("return committed decision");
    assert_eq!(retried.decision.id, repeated.decision.id);
    assert_eq!(model_calls.load(Ordering::SeqCst), 2);
    let events = restarted_store.events().await.expect("events");
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_kind == EventKind::ObservationRecorded)
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_kind == EventKind::DecisionCreated)
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_kind == EventKind::AttemptStarted)
            .count(),
        2
    );
    assert!(
        restarted
            .recovery_report()
            .await
            .expect("final replay")
            .projection_verified
    );
}

#[tokio::test]
async fn live_lease_defers_sleep_across_connections_and_release_is_owner_scoped() {
    let url = database_url("live-lease");
    let foreground_store = Arc::new(SqliteStore::open(&url).await.expect("foreground store"));
    let sleep_store = Arc::new(SqliteStore::open(&url).await.expect("sleep store"));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (foreground, _) = ForegroundModel::blocked(started.clone(), release.clone());
    let (sleep, sleep_calls) = SleepModel::normal();
    let foreground_engine = Arc::new(engine(
        foreground_store.clone(),
        foreground,
        SleepModel::normal().0,
        unused_calls(),
    ));
    let input = observation("hold foreground while sleep checks");
    let foreground_task = {
        let engine = foreground_engine.clone();
        tokio::spawn(async move { engine.handle(input).await })
    };
    started.notified().await;

    let sleep_engine = engine(
        sleep_store.clone(),
        ForegroundModel::normal().0,
        sleep,
        unused_calls(),
    );
    assert!(sleep_engine.foreground_active().await.expect("lease check"));
    let result = sleep_engine.sleep_once().await.expect("deferred sleep");
    assert!(matches!(result.status, SleepOnceStatus::Deferred));
    assert_eq!(sleep_calls.load(Ordering::SeqCst), 0);

    release.notify_one();
    foreground_task
        .await
        .expect("foreground task")
        .expect("foreground response");
    assert!(!sleep_engine
        .foreground_active()
        .await
        .expect("released lease"));
}

#[tokio::test]
async fn expired_lease_is_reacquired_without_old_owner_clearing_new_owner() {
    let url = database_url("expired-lease");
    let first = SqliteStore::open(&url).await.expect("first store");
    let second = SqliteStore::open(&url).await.expect("second store");
    assert!(first
        .acquire_foreground_lease("old-owner", Duration::from_millis(10))
        .await
        .expect("old lease"));
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!second
        .has_active_foreground_lease()
        .await
        .expect("expired lease"));
    assert!(second
        .acquire_foreground_lease("new-owner", Duration::from_secs(5))
        .await
        .expect("new lease"));
    first
        .release_foreground_lease("old-owner")
        .await
        .expect("old owner release");
    assert!(first
        .has_active_foreground_lease()
        .await
        .expect("new owner still active"));
    second
        .release_foreground_lease("new-owner")
        .await
        .expect("new owner release");
}

#[tokio::test]
async fn sleep_ignores_open_run_stale_attempt_pending_approval_and_unknown_operation() {
    let url = database_url("stale-foreground-state");
    let store = Arc::new(SqliteStore::open(&url).await.expect("store"));
    let projector = Projector::new(store.clone());
    let seed = observation("sleep despite stale foreground records");
    projector
        .record(observation_event(&seed))
        .await
        .expect("seed observation");
    let run = Run {
        id: RunId::new(),
        task_id: None,
        status: RunStatus::Running,
        started_at: now(),
        completed_at: None,
    };
    projector
        .record(event_for(
            Config::default().user_principal_id,
            EventKind::RunStarted,
            EntityKind::Run,
            run.id.uuid(),
            &run,
        ))
        .await
        .expect("open run");
    let attempt = Attempt {
        id: AttemptId::new(),
        run_id: run.id,
        status: AttemptStatus::Started,
        started_at: now(),
        finished_at: None,
    };
    projector
        .record(event_for(
            Config::default().user_principal_id,
            EventKind::AttemptStarted,
            EntityKind::Attempt,
            attempt.id.uuid(),
            &attempt,
        ))
        .await
        .expect("stale attempt");
    let operation_id = OperationId::new();
    let approval = Approval {
        id: ApprovalId::new(),
        intent_id: hekate::core::ActionIntentId::new(),
        operation_id,
        requested_by: Config::default().hekate_principal_id,
        status: ApprovalStatus::Pending,
        reason: "test approval".to_owned(),
        resolved_by: None,
        resolved_at: None,
        created_at: now(),
    };
    projector
        .record(event_for(
            Config::default().hekate_principal_id,
            EventKind::ApprovalRequested,
            EntityKind::Approval,
            approval.id.uuid(),
            &approval,
        ))
        .await
        .expect("pending approval");
    let operation = Operation {
        id: operation_id,
        intent_id: approval.intent_id,
        status: OperationStatus::Unknown,
        idempotency_key: "unknown-op".to_owned(),
        approval_id: Some(approval.id),
        started_at: Some(now()),
        finished_at: None,
    };
    projector
        .record(event_for(
            Config::default().hekate_principal_id,
            EventKind::OperationStateUnknown,
            EntityKind::Operation,
            operation.id.uuid(),
            &operation,
        ))
        .await
        .expect("unknown operation");

    let capability_calls = unused_calls();
    let (sleep, sleep_calls) = SleepModel::normal();
    let engine = engine(
        store.clone(),
        ForegroundModel::normal().0,
        sleep,
        capability_calls.clone(),
    );
    let result = engine.sleep_once().await.expect("sleep with stale records");
    assert!(matches!(result.status, SleepOnceStatus::Completed));
    assert_eq!(sleep_calls.load(Ordering::SeqCst), 1);
    assert_eq!(capability_calls.load(Ordering::SeqCst), 0);
    let state = store.state().await.expect("state after sleep");
    assert!(matches!(
        state.attempts[&attempt.id].status,
        AttemptStatus::Started
    ));
    assert!(matches!(
        state.approvals[&approval.id].status,
        ApprovalStatus::Pending
    ));
    assert!(matches!(
        state.operations[&operation.id].status,
        OperationStatus::Unknown
    ));
    assert!(
        engine
            .recovery_report()
            .await
            .expect("replay")
            .projection_verified
    );
}

#[tokio::test]
async fn foreground_input_interrupts_sleep_without_advancing_cursor_or_candidates() {
    let url = database_url("concurrent");
    let sleep_store = Arc::new(SqliteStore::open(&url).await.expect("sleep store"));
    let seed = observation("seed before concurrent foreground input");
    Projector::new(sleep_store.clone())
        .record(observation_event(&seed))
        .await
        .expect("seed");
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (sleep, sleep_calls) = SleepModel::blocked(started.clone(), release.clone());
    let sleep_engine = Arc::new(engine(
        sleep_store.clone(),
        ForegroundModel::normal().0,
        sleep,
        unused_calls(),
    ));
    let sleep_task = {
        let engine = sleep_engine.clone();
        tokio::spawn(async move { engine.sleep_once().await })
    };
    started.notified().await;

    let foreground_store = Arc::new(SqliteStore::open(&url).await.expect("foreground store"));
    let (foreground, _) = ForegroundModel::normal();
    let foreground_engine = engine(
        foreground_store.clone(),
        foreground,
        SleepModel::normal().0,
        unused_calls(),
    );
    foreground_engine
        .handle(observation("foreground arrives during sleep"))
        .await
        .expect("foreground response");
    release.notify_one();
    let result = sleep_task
        .await
        .expect("sleep task")
        .expect("interrupted sleep");
    assert!(matches!(result.status, SleepOnceStatus::Interrupted));
    assert_eq!(sleep_calls.load(Ordering::SeqCst), 1);
    assert_eq!(result.cursor_after, None);
    let state = sleep_store.state().await.expect("state");
    assert_eq!(
        state.sleep_cursor,
        result.cursor_before.expect("cursor before")
    );
    assert!(state.integration_candidates.is_empty());
    assert!(
        sleep_engine
            .recovery_report()
            .await
            .expect("replay")
            .projection_verified
    );
}

#[tokio::test]
async fn ten_independent_lifecycle_runs_write_jsonl_evaluation() {
    let commit = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .unwrap_or_else(|| "unknown".to_owned());
    let mut records = Vec::new();
    for iteration in 0..10 {
        let started_at = Instant::now();
        let url = database_url(&format!("eval-{iteration}"));
        let store = Arc::new(SqliteStore::open(&url).await.expect("eval store"));
        let (foreground, _) = ForegroundModel::normal();
        let (sleep, _) = SleepModel::normal();
        let engine = engine(store.clone(), foreground, sleep, unused_calls());
        engine
            .handle(observation("evaluation foreground input"))
            .await
            .expect("eval foreground");
        let slept = engine.sleep_once().await.expect("eval sleep");
        let recovery = engine.recovery_report().await.expect("eval projection");
        let events = store.events().await.expect("eval events");
        let state = store.state().await.expect("eval state");
        let sleep_status = serde_json::to_value(&slept.status)
            .expect("sleep status")
            .as_str()
            .expect("status string")
            .to_owned();
        records.push(serde_json::json!({
            "scenario": "foreground_success_then_sleep",
            "iteration": iteration + 1,
            "result": if matches!(slept.status, SleepOnceStatus::Completed) && recovery.projection_verified { "passed" } else { "failed" },
            "elapsed_ms": started_at.elapsed().as_millis(),
            "sleep_status": sleep_status,
            "cursor_before": slept.cursor_before,
            "cursor_after": slept.cursor_after,
            "observation_count": state.observations.len(),
            "decision_count": state.decisions.len(),
            "started_attempt_count": events.iter().filter(|event| event.event_kind == EventKind::AttemptStarted).count(),
            "projection_verified": recovery.projection_verified,
            "error_kind": slept.error_kind,
            "commit": commit,
        }));
    }
    let path = std::path::Path::new("target/hekate-evals/foreground-sleep-lifecycle.jsonl");
    fs::create_dir_all(path.parent().expect("eval parent")).expect("eval directory");
    let jsonl = records
        .iter()
        .map(serde_json::Value::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(path, format!("{jsonl}\n")).expect("write lifecycle evaluation");
    assert_eq!(records.len(), 10);
    assert!(records
        .iter()
        .all(|record| record["result"] == "passed" && record["projection_verified"] == true));
}

fn event_for<T: serde::Serialize>(
    actor: hekate::core::PrincipalId,
    kind: EventKind,
    entity_kind: EntityKind,
    entity_id: Uuid,
    payload: &T,
) -> ExperienceEvent {
    ExperienceEvent::new(
        actor,
        kind,
        Some(EntityRef::new(entity_kind, entity_id)),
        serde_json::to_value(payload).expect("event payload"),
        EventSource::new("test", None),
        None,
        None,
        Some(1.0),
    )
    .expect("event")
}
