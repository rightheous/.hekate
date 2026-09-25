use std::fs;
use std::process::{Command, Stdio};
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
use tokio::sync::{Barrier, Notify};
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

struct KilledForegroundModel {
    ready_path: String,
}

#[async_trait]
impl CognitiveModel for KilledForegroundModel {
    async fn think(&self, _: &ThoughtContext) -> Result<ThoughtCycle, CognitiveError> {
        fs::write(&self.ready_path, "ready").expect("write child ready marker");
        std::future::pending().await
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

struct EvalCase {
    scenario: &'static str,
    elapsed_ms: u128,
    sleep_status: String,
    cursor_before: Option<u64>,
    cursor_after: Option<u64>,
    observation_count: usize,
    decision_count: usize,
    started_attempt_count: usize,
    projection_verified: bool,
    error_kind: Option<String>,
}

impl EvalCase {
    fn json(&self, iteration: usize, commit: &str) -> serde_json::Value {
        serde_json::json!({
            "scenario": self.scenario,
            "iteration": iteration,
            "result": "passed",
            "elapsed_ms": self.elapsed_ms,
            "sleep_status": self.sleep_status,
            "cursor_before": self.cursor_before,
            "cursor_after": self.cursor_after,
            "observation_count": self.observation_count,
            "decision_count": self.decision_count,
            "started_attempt_count": self.started_attempt_count,
            "projection_verified": self.projection_verified,
            "error_kind": self.error_kind,
            "commit": commit,
        })
    }
}

fn eval_case(
    scenario: &'static str,
    started_at: Instant,
    sleep_status: String,
    cursor_before: Option<u64>,
    cursor_after: Option<u64>,
    state: &hekate::core::CurrentState,
    events: &[ExperienceEvent],
    projection_verified: bool,
    error_kind: Option<&str>,
) -> EvalCase {
    EvalCase {
        scenario,
        elapsed_ms: started_at.elapsed().as_millis(),
        sleep_status,
        cursor_before,
        cursor_after,
        observation_count: state.observations.len(),
        decision_count: state.decisions.len(),
        started_attempt_count: events
            .iter()
            .filter(|event| event.event_kind == EventKind::AttemptStarted)
            .count(),
        projection_verified,
        error_kind: error_kind.map(str::to_owned),
    }
}

fn status_text(status: &SleepOnceStatus) -> String {
    serde_json::to_value(status)
        .expect("sleep status")
        .as_str()
        .expect("status string")
        .to_owned()
}

async fn run_normal_decision_scenario() -> EvalCase {
    let started_at = Instant::now();
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
    let recovery = engine.recovery_report().await.expect("recovery");
    let state = store.state().await.expect("final state");
    let events = store.events().await.expect("events");
    eval_case(
        "normal_response_open_run",
        started_at,
        status_text(&slept.status),
        slept.cursor_before,
        slept.cursor_after,
        &state,
        &events,
        recovery.projection_verified,
        None,
    )
}

async fn run_timeout_retry_scenario() -> EvalCase {
    let started_at = Instant::now();
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
    let observation = message_observation("new task: retry this message", "message-17");
    assert!(matches!(
        first_engine.handle(observation.clone()).await,
        Err(EngineError::Cognitive(CognitiveError::Timeout { .. }))
    ));
    let after_timeout = first_store.state().await.expect("failed state");
    assert_eq!(after_timeout.observations.len(), 1);
    assert_eq!(after_timeout.runs.len(), 1);
    assert_eq!(after_timeout.tasks.len(), 1);
    assert_eq!(after_timeout.goals.len(), 1);
    let original_run_id = after_timeout.runs.values().next().expect("original run").id;
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
    assert_eq!(retried.focus.run_id, Some(original_run_id));
    let after_retry = restarted_store.state().await.expect("retried state");
    assert_eq!(after_retry.runs.len(), 1);
    assert_eq!(after_retry.tasks.len(), 1);
    assert_eq!(after_retry.goals.len(), 1);
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
    let recovery = restarted.recovery_report().await.expect("final replay");
    let state = restarted_store.state().await.expect("final state");
    let events = restarted_store.events().await.expect("final events");
    eval_case(
        "timeout_restart_and_retry",
        started_at,
        status_text(&sleep_result.status),
        sleep_result.cursor_before,
        sleep_result.cursor_after,
        &state,
        &events,
        recovery.projection_verified,
        Some("timeout"),
    )
}

async fn run_live_lease_scenario() -> EvalCase {
    let started_at = Instant::now();
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
    let recovery = sleep_engine.recovery_report().await.expect("recovery");
    let state = sleep_store.state().await.expect("state");
    let events = sleep_store.events().await.expect("events");
    eval_case(
        "live_lease_defers_sleep",
        started_at,
        status_text(&result.status),
        result.cursor_before,
        result.cursor_after,
        &state,
        &events,
        recovery.projection_verified,
        None,
    )
}

async fn run_expired_lease_scenario() -> EvalCase {
    let started_at = Instant::now();
    let url = database_url("expired-lease");
    let first = SqliteStore::open(&url).await.expect("first store");
    let second = SqliteStore::open(&url).await.expect("second store");
    let seed = observation("sleep resumes after foreground owner expires");
    Projector::new(Arc::new(second.clone()))
        .record(observation_event(&seed))
        .await
        .expect("seed observation");
    assert!(first
        .acquire_foreground_lease("old-owner", Duration::from_millis(10))
        .await
        .expect("old lease"));
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!second
        .has_active_foreground_lease()
        .await
        .expect("expired lease"));

    let (sleep, sleep_calls) = SleepModel::normal();
    let sleep_engine = engine(
        Arc::new(second.clone()),
        ForegroundModel::normal().0,
        sleep,
        unused_calls(),
    );
    let slept = sleep_engine.sleep_once().await.expect("sleep after expiry");
    assert!(matches!(slept.status, SleepOnceStatus::Completed));
    assert_eq!(sleep_calls.load(Ordering::SeqCst), 1);

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
    let recovery = sleep_engine.recovery_report().await.expect("recovery");
    let state = second.state().await.expect("state");
    let events = second.events().await.expect("events");
    eval_case(
        "expired_lease_releases_sleep",
        started_at,
        status_text(&slept.status),
        slept.cursor_before,
        slept.cursor_after,
        &state,
        &events,
        recovery.projection_verified,
        None,
    )
}

async fn run_stale_records_scenario() -> EvalCase {
    let started_at = Instant::now();
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
    let recovery = engine.recovery_report().await.expect("replay");
    let events = store.events().await.expect("events");
    eval_case(
        "stale_records_do_not_block_sleep",
        started_at,
        status_text(&result.status),
        result.cursor_before,
        result.cursor_after,
        &state,
        &events,
        recovery.projection_verified,
        None,
    )
}

async fn run_foreground_sleep_race_scenario() -> EvalCase {
    let started_at = Instant::now();
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
    let (next_sleep, next_sleep_calls) = SleepModel::normal();
    let next_engine = engine(
        sleep_store.clone(),
        ForegroundModel::normal().0,
        next_sleep,
        unused_calls(),
    );
    let retried = next_engine.sleep_once().await.expect("next sleep cycle");
    assert!(matches!(retried.status, SleepOnceStatus::Completed));
    assert!(retried.cursor_after.is_some());
    assert_eq!(next_sleep_calls.load(Ordering::SeqCst), 1);
    let recovery = next_engine.recovery_report().await.expect("replay");
    let state = sleep_store.state().await.expect("final state");
    let events = sleep_store.events().await.expect("events");
    eval_case(
        "foreground_interrupts_sleep_then_retries",
        started_at,
        "interrupted_then_completed".to_owned(),
        result.cursor_before,
        retried.cursor_after,
        &state,
        &events,
        recovery.projection_verified,
        Some("foreground_activity"),
    )
}

#[tokio::test]
async fn lifecycle_contract_scenarios_pass() {
    for case in [
        run_normal_decision_scenario().await,
        run_timeout_retry_scenario().await,
        run_live_lease_scenario().await,
        run_expired_lease_scenario().await,
        run_stale_records_scenario().await,
        run_foreground_sleep_race_scenario().await,
    ] {
        assert!(case.projection_verified, "{} replay", case.scenario);
    }
}

#[tokio::test]
async fn concurrent_foreground_lease_acquisition_has_one_winner() {
    let url = database_url("single-lease");
    let first = SqliteStore::open(&url).await.expect("first store");
    let second = SqliteStore::open(&url).await.expect("second store");
    let barrier = Arc::new(Barrier::new(3));
    let first_task = {
        let barrier = barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            first
                .acquire_foreground_lease("owner-a", Duration::from_secs(5))
                .await
                .expect("first acquisition")
        })
    };
    let second_task = {
        let barrier = barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            second
                .acquire_foreground_lease("owner-b", Duration::from_secs(5))
                .await
                .expect("second acquisition")
        })
    };
    barrier.wait().await;
    let first_won = first_task.await.expect("first task");
    let second_won = second_task.await.expect("second task");
    assert_ne!(first_won, second_won);
}

#[tokio::test]
async fn hard_kill_foreground_process_allows_sleep_after_lease_expiry() {
    const DB_ENV: &str = "HEKATE_FOREGROUND_KILL_TEST_DB";
    const READY_ENV: &str = "HEKATE_FOREGROUND_KILL_TEST_READY";

    if let (Ok(url), Ok(ready_path)) = (std::env::var(DB_ENV), std::env::var(READY_ENV)) {
        let store = Arc::new(SqliteStore::open(&url).await.expect("child store"));
        let foreground: Arc<dyn CognitiveModel> = Arc::new(KilledForegroundModel { ready_path });
        let child_engine = engine(store, foreground, SleepModel::normal().0, unused_calls());
        let _ = child_engine
            .handle(message_observation(
                "new task: killed foreground",
                "killed-message",
            ))
            .await;
        panic!("blocked child foreground model unexpectedly returned");
    }

    let url = database_url("hard-kill");
    let store = Arc::new(SqliteStore::open(&url).await.expect("parent store"));
    let ready_path = std::env::temp_dir().join(format!("hekate-ready-{}.txt", Uuid::new_v4()));
    let mut child = Command::new(std::env::current_exe().expect("test executable"))
        .args([
            "--exact",
            "hard_kill_foreground_process_allows_sleep_after_lease_expiry",
            "--nocapture",
        ])
        .env(DB_ENV, &url)
        .env(READY_ENV, &ready_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn foreground child");
    let ready = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if ready_path.exists() {
                break true;
            }
            if child.try_wait().expect("check child status").is_some() {
                break false;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or(false);
    if !ready {
        let _ = child.kill();
        let _ = child.wait();
        panic!("child did not enter the blocked foreground model");
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    child.kill().expect("forcibly kill foreground child");
    assert!(!child.wait().expect("wait for killed child").success());

    let killed_state = store.state().await.expect("state after process death");
    assert_eq!(killed_state.observations.len(), 1);
    assert_eq!(killed_state.attempts.len(), 1);
    assert!(matches!(
        killed_state
            .attempts
            .values()
            .next()
            .expect("started attempt")
            .status,
        AttemptStatus::Started
    ));
    assert!(store
        .has_active_foreground_lease()
        .await
        .expect("lease remains until expiry"));

    tokio::time::timeout(Duration::from_secs(40), async {
        while store
            .has_active_foreground_lease()
            .await
            .expect("check expiring lease")
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("lease did not expire after process death");

    let external_calls = unused_calls();
    let (sleep, sleep_calls) = SleepModel::normal();
    let sleep_engine = engine(
        store.clone(),
        ForegroundModel::normal().0,
        sleep,
        external_calls.clone(),
    );
    let slept = sleep_engine
        .sleep_once()
        .await
        .expect("sleep after lease expiry");
    assert!(matches!(slept.status, SleepOnceStatus::Completed));
    assert!(slept.cursor_after.is_some());
    assert_eq!(sleep_calls.load(Ordering::SeqCst), 1);
    assert_eq!(external_calls.load(Ordering::SeqCst), 0);

    let state = store.state().await.expect("state after sleep");
    assert!(state.operations.is_empty());
    let replay = sleep_engine
        .recovery_report()
        .await
        .expect("replay after sleep");
    assert!(replay.projection_verified);
    assert_eq!(replay.incomplete_attempts.len(), 1);
    let _ = fs::remove_file(ready_path);
}

#[tokio::test]
async fn ten_runs_of_each_lifecycle_scenario_write_jsonl_evaluation() {
    let commit = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .unwrap_or_else(|| "unknown".to_owned());
    let mut records = Vec::new();
    for iteration in 1..=10 {
        let cases = [
            run_normal_decision_scenario().await,
            run_timeout_retry_scenario().await,
            run_live_lease_scenario().await,
            run_expired_lease_scenario().await,
            run_stale_records_scenario().await,
            run_foreground_sleep_race_scenario().await,
        ];
        for case in cases {
            assert!(case.projection_verified, "{} replay", case.scenario);
            records.push(case.json(iteration, &commit));
        }
    }
    let path = std::path::Path::new("target/hekate-evals/foreground-sleep-lifecycle.jsonl");
    fs::create_dir_all(path.parent().expect("eval parent")).expect("eval directory");
    let jsonl = records
        .iter()
        .map(serde_json::Value::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(path, format!("{jsonl}\n")).expect("write lifecycle evaluation");
    assert_eq!(records.len(), 60);
    assert!(records
        .iter()
        .all(|record| record["result"] == "passed" && record["projection_verified"] == true));
    for scenario in [
        "normal_response_open_run",
        "timeout_restart_and_retry",
        "live_lease_defers_sleep",
        "expired_lease_releases_sleep",
        "stale_records_do_not_block_sleep",
        "foreground_interrupts_sleep_then_retries",
    ] {
        assert_eq!(
            records
                .iter()
                .filter(|record| record["scenario"] == scenario)
                .count(),
            10
        );
    }
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
