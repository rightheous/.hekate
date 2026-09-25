use std::fs;
use std::process::Command;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use async_trait::async_trait;
use hekate::adapters::local_policy::LocalPolicy;
use hekate::adapters::sqlite::SqliteStore;
use hekate::config::Config;
use hekate::core::{
    now, ActionIntentId, Approval, ApprovalId, ApprovalStatus, CognitiveTrace, CommittedJudgment,
    Conflict, ConflictId, ConflictStatus, DecisionKind, EntityKind, EntityRef, EventKind,
    EventSource, ExperienceEvent, FocusOutcome, MemoryCandidate, MemoryCandidateId,
    MemoryCandidateStatus, MemoryKind, Observation, ObservationId, Operation, OperationId,
    OperationStatus, Position, PositionId, PositionStatus, PrincipalId, RunStatus, SelfReview,
    SleepRun, SleepRunId, SleepRunStatus, Stance, TaskId, TaskStatus, ThoughtContext, ThoughtCycle,
    ThoughtDraft,
};
use hekate::ports::{
    CapabilityCatalog, CapabilityError, CapabilityResult, CognitiveError, CognitiveModel,
};
use hekate::runtime::{Engine, EngineError, Projector};
use serde::Serialize;
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

struct ModelSpy {
    seen: Arc<Mutex<Vec<ThoughtContext>>>,
    calls: Arc<AtomicUsize>,
    fail_first: bool,
}

fn model(fail_first: bool) -> (Arc<ModelSpy>, Arc<Mutex<Vec<ThoughtContext>>>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    (
        Arc::new(ModelSpy {
            seen: seen.clone(),
            calls: Arc::new(AtomicUsize::new(0)),
            fail_first,
        }),
        seen,
    )
}

#[async_trait]
impl CognitiveModel for ModelSpy {
    async fn think(&self, context: &ThoughtContext) -> Result<ThoughtCycle, CognitiveError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        self.seen
            .lock()
            .expect("model capture lock")
            .push(context.clone());
        if self.fail_first && call == 1 {
            return Err(CognitiveError::Timeout {
                trace: trace(context.event_sequence, &context.snapshot_hash, "timeout"),
            });
        }
        let commitment = CommittedJudgment {
            final_act: DecisionKind::Agree,
            reasons: vec!["continuity test response".to_owned()],
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
                interpretation: "continuity test".to_owned(),
                initial_judgment: DecisionKind::Agree,
                reasons: vec!["fake model".to_owned()],
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

fn trace(sequence: u64, hash: &str, outcome: &str) -> CognitiveTrace {
    CognitiveTrace {
        trace_id: Uuid::new_v4().to_string(),
        outcome: outcome.to_owned(),
        provider: "continuity-test".to_owned(),
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
                "hekate-continuity-v2-{label}-{}.db",
                Uuid::new_v4()
            ))
            .display()
    )
}

fn observation(content: &str, thread_id: &str, message_id: &str) -> Observation {
    Observation {
        id: ObservationId::new(),
        actor_id: Config::default().user_principal_id,
        content: content.to_owned(),
        source_type: "continuity-test".to_owned(),
        source_ref: Some("account-1".to_owned()),
        thread_id: Some(thread_id.to_owned()),
        message_id: Some(message_id.to_owned()),
        received_at: now(),
    }
}

fn engine(
    store: Arc<SqliteStore>,
    model: Arc<ModelSpy>,
    external_calls: Arc<AtomicUsize>,
) -> Engine {
    let config = Config::default();
    Engine::new(
        store,
        model,
        Arc::new(LocalPolicy),
        Arc::new(NoCapabilities {
            calls: external_calls,
        }),
        config.hekate_principal_id,
        config.user_principal_id,
    )
}

async fn start_task(
    engine: &Engine,
    title: &str,
    thread_id: &str,
) -> Result<hekate::core::InteractionResult, EngineError> {
    engine
        .handle(observation(
            &format!("new task: {title}"),
            thread_id,
            &Uuid::new_v4().to_string(),
        ))
        .await
}

#[derive(Serialize)]
struct EvalRecord {
    scenario: &'static str,
    iteration: usize,
    input: String,
    classification: String,
    selected_task_id: Option<String>,
    selected_run_id: Option<String>,
    decision_kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    original_run_id: Option<String>,
    event_count: usize,
    events_added: usize,
    replay_verified: bool,
    external_capability_executed: bool,
    result: &'static str,
    commit: String,
}

async fn eval_record(
    scenario: &'static str,
    iteration: usize,
    input: &str,
    before_events: usize,
    engine: &Engine,
    store: &SqliteStore,
    result: &hekate::core::InteractionResult,
    commit: &str,
) -> EvalRecord {
    let state = store.state().await.expect("state");
    let events = store.events().await.expect("events");
    let resolution = state
        .focus_resolutions
        .get(&result.observation_id)
        .expect("persisted focus resolution");
    let recovery = engine.recovery_report().await.expect("replay check");
    assert!(recovery.projection_verified, "{scenario} replay");
    EvalRecord {
        scenario,
        iteration,
        input: input.to_owned(),
        classification: serde_json::to_value(resolution.outcome)
            .expect("focus outcome")
            .as_str()
            .expect("focus outcome string")
            .to_owned(),
        selected_task_id: result.focus.task_id.map(|id| id.to_string()),
        selected_run_id: result.focus.run_id.map(|id| id.to_string()),
        decision_kind: serde_json::to_value(&result.decision.kind)
            .expect("decision kind")
            .as_str()
            .expect("decision kind string")
            .to_owned(),
        original_run_id: None,
        event_count: events.len(),
        events_added: events.len().saturating_sub(before_events),
        replay_verified: recovery.projection_verified,
        external_capability_executed: false,
        result: "passed",
        commit: commit.to_owned(),
    }
}

async fn scenario_cross_thread_continuation(iteration: usize, commit: &str) -> EvalRecord {
    let url = database_url("cross-thread");
    let store = Arc::new(SqliteStore::open(&url).await.expect("store"));
    let (model, _) = model(false);
    let engine = engine(store.clone(), model, Arc::new(AtomicUsize::new(0)));
    let initial = start_task(&engine, "Build continuity planner", "thread-first")
        .await
        .expect("start task");
    assert!(matches!(
        store.state().await.expect("state").focus_resolutions[&initial.observation_id].outcome,
        FocusOutcome::NewWork
    ));
    let before = store.events().await.expect("events").len();
    let input = "아까 하던 작업 계속";
    let result = engine
        .handle(observation(input, "thread-new", "cross-thread-continue"))
        .await
        .expect("continue task from another thread");
    assert_eq!(result.focus.task_id, initial.focus.task_id);
    assert_eq!(result.focus.run_id, initial.focus.run_id);
    let state = store.state().await.expect("state");
    assert_eq!(state.goals.len(), 1);
    assert_eq!(state.tasks.len(), 1);
    assert_eq!(state.runs.len(), 1);
    let resolution = &state.focus_resolutions[&result.observation_id];
    assert_eq!(resolution.outcome, FocusOutcome::Continue);
    assert!(resolution.as_of_revision < state.revision);
    eval_record(
        "cross_thread_continuation",
        iteration,
        input,
        before,
        &engine,
        store.as_ref(),
        &result,
        commit,
    )
    .await
}

async fn scenario_unrelated_input_is_conversation(iteration: usize, commit: &str) -> EvalRecord {
    let url = database_url("unrelated");
    let store = Arc::new(SqliteStore::open(&url).await.expect("store"));
    let (model, _) = model(false);
    let engine = engine(store.clone(), model, Arc::new(AtomicUsize::new(0)));
    start_task(&engine, "Build continuity planner", "thread-work")
        .await
        .expect("start task");
    let before = store.events().await.expect("events").len();
    let input = "What's the weather in Reykjavík?";
    let result = engine
        .handle(observation(input, "thread-weather", "unrelated-weather"))
        .await
        .expect("ordinary conversation");
    assert_eq!(result.focus.task_id, None);
    let state = store.state().await.expect("state");
    assert_eq!(state.goals.len(), 1);
    assert_eq!(state.tasks.len(), 1);
    assert_eq!(
        state.focus_resolutions[&result.observation_id].outcome,
        FocusOutcome::Conversation
    );
    assert!(matches!(
        state.runs[&result.focus.run_id.expect("conversation run")].status,
        RunStatus::Completed
    ));
    eval_record(
        "unrelated_input_is_conversation",
        iteration,
        input,
        before,
        &engine,
        store.as_ref(),
        &result,
        commit,
    )
    .await
}

async fn scenario_explicit_new_work(iteration: usize, commit: &str) -> EvalRecord {
    let url = database_url("new-work");
    let store = Arc::new(SqliteStore::open(&url).await.expect("store"));
    let (model, _) = model(false);
    let engine = engine(store.clone(), model, Arc::new(AtomicUsize::new(0)));
    let initial = start_task(&engine, "Build existing parser", "thread-same")
        .await
        .expect("start first task");
    let before = store.events().await.expect("events").len();
    let input = "new task: Index citation ledger";
    let result = engine
        .handle(observation(input, "thread-same", "explicit-new-work"))
        .await
        .expect("start new work");
    assert_ne!(result.focus.task_id, initial.focus.task_id);
    assert_ne!(result.focus.run_id, initial.focus.run_id);
    let state = store.state().await.expect("state");
    assert_eq!(state.goals.len(), 2);
    assert_eq!(state.tasks.len(), 2);
    assert_eq!(
        state.focus_resolutions[&result.observation_id].outcome,
        FocusOutcome::NewWork
    );
    eval_record(
        "explicit_new_work",
        iteration,
        input,
        before,
        &engine,
        store.as_ref(),
        &result,
        commit,
    )
    .await
}

async fn scenario_ambiguous_tasks_ask(iteration: usize, commit: &str) -> EvalRecord {
    let url = database_url("ambiguous");
    let store = Arc::new(SqliteStore::open(&url).await.expect("store"));
    let (model, _) = model(false);
    let engine = engine(store.clone(), model.clone(), Arc::new(AtomicUsize::new(0)));
    start_task(&engine, "Polish release notes", "thread-one")
        .await
        .expect("start first task");
    start_task(&engine, "Repair timer service", "thread-two")
        .await
        .expect("start second task");
    let before = store.events().await.expect("events").len();
    let input = "이전 작업 계속";
    let message = observation(input, "thread-third", "ambiguous-continuation");
    let result = engine
        .handle(message.clone())
        .await
        .expect("ask which task to continue");
    assert!(matches!(
        result.decision.kind,
        DecisionKind::RequestClarification
    ));
    assert!(result.decision.action.is_none());
    assert!(!result.decision.message.is_empty());
    let state = store.state().await.expect("state");
    assert_eq!(state.goals.len(), 2);
    assert_eq!(state.tasks.len(), 2);
    assert_eq!(state.runs.len(), 2);
    assert_eq!(result.focus.task_id, None);
    assert_eq!(result.focus.run_id, None);
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    let repeated = engine.handle(message).await.expect("repeat question");
    assert_eq!(repeated.decision.id, result.decision.id);
    assert_eq!(repeated.decision.message, result.decision.message);
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
    eval_record(
        "ambiguous_tasks_request_clarification",
        iteration,
        input,
        before,
        &engine,
        store.as_ref(),
        &result,
        commit,
    )
    .await
}

async fn scenario_closed_task_not_reopened(
    iteration: usize,
    status: TaskStatus,
    scenario: &'static str,
    commit: &str,
) -> EvalRecord {
    let url = database_url(scenario);
    let store = Arc::new(SqliteStore::open(&url).await.expect("store"));
    let (model, _) = model(false);
    let engine = engine(store.clone(), model, Arc::new(AtomicUsize::new(0)));
    let started = start_task(&engine, "Verify migration history", "thread-task")
        .await
        .expect("start task");
    let mut task =
        store.state().await.expect("state").tasks[&started.focus.task_id.expect("task id")].clone();
    if status == TaskStatus::Completed {
        task.status = TaskStatus::Completed;
        Projector::new(store.clone())
            .record(event_for(
                task_owner(&store).await,
                EventKind::TaskCompleted,
                EntityKind::Task,
                task.id.uuid(),
                &task,
            ))
            .await
            .expect("complete task");
    } else {
        task.id = TaskId::new();
        task.status = TaskStatus::Cancelled;
        Projector::new(store.clone())
            .record(event_for(
                task_owner(&store).await,
                EventKind::TaskCreated,
                EntityKind::Task,
                task.id.uuid(),
                &task,
            ))
            .await
            .expect("seed cancelled task");
    }
    let before = store.events().await.expect("events").len();
    let input = format!("continue task {}", task.id);
    let result = engine
        .handle(observation(
            &input,
            "thread-retry",
            &Uuid::new_v4().to_string(),
        ))
        .await
        .expect("clarify closed task");
    assert!(matches!(
        result.decision.kind,
        DecisionKind::RequestClarification
    ));
    assert!(result.decision.action.is_none());
    assert_eq!(result.focus.task_id, None);
    let state = store.state().await.expect("state");
    assert_eq!(state.tasks[&task.id].status, status);
    assert_eq!(
        state.tasks.len(),
        if status == TaskStatus::Completed {
            1
        } else {
            2
        }
    );
    assert_eq!(state.goals.len(), 1);
    eval_record(
        scenario,
        iteration,
        &input,
        before,
        &engine,
        store.as_ref(),
        &result,
        commit,
    )
    .await
}

async fn task_owner(store: &SqliteStore) -> PrincipalId {
    store
        .state()
        .await
        .expect("state")
        .observations
        .values()
        .next()
        .expect("initial observation")
        .actor_id
}

async fn scenario_retry_after_failure_and_restart(iteration: usize, commit: &str) -> EvalRecord {
    let url = database_url("retry-restart");
    let store = Arc::new(SqliteStore::open(&url).await.expect("store"));
    let input = "new task: Preserve retry identity";
    let message = observation(input, "thread-retry", "retry-after-restart");
    let (failing_model, _) = model(true);
    let failure_capability_calls = Arc::new(AtomicUsize::new(0));
    let failing_engine = engine(
        store.clone(),
        failing_model,
        failure_capability_calls.clone(),
    );
    assert!(matches!(
        failing_engine.handle(message.clone()).await,
        Err(EngineError::Cognitive(CognitiveError::Timeout { .. }))
    ));
    let failed_state = store.state().await.expect("failed state");
    let original_resolution = failed_state.focus_resolutions[&message.id].clone();
    assert_eq!(original_resolution.outcome, FocusOutcome::NewWork);
    let original_task_id = original_resolution.focus.task_id.expect("new task id");
    let original_run_id = original_resolution.focus.run_id.expect("new run id");
    assert_eq!(failure_capability_calls.load(Ordering::SeqCst), 0);
    drop(failing_engine);
    drop(store);

    let restarted_store = Arc::new(SqliteStore::open(&url).await.expect("reopened store"));
    let (model_b, _) = model(false);
    let competing_capability_calls = Arc::new(AtomicUsize::new(0));
    let engine_b = engine(
        restarted_store.clone(),
        model_b,
        competing_capability_calls.clone(),
    );
    start_task(&engine_b, "Create competing active work", "thread-other")
        .await
        .expect("start competing task");
    let before_retry = restarted_store
        .events()
        .await
        .expect("events before retry")
        .len();
    let (retry_model, _) = model(false);
    let retry_capability_calls = Arc::new(AtomicUsize::new(0));
    let restarted = engine(
        restarted_store.clone(),
        retry_model,
        retry_capability_calls.clone(),
    );
    let redelivery = observation(input, "thread-retry", "retry-after-restart");
    assert_ne!(redelivery.id, message.id);
    let result = restarted
        .handle(redelivery.clone())
        .await
        .expect("retry after restart");
    assert_eq!(result.observation_id, message.id);
    assert_eq!(result.focus.task_id, Some(original_task_id));
    assert_eq!(result.focus.run_id, Some(original_run_id));
    assert_eq!(
        restarted_store
            .state()
            .await
            .expect("state")
            .focus_resolutions[&message.id],
        original_resolution
    );
    let repeated = restarted
        .handle(redelivery)
        .await
        .expect("return same committed decision");
    assert_eq!(repeated.decision.id, result.decision.id);
    assert_eq!(repeated.focus.task_id, result.focus.task_id);
    assert_eq!(repeated.focus.run_id, Some(original_run_id));
    let state = restarted_store.state().await.expect("final state");
    assert_eq!(state.goals.len(), 2);
    assert_eq!(state.tasks.len(), 2);
    assert_eq!(state.runs.len(), 2);
    assert_eq!(competing_capability_calls.load(Ordering::SeqCst), 0);
    assert_eq!(retry_capability_calls.load(Ordering::SeqCst), 0);
    let mut record = eval_record(
        "same_message_new_work_retry_reuses_original_run",
        iteration,
        input,
        before_retry,
        &restarted,
        restarted_store.as_ref(),
        &result,
        commit,
    )
    .await;
    record.original_run_id = Some(original_run_id.to_string());
    assert_eq!(
        record.selected_run_id.as_deref(),
        record.original_run_id.as_deref()
    );
    assert!(!record.external_capability_executed);
    record
}

async fn scenario_snapshot_matches_new_focus(iteration: usize, commit: &str) -> EvalRecord {
    let url = database_url("snapshot-focus");
    let store = Arc::new(SqliteStore::open(&url).await.expect("store"));
    let (model, seen) = model(false);
    let engine = engine(store.clone(), model, Arc::new(AtomicUsize::new(0)));
    let before = store.events().await.expect("events").len();
    let input = "new task: Verify focus reaches the snapshot";
    let result = engine
        .handle(observation(input, "thread-snapshot", "snapshot-focus"))
        .await
        .expect("create and use new focus");
    let context = seen
        .lock()
        .expect("model capture lock")
        .last()
        .cloned()
        .expect("model context");
    assert_eq!(context.focus, result.focus);
    assert_eq!(
        context.task.as_ref().map(|task| task.id),
        result.focus.task_id
    );
    assert_eq!(context.run.as_ref().map(|run| run.id), result.focus.run_id);
    let snapshot = context.context_snapshot.expect("context snapshot");
    assert_eq!(
        snapshot["as_of_revision"].as_u64(),
        Some(context.event_sequence - 1)
    );
    let anchors = snapshot["anchors"].as_array().expect("snapshot anchors");
    for (kind, id) in [
        ("goal", result.focus.goal_id.expect("goal").to_string()),
        ("task", result.focus.task_id.expect("task").to_string()),
        ("run", result.focus.run_id.expect("run").to_string()),
    ] {
        assert!(anchors
            .iter()
            .any(|anchor| { anchor["kind"] == kind && anchor["entity"]["id"] == id }));
    }
    let state = store.state().await.expect("state");
    assert_eq!(state.goals.len(), 1);
    assert_eq!(state.tasks.len(), 1);
    assert_eq!(state.runs.len(), 1);
    eval_record(
        "focused_snapshot_and_replay",
        iteration,
        input,
        before,
        &engine,
        store.as_ref(),
        &result,
        commit,
    )
    .await
}

async fn scenario_preserves_other_state(iteration: usize, commit: &str) -> EvalRecord {
    let url = database_url("preserve-state");
    let store = Arc::new(SqliteStore::open(&url).await.expect("store"));
    seed_protected_state(store.clone()).await;
    let before_state = store.state().await.expect("seeded state");
    let before = store.events().await.expect("seeded events").len();
    let (model, _) = model(false);
    let external_calls = Arc::new(AtomicUsize::new(0));
    let engine = engine(store.clone(), model, external_calls.clone());
    let input = "What is the weather today?";
    let result = engine
        .handle(observation(input, "thread-general", "state-preservation"))
        .await
        .expect("ordinary conversation beside prior state");
    let after = store.state().await.expect("state after conversation");
    assert_eq!(after.positions, before_state.positions);
    assert_eq!(after.conflicts, before_state.conflicts);
    assert_eq!(after.memory_candidates, before_state.memory_candidates);
    assert_eq!(after.sleep_cursor, before_state.sleep_cursor);
    assert_eq!(after.approvals, before_state.approvals);
    assert_eq!(after.operations, before_state.operations);
    assert_eq!(external_calls.load(Ordering::SeqCst), 0);
    eval_record(
        "state_preservation",
        iteration,
        input,
        before,
        &engine,
        store.as_ref(),
        &result,
        commit,
    )
    .await
}

async fn seed_protected_state(store: Arc<SqliteStore>) {
    let config = Config::default();
    let observation = observation("sleep cursor seed", "seed-thread", "sleep-seed");
    let observation_event = event_for(
        config.user_principal_id,
        EventKind::ObservationRecorded,
        EntityKind::Observation,
        observation.id.uuid(),
        &observation,
    );
    Projector::new(store.clone())
        .record(observation_event.clone())
        .await
        .expect("seed observation");
    let state = store.state().await.expect("state after observation");
    let sleep_run = SleepRun {
        id: SleepRunId::new(),
        status: SleepRunStatus::Running,
        high_water_revision: state.revision,
        cursor_before: state.sleep_cursor,
        cursor_after: None,
        seed_event_ids: vec![observation_event.event_id],
        processed_observation_count: 0,
        created_candidate_count: 0,
        started_at: now(),
        finished_at: None,
        error_kind: None,
        context_budget_report: None,
    };
    let projector = Projector::new(store.clone());
    projector
        .record(event_for(
            config.hekate_principal_id,
            EventKind::SleepRunStarted,
            EntityKind::SleepRun,
            sleep_run.id.uuid(),
            &sleep_run,
        ))
        .await
        .expect("sleep run start");
    let mut completed_sleep = sleep_run.clone();
    completed_sleep.status = SleepRunStatus::Completed;
    completed_sleep.cursor_after = Some(sleep_run.high_water_revision);
    completed_sleep.finished_at = Some(now());
    projector
        .record(event_for(
            config.hekate_principal_id,
            EventKind::SleepRunCompleted,
            EntityKind::SleepRun,
            completed_sleep.id.uuid(),
            &completed_sleep,
        ))
        .await
        .expect("sleep run completion");

    let position = Position {
        id: PositionId::new(),
        principal_id: config.user_principal_id,
        subject: "continuity state preservation".to_owned(),
        stance: Stance::Support,
        version: 1,
        status: PositionStatus::Active,
        confidence: 90,
        supersedes: None,
        reasons: vec!["seeded invariant".to_owned()],
        evidence_refs: Vec::new(),
        reconsideration_conditions: Vec::new(),
        created_at: now(),
    };
    projector
        .record(event_for(
            config.user_principal_id,
            EventKind::PositionEstablished,
            EntityKind::Position,
            position.id.uuid(),
            &position,
        ))
        .await
        .expect("position");
    let conflict = Conflict {
        id: ConflictId::new(),
        subject: "seeded continuity conflict".to_owned(),
        participant_positions: vec![position.id],
        status: ConflictStatus::Open,
        revision: 1,
        reasons: vec!["seeded invariant".to_owned()],
        evidence_refs: Vec::new(),
        alternatives: Vec::new(),
        reconsideration_conditions: Vec::new(),
        unresolved_questions: Vec::new(),
        resolution: None,
        resolved_at: None,
        created_at: now(),
    };
    projector
        .record(event_for(
            config.user_principal_id,
            EventKind::ConflictOpened,
            EntityKind::Conflict,
            conflict.id.uuid(),
            &conflict,
        ))
        .await
        .expect("conflict");
    let memory_candidate = MemoryCandidate {
        id: MemoryCandidateId::new(),
        kind: MemoryKind::ExplicitPreference,
        content: "preserve the existing continuity memory candidate".to_owned(),
        subject_principal_id: Some(config.user_principal_id),
        status: MemoryCandidateStatus::Candidate,
        confidence: 100,
        source_event_ids: vec![observation_event.event_id],
        valid_from: None,
        valid_until: None,
        supersedes: None,
        created_at: now(),
    };
    projector
        .record(event_for(
            config.hekate_principal_id,
            EventKind::MemoryCandidateCreated,
            EntityKind::MemoryCandidate,
            memory_candidate.id.uuid(),
            &memory_candidate,
        ))
        .await
        .expect("memory candidate");
    let operation_id = OperationId::new();
    let intent_id = ActionIntentId::new();
    let approval = Approval {
        id: ApprovalId::new(),
        intent_id,
        operation_id,
        requested_by: config.hekate_principal_id,
        status: ApprovalStatus::Pending,
        reason: "preservation test".to_owned(),
        resolved_by: None,
        resolved_at: None,
        created_at: now(),
    };
    projector
        .record(event_for(
            config.hekate_principal_id,
            EventKind::ApprovalRequested,
            EntityKind::Approval,
            approval.id.uuid(),
            &approval,
        ))
        .await
        .expect("approval");
    let operation = Operation {
        id: operation_id,
        intent_id,
        status: OperationStatus::Unknown,
        idempotency_key: "continuity-preserve-unknown".to_owned(),
        approval_id: Some(approval.id),
        started_at: Some(now()),
        finished_at: None,
    };
    projector
        .record(event_for(
            config.hekate_principal_id,
            EventKind::OperationStateUnknown,
            EntityKind::Operation,
            operation.id.uuid(),
            &operation,
        ))
        .await
        .expect("unknown operation");
}

fn event_for<T: Serialize>(
    actor_id: PrincipalId,
    event_kind: EventKind,
    entity_kind: EntityKind,
    entity_id: Uuid,
    payload: &T,
) -> ExperienceEvent {
    ExperienceEvent::new(
        actor_id,
        event_kind,
        Some(EntityRef::new(entity_kind, entity_id)),
        serde_json::to_value(payload).expect("event payload"),
        EventSource::new("continuity-test", None),
        None,
        None,
        Some(1.0),
    )
    .expect("event")
}

#[tokio::test]
async fn continuity_scenarios_repeat_ten_times_and_write_jsonl() {
    let commit = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .unwrap_or_else(|| "unknown".to_owned());
    let mut records = Vec::new();
    for iteration in 1..=10 {
        records.push(scenario_cross_thread_continuation(iteration, &commit).await);
        records.push(scenario_unrelated_input_is_conversation(iteration, &commit).await);
        records.push(scenario_explicit_new_work(iteration, &commit).await);
        records.push(scenario_ambiguous_tasks_ask(iteration, &commit).await);
        records.push(
            scenario_closed_task_not_reopened(
                iteration,
                TaskStatus::Completed,
                "completed_task_not_reopened",
                &commit,
            )
            .await,
        );
        records.push(
            scenario_closed_task_not_reopened(
                iteration,
                TaskStatus::Cancelled,
                "cancelled_task_not_reopened",
                &commit,
            )
            .await,
        );
        records.push(scenario_retry_after_failure_and_restart(iteration, &commit).await);
        records.push(scenario_snapshot_matches_new_focus(iteration, &commit).await);
        records.push(scenario_preserves_other_state(iteration, &commit).await);
    }
    let path = std::path::Path::new("target/hekate-evals/continuity-v2.jsonl");
    fs::create_dir_all(path.parent().expect("eval parent")).expect("eval directory");
    let jsonl = records
        .iter()
        .map(|record| serde_json::to_string(record).expect("eval record"))
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(path, format!("{jsonl}\n")).expect("write continuity evaluation");

    let retry_records = records
        .iter()
        .filter(|record| record.scenario == "same_message_new_work_retry_reuses_original_run")
        .map(|record| serde_json::to_value(record).expect("retry eval record"))
        .collect::<Vec<_>>();
    let retry_path = std::path::Path::new("target/hekate-evals/consolidation-retry.jsonl");
    fs::create_dir_all(retry_path.parent().expect("retry eval parent"))
        .expect("retry eval directory");
    let retry_jsonl = retry_records
        .iter()
        .map(serde_json::Value::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(retry_path, format!("{retry_jsonl}\n")).expect("write retry evaluation");
    assert_eq!(retry_records.len(), 10);
    assert_eq!(records.len(), 90);
    for scenario in [
        "cross_thread_continuation",
        "unrelated_input_is_conversation",
        "explicit_new_work",
        "ambiguous_tasks_request_clarification",
        "completed_task_not_reopened",
        "cancelled_task_not_reopened",
        "same_message_new_work_retry_reuses_original_run",
        "focused_snapshot_and_replay",
        "state_preservation",
    ] {
        assert_eq!(
            records
                .iter()
                .filter(|record| record.scenario == scenario)
                .count(),
            10,
            "{scenario} iterations"
        );
    }
    assert!(records
        .iter()
        .all(|record| record.result == "passed" && record.replay_verified));
}
