use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use hekate::adapters::sqlite::SqliteStore;
use hekate::core::event::{EntityKind, EntityRef, EventKind, EventSource, ExperienceEvent};
use hekate::core::{
    Commitment, CommitmentId, CommitmentStatus, Conflict, ConflictId, ConflictStatus, EventId,
    Goal, GoalId, GoalStatus, InitiativeKind, InitiativeProposal, InitiativeStatus, Observation,
    ObservationId, Position, PositionId, PositionStatus, Principal, PrincipalId, PrincipalKind,
    Run, RunId, RunStatus, Stance, Task, TaskId, TaskStatus,
};
use hekate::ports::Storage;
use hekate::runtime::initiative::service::{InitiativeRunResult, InitiativeService};
use hekate::runtime::projector::Projector;
use serde::Serialize;
use uuid::Uuid;

const REPETITIONS: u8 = 10;

#[derive(Serialize)]
struct EvalRow {
    scenario: &'static str,
    repetition: u8,
    result: &'static str,
    replay_match: bool,
    external_capability_execution_count: usize,
    final_commit: String,
    detail: String,
}

struct CaseResult {
    passed: bool,
    replay_match: bool,
    external_capability_execution_count: usize,
    detail: String,
}

struct TempDb(PathBuf);

impl TempDb {
    fn new() -> anyhow::Result<Self> {
        let root = std::env::var_os("HEKATE_TEST_TMPDIR")
            .map(PathBuf::from)
            .filter(|path| path.is_dir())
            .unwrap_or_else(|| {
                let shared_memory = PathBuf::from("/dev/shm");
                if shared_memory.is_dir() {
                    shared_memory
                } else {
                    std::env::temp_dir()
                }
            });
        Ok(Self(root.join(format!(
            "hekate-initiative-eval-{}.db",
            Uuid::new_v4()
        ))))
    }

    fn url(&self) -> String {
        format!("sqlite://{}", self.0.display())
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
        let _ = std::fs::remove_file(format!("{}-wal", self.0.display()));
        let _ = std::fs::remove_file(format!("{}-shm", self.0.display()));
    }
}

#[tokio::test]
async fn initiative_v1_six_scenarios_ten_independent_repetitions() -> anyhow::Result<()> {
    let Some(output) = std::env::var_os("HEKATE_INITIATIVE_EVAL_OUTPUT") else {
        return Ok(());
    };
    let output = PathBuf::from(output);
    anyhow::ensure!(
        !output.exists(),
        "refusing to overwrite existing evaluation artifact: {}",
        output.display()
    );
    let final_commit = git_commit()?;
    let scenarios = [
        "open_conflict_proposes_question",
        "hekate_commitment_proposes_follow_up",
        "new_position_evidence_triggers_reconsideration",
        "same_fingerprint_retry_and_restart_are_idempotent",
        "dismissed_same_evidence_is_not_reproposed",
        "old_user_owned_deletion_goal_and_incomplete_run_do_nothing",
    ];
    let mut rows = Vec::with_capacity(scenarios.len() * usize::from(REPETITIONS));

    for (scenario_index, scenario) in scenarios.into_iter().enumerate() {
        for repetition in 1..=REPETITIONS {
            let outcome = run_scenario(scenario_index, repetition).await;
            let row = match outcome {
                Ok(outcome) => EvalRow {
                    scenario,
                    repetition,
                    result: if outcome.passed { "success" } else { "failure" },
                    replay_match: outcome.replay_match,
                    external_capability_execution_count: outcome
                        .external_capability_execution_count,
                    final_commit: final_commit.clone(),
                    detail: outcome.detail,
                },
                Err(error) => EvalRow {
                    scenario,
                    repetition,
                    result: "failure",
                    replay_match: false,
                    external_capability_execution_count: 0,
                    final_commit: final_commit.clone(),
                    detail: format!("harness error: {error:#}"),
                },
            };
            rows.push(row);
        }
    }

    write_jsonl_atomically(&output, &rows)?;
    anyhow::ensure!(
        rows.len() == 60,
        "expected 60 atomic results, got {}",
        rows.len()
    );
    for row in &rows {
        anyhow::ensure!(
            row.result == "success"
                && row.replay_match
                && row.external_capability_execution_count == 0,
            "evaluation failure recorded in {}: {} #{}: {}",
            output.display(),
            row.scenario,
            row.repetition,
            row.detail
        );
    }
    Ok(())
}

async fn run_scenario(scenario: usize, repetition: u8) -> anyhow::Result<CaseResult> {
    match scenario {
        0 => evaluate_conflict_proposal().await,
        1 => evaluate_commitment_proposal().await,
        2 => evaluate_position_reconsideration().await,
        3 => evaluate_retry_and_restart().await,
        4 => evaluate_dismissal().await,
        5 => evaluate_old_goal_and_run().await,
        _ => anyhow::bail!("unknown scenario {scenario} #{}", repetition),
    }
}

async fn open_store(durable: bool) -> anyhow::Result<(Arc<SqliteStore>, Option<TempDb>)> {
    let db = durable.then(TempDb::new).transpose()?;
    let url = db
        .as_ref()
        .map(TempDb::url)
        .unwrap_or_else(|| "sqlite::memory:".to_owned());
    Ok((Arc::new(SqliteStore::open(&url).await?), db))
}

async fn seed_principals(store: &Arc<SqliteStore>) -> anyhow::Result<(PrincipalId, PrincipalId)> {
    let hekate = Principal {
        id: PrincipalId::new(),
        kind: PrincipalKind::Hekate,
        name: "HEKATE".to_owned(),
        identity_version_id: None,
    };
    let user = Principal {
        id: PrincipalId::new(),
        kind: PrincipalKind::User,
        name: "Evaluation user".to_owned(),
        identity_version_id: None,
    };
    record(
        store,
        hekate.id,
        EventKind::PrincipalCreated,
        EntityKind::Principal,
        hekate.id.uuid(),
        &hekate,
    )
    .await?;
    record(
        store,
        hekate.id,
        EventKind::PrincipalCreated,
        EntityKind::Principal,
        user.id.uuid(),
        &user,
    )
    .await?;
    Ok((hekate.id, user.id))
}

async fn record<T: Serialize>(
    store: &Arc<SqliteStore>,
    actor_id: PrincipalId,
    kind: EventKind,
    entity_kind: EntityKind,
    entity_id: Uuid,
    payload: &T,
) -> anyhow::Result<EventId> {
    let event = ExperienceEvent::new(
        actor_id,
        kind,
        Some(EntityRef::new(entity_kind, entity_id)),
        serde_json::to_value(payload)?,
        EventSource::new("initiative_v1_eval", None),
        None,
        None,
        None,
    )?;
    let id = event.event_id;
    Projector::new(store.clone()).record(event).await?;
    Ok(id)
}

async fn record_observation(
    store: &Arc<SqliteStore>,
    actor: PrincipalId,
    content: &str,
) -> anyhow::Result<(Observation, EventId)> {
    let observation = Observation {
        id: ObservationId::new(),
        actor_id: actor,
        content: content.to_owned(),
        source_type: "deterministic_eval".to_owned(),
        source_ref: None,
        thread_id: None,
        message_id: None,
        received_at: "2026-01-01T00:00:00Z".to_owned(),
    };
    let event_id = record(
        store,
        actor,
        EventKind::ObservationRecorded,
        EntityKind::Observation,
        observation.id.uuid(),
        &observation,
    )
    .await?;
    Ok((observation, event_id))
}

fn new_position(principal_id: PrincipalId, subject: &str, stance: Stance) -> Position {
    Position {
        id: PositionId::new(),
        principal_id,
        subject: subject.to_owned(),
        stance,
        version: 1,
        status: PositionStatus::Active,
        confidence: 80,
        supersedes: None,
        reasons: vec!["the available workspace retention evidence supports this view".to_owned()],
        evidence_refs: Vec::new(),
        reconsideration_conditions: vec!["new workspace retention evidence".to_owned()],
        created_at: "2026-01-01T00:00:00Z".to_owned(),
    }
}

async fn seed_conflict(
    store: &Arc<SqliteStore>,
    hekate: PrincipalId,
    user: PrincipalId,
) -> anyhow::Result<EventId> {
    let (_, evidence) = record_observation(
        store,
        user,
        "The workspace retention policy has new evidence.",
    )
    .await?;
    let hekate_position = new_position(hekate, "workspace retention", Stance::Support);
    let user_position = new_position(user, "workspace retention", Stance::Oppose);
    record(
        store,
        hekate,
        EventKind::PositionEstablished,
        EntityKind::Position,
        hekate_position.id.uuid(),
        &hekate_position,
    )
    .await?;
    record(
        store,
        user,
        EventKind::PositionEstablished,
        EntityKind::Position,
        user_position.id.uuid(),
        &user_position,
    )
    .await?;
    let conflict = Conflict {
        id: ConflictId::new(),
        subject: "workspace retention".to_owned(),
        participant_positions: vec![hekate_position.id, user_position.id],
        status: ConflictStatus::Open,
        revision: 1,
        reasons: vec!["the participants disagree on retention".to_owned()],
        evidence_refs: vec![evidence],
        alternatives: Vec::new(),
        reconsideration_conditions: vec!["new retention evidence".to_owned()],
        unresolved_questions: vec!["Should HEKATE retain the current workspace?".to_owned()],
        resolution: None,
        resolved_at: None,
        created_at: "2026-01-01T00:00:00Z".to_owned(),
    };
    record(
        store,
        hekate,
        EventKind::ConflictOpened,
        EntityKind::Conflict,
        conflict.id.uuid(),
        &conflict,
    )
    .await?;
    Ok(evidence)
}

async fn evaluate_conflict_proposal() -> anyhow::Result<CaseResult> {
    let (store, _) = open_store(false).await?;
    let (hekate, user) = seed_principals(&store).await?;
    let evidence = seed_conflict(&store, hekate, user).await?;
    let before = store.load_events().await?.len();
    let result = InitiativeService::new(store.clone(), hekate, user)
        .run_once()
        .await?;
    let proposal = proposed(&result);
    let state = store.load_state().await?;
    let passed = proposal.as_ref().is_some_and(|proposal| {
        proposal.kind == InitiativeKind::Question
            && proposal.source_entity.kind == EntityKind::Conflict
            && proposal.status == InitiativeStatus::Ready
            && proposal.source_event_ids.contains(&evidence)
            && proposal.as_of_revision == before as u64
    }) && state.initiatives.len() == 1
        && store.load_events().await?.len() == before + 2;
    finish(
        &store,
        passed,
        format!("observed={result:?}; before={before}; expected=one_ready_question"),
    )
    .await
}

async fn evaluate_commitment_proposal() -> anyhow::Result<CaseResult> {
    let (store, _) = open_store(false).await?;
    let (hekate, user) = seed_principals(&store).await?;
    let (_, evidence) = record_observation(
        &store,
        user,
        "I requested the report HEKATE promised to send.",
    )
    .await?;
    let commitment = Commitment {
        id: CommitmentId::new(),
        debtor_principal_id: hekate,
        creditor_principal_id: user,
        promise: "send the report".to_owned(),
        status: CommitmentStatus::Open,
        source_event_id: Some(evidence),
        created_at: "2026-01-01T00:00:00Z".to_owned(),
    };
    let creation_event = record(
        &store,
        hekate,
        EventKind::CommitmentCreated,
        EntityKind::Commitment,
        commitment.id.uuid(),
        &commitment,
    )
    .await?;
    let events_before = store.load_events().await?;
    let creation_sequence = events_before
        .iter()
        .position(|event| event.event_id == creation_event)
        .map(|index| index as u64 + 1)
        .unwrap_or_default();
    let result = InitiativeService::new(store.clone(), hekate, user)
        .run_once()
        .await?;
    let proposal = proposed(&result);
    let passed = proposal.as_ref().is_some_and(|proposal| {
        proposal.kind == InitiativeKind::FollowUp
            && proposal.source_entity.kind == EntityKind::Commitment
            && proposal.source_version == creation_sequence
            && proposal.source_event_ids == vec![evidence]
            && proposal.status == InitiativeStatus::Ready
    });
    finish(
        &store,
        passed,
        format!(
            "observed={result:?}; verified_creation_sequence={creation_sequence}; expected=ready_follow_up"
        ),
    )
    .await
}

async fn evaluate_position_reconsideration() -> anyhow::Result<CaseResult> {
    let (store, _) = open_store(false).await?;
    let (hekate, user) = seed_principals(&store).await?;
    let position = new_position(hekate, "workspace retention", Stance::Support);
    record(
        &store,
        hekate,
        EventKind::PositionEstablished,
        EntityKind::Position,
        position.id.uuid(),
        &position,
    )
    .await?;
    let service = InitiativeService::new(store.clone(), hekate, user);
    let initial = service.run_once().await?;
    let (_, evidence) = record_observation(
        &store,
        user,
        "New workspace retention evidence changes the assumption.",
    )
    .await?;
    let reconsidered = service.run_once().await?;
    let proposal = proposed(&reconsidered);
    let passed = matches!(initial, InitiativeRunResult::NoCandidate)
        && proposal.as_ref().is_some_and(|proposal| {
            proposal.kind == InitiativeKind::Challenge
                && proposal.source_entity.kind == EntityKind::Position
                && proposal.source_entity.id == position.id.uuid()
                && proposal.source_event_ids.contains(&evidence)
                && proposal.status == InitiativeStatus::Ready
        });
    finish(
        &store,
        passed,
        format!("initial={initial:?}; after_new_evidence={reconsidered:?}"),
    )
    .await
}

async fn evaluate_retry_and_restart() -> anyhow::Result<CaseResult> {
    let (store, temp_db) = open_store(true).await?;
    let (hekate, user) = seed_principals(&store).await?;
    seed_conflict(&store, hekate, user).await?;
    let service = InitiativeService::new(store.clone(), hekate, user);
    let first = service.run_once().await?;
    let Some(proposal) = proposed(&first) else {
        let failed = finish(
            &store,
            false,
            format!("first_run={first:?}; expected=proposal"),
        )
        .await?;
        drop(service);
        drop(store);
        drop(temp_db);
        return Ok(failed);
    };
    let count_after_proposal = store.load_events().await?.len();
    let retry = service.run_once().await?;
    drop(service);
    drop(store);

    let db = temp_db.as_ref().expect("durable test db");
    let restarted_store = Arc::new(SqliteStore::open(&db.url()).await?);
    let restarted_service = InitiativeService::new(restarted_store.clone(), hekate, user);
    let restart = restarted_service.run_once().await?;
    let final_count = restarted_store.load_events().await?.len();
    let passed = matches!(retry, InitiativeRunResult::Duplicate { ref fingerprint } if fingerprint == &proposal.fingerprint)
        && matches!(restart, InitiativeRunResult::Duplicate { ref fingerprint } if fingerprint == &proposal.fingerprint)
        && final_count == count_after_proposal;
    let result = finish(
        &restarted_store,
        passed,
        format!(
            "first={first:?}; retry={retry:?}; restart={restart:?}; event_count={count_after_proposal}->{final_count}"
        ),
    )
    .await?;
    drop(restarted_service);
    drop(restarted_store);
    drop(temp_db);
    Ok(result)
}

async fn evaluate_dismissal() -> anyhow::Result<CaseResult> {
    let (store, _) = open_store(false).await?;
    let (hekate, user) = seed_principals(&store).await?;
    seed_conflict(&store, hekate, user).await?;
    let service = InitiativeService::new(store.clone(), hekate, user);
    let first = service.run_once().await?;
    let Some(proposal) = proposed(&first) else {
        return finish(
            &store,
            false,
            format!("first_run={first:?}; expected=proposal"),
        )
        .await;
    };
    let dismissed = service.dismiss(proposal.id).await?;
    let after_dismissal = store.load_events().await?.len();
    let retry = service.run_once().await?;
    let state = store.load_state().await?;
    let final_count = store.load_events().await?.len();
    let event_counts = store.load_events().await?.iter().fold(
        (0, 0, 0),
        |(proposed, readied, dismissed), event| match event.event_kind {
            EventKind::InitiativeProposed => (proposed + 1, readied, dismissed),
            EventKind::InitiativeReadied => (proposed, readied + 1, dismissed),
            EventKind::InitiativeDismissed => (proposed, readied, dismissed + 1),
            _ => (proposed, readied, dismissed),
        },
    );
    let passed = matches!(dismissed, InitiativeRunResult::Dismissed { .. })
        && matches!(retry, InitiativeRunResult::Duplicate { .. })
        && state
            .initiatives
            .get(&proposal.id)
            .is_some_and(|proposal| proposal.status == InitiativeStatus::Dismissed)
        && final_count == after_dismissal
        && event_counts == (1, 1, 1);
    finish(
        &store,
        passed,
        format!(
            "first={first:?}; dismissed={dismissed:?}; retry={retry:?}; events={after_dismissal}->{final_count}; lifecycle_counts={event_counts:?}"
        ),
    )
    .await
}

async fn evaluate_old_goal_and_run() -> anyhow::Result<CaseResult> {
    let (store, _) = open_store(false).await?;
    let (hekate, user) = seed_principals(&store).await?;
    let goal = Goal {
        id: GoalId::new(),
        owner_principal_id: user,
        participants: vec![user],
        title: "Delete the obsolete workspace".to_owned(),
        description: "Historical request; no active authorization".to_owned(),
        status: GoalStatus::Active,
        created_at: "2020-01-01T00:00:00Z".to_owned(),
    };
    record(
        &store,
        user,
        EventKind::GoalCreated,
        EntityKind::Goal,
        goal.id.uuid(),
        &goal,
    )
    .await?;
    let task = Task {
        id: TaskId::new(),
        goal_id: Some(goal.id),
        title: "Old unfinished deletion run".to_owned(),
        status: TaskStatus::Blocked,
        created_at: "2020-01-01T00:00:00Z".to_owned(),
    };
    record(
        &store,
        user,
        EventKind::TaskCreated,
        EntityKind::Task,
        task.id.uuid(),
        &task,
    )
    .await?;
    let run = Run {
        id: RunId::new(),
        task_id: Some(task.id),
        status: RunStatus::NeedsAttention,
        started_at: "2020-01-01T00:00:00Z".to_owned(),
        completed_at: None,
    };
    record(
        &store,
        hekate,
        EventKind::RunStarted,
        EntityKind::Run,
        run.id.uuid(),
        &run,
    )
    .await?;
    let result = InitiativeService::new(store.clone(), hekate, user)
        .run_once()
        .await?;
    let events = store.load_events().await?;
    let external_count = capability_execution_events(&events);
    let state = store.load_state().await?;
    let passed = matches!(result, InitiativeRunResult::NoCandidate)
        && state.initiatives.is_empty()
        && !events.iter().any(|event| {
            matches!(
                event.event_kind,
                EventKind::InitiativeProposed
                    | EventKind::InitiativeReadied
                    | EventKind::OperationStarted
                    | EventKind::OperationSucceeded
                    | EventKind::OperationFailed
                    | EventKind::OperationStateUnknown
            )
        });
    finish(
        &store,
        passed,
        format!("observed={result:?}; old_user_owned_deletion_goal; run=needs_attention; capability_events={external_count}"),
    )
    .await
}

fn proposed(result: &InitiativeRunResult) -> Option<&InitiativeProposal> {
    match result {
        InitiativeRunResult::Proposed { proposal } => Some(proposal),
        _ => None,
    }
}

async fn finish(
    store: &Arc<SqliteStore>,
    passed: bool,
    detail: String,
) -> anyhow::Result<CaseResult> {
    let events = store.load_events().await?;
    let state = store.load_state().await?;
    let replay_match = Projector::replay(&events)? == state;
    Ok(CaseResult {
        passed: passed && replay_match,
        replay_match,
        external_capability_execution_count: capability_execution_events(&events),
        detail,
    })
}

fn capability_execution_events(events: &[ExperienceEvent]) -> usize {
    events
        .iter()
        .filter(|event| {
            matches!(
                event.event_kind,
                EventKind::OperationStarted
                    | EventKind::OperationSucceeded
                    | EventKind::OperationFailed
                    | EventKind::OperationStateUnknown
            )
        })
        .count()
}

fn git_commit() -> anyhow::Result<String> {
    let output = Command::new("git").args(["rev-parse", "HEAD"]).output()?;
    anyhow::ensure!(output.status.success(), "git rev-parse HEAD failed");
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

fn write_jsonl_atomically(path: &Path, rows: &[EvalRow]) -> anyhow::Result<()> {
    use std::io::Write;

    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("output path has no parent"))?;
    std::fs::create_dir_all(parent)?;
    anyhow::ensure!(!path.exists(), "refusing to overwrite {}", path.display());
    let temp = path.with_extension(format!("jsonl.tmp-{}", Uuid::new_v4()));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    for row in rows {
        serde_json::to_writer(&mut file, row)?;
        file.write_all(b"\n")?;
    }
    file.sync_all()?;
    std::fs::rename(&temp, path)?;
    Ok(())
}
