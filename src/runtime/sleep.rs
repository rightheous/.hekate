use std::collections::{HashMap, HashSet};

use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::core::event::EventError;
use crate::core::{
    integration_candidate_fingerprint, now, ConflictStatus, ContextBudgetReport, EntityKind,
    EntityRef, EventId, EventKind, EventSource, ExperienceEvent, IntegrationCandidate, RecallQuery,
    RecalledItem, SleepContext, SleepDeliberation, SleepRun, SleepRunId, SleepRunStatus, SleepSeed,
    VerificationDisposition, MAX_CANDIDATE_SOURCES, MAX_SLEEP_CANDIDATES, MAX_SLEEP_RECALL,
    MAX_SLEEP_SEEDS, MAX_SLEEP_TEXT, SLEEP_ANCHOR_BUDGET_BYTES, SLEEP_CONTEXT_HARD_LIMIT_BYTES,
    SLEEP_RECALL_BUDGET_BYTES, SLEEP_SEED_BUDGET_BYTES,
};
use crate::ports::{SleepCognitiveModel, Storage, StorageError};
use crate::runtime::projector::{ProjectionError, Projector};
use crate::runtime::recall::SemanticRecall;

#[derive(Debug, Error)]
pub enum SleepRuntimeError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Projection(#[from] ProjectionError),
    #[error(transparent)]
    Event(#[from] EventError),
    #[error("sleep serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("invalid sleep state: {0}")]
    Invalid(String),
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SleepOnceStatus {
    Idle,
    Deferred,
    Completed,
    RunningResumed,
    Interrupted,
    Failed,
}

#[derive(Clone, Debug, Serialize)]
pub struct SleepOnceResult {
    pub status: SleepOnceStatus,
    pub run_id: Option<SleepRunId>,
    pub high_water_revision: Option<u64>,
    pub cursor_before: Option<u64>,
    pub cursor_after: Option<u64>,
    pub processed_observations: u32,
    pub created_candidates: u32,
    pub resumed: bool,
    pub error_kind: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct SleepRunSummary {
    pub id: SleepRunId,
    pub status: SleepRunStatus,
    pub high_water_revision: u64,
    pub cursor_before: u64,
    pub cursor_after: Option<u64>,
    pub processed_observations: u32,
    pub created_candidates: u32,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub error_kind: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct SleepStatus {
    pub cursor: u64,
    pub active_run: Option<SleepRunSummary>,
    pub recent_runs: Vec<SleepRunSummary>,
    pub pending_candidates: usize,
    pub projection_verified: bool,
}

struct SeedWindow {
    event_ids: Vec<EventId>,
    seeds: Vec<SleepSeed>,
    cursor_after: Option<u64>,
    invalid_count: usize,
    deferred_seed_count: usize,
}

struct BuiltSleepContext {
    context: SleepContext,
    budget_report: ContextBudgetReport,
}

#[derive(Serialize)]
struct SleepAnchors {
    identity: Option<crate::core::IdentityVersion>,
    active_positions: Vec<crate::core::Position>,
    active_conflicts: Vec<crate::core::Conflict>,
    relationship: Option<crate::core::Relationship>,
    high_water_revision: u64,
}

pub struct SleepCoordinator<'a> {
    storage: &'a dyn Storage,
    projector: &'a Projector,
    model: Option<&'a dyn SleepCognitiveModel>,
    recall: Option<&'a SemanticRecall>,
    hekate_id: crate::core::PrincipalId,
    user_id: crate::core::PrincipalId,
}

impl<'a> SleepCoordinator<'a> {
    pub fn new(
        storage: &'a dyn Storage,
        projector: &'a Projector,
        model: Option<&'a dyn SleepCognitiveModel>,
        recall: Option<&'a SemanticRecall>,
        hekate_id: crate::core::PrincipalId,
        user_id: crate::core::PrincipalId,
    ) -> Self {
        Self {
            storage,
            projector,
            model,
            recall,
            hekate_id,
            user_id,
        }
    }

    pub async fn sleep_once(&self) -> Result<SleepOnceResult, SleepRuntimeError> {
        let state = self.storage.load_state().await?;
        if foreground_blocked(&state) {
            return Ok(empty_result(SleepOnceStatus::Deferred));
        }

        let events = self.storage.load_events().await?;
        let (run, resumed, state) = if let Some(run) = state
            .sleep_runs
            .values()
            .find(|run| matches!(run.status, SleepRunStatus::Running))
            .cloned()
        {
            (run, true, state)
        } else {
            let high_water_revision = state.revision;
            let window = seed_window(&events, state.sleep_cursor, high_water_revision);
            if window.invalid_count > 0 {
                tracing::warn!(
                    invalid_observation_count = window.invalid_count,
                    error_kind = "corrupt_observation_payload",
                    "sleep skipped damaged observation payloads"
                );
            }
            if window.seeds.is_empty() {
                return Ok(empty_result(if window.deferred_seed_count > 0 {
                    SleepOnceStatus::Deferred
                } else {
                    SleepOnceStatus::Idle
                }));
            }
            let run = SleepRun {
                id: SleepRunId::new(),
                status: SleepRunStatus::Running,
                high_water_revision,
                cursor_before: state.sleep_cursor,
                cursor_after: None,
                seed_event_ids: window.event_ids,
                processed_observation_count: window.seeds.len() as u32,
                created_candidate_count: 0,
                started_at: now(),
                finished_at: None,
                error_kind: (window.invalid_count > 0)
                    .then_some("skipped_corrupt_observation_payload".to_owned()),
                context_budget_report: None,
            };
            let event = self.sleep_event(
                self.hekate_id,
                EventKind::SleepRunStarted,
                &run,
                run.id,
                None,
            )?;
            let state = match self
                .projector
                .record_batch(&[event], Some(state.revision), None)
                .await
            {
                Ok(state) => state,
                Err(ProjectionError::StaleContext { .. })
                | Err(ProjectionError::Storage(StorageError::StaleContext { .. })) => {
                    let latest = self.storage.load_state().await?;
                    if foreground_blocked(&latest) {
                        return Ok(empty_result(SleepOnceStatus::Deferred));
                    }
                    return Err(SleepRuntimeError::Invalid(
                        "sleep start lost a concurrent state race".to_owned(),
                    ));
                }
                Err(error) => return Err(error.into()),
            };
            (run, false, state)
        };

        let events = self.storage.load_events().await?;
        let window = seed_window_from_run(&events, &run);
        if window.invalid_count > 0 {
            tracing::warn!(
                invalid_observation_count = window.invalid_count,
                error_kind = "corrupt_observation_payload",
                "sleep skipped damaged observation payloads"
            );
        }
        if window.seeds.is_empty() {
            return self
                .fail_run(run, "corrupt_observation_payload", state, None)
                .await;
        }
        if self.model.is_none() {
            return self
                .fail_run(run, "sleep_model_unavailable", state, None)
                .await;
        }

        let built_context = match self
            .build_context(
                &run,
                &window.seeds,
                &events,
                &state,
                window.deferred_seed_count,
            )
            .await
        {
            Ok(context) => context,
            Err(error) => {
                tracing::warn!(error = %error, "sleep context construction failed");
                return self
                    .fail_run(run, "context_build_failed", state, None)
                    .await;
            }
        };
        let context = built_context.context;
        let budget_report = built_context.budget_report;
        let deliberation = match self
            .model
            .expect("sleep model checked above")
            .deliberate_sleep(&context)
            .await
        {
            Ok(deliberation) => deliberation,
            Err(error) => {
                self.record_trace(error.trace()).await;
                let latest = self.storage.load_state().await?;
                if latest.revision != state.revision || foreground_blocked(&latest) {
                    return self
                        .interrupt_run(run, latest, Some(budget_report.clone()))
                        .await;
                }
                return self
                    .fail_run(run, error.kind(), latest, Some(budget_report.clone()))
                    .await;
            }
        };

        let latest = self.storage.load_state().await?;
        if latest.revision != state.revision || foreground_blocked(&latest) {
            return self
                .interrupt_run(run, latest, Some(budget_report.clone()))
                .await;
        }

        let candidates = match validate_deliberation(&deliberation, &context, &latest) {
            Ok(candidates) => candidates,
            Err(error) => {
                if let Some(trace) = deliberation.trace.as_ref() {
                    self.record_trace(trace).await;
                }
                return self
                    .fail_run(run, &error, latest, Some(budget_report.clone()))
                    .await;
            }
        };
        if let Some(trace) = deliberation.trace.as_ref() {
            if !trace.context_hash.is_empty() && trace.context_hash != context.snapshot_hash {
                self.record_trace(trace).await;
                return self
                    .fail_run(
                        run,
                        "sleep_trace_context_mismatch",
                        latest,
                        Some(budget_report.clone()),
                    )
                    .await;
            }
        }

        let cursor_after = window
            .cursor_after
            .ok_or_else(|| SleepRuntimeError::Invalid("sleep seed cursor is empty".to_owned()))?;
        let mut completed = run.clone();
        completed.status = SleepRunStatus::Completed;
        completed.cursor_after = Some(cursor_after);
        completed.created_candidate_count = candidates.len() as u32;
        completed.finished_at = Some(now());
        completed.error_kind = run.error_kind.clone();
        completed.context_budget_report = Some(budget_report.clone());

        let mut events_to_commit = Vec::with_capacity(candidates.len() + 1);
        for candidate in candidates {
            events_to_commit.push(self.sleep_event(
                self.hekate_id,
                EventKind::IntegrationCandidateCreated,
                &candidate,
                run.id,
                None,
            )?);
        }
        events_to_commit.push(self.sleep_event(
            self.hekate_id,
            EventKind::SleepRunCompleted,
            &completed,
            run.id,
            None,
        )?);
        let trace = deliberation.trace.as_ref();
        let committed = match self
            .projector
            .record_batch(&events_to_commit, Some(latest.revision), trace)
            .await
        {
            Ok(state) => state,
            Err(ProjectionError::StaleContext { .. })
            | Err(ProjectionError::Storage(StorageError::StaleContext { .. })) => {
                let current = self.storage.load_state().await?;
                return self.interrupt_run(run, current, Some(budget_report)).await;
            }
            Err(error) => return Err(error.into()),
        };

        Ok(result_from_run(
            &committed
                .sleep_runs
                .get(&run.id)
                .cloned()
                .unwrap_or(completed),
            if resumed {
                SleepOnceStatus::RunningResumed
            } else {
                SleepOnceStatus::Completed
            },
            resumed,
        ))
    }

    async fn build_context(
        &self,
        run: &SleepRun,
        seeds: &[SleepSeed],
        events: &[ExperienceEvent],
        state: &crate::core::CurrentState,
        deferred_seed_count: usize,
    ) -> Result<BuiltSleepContext, SleepRuntimeError> {
        let seed_ids = run.seed_event_ids.clone();
        let sequence_by_id = events
            .iter()
            .enumerate()
            .map(|(index, event)| (event.event_id, index as u64 + 1))
            .collect::<HashMap<_, _>>();
        let mut recalled = HashMap::<EventId, RecalledItem>::new();
        if let Some(recall) = self.recall {
            for seed in seeds {
                let query = RecallQuery {
                    text: seed.observation.content.clone(),
                    limit: 6,
                    exclude_event_ids: seed_ids.clone(),
                };
                let bundle = match recall.recall(&query, state, events).await {
                    Ok(bundle) => bundle,
                    Err(error) => {
                        tracing::warn!(
                            sleep_run_id = %run.id,
                            error = %error,
                            "sleep semantic recall unavailable"
                        );
                        continue;
                    }
                };
                for item in bundle.items {
                    if seed_ids.contains(&item.source_event_id)
                        || sequence_by_id
                            .get(&item.source_event_id)
                            .map(|sequence| *sequence > run.high_water_revision)
                            .unwrap_or(true)
                    {
                        continue;
                    }
                    let replace = recalled
                        .get(&item.source_event_id)
                        .map(|existing| item.score > existing.score)
                        .unwrap_or(true);
                    if replace {
                        recalled.insert(item.source_event_id, item);
                    }
                }
            }
        }
        let mut recalled_candidates = recalled.into_values().collect::<Vec<_>>();
        recalled_candidates.sort_by(|left, right| {
            right.score.total_cmp(&left.score).then_with(|| {
                left.source_event_id
                    .to_string()
                    .cmp(&right.source_event_id.to_string())
            })
        });

        let relationship = state
            .relationships
            .values()
            .find(|relationship| {
                relationship.participants.contains(&self.hekate_id)
                    && relationship.participants.contains(&self.user_id)
            })
            .cloned();
        let active_positions = state
            .active_positions(self.hekate_id)
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        let active_conflicts = state
            .conflicts
            .values()
            .filter(|conflict| {
                matches!(
                    conflict.status,
                    ConflictStatus::Open | ConflictStatus::Negotiating
                )
            })
            .cloned()
            .collect::<Vec<_>>();

        let anchors = SleepAnchors {
            identity: state.hekate_identity().cloned(),
            active_positions: active_positions.clone(),
            active_conflicts: active_conflicts.clone(),
            relationship: relationship.clone(),
            high_water_revision: run.high_water_revision,
        };
        let anchor_bytes = serialized_len(&anchors)?;
        if anchor_bytes > SLEEP_ANCHOR_BUDGET_BYTES {
            return Err(SleepRuntimeError::Invalid(
                "sleep anchors exceed their bounded size".to_owned(),
            ));
        }

        let seed_observations = seeds.to_vec();
        let seed_bytes = serialized_len(&seed_observations)?;
        if seed_bytes > SLEEP_SEED_BUDGET_BYTES {
            return Err(SleepRuntimeError::Invalid(
                "sleep seeds exceed their bounded size".to_owned(),
            ));
        }

        let mut recalled_experiences = Vec::new();
        let mut dropped_recall_count = 0;
        for (index, item) in recalled_candidates.into_iter().enumerate() {
            if index >= MAX_SLEEP_RECALL {
                dropped_recall_count += 1;
                continue;
            }
            recalled_experiences.push(item);
            if serialized_len(&recalled_experiences)? > SLEEP_RECALL_BUDGET_BYTES {
                recalled_experiences.pop();
                dropped_recall_count += 1;
            }
        }

        let mut context = SleepContext {
            sleep_run_id: run.id,
            high_water_revision: run.high_water_revision,
            seed_observations,
            recalled_experiences,
            identity: anchors.identity,
            active_positions,
            active_conflicts,
            relationship: anchors.relationship,
            snapshot_hash: String::new(),
        };

        loop {
            context.snapshot_hash = context_hash(&context)?;
            if serialized_len(&context)? <= SLEEP_CONTEXT_HARD_LIMIT_BYTES {
                break;
            }
            if context.recalled_experiences.pop().is_none() {
                return Err(SleepRuntimeError::Invalid(
                    "sleep context exceeds its bounded size".to_owned(),
                ));
            }
            dropped_recall_count += 1;
        }

        let budget_report = ContextBudgetReport {
            anchor_bytes,
            seed_bytes,
            recalled_bytes: serialized_len(&context.recalled_experiences)?,
            total_bytes: serialized_len(&context)?,
            included_seed_count: context.seed_observations.len(),
            deferred_seed_count,
            included_recall_count: context.recalled_experiences.len(),
            dropped_recall_count,
            hard_limit_bytes: SLEEP_CONTEXT_HARD_LIMIT_BYTES,
        };
        Ok(BuiltSleepContext {
            context,
            budget_report,
        })
    }

    async fn fail_run(
        &self,
        run: SleepRun,
        error_kind: &str,
        state: crate::core::CurrentState,
        context_budget_report: Option<ContextBudgetReport>,
    ) -> Result<SleepOnceResult, SleepRuntimeError> {
        let mut failed = run;
        failed.status = SleepRunStatus::Failed;
        failed.finished_at = Some(now());
        failed.error_kind = Some(error_kind.to_owned());
        failed.cursor_after = None;
        failed.context_budget_report = context_budget_report;
        let event = self.sleep_event(
            self.hekate_id,
            EventKind::SleepRunFailed,
            &failed,
            failed.id,
            None,
        )?;
        let state = self
            .projector
            .record_batch(&[event], Some(state.revision), None)
            .await?;
        Ok(result_from_run(
            state.sleep_runs.get(&failed.id).unwrap_or(&failed),
            SleepOnceStatus::Failed,
            false,
        ))
    }

    async fn interrupt_run(
        &self,
        run: SleepRun,
        state: crate::core::CurrentState,
        context_budget_report: Option<ContextBudgetReport>,
    ) -> Result<SleepOnceResult, SleepRuntimeError> {
        let mut interrupted = run;
        interrupted.status = SleepRunStatus::Interrupted;
        interrupted.finished_at = Some(now());
        interrupted.error_kind = Some("foreground_activity".to_owned());
        interrupted.cursor_after = None;
        interrupted.context_budget_report = context_budget_report;
        let event = self.sleep_event(
            self.hekate_id,
            EventKind::SleepRunInterrupted,
            &interrupted,
            interrupted.id,
            None,
        )?;
        let state = self
            .projector
            .record_batch(&[event], Some(state.revision), None)
            .await?;
        Ok(result_from_run(
            state
                .sleep_runs
                .get(&interrupted.id)
                .unwrap_or(&interrupted),
            SleepOnceStatus::Interrupted,
            false,
        ))
    }

    async fn record_trace(&self, trace: &crate::core::CognitiveTrace) {
        if let Err(error) = self.storage.record_cognitive_trace(trace).await {
            tracing::warn!(error = %error, "sleep cognitive trace was not stored");
        }
    }

    fn sleep_event<T: Serialize>(
        &self,
        actor_id: crate::core::PrincipalId,
        event_kind: EventKind,
        payload: &T,
        run_id: SleepRunId,
        causation_id: Option<EventId>,
    ) -> Result<ExperienceEvent, SleepRuntimeError> {
        Ok(ExperienceEvent::new(
            actor_id,
            event_kind,
            Some(match payload_subject(payload, run_id)? {
                Some((kind, id)) => EntityRef::new(kind, id),
                None => EntityRef::new(EntityKind::SleepRun, run_id.uuid()),
            }),
            serde_json::to_value(payload)?,
            EventSource::new("sleep", Some(run_id.to_string())),
            causation_id,
            Some(run_id.to_string()),
            Some(1.0),
        )?)
    }
}

fn payload_subject<T: Serialize>(
    payload: &T,
    run_id: SleepRunId,
) -> Result<Option<(EntityKind, uuid::Uuid)>, SleepRuntimeError> {
    let value = serde_json::to_value(payload)?;
    if value.get("sleep_run_id").is_some() {
        let id = value
            .get("id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| SleepRuntimeError::Invalid("sleep payload has no ID".to_owned()))?;
        let uuid = uuid::Uuid::parse_str(id)
            .map_err(|_| SleepRuntimeError::Invalid("sleep payload ID is invalid".to_owned()))?;
        return Ok(Some((EntityKind::IntegrationCandidate, uuid)));
    }
    Ok(Some((EntityKind::SleepRun, run_id.uuid())))
}

pub async fn sleep_status(storage: &dyn Storage) -> Result<SleepStatus, SleepRuntimeError> {
    let state = storage.load_state().await?;
    let projection_verified = crate::runtime::recovery::recover(storage)
        .await
        .map(|report| report.projection_verified)
        .unwrap_or(false);
    let active_run = state
        .sleep_runs
        .values()
        .find(|run| matches!(run.status, SleepRunStatus::Running))
        .map(summary);
    let mut recent_runs = state.sleep_runs.values().map(summary).collect::<Vec<_>>();
    recent_runs.sort_by(|left, right| right.started_at.cmp(&left.started_at));
    recent_runs.truncate(5);
    Ok(SleepStatus {
        cursor: state.sleep_cursor,
        active_run,
        recent_runs,
        pending_candidates: state
            .integration_candidates
            .values()
            .filter(|candidate| {
                matches!(
                    candidate.disposition,
                    VerificationDisposition::NeedsValidation
                )
            })
            .count(),
        projection_verified,
    })
}

fn summary(run: &SleepRun) -> SleepRunSummary {
    SleepRunSummary {
        id: run.id,
        status: run.status.clone(),
        high_water_revision: run.high_water_revision,
        cursor_before: run.cursor_before,
        cursor_after: run.cursor_after,
        processed_observations: run.processed_observation_count,
        created_candidates: run.created_candidate_count,
        started_at: run.started_at.clone(),
        finished_at: run.finished_at.clone(),
        error_kind: run.error_kind.clone(),
    }
}

fn result_from_run(run: &SleepRun, status: SleepOnceStatus, resumed: bool) -> SleepOnceResult {
    SleepOnceResult {
        status,
        run_id: Some(run.id),
        high_water_revision: Some(run.high_water_revision),
        cursor_before: Some(run.cursor_before),
        cursor_after: run.cursor_after,
        processed_observations: run.processed_observation_count,
        created_candidates: run.created_candidate_count,
        resumed,
        error_kind: run.error_kind.clone(),
    }
}

fn empty_result(status: SleepOnceStatus) -> SleepOnceResult {
    SleepOnceResult {
        status,
        run_id: None,
        high_water_revision: None,
        cursor_before: None,
        cursor_after: None,
        processed_observations: 0,
        created_candidates: 0,
        resumed: false,
        error_kind: None,
    }
}

fn foreground_blocked(state: &crate::core::CurrentState) -> bool {
    state.active_run().is_some()
        || state
            .attempts
            .values()
            .any(|attempt| matches!(attempt.status, crate::core::AttemptStatus::Started))
        || state
            .approvals
            .values()
            .any(|approval| matches!(approval.status, crate::core::ApprovalStatus::Pending))
        || state.operations.values().any(|operation| {
            matches!(
                operation.status,
                crate::core::OperationStatus::Started | crate::core::OperationStatus::Unknown
            )
        })
}

fn serialized_len<T: Serialize>(value: &T) -> Result<usize, serde_json::Error> {
    Ok(serde_json::to_vec(value)?.len())
}

fn fits_seed_budget(seeds: &[SleepSeed], candidate: &SleepSeed) -> bool {
    let mut next = seeds.to_vec();
    next.push(candidate.clone());
    serialized_len(&next)
        .map(|bytes| bytes <= SLEEP_SEED_BUDGET_BYTES)
        .unwrap_or(false)
}

fn seed_window(events: &[ExperienceEvent], cursor: u64, high_water_revision: u64) -> SeedWindow {
    let mut seeds = Vec::new();
    let mut invalid_count = 0;
    let mut available_seed_count = 0usize;
    let mut accepting = true;
    for (index, event) in events.iter().enumerate() {
        let sequence = index as u64 + 1;
        if sequence <= cursor || sequence > high_water_revision {
            continue;
        }
        if event.event_kind != EventKind::ObservationRecorded {
            continue;
        }
        let Some(seed) = decode_seed(events, event.event_id, high_water_revision) else {
            invalid_count += 1;
            continue;
        };
        available_seed_count += 1;
        if !accepting {
            continue;
        }
        if seeds.len() >= MAX_SLEEP_SEEDS || !fits_seed_budget(&seeds, &seed) {
            accepting = false;
            continue;
        }
        seeds.push(seed);
    }
    let event_ids = seeds.iter().map(|seed| seed.event_id).collect::<Vec<_>>();
    let included_seed_count = event_ids.len();
    let cursor_after = seeds.last().map(|seed| seed.sequence);
    SeedWindow {
        event_ids,
        seeds,
        cursor_after,
        invalid_count,
        deferred_seed_count: available_seed_count.saturating_sub(included_seed_count),
    }
}

fn seed_window_from_run(events: &[ExperienceEvent], run: &SleepRun) -> SeedWindow {
    let cursor_after = run
        .seed_event_ids
        .iter()
        .filter_map(|event_id| {
            events
                .iter()
                .enumerate()
                .find(|(_, event)| event.event_id == *event_id)
                .map(|(index, _)| index as u64 + 1)
        })
        .max();
    let seeds: Vec<SleepSeed> = run
        .seed_event_ids
        .iter()
        .filter_map(|event_id| decode_seed(events, *event_id, run.high_water_revision))
        .collect();
    let deferred_seed_count = events
        .iter()
        .enumerate()
        .filter(|(index, event)| {
            let sequence = *index as u64 + 1;
            sequence > cursor_after.unwrap_or(run.cursor_before)
                && sequence <= run.high_water_revision
                && event.event_kind == EventKind::ObservationRecorded
        })
        .filter(|(_, event)| decode_seed(events, event.event_id, run.high_water_revision).is_some())
        .count();
    SeedWindow {
        event_ids: run.seed_event_ids.clone(),
        invalid_count: run.seed_event_ids.len().saturating_sub(seeds.len()),
        seeds,
        cursor_after,
        deferred_seed_count,
    }
}

fn decode_seed(
    events: &[ExperienceEvent],
    event_id: EventId,
    high_water_revision: u64,
) -> Option<SleepSeed> {
    let (index, event) = events
        .iter()
        .enumerate()
        .find(|(_, event)| event.event_id == event_id)?;
    let sequence = index as u64 + 1;
    if sequence > high_water_revision || event.event_kind != EventKind::ObservationRecorded {
        return None;
    }
    let observation: crate::core::Observation =
        serde_json::from_value(event.payload.clone()).ok()?;
    if observation.content.trim().is_empty()
        || observation.id.uuid()
            != event
                .subject
                .as_ref()
                .filter(|subject| subject.kind == EntityKind::Observation)
                .map(|subject| subject.id)?
    {
        return None;
    }
    Some(SleepSeed {
        event_id,
        sequence,
        observation,
    })
}

fn validate_deliberation(
    deliberation: &SleepDeliberation,
    context: &SleepContext,
    state: &crate::core::CurrentState,
) -> Result<Vec<IntegrationCandidate>, String> {
    if deliberation.candidates.len() > MAX_SLEEP_CANDIDATES
        || deliberation.draft_summary.trim().len() > MAX_SLEEP_TEXT
        || deliberation.self_review.weak_points.len() > MAX_SLEEP_CANDIDATES
        || deliberation
            .self_review
            .possible_counterevidence_event_ids
            .len()
            > MAX_CANDIDATE_SOURCES
        || deliberation
            .self_review
            .weak_points
            .iter()
            .any(|item| item.trim().len() > MAX_SLEEP_TEXT)
    {
        return Err("sleep deliberation exceeds its bounds".to_owned());
    }
    let allowed = context_event_ids(context);
    if deliberation
        .self_review
        .possible_counterevidence_event_ids
        .iter()
        .any(|event_id| !allowed.contains(event_id))
    {
        return Err("sleep self-review references an event outside the context".to_owned());
    }

    let mut candidates: Vec<IntegrationCandidate> = Vec::new();
    for draft in &deliberation.candidates {
        if draft.content.trim().is_empty() || draft.rationale.trim().is_empty() {
            return Err("candidate content and rationale cannot be empty".to_owned());
        }
        if draft.content.trim().len() > MAX_SLEEP_TEXT
            || draft.rationale.trim().len() > MAX_SLEEP_TEXT
            || draft.source_event_ids.len() > MAX_CANDIDATE_SOURCES
            || draft.counterevidence_event_ids.len() > MAX_CANDIDATE_SOURCES
            || draft.confidence > 100
        {
            return Err("candidate exceeds its bounds".to_owned());
        }
        let source_event_ids = dedup_ids(&draft.source_event_ids);
        let counterevidence_event_ids = dedup_ids(&draft.counterevidence_event_ids);
        if source_event_ids.is_empty() {
            return Err("candidate needs at least one source event".to_owned());
        }
        if source_event_ids
            .iter()
            .chain(counterevidence_event_ids.iter())
            .any(|event_id| !allowed.contains(event_id))
        {
            return Err("candidate provenance references an event outside the context".to_owned());
        }
        if source_event_ids
            .iter()
            .any(|event_id| counterevidence_event_ids.contains(event_id))
        {
            return Err("candidate source and counterevidence overlap".to_owned());
        }
        let fingerprint = integration_candidate_fingerprint(
            &draft.kind,
            draft.content.trim(),
            &source_event_ids,
            &counterevidence_event_ids,
        );
        if state
            .integration_candidates
            .values()
            .any(|candidate| candidate.fingerprint == fingerprint)
            || candidates
                .iter()
                .any(|candidate| candidate.fingerprint == fingerprint)
        {
            continue;
        }
        candidates.push(IntegrationCandidate {
            id: crate::core::IntegrationCandidateId::new(),
            sleep_run_id: context.sleep_run_id,
            kind: draft.kind.clone(),
            disposition: VerificationDisposition::NeedsValidation,
            as_of_revision: context.high_water_revision,
            content: draft.content.trim().to_owned(),
            rationale: draft.rationale.trim().to_owned(),
            source_event_ids,
            counterevidence_event_ids,
            confidence: draft.confidence,
            fingerprint,
            created_at: now(),
        });
    }
    Ok(candidates)
}

fn dedup_ids(ids: &[EventId]) -> Vec<EventId> {
    let mut result = ids.to_vec();
    result.sort();
    result.dedup();
    result
}

fn context_event_ids(context: &SleepContext) -> HashSet<EventId> {
    context
        .seed_observations
        .iter()
        .map(|seed| seed.event_id)
        .chain(
            context
                .recalled_experiences
                .iter()
                .map(|item| item.source_event_id),
        )
        .collect()
}

fn context_hash(context: &SleepContext) -> Result<String, serde_json::Error> {
    let mut snapshot = context.clone();
    snapshot.snapshot_hash.clear();
    let digest = Sha256::digest(serde_json::to_vec(&snapshot)?);
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}
