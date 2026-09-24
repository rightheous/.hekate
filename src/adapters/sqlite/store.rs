use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;
use sqlx::{Row, Sqlite, Transaction};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use uuid::Uuid;

use crate::adapters::sqlite::database::{Database, DatabaseError};
use crate::core::{
    normalize_description, sha256_hex, CognitiveTrace, CurrentState, EntityKind, EntityRef,
    EventId, EventKind, EventSource, ExperienceEvent,
};
use crate::ports::{Storage, StorageError};

#[derive(Debug, Error)]
pub enum StoreError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
}

#[derive(Clone)]
pub struct SqliteStore {
    database: Database,
}

impl SqliteStore {
    pub async fn open(url: &str) -> Result<Self, StoreError> {
        Ok(Self {
            database: Database::connect(url).await?,
        })
    }

    pub fn new(database: Database) -> Self {
        Self { database }
    }

    pub async fn state(&self) -> Result<CurrentState, StorageError> {
        self.load_state().await
    }

    pub async fn events(&self) -> Result<Vec<ExperienceEvent>, StorageError> {
        self.load_events().await
    }
}

#[async_trait]
impl Storage for SqliteStore {
    async fn load_state(&self) -> Result<CurrentState, StorageError> {
        let row = sqlx::query(
            "SELECT revision, state_json, integrity_hash FROM current_state WHERE id = 1",
        )
        .fetch_optional(self.database.pool())
        .await
        .map_err(|error| StorageError::Backend(error.to_string()))?;

        let Some(row) = row else {
            return Ok(CurrentState::default());
        };
        let revision: i64 = row
            .try_get("revision")
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        let state_json: String = row
            .try_get("state_json")
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        let integrity_hash: String = row
            .try_get("integrity_hash")
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        if !integrity_hash.is_empty() && snapshot_hash(&state_json) != integrity_hash {
            return Err(StorageError::Integrity {
                event_id: "current_state".to_owned(),
            });
        }
        if state_json.trim().is_empty() || state_json.trim() == "{}" {
            return Ok(CurrentState {
                revision: revision.max(0) as u64,
                ..CurrentState::default()
            });
        }
        let mut state: CurrentState = serde_json::from_str(&state_json)
            .map_err(|error| StorageError::InvalidState(error.to_string()))?;
        state.revision = revision.max(0) as u64;
        for candidate in state.integration_candidates.values_mut() {
            if candidate.as_of_revision == 0 {
                if let Some(run) = state.sleep_runs.get(&candidate.sleep_run_id) {
                    candidate.as_of_revision = run.high_water_revision;
                }
            }
        }
        Ok(state)
    }

    async fn load_events(&self) -> Result<Vec<ExperienceEvent>, StorageError> {
        let rows = sqlx::query(
            "SELECT event_id, occurred_at, recorded_at, kind, actor_id, subject_kind, subject_id, \
             payload, source_type, source_ref, causation_id, correlation_id, confidence, \
             integrity_hash FROM events ORDER BY sequence",
        )
        .fetch_all(self.database.pool())
        .await
        .map_err(|error| StorageError::Backend(error.to_string()))?;

        rows.into_iter()
            .map(|row| {
                let event = event_from_row(&row)?;
                if !event
                    .verify_integrity()
                    .map_err(|error| StorageError::Backend(error.to_string()))?
                {
                    return Err(StorageError::Integrity {
                        event_id: event.event_id.to_string(),
                    });
                }
                Ok(event)
            })
            .collect()
    }

    async fn commit_batch(
        &self,
        events: &[ExperienceEvent],
        state: &CurrentState,
        expected_base_revision: Option<u64>,
        cognitive_trace: Option<&CognitiveTrace>,
    ) -> Result<(), StorageError> {
        if events.is_empty() {
            return Err(StorageError::InvalidState(
                "cannot commit an empty event batch".to_owned(),
            ));
        }
        for event in events {
            let valid = event
                .verify_integrity()
                .map_err(|error| StorageError::Backend(error.to_string()))?;
            if !valid {
                return Err(StorageError::Integrity {
                    event_id: event.event_id.to_string(),
                });
            }
        }

        let mut transaction = self
            .database
            .pool()
            .begin()
            .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        let row = sqlx::query("SELECT COALESCE(MAX(sequence), 0) AS revision FROM events")
            .fetch_one(&mut *transaction)
            .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        let revision: i64 = row
            .try_get("revision")
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        let actual = revision.max(0) as u64;
        if let Some(expected) = expected_base_revision {
            if actual != expected {
                return Err(StorageError::StaleContext { expected, actual });
            }
        }
        let expected = actual + events.len() as u64;
        if state.revision != expected {
            return Err(StorageError::RevisionConflict {
                expected,
                received: state.revision,
            });
        }
        let start = state.applied_events.len().saturating_sub(events.len());
        for (index, event) in events.iter().enumerate() {
            if state.applied_events.get(start + index) != Some(&event.event_id) {
                return Err(StorageError::InvalidState(
                    "projected state does not end with the committed event batch".to_owned(),
                ));
            }
        }

        for event in events {
            let (subject_kind, subject_id) = event
                .subject
                .as_ref()
                .map(|subject| {
                    (
                        Some(entity_kind_text(&subject.kind)),
                        Some(subject.id.to_string()),
                    )
                })
                .unwrap_or((None, None));
            let confidence = event.confidence.map(f64::from);
            sqlx::query(
                "INSERT INTO events (event_id, occurred_at, recorded_at, kind, actor_id, subject_kind, subject_id,
                 payload, source_type, source_ref, causation_id, correlation_id, confidence, integrity_hash)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(event.event_id.to_string())
            .bind(&event.occurred_at)
            .bind(&event.recorded_at)
            .bind(event_kind_text(&event.event_kind))
            .bind(event.actor_id.to_string())
            .bind(subject_kind)
            .bind(subject_id)
            .bind(serde_json::to_string(&event.payload).map_err(|error| StorageError::Backend(error.to_string()))?)
            .bind(&event.source.source_type)
            .bind(&event.source.source_ref)
            .bind(event.causation_id.map(|id| id.to_string()))
            .bind(&event.correlation_id)
            .bind(confidence)
            .bind(&event.integrity_hash)
            .execute(&mut *transaction)
            .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        }

        let state_json = serde_json::to_string(state)
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        sqlx::query(
            "UPDATE current_state SET revision = ?, state_json = ?, integrity_hash = ? WHERE id = 1",
        )
            .bind(state.revision as i64)
            .bind(&state_json)
            .bind(snapshot_hash(&state_json))
            .execute(&mut *transaction)
            .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        write_projection(&mut transaction, state).await?;
        if let Some(trace) = cognitive_trace {
            write_cognitive_trace(&mut transaction, trace).await?;
        }
        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        Ok(())
    }

    async fn record_cognitive_trace(&self, trace: &CognitiveTrace) -> Result<(), StorageError> {
        let mut transaction = self
            .database
            .pool()
            .begin()
            .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        write_cognitive_trace(&mut transaction, trace).await?;
        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        Ok(())
    }

    async fn acquire_foreground_lease(
        &self,
        owner: &str,
        ttl: Duration,
    ) -> Result<bool, StorageError> {
        let now = unix_time_ms();
        let expires = lease_deadline(now, ttl);
        let mut transaction = self
            .database
            .pool()
            .begin()
            .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        sqlx::query("DELETE FROM foreground_leases WHERE expires_at_ms <= ?")
            .bind(now)
            .execute(&mut *transaction)
            .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        let result = sqlx::query(
            "INSERT INTO foreground_leases (owner_id, expires_at_ms) VALUES (?, ?) \
             ON CONFLICT(owner_id) DO NOTHING",
        )
        .bind(owner)
        .bind(expires)
        .execute(&mut *transaction)
        .await
        .map_err(|error| StorageError::Backend(error.to_string()))?;
        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        Ok(result.rows_affected() == 1)
    }

    async fn renew_foreground_lease(
        &self,
        owner: &str,
        ttl: Duration,
    ) -> Result<bool, StorageError> {
        let now = unix_time_ms();
        let result = sqlx::query(
            "UPDATE foreground_leases SET expires_at_ms = ? \
             WHERE owner_id = ? AND expires_at_ms > ?",
        )
        .bind(lease_deadline(now, ttl))
        .bind(owner)
        .bind(now)
        .execute(self.database.pool())
        .await
        .map_err(|error| StorageError::Backend(error.to_string()))?;
        Ok(result.rows_affected() == 1)
    }

    async fn release_foreground_lease(&self, owner: &str) -> Result<(), StorageError> {
        sqlx::query("DELETE FROM foreground_leases WHERE owner_id = ?")
            .bind(owner)
            .execute(self.database.pool())
            .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        Ok(())
    }

    async fn has_active_foreground_lease(&self) -> Result<bool, StorageError> {
        let row = sqlx::query(
            "SELECT EXISTS(SELECT 1 FROM foreground_leases WHERE expires_at_ms > ?) AS active",
        )
        .bind(unix_time_ms())
        .fetch_one(self.database.pool())
        .await
        .map_err(|error| StorageError::Backend(error.to_string()))?;
        let active: i64 = row
            .try_get("active")
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        Ok(active != 0)
    }

    async fn shutdown(&self) -> Result<(), StorageError> {
        self.database.close().await;
        Ok(())
    }
}

fn unix_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn lease_deadline(now: i64, ttl: Duration) -> i64 {
    now.saturating_add(ttl.as_millis().min(i64::MAX as u128) as i64)
}

async fn write_cognitive_trace(
    transaction: &mut Transaction<'_, Sqlite>,
    trace: &CognitiveTrace,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO cognitive_traces
         (trace_id, outcome, provider, model, schema_version, context_sequence, context_hash,
          trace_json, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&trace.trace_id)
    .bind(&trace.outcome)
    .bind(&trace.provider)
    .bind(&trace.model)
    .bind(&trace.schema_version)
    .bind(trace.context_sequence as i64)
    .bind(&trace.context_hash)
    .bind(serde_json::to_string(trace).map_err(|error| StorageError::Backend(error.to_string()))?)
    .bind(&trace.created_at)
    .execute(&mut **transaction)
    .await
    .map_err(|error| StorageError::Backend(error.to_string()))?;
    Ok(())
}

async fn write_projection(
    transaction: &mut Transaction<'_, Sqlite>,
    state: &CurrentState,
) -> Result<(), StorageError> {
    for table in [
        "position_integration_materializations",
        "integration_candidate_materializations",
        "integration_candidate_verifications",
        "completion_claims",
        "completion_criteria",
        "principals",
        "identity_versions",
        "goals",
        "tasks",
        "runs",
        "working_states",
        "attempts",
        "relationships",
        "positions",
        "conflicts",
        "commitments",
        "action_intents",
        "operations",
        "artifacts",
        "memory_candidates",
        "active_memories",
        "approvals",
        "receipts",
        "verifications",
        "sleep_runs",
        "integration_candidates",
        "sleep_state",
    ] {
        sqlx::query(&format!("DELETE FROM {table}"))
            .execute(&mut **transaction)
            .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
    }
    for principal in state.principals.values() {
        sqlx::query(
            "INSERT INTO principals (principal_id, kind, name, identity_version_id) VALUES (?, ?, ?, ?)",
        )
        .bind(principal.id.to_string())
        .bind(enum_text(&principal.kind))
        .bind(&principal.name)
        .bind(principal.identity_version_id.map(|id| id.to_string()))
        .execute(&mut **transaction)
        .await
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    }
    for identity in state.identity_versions.values() {
        sqlx::query(
            "INSERT INTO identity_versions (identity_version_id, principal_id, version, name, values_json, boundaries_json, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(identity.id.to_string())
        .bind(identity.principal_id.to_string())
        .bind(identity.version as i64)
        .bind(&identity.name)
        .bind(json_text(&identity.values)?)
        .bind(json_text(&identity.boundaries)?)
        .bind(&identity.created_at)
        .execute(&mut **transaction)
        .await
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    }
    for goal in state.goals.values() {
        sqlx::query(
            "INSERT INTO goals (goal_id, owner_principal_id, title, status) VALUES (?, ?, ?, ?)",
        )
        .bind(goal.id.to_string())
        .bind(goal.owner_principal_id.to_string())
        .bind(&goal.title)
        .bind(enum_text(&goal.status))
        .execute(&mut **transaction)
        .await
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    }
    for task in state.tasks.values() {
        sqlx::query("INSERT INTO tasks (task_id, goal_id, title, status) VALUES (?, ?, ?, ?)")
            .bind(task.id.to_string())
            .bind(task.goal_id.map(|id| id.to_string()))
            .bind(&task.title)
            .bind(enum_text(&task.status))
            .execute(&mut **transaction)
            .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
    }
    for run in state.runs.values() {
        sqlx::query("INSERT INTO runs (run_id, task_id, status, started_at) VALUES (?, ?, ?, ?)")
            .bind(run.id.to_string())
            .bind(run.task_id.map(|id| id.to_string()))
            .bind(enum_text(&run.status))
            .bind(&run.started_at)
            .execute(&mut **transaction)
            .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
    }
    for working_state in state.working_states.values() {
        sqlx::query("INSERT INTO working_states (run_id, revision, state_json) VALUES (?, ?, ?)")
            .bind(working_state.run_id.to_string())
            .bind(working_state.revision as i64)
            .bind(json_text(working_state)?)
            .execute(&mut **transaction)
            .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
    }
    for attempt in state.attempts.values() {
        sqlx::query(
            "INSERT INTO attempts (attempt_id, run_id, status, state_json) VALUES (?, ?, ?, ?)",
        )
        .bind(attempt.id.to_string())
        .bind(attempt.run_id.to_string())
        .bind(enum_text(&attempt.status))
        .bind(json_text(attempt)?)
        .execute(&mut **transaction)
        .await
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    }
    for relationship in state.relationships.values() {
        sqlx::query("INSERT INTO relationships (relationship_id, state_json) VALUES (?, ?)")
            .bind(relationship.id.to_string())
            .bind(json_text(relationship)?)
            .execute(&mut **transaction)
            .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
    }
    for position in state.positions.values() {
        sqlx::query(
            "INSERT INTO positions (position_id, principal_id, subject, state_json) VALUES (?, ?, ?, ?)",
        )
        .bind(position.id.to_string())
        .bind(position.principal_id.to_string())
        .bind(&position.subject)
        .bind(json_text(position)?)
        .execute(&mut **transaction)
        .await
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    }
    for conflict in state.conflicts.values() {
        sqlx::query("INSERT INTO conflicts (conflict_id, subject, state_json) VALUES (?, ?, ?)")
            .bind(conflict.id.to_string())
            .bind(&conflict.subject)
            .bind(json_text(conflict)?)
            .execute(&mut **transaction)
            .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
    }
    for commitment in state.commitments.values() {
        sqlx::query(
            "INSERT INTO commitments (commitment_id, debtor_principal_id, creditor_principal_id, state_json)
             VALUES (?, ?, ?, ?)",
        )
        .bind(commitment.id.to_string())
        .bind(commitment.debtor_principal_id.to_string())
        .bind(commitment.creditor_principal_id.to_string())
        .bind(json_text(commitment)?)
        .execute(&mut **transaction)
        .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
    }
    for intent in state.action_intents.values() {
        sqlx::query(
            "INSERT INTO action_intents (intent_id, capability, operation, state_json) VALUES (?, ?, ?, ?)",
        )
        .bind(intent.id.to_string())
        .bind(&intent.capability)
        .bind(&intent.operation)
        .bind(json_text(intent)?)
        .execute(&mut **transaction)
        .await
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    }
    for operation in state.operations.values() {
        sqlx::query("INSERT INTO operations (operation_id, status, state_json) VALUES (?, ?, ?)")
            .bind(operation.id.to_string())
            .bind(enum_text(&operation.status))
            .bind(json_text(operation)?)
            .execute(&mut **transaction)
            .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
    }
    for artifact in state.artifacts.values() {
        sqlx::query(
            "INSERT INTO artifacts (artifact_id, path, content_hash, state_json) VALUES (?, ?, ?, ?)",
        )
        .bind(artifact.id.to_string())
        .bind(&artifact.path)
        .bind(&artifact.content_hash)
        .bind(json_text(artifact)?)
        .execute(&mut **transaction)
        .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
    }
    for candidate in state.memory_candidates.values() {
        sqlx::query(
            "INSERT INTO memory_candidates (memory_candidate_id, status, state_json) VALUES (?, ?, ?)",
        )
        .bind(candidate.id.to_string())
        .bind(enum_text(&candidate.status))
        .bind(json_text(candidate)?)
        .execute(&mut **transaction)
        .await
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    }
    for memory in state.active_memories.values() {
        sqlx::query("INSERT INTO active_memories (memory_id, status, state_json) VALUES (?, ?, ?)")
            .bind(memory.id.to_string())
            .bind(enum_text(&memory.status))
            .bind(json_text(memory)?)
            .execute(&mut **transaction)
            .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
    }
    for approval in state.approvals.values() {
        sqlx::query(
            "INSERT INTO approvals (approval_id, operation_id, status, state_json) VALUES (?, ?, ?, ?)",
        )
        .bind(approval.id.to_string())
        .bind(approval.operation_id.to_string())
        .bind(enum_text(&approval.status))
        .bind(json_text(approval)?)
        .execute(&mut **transaction)
        .await
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    }
    for receipt in state.receipts.values() {
        sqlx::query("INSERT INTO receipts (receipt_id, operation_id, state_json) VALUES (?, ?, ?)")
            .bind(receipt.id.to_string())
            .bind(receipt.operation_id.to_string())
            .bind(json_text(receipt)?)
            .execute(&mut **transaction)
            .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
    }
    for verification in state.verifications.values() {
        sqlx::query(
            "INSERT INTO verifications (verification_id, operation_id, status, state_json) VALUES (?, ?, ?, ?)",
        )
        .bind(verification.id.to_string())
        .bind(verification.operation_id.to_string())
        .bind(enum_text(&verification.status))
        .bind(json_text(verification)?)
        .execute(&mut **transaction)
        .await
            .map_err(|error| StorageError::Backend(error.to_string()))?;
    }
    for criterion in state.completion_criteria.values() {
        sqlx::query(
            "INSERT INTO completion_criteria
             (criterion_id, task_id, description, normalized_description, required, created_at)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(criterion.id.to_string())
        .bind(criterion.task_id.to_string())
        .bind(&criterion.description)
        .bind(normalize_description(&criterion.description))
        .bind(if criterion.required { 1_i64 } else { 0_i64 })
        .bind(&criterion.created_at)
        .execute(&mut **transaction)
        .await
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    }
    for claim in state.completion_claims.values() {
        sqlx::query(
            "INSERT INTO completion_claims
             (claim_id, task_id, criterion_id, disposition, confidence, evidence_json, blocker,
              fingerprint, as_of_sequence, supersedes, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(claim.id.to_string())
        .bind(claim.task_id.to_string())
        .bind(claim.criterion_id.to_string())
        .bind(enum_text(&claim.disposition))
        .bind(i64::from(claim.confidence))
        .bind(json_text(&claim.evidence_refs)?)
        .bind(&claim.blocker)
        .bind(&claim.fingerprint)
        .bind(claim.as_of_sequence as i64)
        .bind(claim.supersedes.map(|id| id.to_string()))
        .bind(&claim.created_at)
        .execute(&mut **transaction)
        .await
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    }
    for run in state.sleep_runs.values() {
        sqlx::query(
            "INSERT INTO sleep_runs
             (run_id, status, high_water_revision, cursor_before, cursor_after,
              seed_event_ids_json, processed_observation_count, created_candidate_count,
              started_at, finished_at, error_kind, context_budget_report_json)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(run.id.to_string())
        .bind(enum_text(&run.status))
        .bind(run.high_water_revision as i64)
        .bind(run.cursor_before as i64)
        .bind(run.cursor_after.map(|value| value as i64))
        .bind(json_text(&run.seed_event_ids)?)
        .bind(run.processed_observation_count as i64)
        .bind(run.created_candidate_count as i64)
        .bind(&run.started_at)
        .bind(&run.finished_at)
        .bind(&run.error_kind)
        .bind(
            run.context_budget_report
                .as_ref()
                .map(json_text)
                .transpose()?,
        )
        .execute(&mut **transaction)
        .await
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    }
    for candidate in state.integration_candidates.values() {
        sqlx::query(
            "INSERT INTO integration_candidates
             (candidate_id, sleep_run_id, kind, status, content, rationale,
              source_event_ids_json, counterevidence_event_ids_json, confidence,
              fingerprint, created_at, disposition, as_of_revision)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(candidate.id.to_string())
        .bind(candidate.sleep_run_id.to_string())
        .bind(enum_text(&candidate.kind))
        .bind(enum_text(&candidate.disposition))
        .bind(&candidate.content)
        .bind(&candidate.rationale)
        .bind(json_text(&candidate.source_event_ids)?)
        .bind(json_text(&candidate.counterevidence_event_ids)?)
        .bind(i64::from(candidate.confidence))
        .bind(&candidate.fingerprint)
        .bind(&candidate.created_at)
        .bind(enum_text(&candidate.disposition))
        .bind(candidate.as_of_revision as i64)
        .execute(&mut **transaction)
        .await
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    }
    for verification in state.integration_verifications.values() {
        sqlx::query(
            "INSERT INTO integration_candidate_verifications
             (candidate_id, previous_disposition, disposition, actor_id, reason,
              evidence_refs_json, as_of_revision, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(verification.candidate_id.to_string())
        .bind(enum_text(&verification.previous_disposition))
        .bind(enum_text(&verification.new_disposition))
        .bind(verification.actor_id.to_string())
        .bind(&verification.reason)
        .bind(json_text(&verification.evidence_refs)?)
        .bind(verification.as_of_revision as i64)
        .bind(&verification.created_at)
        .execute(&mut **transaction)
        .await
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    }
    for materialization in state.integration_materializations.values() {
        sqlx::query(
            "INSERT INTO integration_candidate_materializations
             (candidate_id, memory_candidate_id, memory_id, source_event_ids_json,
              counterevidence_event_ids_json, evidence_refs_json, fingerprint,
              as_of_revision, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(materialization.candidate_id.to_string())
        .bind(materialization.memory_candidate_id.to_string())
        .bind(materialization.memory_id.to_string())
        .bind(json_text(&materialization.source_event_ids)?)
        .bind(json_text(&materialization.counterevidence_event_ids)?)
        .bind(json_text(&materialization.evidence_refs)?)
        .bind(&materialization.fingerprint)
        .bind(materialization.as_of_revision as i64)
        .bind(&materialization.created_at)
        .execute(&mut **transaction)
        .await
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    }
    for materialization in state.position_integration_materializations.values() {
        sqlx::query(
            "INSERT INTO position_integration_materializations
             (candidate_id, position_id, position_event_id, action, prior_position_json,
              source_event_ids_json, counterevidence_event_ids_json, evidence_refs_json,
              fingerprint, as_of_revision, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(materialization.candidate_id.to_string())
        .bind(materialization.position_id.to_string())
        .bind(materialization.position_event_id.to_string())
        .bind(enum_text(&materialization.action))
        .bind(
            materialization
                .prior_position
                .as_ref()
                .map(json_text)
                .transpose()?,
        )
        .bind(json_text(&materialization.source_event_ids)?)
        .bind(json_text(&materialization.counterevidence_event_ids)?)
        .bind(json_text(&materialization.evidence_refs)?)
        .bind(&materialization.fingerprint)
        .bind(materialization.as_of_revision as i64)
        .bind(&materialization.created_at)
        .execute(&mut **transaction)
        .await
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    }
    sqlx::query("INSERT INTO sleep_state (id, cursor) VALUES (1, ?)")
        .bind(state.sleep_cursor as i64)
        .execute(&mut **transaction)
        .await
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    Ok(())
}

fn json_text<T: Serialize>(value: &T) -> Result<String, StorageError> {
    serde_json::to_string(value).map_err(|error| StorageError::Backend(error.to_string()))
}

fn enum_text<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value)
        .unwrap_or_default()
        .trim_matches('"')
        .to_owned()
}

fn snapshot_hash(value: &str) -> String {
    sha256_hex(value.as_bytes())
}

fn event_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<ExperienceEvent, StorageError> {
    let event_id = parse_id::<EventId>(
        row.try_get("event_id")
            .map_err(|error| StorageError::Backend(error.to_string()))?,
    )?;
    let actor_id = parse_id(
        row.try_get("actor_id")
            .map_err(|error| StorageError::Backend(error.to_string()))?,
    )?;
    let event_kind = parse_event_kind(
        row.try_get("kind")
            .map_err(|error| StorageError::Backend(error.to_string()))?,
    )?;
    let payload: Value = serde_json::from_str(
        &row.try_get::<String, _>("payload")
            .map_err(|error| StorageError::Backend(error.to_string()))?,
    )
    .map_err(|error| StorageError::Backend(error.to_string()))?;
    let subject_kind: Option<String> = row
        .try_get("subject_kind")
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    let subject_id: Option<String> = row
        .try_get("subject_id")
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    let subject = match (subject_kind, subject_id) {
        (Some(kind), Some(id)) => Some(EntityRef::new(parse_entity_kind(&kind)?, parse_uuid(&id)?)),
        (None, None) => None,
        _ => {
            return Err(StorageError::Backend(
                "event subject is incomplete".to_owned(),
            ))
        }
    };
    let confidence: Option<f64> = row
        .try_get("confidence")
        .map_err(|error| StorageError::Backend(error.to_string()))?;
    Ok(ExperienceEvent {
        event_id,
        occurred_at: row
            .try_get("occurred_at")
            .map_err(|error| StorageError::Backend(error.to_string()))?,
        recorded_at: row
            .try_get("recorded_at")
            .map_err(|error| StorageError::Backend(error.to_string()))?,
        actor_id,
        event_kind,
        subject,
        payload,
        source: EventSource {
            source_type: row
                .try_get("source_type")
                .map_err(|error| StorageError::Backend(error.to_string()))?,
            source_ref: row
                .try_get("source_ref")
                .map_err(|error| StorageError::Backend(error.to_string()))?,
        },
        causation_id: parse_optional_id(
            row.try_get::<Option<String>, _>("causation_id")
                .map_err(|error| StorageError::Backend(error.to_string()))?,
        )?,
        correlation_id: row
            .try_get("correlation_id")
            .map_err(|error| StorageError::Backend(error.to_string()))?,
        confidence: confidence.map(|value| value as f32),
        integrity_hash: row
            .try_get("integrity_hash")
            .map_err(|error| StorageError::Backend(error.to_string()))?,
    })
}

fn parse_uuid(value: &str) -> Result<Uuid, StorageError> {
    Uuid::parse_str(value).map_err(|error| StorageError::Backend(error.to_string()))
}

fn parse_id<T>(value: String) -> Result<T, StorageError>
where
    T: From<Uuid>,
{
    Ok(T::from(parse_uuid(&value)?))
}

fn parse_optional_id<T>(value: Option<String>) -> Result<Option<T>, StorageError>
where
    T: From<Uuid>,
{
    value.map(|item| parse_id(item)).transpose()
}

fn event_kind_text(kind: &EventKind) -> String {
    serde_json::to_string(kind)
        .unwrap_or_default()
        .trim_matches('"')
        .to_owned()
}

fn entity_kind_text(kind: &EntityKind) -> String {
    serde_json::to_string(kind)
        .unwrap_or_default()
        .trim_matches('"')
        .to_owned()
}

fn parse_event_kind(value: String) -> Result<EventKind, StorageError> {
    serde_json::from_str(&format!("\"{value}\""))
        .map_err(|error| StorageError::Backend(error.to_string()))
}

fn parse_entity_kind(value: &str) -> Result<EntityKind, StorageError> {
    serde_json::from_str(&format!("\"{value}\""))
        .map_err(|error| StorageError::Backend(error.to_string()))
}
