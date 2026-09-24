use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Serialize;
use thiserror::Error;

use crate::core::transition::transition_task;
use crate::core::{
    completion_fingerprint, normalize_description, normalize_evidence_refs, ArtifactId,
    CompletionClaim, CompletionClaimId, CompletionClaimTransition, CompletionCriterion,
    CompletionCriterionId, CurrentState, EntityKind, EntityRef, EventId, EventKind, EventSource,
    EvidenceRef, ExperienceEvent, Task, TaskId, TaskStatus, VerificationDisposition,
};
use crate::ports::{Storage, StorageError};
use crate::runtime::projector::{ProjectionError, Projector};

#[derive(Debug, Error)]
pub enum CompletionError {
    #[error("unknown task {0}")]
    UnknownTask(TaskId),
    #[error("unknown criterion {0}")]
    UnknownCriterion(CompletionCriterionId),
    #[error("criterion {criterion} does not belong to task {task}")]
    CriterionTaskMismatch {
        criterion: CompletionCriterionId,
        task: TaskId,
    },
    #[error("unknown claim {0}")]
    UnknownClaim(CompletionClaimId),
    #[error("criterion description cannot be empty")]
    EmptyCriterionDescription,
    #[error("criterion description already exists for task")]
    DuplicateCriterion,
    #[error("confidence must be between 0 and 100")]
    InvalidConfidence(u8),
    #[error("invalid source hash: {0}")]
    InvalidSourceHash(String),
    #[error("unknown event evidence {0}")]
    UnknownEventEvidence(EventId),
    #[error("unknown artifact evidence {0}")]
    UnknownArtifactEvidence(ArtifactId),
    #[error("artifact {artifact} is not sourced by event {event}")]
    ArtifactProvenanceMismatch {
        artifact: ArtifactId,
        event: EventId,
    },
    #[error("artifact {artifact} source hash does not match evidence")]
    ArtifactHashMismatch { artifact: ArtifactId },
    #[error("event {event} source hash does not match evidence")]
    EventHashMismatch { event: EventId },
    #[error("evidence sequence {sequence} is after claim as-of sequence {as_of}")]
    FutureEvidenceSequence { sequence: u64, as_of: u64 },
    #[error("duplicate claim fingerprint {0}")]
    DuplicateFingerprint(String),
    #[error("claim supersedes an unknown claim {0}")]
    UnknownSupersededClaim(CompletionClaimId),
    #[error("claim supersedes a different task or criterion")]
    SupersededClaimMismatch,
    #[error("invalid claim disposition transition from {from:?} to {to:?}")]
    InvalidDispositionTransition {
        from: VerificationDisposition,
        to: VerificationDisposition,
    },
    #[error("claim verification requires evidence")]
    EvidenceRequired,
    #[error("claim has an unresolved blocker")]
    BlockerPresent,
    #[error("verification reason cannot be empty")]
    InvalidReason,
    #[error("the model principal cannot verify completion claims")]
    ModelCannotVerify,
    #[error("stale revision: expected {expected}, actual {actual}")]
    StaleRevision { expected: u64, actual: u64 },
    #[error("task completion blocked: {gate:?}")]
    GateBlocked { gate: CompletionGateResult },
    #[error(transparent)]
    Transition(#[from] crate::core::transition::TransitionError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Projection(#[from] ProjectionError),
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
    #[error(transparent)]
    Event(#[from] crate::core::event::EventError),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CompletionGateResult {
    Ready {
        verified_claim_ids: Vec<CompletionClaimId>,
    },
    Blocked {
        missing_criterion_ids: Vec<CompletionCriterionId>,
        needs_validation_claim_ids: Vec<CompletionClaimId>,
        rejected_claim_ids: Vec<CompletionClaimId>,
        blockers: Vec<String>,
        pending_approval_ids: Vec<crate::core::ApprovalId>,
        unknown_operation_ids: Vec<crate::core::OperationId>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CompletionCriterionStatus {
    pub criterion_id: CompletionCriterionId,
    pub description: String,
    pub required: bool,
    pub latest_claim: Option<CompletionClaim>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CompletionStatusReport {
    pub task_id: TaskId,
    pub as_of_sequence: u64,
    pub criteria: Vec<CompletionCriterionStatus>,
    pub gate: CompletionGateResult,
}

#[derive(Clone)]
pub struct CompletionGate {
    storage: Arc<dyn Storage>,
}

impl CompletionGate {
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self { storage }
    }

    pub async fn define_criterion(
        &self,
        actor_id: crate::core::PrincipalId,
        task_id: TaskId,
        description: impl Into<String>,
        required: bool,
    ) -> Result<CompletionCriterion, CompletionError> {
        let state = self.storage.load_state().await?;
        ensure_task(&state, task_id)?;
        ensure_non_model_actor(&state, actor_id)?;
        let description = description.into().trim().to_owned();
        if description.is_empty() {
            return Err(CompletionError::EmptyCriterionDescription);
        }
        let normalized = normalize_description(&description);
        if state.completion_criteria.values().any(|criterion| {
            criterion.task_id == task_id
                && normalize_description(&criterion.description) == normalized
        }) {
            return Err(CompletionError::DuplicateCriterion);
        }
        let criterion = CompletionCriterion {
            id: CompletionCriterionId::new(),
            task_id,
            description,
            required,
            created_at: crate::core::model::now(),
        };
        let event = completion_event(
            actor_id,
            EventKind::CompletionCriterionDefined,
            EntityKind::CompletionCriterion,
            criterion.id.uuid(),
            &criterion,
            Some(task_id.to_string()),
            None,
            None,
        )?;
        self.commit(state.revision, event).await?;
        Ok(criterion)
    }

    pub async fn create_claim(
        &self,
        actor_id: crate::core::PrincipalId,
        task_id: TaskId,
        criterion_id: CompletionCriterionId,
        confidence: u8,
        evidence_refs: Vec<EvidenceRef>,
        blocker: Option<String>,
        supersedes: Option<CompletionClaimId>,
    ) -> Result<CompletionClaim, CompletionError> {
        let state = self.storage.load_state().await?;
        ensure_task(&state, task_id)?;
        ensure_non_model_actor(&state, actor_id)?;
        ensure_criterion(&state, task_id, criterion_id)?;
        if confidence > 100 {
            return Err(CompletionError::InvalidConfidence(confidence));
        }
        let events = self.storage.load_events().await?;
        let evidence_refs = normalize_evidence_refs(evidence_refs);
        validate_evidence_refs(&state, &events, &evidence_refs, state.revision)?;
        if state.completion_claims.values().any(|claim| {
            claim.fingerprint == completion_fingerprint(task_id, criterion_id, &evidence_refs)
        }) {
            return Err(CompletionError::DuplicateFingerprint(
                completion_fingerprint(task_id, criterion_id, &evidence_refs),
            ));
        }
        validate_supersedes(&state, task_id, criterion_id, supersedes)?;
        let fingerprint = completion_fingerprint(task_id, criterion_id, &evidence_refs);
        let claim = CompletionClaim {
            id: CompletionClaimId::new(),
            task_id,
            criterion_id,
            disposition: VerificationDisposition::NeedsValidation,
            confidence,
            evidence_refs,
            blocker: blocker.and_then(non_empty),
            fingerprint,
            as_of_sequence: state.revision,
            supersedes,
            created_at: crate::core::model::now(),
        };
        let claim = claim;
        if state
            .completion_claims
            .values()
            .any(|existing| existing.fingerprint == claim.fingerprint)
        {
            return Err(CompletionError::DuplicateFingerprint(claim.fingerprint));
        }
        let event = completion_event(
            actor_id,
            EventKind::CompletionClaimCreated,
            EntityKind::CompletionClaim,
            claim.id.uuid(),
            &claim,
            Some(task_id.to_string()),
            None,
            None,
        )?;
        self.commit(state.revision, event).await?;
        Ok(claim)
    }

    pub async fn verify_claim(
        &self,
        actor_id: crate::core::PrincipalId,
        claim_id: CompletionClaimId,
        reason: impl Into<String>,
    ) -> Result<CompletionClaim, CompletionError> {
        self.transition_claim(
            actor_id,
            claim_id,
            VerificationDisposition::Verified,
            reason.into(),
        )
        .await
    }

    pub async fn reject_claim(
        &self,
        actor_id: crate::core::PrincipalId,
        claim_id: CompletionClaimId,
        reason: impl Into<String>,
    ) -> Result<CompletionClaim, CompletionError> {
        self.transition_claim(
            actor_id,
            claim_id,
            VerificationDisposition::Rejected,
            reason.into(),
        )
        .await
    }

    pub async fn evaluate_task_completion(
        &self,
        task_id: TaskId,
        as_of_sequence: u64,
    ) -> Result<CompletionGateResult, CompletionError> {
        let events = self.storage.load_events().await?;
        let state = state_as_of(&events, as_of_sequence)?;
        evaluate_task_completion_in(&state, &events[..as_of_sequence as usize], task_id)
    }

    pub async fn complete_task(
        &self,
        actor_id: crate::core::PrincipalId,
        task_id: TaskId,
        expected_revision: u64,
    ) -> Result<Task, CompletionError> {
        let events = self.storage.load_events().await?;
        let state = Projector::replay(&events)?;
        ensure_task(&state, task_id)?;
        ensure_non_model_actor(&state, actor_id)?;
        let current = state
            .tasks
            .get(&task_id)
            .cloned()
            .ok_or(CompletionError::UnknownTask(task_id))?;
        if matches!(&current.status, TaskStatus::Completed) {
            return Ok(current);
        }
        if expected_revision != state.revision {
            return Err(CompletionError::StaleRevision {
                expected: expected_revision,
                actual: state.revision,
            });
        }

        let gate = evaluate_task_completion_in(&state, &events, task_id)?;
        if !matches!(gate, CompletionGateResult::Ready { .. }) {
            return Err(CompletionError::GateBlocked { gate });
        }

        let mut completed = current;
        transition_task(&mut completed, TaskStatus::Completed)?;
        let event = completion_event(
            actor_id,
            EventKind::TaskCompleted,
            EntityKind::Task,
            task_id.uuid(),
            &completed,
            Some(task_id.to_string()),
            None,
            None,
        )?;
        self.commit(state.revision, event).await?;
        Ok(completed)
    }

    pub async fn status(
        &self,
        task_id: TaskId,
        as_of_sequence: u64,
    ) -> Result<CompletionStatusReport, CompletionError> {
        let events = self.storage.load_events().await?;
        let state = state_as_of(&events, as_of_sequence)?;
        ensure_task(&state, task_id)?;
        let latest_claims = latest_claims(&events[..as_of_sequence as usize])?;
        let criteria = state
            .completion_criteria
            .values()
            .filter(|criterion| criterion.task_id == task_id)
            .map(|criterion| CompletionCriterionStatus {
                criterion_id: criterion.id,
                description: criterion.description.clone(),
                required: criterion.required,
                latest_claim: latest_claims
                    .get(&criterion.id)
                    .and_then(|claim_id| state.completion_claims.get(claim_id))
                    .cloned(),
            })
            .collect();
        let gate =
            evaluate_task_completion_in(&state, &events[..as_of_sequence as usize], task_id)?;
        Ok(CompletionStatusReport {
            task_id,
            as_of_sequence,
            criteria,
            gate,
        })
    }

    async fn transition_claim(
        &self,
        actor_id: crate::core::PrincipalId,
        claim_id: CompletionClaimId,
        disposition: VerificationDisposition,
        reason: String,
    ) -> Result<CompletionClaim, CompletionError> {
        let state = self.storage.load_state().await?;
        let Some(claim) = state.completion_claims.get(&claim_id).cloned() else {
            return Err(CompletionError::UnknownClaim(claim_id));
        };
        ensure_non_model_actor(&state, actor_id)?;
        if claim.disposition != VerificationDisposition::NeedsValidation
            || !matches!(
                disposition,
                VerificationDisposition::Verified | VerificationDisposition::Rejected
            )
        {
            return Err(CompletionError::InvalidDispositionTransition {
                from: claim.disposition,
                to: disposition,
            });
        }
        let reason = non_empty(reason).ok_or(CompletionError::InvalidReason)?;
        let events = self.storage.load_events().await?;
        if matches!(disposition, VerificationDisposition::Verified) {
            if claim.evidence_refs.is_empty() {
                return Err(CompletionError::EvidenceRequired);
            }
            if claim
                .blocker
                .as_deref()
                .and_then(trimmed_non_empty)
                .is_some()
            {
                return Err(CompletionError::BlockerPresent);
            }
        }
        validate_evidence_refs(&state, &events, &claim.evidence_refs, claim.as_of_sequence)?;
        let transition = CompletionClaimTransition {
            claim_id: claim.id,
            task_id: claim.task_id,
            criterion_id: claim.criterion_id,
            previous_disposition: claim.disposition,
            disposition,
            reason,
            evidence_refs: claim.evidence_refs.clone(),
            transitioned_at: crate::core::model::now(),
        };
        let event_kind = if matches!(disposition, VerificationDisposition::Verified) {
            EventKind::CompletionClaimVerified
        } else {
            EventKind::CompletionClaimRejected
        };
        let event = completion_event(
            actor_id,
            event_kind,
            EntityKind::CompletionClaim,
            claim.id.uuid(),
            &transition,
            Some(claim.task_id.to_string()),
            claim_event_id(&events, claim.id),
            Some(actor_id.to_string()),
        )?;
        let committed = self.commit(state.revision, event).await?;
        committed
            .completion_claims
            .get(&claim_id)
            .cloned()
            .ok_or(CompletionError::UnknownClaim(claim_id))
    }

    async fn commit(
        &self,
        expected_revision: u64,
        event: ExperienceEvent,
    ) -> Result<CurrentState, CompletionError> {
        match Projector::new(self.storage.clone())
            .record_batch(std::slice::from_ref(&event), Some(expected_revision), None)
            .await
        {
            Ok(state) => Ok(state),
            Err(ProjectionError::StaleContext { expected, actual })
            | Err(ProjectionError::Storage(StorageError::StaleContext { expected, actual })) => {
                Err(CompletionError::StaleRevision { expected, actual })
            }
            Err(error) => Err(CompletionError::Projection(error)),
        }
    }
}

fn evaluate_task_completion_in(
    state: &CurrentState,
    events: &[ExperienceEvent],
    task_id: TaskId,
) -> Result<CompletionGateResult, CompletionError> {
    ensure_task(state, task_id)?;
    let latest_claims = latest_claims(events)?;
    let mut missing = Vec::new();
    let mut needs_validation = Vec::new();
    let mut rejected = Vec::new();
    let mut blockers = Vec::new();
    let mut verified = Vec::new();

    for criterion in state
        .completion_criteria
        .values()
        .filter(|criterion| criterion.task_id == task_id && criterion.required)
    {
        let claim = latest_claims
            .get(&criterion.id)
            .and_then(|claim_id| state.completion_claims.get(claim_id));
        let Some(claim) = claim else {
            missing.push(criterion.id);
            blockers.push(format!("criterion {} has no active claim", criterion.id));
            continue;
        };
        match claim.disposition {
            VerificationDisposition::NeedsValidation => {
                needs_validation.push(claim.id);
                blockers.push(format!("claim {} needs validation", claim.id));
            }
            VerificationDisposition::Rejected => {
                rejected.push(claim.id);
                blockers.push(format!("claim {} was rejected", claim.id));
            }
            VerificationDisposition::Verified => {
                if let Some(blocker) = claim.blocker.as_deref().and_then(trimmed_non_empty) {
                    blockers.push(blocker);
                    continue;
                }
                if claim.evidence_refs.is_empty() {
                    blockers.push(format!("claim {} has no evidence", claim.id));
                    continue;
                }
                if claim.fingerprint
                    != completion_fingerprint(
                        claim.task_id,
                        claim.criterion_id,
                        &claim.evidence_refs,
                    )
                {
                    blockers.push(format!(
                        "claim {} fingerprint does not match its evidence",
                        claim.id
                    ));
                    continue;
                }
                if let Err(error) = validate_evidence_refs(
                    state,
                    events,
                    &claim.evidence_refs,
                    claim.as_of_sequence,
                ) {
                    blockers.push(error.to_string());
                    continue;
                }
                verified.push(claim.id);
            }
        }
    }

    let pending_approval_ids = state
        .approvals
        .values()
        .filter(|approval| matches!(approval.status, crate::core::ApprovalStatus::Pending))
        .map(|approval| approval.id)
        .collect::<Vec<_>>();
    let unknown_operation_ids = state
        .operations
        .values()
        .filter(|operation| {
            matches!(
                operation.status,
                crate::core::OperationStatus::Started | crate::core::OperationStatus::Unknown
            )
        })
        .map(|operation| operation.id)
        .collect::<Vec<_>>();
    for approval_id in &pending_approval_ids {
        blockers.push(format!("approval {approval_id} is pending"));
    }
    for operation_id in &unknown_operation_ids {
        blockers.push(format!("operation {operation_id} has unknown effect state"));
    }

    if missing.is_empty()
        && needs_validation.is_empty()
        && rejected.is_empty()
        && blockers.is_empty()
        && pending_approval_ids.is_empty()
        && unknown_operation_ids.is_empty()
    {
        Ok(CompletionGateResult::Ready {
            verified_claim_ids: verified,
        })
    } else {
        Ok(CompletionGateResult::Blocked {
            missing_criterion_ids: missing,
            needs_validation_claim_ids: needs_validation,
            rejected_claim_ids: rejected,
            blockers,
            pending_approval_ids,
            unknown_operation_ids,
        })
    }
}

fn completion_event<T: Serialize>(
    actor_id: crate::core::PrincipalId,
    event_kind: EventKind,
    entity_kind: EntityKind,
    entity_id: uuid::Uuid,
    payload: &T,
    correlation_id: Option<String>,
    causation_id: Option<EventId>,
    source_ref: Option<String>,
) -> Result<ExperienceEvent, CompletionError> {
    Ok(ExperienceEvent::new(
        actor_id,
        event_kind,
        Some(EntityRef::new(entity_kind, entity_id)),
        serde_json::to_value(payload)?,
        EventSource::new("completion", source_ref),
        causation_id,
        correlation_id,
        None,
    )?)
}

fn ensure_task(state: &CurrentState, task_id: TaskId) -> Result<(), CompletionError> {
    if state.tasks.contains_key(&task_id) {
        Ok(())
    } else {
        Err(CompletionError::UnknownTask(task_id))
    }
}

fn ensure_non_model_actor(
    state: &CurrentState,
    actor_id: crate::core::PrincipalId,
) -> Result<(), CompletionError> {
    if state
        .principals
        .get(&actor_id)
        .is_some_and(|principal| matches!(principal.kind, crate::core::PrincipalKind::Hekate))
    {
        Err(CompletionError::ModelCannotVerify)
    } else {
        Ok(())
    }
}

fn ensure_criterion(
    state: &CurrentState,
    task_id: TaskId,
    criterion_id: CompletionCriterionId,
) -> Result<(), CompletionError> {
    let Some(criterion) = state.completion_criteria.get(&criterion_id) else {
        return Err(CompletionError::UnknownCriterion(criterion_id));
    };
    if criterion.task_id != task_id {
        return Err(CompletionError::CriterionTaskMismatch {
            criterion: criterion_id,
            task: task_id,
        });
    }
    Ok(())
}

fn validate_supersedes(
    state: &CurrentState,
    task_id: TaskId,
    criterion_id: CompletionCriterionId,
    supersedes: Option<CompletionClaimId>,
) -> Result<(), CompletionError> {
    let Some(previous_id) = supersedes else {
        return Ok(());
    };
    let Some(previous) = state.completion_claims.get(&previous_id) else {
        return Err(CompletionError::UnknownSupersededClaim(previous_id));
    };
    if previous.task_id != task_id || previous.criterion_id != criterion_id {
        return Err(CompletionError::SupersededClaimMismatch);
    }
    Ok(())
}

fn validate_evidence_refs(
    state: &CurrentState,
    events: &[ExperienceEvent],
    evidence_refs: &[EvidenceRef],
    claim_as_of_sequence: u64,
) -> Result<(), CompletionError> {
    for evidence in evidence_refs {
        if !crate::core::valid_sha256_hex(&evidence.source_hash) {
            return Err(CompletionError::InvalidSourceHash(
                evidence.source_hash.clone(),
            ));
        }
        if evidence.as_of_sequence > claim_as_of_sequence {
            return Err(CompletionError::FutureEvidenceSequence {
                sequence: evidence.as_of_sequence,
                as_of: claim_as_of_sequence,
            });
        }
        let Some((event_sequence, event)) = events
            .iter()
            .enumerate()
            .find(|(_, event)| event.event_id == evidence.event_id)
        else {
            return Err(CompletionError::UnknownEventEvidence(evidence.event_id));
        };
        let event_sequence = event_sequence as u64 + 1;
        if event_sequence > evidence.as_of_sequence || event_sequence > claim_as_of_sequence {
            return Err(CompletionError::FutureEvidenceSequence {
                sequence: event_sequence,
                as_of: evidence.as_of_sequence.min(claim_as_of_sequence),
            });
        }
        if let Some(artifact_id) = evidence.artifact_id {
            let Some(artifact) = state.artifacts.get(&artifact_id) else {
                return Err(CompletionError::UnknownArtifactEvidence(artifact_id));
            };
            let Some((artifact_sequence, _)) = events.iter().enumerate().find(|(_, event)| {
                event.event_kind == EventKind::ArtifactCreated
                    && event.subject.as_ref().is_some_and(|subject| {
                        subject.kind == EntityKind::Artifact && subject.id == artifact_id.uuid()
                    })
            }) else {
                return Err(CompletionError::UnknownArtifactEvidence(artifact_id));
            };
            let artifact_sequence = artifact_sequence as u64 + 1;
            if artifact_sequence > evidence.as_of_sequence {
                return Err(CompletionError::FutureEvidenceSequence {
                    sequence: artifact_sequence,
                    as_of: evidence.as_of_sequence,
                });
            }
            if artifact.provenance_event_id != evidence.event_id {
                return Err(CompletionError::ArtifactProvenanceMismatch {
                    artifact: artifact_id,
                    event: evidence.event_id,
                });
            }
            if artifact.content_hash != evidence.source_hash {
                return Err(CompletionError::ArtifactHashMismatch {
                    artifact: artifact_id,
                });
            }
        } else {
            let expected_hash = if event.integrity_hash.is_empty() {
                event.canonical_hash()?
            } else {
                event.integrity_hash.clone()
            };
            if expected_hash != evidence.source_hash {
                return Err(CompletionError::EventHashMismatch {
                    event: evidence.event_id,
                });
            }
        }
    }
    Ok(())
}

fn state_as_of(
    events: &[ExperienceEvent],
    as_of_sequence: u64,
) -> Result<CurrentState, CompletionError> {
    if as_of_sequence > events.len() as u64 {
        return Err(CompletionError::FutureEvidenceSequence {
            sequence: as_of_sequence,
            as_of: events.len() as u64,
        });
    }
    Ok(Projector::replay(&events[..as_of_sequence as usize])?)
}

fn latest_claims(
    events: &[ExperienceEvent],
) -> Result<BTreeMap<CompletionCriterionId, CompletionClaimId>, CompletionError> {
    let mut latest = BTreeMap::new();
    for event in events {
        if event.event_kind != EventKind::CompletionClaimCreated {
            continue;
        }
        let claim: CompletionClaim = serde_json::from_value(event.payload.clone())?;
        latest.insert(claim.criterion_id, claim.id);
    }
    Ok(latest)
}

fn claim_event_id(events: &[ExperienceEvent], claim_id: CompletionClaimId) -> Option<EventId> {
    events.iter().rev().find_map(|event| {
        (event.event_kind == EventKind::CompletionClaimCreated
            && event.subject.as_ref().is_some_and(|subject| {
                subject.kind == EntityKind::CompletionClaim && subject.id == claim_id.uuid()
            }))
        .then_some(event.event_id)
    })
}

fn non_empty(value: String) -> Option<String> {
    let value = value.trim().to_owned();
    (!value.is_empty()).then_some(value)
}

fn trimmed_non_empty(value: &str) -> Option<String> {
    non_empty(value.to_owned())
}
