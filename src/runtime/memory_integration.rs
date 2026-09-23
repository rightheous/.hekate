use std::sync::Arc;

use serde::Serialize;
use thiserror::Error;

use crate::core::{
    integration_candidate_fingerprint, now, ActiveMemory, ActiveMemoryStatus, EntityKind,
    EntityRef, EventId, EventKind, EventSource, EvidenceRef, ExperienceEvent, IntegrationCandidate,
    IntegrationCandidateId, IntegrationCandidateKind, IntegrationMaterialization,
    IntegrationVerification, MemoryCandidate, MemoryCandidateId, MemoryCandidateStatus, MemoryId,
    MemoryKind, Position, PositionId, PositionIntegrationActionKind,
    PositionIntegrationEventPayload, PositionIntegrationMaterialization,
    PositionIntegrationOperation, PositionIntegrationProposal, PositionStatus, PrincipalId,
    PrincipalKind, VerificationDisposition,
};
use crate::ports::{Storage, StorageError};
use crate::runtime::deliberation::validate_position_revision;
use crate::runtime::projector::{ProjectionError, Projector};

#[derive(Debug, Error)]
pub enum IntegrationError {
    #[error("unknown integration candidate {0}")]
    UnknownCandidate(IntegrationCandidateId),
    #[error("integration candidate has an invalid disposition transition from {from:?} to {to:?}")]
    InvalidDispositionTransition {
        from: VerificationDisposition,
        to: VerificationDisposition,
    },
    #[error("unsupported integration candidate kind: {0:?}")]
    UnsupportedCandidateKind(IntegrationCandidateKind),
    #[error("integration verification requires evidence")]
    MissingEvidence,
    #[error("invalid evidence source hash: {0}")]
    InvalidEvidenceHash(String),
    #[error("unknown evidence event {0}")]
    UnknownEvidenceEvent(EventId),
    #[error("evidence event {event} is after candidate as-of revision {as_of}")]
    FutureEvidence { event: EventId, as_of: u64 },
    #[error("evidence sequence {sequence} is after candidate as-of revision {as_of}")]
    FutureEvidenceSequence { sequence: u64, as_of: u64 },
    #[error("evidence {0} is outside candidate provenance")]
    EvidenceOutsideCandidate(EventId),
    #[error("source event {0} failed integrity verification")]
    SourceEventIntegrity(EventId),
    #[error("event {event} source hash does not match evidence")]
    EventHashMismatch { event: EventId },
    #[error("unknown artifact evidence {0}")]
    UnknownArtifactEvidence(crate::core::ArtifactId),
    #[error("artifact {artifact} is not sourced by event {event}")]
    ArtifactProvenanceMismatch {
        artifact: crate::core::ArtifactId,
        event: EventId,
    },
    #[error("artifact {artifact} source hash does not match evidence")]
    ArtifactHashMismatch { artifact: crate::core::ArtifactId },
    #[error("candidate fingerprint does not match its projection")]
    CandidateFingerprintMismatch,
    #[error("verification reason cannot be empty")]
    InvalidReason,
    #[error("verification actor is not a known principal: {0}")]
    UnknownActor(PrincipalId),
    #[error("the model principal cannot verify integration candidates")]
    ModelCannotVerify,
    #[error("stale revision: expected {expected}, actual {actual}")]
    StaleRevision { expected: u64, actual: u64 },
    #[error("integration candidate {0} has no verification record")]
    MissingVerification(IntegrationCandidateId),
    #[error("integration candidate {0} is not verified")]
    CandidateNotVerified(IntegrationCandidateId),
    #[error("integration candidate {0} is already materialized")]
    AlreadyMaterialized(IntegrationCandidateId),
    #[error("duplicate active Memory fingerprint {0}")]
    DuplicateFingerprint(String),
    #[error("Memory materialization failed: {0}")]
    MemoryMaterializationFailure(String),
    #[error("invalid typed Position proposal: {0}")]
    InvalidPositionProposal(String),
    #[error("unknown Position {0}")]
    UnknownPosition(PositionId),
    #[error("Position {0} belongs to a different principal")]
    WrongPositionPrincipal(PositionId),
    #[error("Position {position_id} version mismatch: expected {expected}, actual {actual}")]
    PositionVersionMismatch {
        position_id: PositionId,
        expected: u32,
        actual: u32,
    },
    #[error("Position {0} changed after the candidate as-of revision")]
    PositionChanged(PositionId),
    #[error("an active Position or unresolved conflict blocks subject: {0}")]
    PositionConflict(String),
    #[error("candidate as-of revision {0} is not present in the event ledger")]
    InvalidPositionAsOf(u64),
    #[error("Position candidate {0} is already materialized")]
    PositionAlreadyMaterialized(IntegrationCandidateId),
    #[error("Position transition is not allowed by the existing lifecycle")]
    InvalidPositionTransition,
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
pub struct IntegrationCandidateInspection {
    pub candidate: IntegrationCandidate,
    pub verification: Option<IntegrationVerification>,
    pub materialization: Option<IntegrationMaterialization>,
    pub position_materialization: Option<PositionIntegrationMaterialization>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct MemoryIntegrationResult {
    pub memory_candidate: MemoryCandidate,
    pub memory: ActiveMemory,
    pub materialization: IntegrationMaterialization,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PositionIntegrationResult {
    pub position: Position,
    pub materialization: PositionIntegrationMaterialization,
}

pub(crate) struct MaterializationCommit {
    pub result: MemoryIntegrationResult,
    pub state: crate::core::CurrentState,
    pub events: Vec<ExperienceEvent>,
}

pub(crate) struct PositionMaterializationCommit {
    pub result: PositionIntegrationResult,
    pub state: crate::core::CurrentState,
    pub events: Vec<ExperienceEvent>,
}

#[derive(Clone)]
pub struct MemoryIntegration {
    storage: Arc<dyn Storage>,
    projector: Arc<Projector>,
}

impl MemoryIntegration {
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self {
            projector: Arc::new(Projector::new(storage.clone())),
            storage,
        }
    }

    pub async fn list(&self) -> Result<Vec<IntegrationCandidateInspection>, IntegrationError> {
        let state = self.storage.load_state().await?;
        Ok(state
            .integration_candidates
            .values()
            .map(|candidate| IntegrationCandidateInspection {
                candidate: candidate.clone(),
                verification: state.integration_verifications.get(&candidate.id).cloned(),
                materialization: state
                    .integration_materializations
                    .get(&candidate.id)
                    .cloned(),
                position_materialization: state
                    .position_integration_materializations
                    .get(&candidate.id)
                    .cloned(),
            })
            .collect())
    }

    pub async fn inspect(
        &self,
        candidate_id: IntegrationCandidateId,
    ) -> Result<IntegrationCandidateInspection, IntegrationError> {
        let state = self.storage.load_state().await?;
        let candidate = state
            .integration_candidates
            .get(&candidate_id)
            .cloned()
            .ok_or(IntegrationError::UnknownCandidate(candidate_id))?;
        Ok(IntegrationCandidateInspection {
            candidate,
            verification: state.integration_verifications.get(&candidate_id).cloned(),
            materialization: state
                .integration_materializations
                .get(&candidate_id)
                .cloned(),
            position_materialization: state
                .position_integration_materializations
                .get(&candidate_id)
                .cloned(),
        })
    }

    pub async fn verify_candidate(
        &self,
        candidate_id: IntegrationCandidateId,
        actor_id: PrincipalId,
        reason: impl Into<String>,
    ) -> Result<IntegrationCandidate, IntegrationError> {
        self.transition(
            candidate_id,
            actor_id,
            VerificationDisposition::Verified,
            reason.into(),
            None,
        )
        .await
    }

    pub async fn verify_candidate_with_evidence(
        &self,
        candidate_id: IntegrationCandidateId,
        actor_id: PrincipalId,
        reason: impl Into<String>,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<IntegrationCandidate, IntegrationError> {
        self.transition(
            candidate_id,
            actor_id,
            VerificationDisposition::Verified,
            reason.into(),
            Some(evidence_refs),
        )
        .await
    }

    pub async fn reject_candidate(
        &self,
        candidate_id: IntegrationCandidateId,
        actor_id: PrincipalId,
        reason: impl Into<String>,
    ) -> Result<IntegrationCandidate, IntegrationError> {
        self.transition(
            candidate_id,
            actor_id,
            VerificationDisposition::Rejected,
            reason.into(),
            None,
        )
        .await
    }

    pub async fn reject_candidate_with_evidence(
        &self,
        candidate_id: IntegrationCandidateId,
        actor_id: PrincipalId,
        reason: impl Into<String>,
        evidence_refs: Vec<EvidenceRef>,
    ) -> Result<IntegrationCandidate, IntegrationError> {
        self.transition(
            candidate_id,
            actor_id,
            VerificationDisposition::Rejected,
            reason.into(),
            Some(evidence_refs),
        )
        .await
    }

    pub(crate) async fn materialize_memory(
        &self,
        candidate_id: IntegrationCandidateId,
        actor_id: PrincipalId,
    ) -> Result<MaterializationCommit, IntegrationError> {
        let state = self.storage.load_state().await?;
        ensure_actor(&state, actor_id, false)?;
        let candidate = state
            .integration_candidates
            .get(&candidate_id)
            .cloned()
            .ok_or(IntegrationError::UnknownCandidate(candidate_id))?;
        if candidate.disposition != VerificationDisposition::Verified {
            return Err(IntegrationError::CandidateNotVerified(candidate_id));
        }
        if !matches!(&candidate.kind, IntegrationCandidateKind::Memory) {
            return Err(IntegrationError::UnsupportedCandidateKind(candidate.kind));
        }
        if state
            .integration_materializations
            .contains_key(&candidate_id)
        {
            return Err(IntegrationError::AlreadyMaterialized(candidate_id));
        }
        let verification = state
            .integration_verifications
            .get(&candidate_id)
            .cloned()
            .ok_or(IntegrationError::MissingVerification(candidate_id))?;
        if verification.new_disposition != VerificationDisposition::Verified {
            return Err(IntegrationError::CandidateNotVerified(candidate_id));
        }
        let events = self.storage.load_events().await?;
        validate_candidate(&state, &events, &candidate, &verification.evidence_refs)?;
        if state.integration_materializations.values().any(|item| {
            item.fingerprint == candidate.fingerprint
                && state
                    .active_memories
                    .get(&item.memory_id)
                    .is_some_and(|memory| memory.status == ActiveMemoryStatus::Active)
        }) {
            return Err(IntegrationError::DuplicateFingerprint(
                candidate.fingerprint.clone(),
            ));
        }
        if state.active_memories.values().any(|memory| {
            memory.status == ActiveMemoryStatus::Active
                && memory.kind == MemoryKind::Lesson
                && memory.content == candidate.content
                && memory.source_event_ids == candidate.source_event_ids
        }) {
            return Err(IntegrationError::DuplicateFingerprint(
                candidate.fingerprint.clone(),
            ));
        }

        let memory_candidate = MemoryCandidate {
            id: MemoryCandidateId::new(),
            kind: MemoryKind::Lesson,
            content: candidate.content.clone(),
            subject_principal_id: None,
            status: MemoryCandidateStatus::Candidate,
            confidence: candidate.confidence,
            source_event_ids: candidate.source_event_ids.clone(),
            valid_from: Some(now()),
            valid_until: None,
            supersedes: None,
            created_at: now(),
        };
        let memory = ActiveMemory {
            id: MemoryId::new(),
            candidate_id: memory_candidate.id,
            kind: memory_candidate.kind.clone(),
            content: memory_candidate.content.clone(),
            subject_principal_id: memory_candidate.subject_principal_id,
            status: ActiveMemoryStatus::Active,
            confidence: memory_candidate.confidence,
            source_event_ids: memory_candidate.source_event_ids.clone(),
            valid_from: memory_candidate.valid_from.clone(),
            valid_until: memory_candidate.valid_until.clone(),
            supersedes: None,
            last_verified_at: Some(now()),
            created_at: now(),
        };
        let materialization = IntegrationMaterialization {
            candidate_id,
            memory_candidate_id: memory_candidate.id,
            memory_id: memory.id,
            source_event_ids: candidate.source_event_ids.clone(),
            counterevidence_event_ids: candidate.counterevidence_event_ids.clone(),
            evidence_refs: verification.evidence_refs.clone(),
            fingerprint: candidate.fingerprint.clone(),
            as_of_revision: candidate.as_of_revision,
            created_at: now(),
        };
        let candidate_event = integration_event(
            actor_id,
            EventKind::MemoryCandidateCreated,
            EntityKind::MemoryCandidate,
            memory_candidate.id.uuid(),
            &memory_candidate,
            candidate_id,
            None,
        )?;
        let memory_event = integration_event(
            actor_id,
            EventKind::MemoryPromoted,
            EntityKind::Memory,
            memory.id.uuid(),
            &memory,
            candidate_id,
            Some(candidate_event.event_id),
        )?;
        let materialized_event = integration_event(
            actor_id,
            EventKind::IntegrationCandidateMaterialized,
            EntityKind::IntegrationCandidate,
            candidate_id.uuid(),
            &materialization,
            candidate_id,
            Some(memory_event.event_id),
        )?;
        let committed = self
            .commit(
                state.revision,
                &[
                    candidate_event.clone(),
                    memory_event.clone(),
                    materialized_event.clone(),
                ],
            )
            .await?;
        let committed_memory_candidate = committed
            .memory_candidates
            .get(&memory_candidate.id)
            .cloned()
            .ok_or_else(|| {
                IntegrationError::MemoryMaterializationFailure(
                    "committed Memory candidate is missing".to_owned(),
                )
            })?;
        let committed_memory = committed
            .active_memories
            .get(&memory.id)
            .cloned()
            .ok_or_else(|| {
                IntegrationError::MemoryMaterializationFailure(
                    "committed Memory is missing".to_owned(),
                )
            })?;
        let committed_materialization = committed
            .integration_materializations
            .get(&candidate_id)
            .cloned()
            .ok_or_else(|| {
                IntegrationError::MemoryMaterializationFailure(
                    "committed materialization link is missing".to_owned(),
                )
            })?;
        Ok(MaterializationCommit {
            result: MemoryIntegrationResult {
                memory_candidate: committed_memory_candidate,
                memory: committed_memory,
                materialization: committed_materialization,
            },
            state: committed,
            events: vec![candidate_event, memory_event, materialized_event],
        })
    }

    pub(crate) async fn materialize_position(
        &self,
        candidate_id: IntegrationCandidateId,
        hekate_id: PrincipalId,
        actor_id: PrincipalId,
    ) -> Result<PositionMaterializationCommit, IntegrationError> {
        let state = self.storage.load_state().await?;
        ensure_actor(&state, actor_id, true)?;
        let candidate = state
            .integration_candidates
            .get(&candidate_id)
            .cloned()
            .ok_or(IntegrationError::UnknownCandidate(candidate_id))?;
        if candidate.disposition != VerificationDisposition::Verified {
            return Err(IntegrationError::CandidateNotVerified(candidate_id));
        }
        if !matches!(&candidate.kind, IntegrationCandidateKind::Position) {
            return Err(IntegrationError::UnsupportedCandidateKind(candidate.kind));
        }
        if state
            .position_integration_materializations
            .contains_key(&candidate_id)
        {
            return Err(IntegrationError::PositionAlreadyMaterialized(candidate_id));
        }
        let verification = state
            .integration_verifications
            .get(&candidate_id)
            .cloned()
            .ok_or(IntegrationError::MissingVerification(candidate_id))?;
        if verification.new_disposition != VerificationDisposition::Verified {
            return Err(IntegrationError::CandidateNotVerified(candidate_id));
        }
        let events = self.storage.load_events().await?;
        validate_candidate(&state, &events, &candidate, &verification.evidence_refs)?;
        let proposal = parse_position_proposal(&candidate)?;
        let (position, action, prior_position) =
            position_plan(&state, &events, &candidate, hekate_id, &proposal)?;
        let verification_event_id = integration_transition_event_id(
            &events,
            candidate_id,
            EventKind::IntegrationCandidateVerified,
        )
        .ok_or(IntegrationError::MissingVerification(candidate_id))?;
        let position_event_id = EventId::new();
        let materialization = PositionIntegrationMaterialization {
            candidate_id,
            position_id: position.id,
            position_event_id,
            verification_event_id,
            action,
            prior_position,
            source_event_ids: candidate.source_event_ids.clone(),
            counterevidence_event_ids: candidate.counterevidence_event_ids.clone(),
            evidence_refs: verification.evidence_refs.clone(),
            fingerprint: candidate.fingerprint.clone(),
            as_of_revision: candidate.as_of_revision,
            created_at: now(),
        };
        let payload = PositionIntegrationEventPayload {
            position: position.clone(),
            position_integration: materialization.clone(),
        };
        let event_kind = match action {
            PositionIntegrationActionKind::Establish => EventKind::PositionEstablished,
            PositionIntegrationActionKind::Revise => EventKind::PositionRevised,
            PositionIntegrationActionKind::Withdraw => EventKind::PositionRetracted,
        };
        let event = ExperienceEvent::new_with_id(
            position_event_id,
            actor_id,
            event_kind,
            Some(EntityRef::new(EntityKind::Position, position.id.uuid())),
            serde_json::to_value(payload)?,
            EventSource::new("position_integration", Some(candidate_id.to_string())),
            Some(verification_event_id),
            Some(candidate_id.to_string()),
            None,
        )?;
        let committed = self
            .commit(state.revision, std::slice::from_ref(&event))
            .await?;
        let committed_position =
            committed
                .positions
                .get(&position.id)
                .cloned()
                .ok_or_else(|| {
                    IntegrationError::MemoryMaterializationFailure(
                        "committed Position is missing".to_owned(),
                    )
                })?;
        let committed_materialization = committed
            .position_integration_materializations
            .get(&candidate_id)
            .cloned()
            .ok_or_else(|| {
                IntegrationError::MemoryMaterializationFailure(
                    "committed Position materialization link is missing".to_owned(),
                )
            })?;
        Ok(PositionMaterializationCommit {
            result: PositionIntegrationResult {
                position: committed_position,
                materialization: committed_materialization,
            },
            state: committed,
            events: vec![event],
        })
    }

    async fn transition(
        &self,
        candidate_id: IntegrationCandidateId,
        actor_id: PrincipalId,
        disposition: VerificationDisposition,
        reason: String,
        evidence_refs: Option<Vec<EvidenceRef>>,
    ) -> Result<IntegrationCandidate, IntegrationError> {
        let state = self.storage.load_state().await?;
        let candidate = state
            .integration_candidates
            .get(&candidate_id)
            .cloned()
            .ok_or(IntegrationError::UnknownCandidate(candidate_id))?;
        ensure_actor(&state, actor_id, true)?;
        if candidate.disposition != VerificationDisposition::NeedsValidation {
            return Err(IntegrationError::InvalidDispositionTransition {
                from: candidate.disposition,
                to: disposition,
            });
        }
        if !matches!(
            disposition,
            VerificationDisposition::Verified | VerificationDisposition::Rejected
        ) {
            return Err(IntegrationError::InvalidDispositionTransition {
                from: candidate.disposition,
                to: disposition,
            });
        }
        let reason = non_empty(reason).ok_or(IntegrationError::InvalidReason)?;
        let events = self.storage.load_events().await?;
        let evidence_refs = match evidence_refs {
            Some(evidence_refs) => crate::core::normalize_evidence_refs(evidence_refs),
            None => default_evidence(&candidate, &events)?,
        };
        validate_candidate(&state, &events, &candidate, &evidence_refs)?;
        if disposition == VerificationDisposition::Verified
            && matches!(&candidate.kind, IntegrationCandidateKind::Position)
        {
            let hekate_id = unique_hekate_principal(&state)?;
            let proposal = parse_position_proposal(&candidate)?;
            position_plan(&state, &events, &candidate, hekate_id, &proposal)?;
        }
        let verification = IntegrationVerification {
            candidate_id,
            previous_disposition: candidate.disposition,
            new_disposition: disposition,
            actor_id,
            reason,
            evidence_refs,
            as_of_revision: candidate.as_of_revision,
            created_at: now(),
        };
        let event_kind = if disposition == VerificationDisposition::Verified {
            EventKind::IntegrationCandidateVerified
        } else {
            EventKind::IntegrationCandidateRejected
        };
        let event = integration_event(
            actor_id,
            event_kind,
            EntityKind::IntegrationCandidate,
            candidate_id.uuid(),
            &verification,
            candidate_id,
            candidate_event_id(&events, candidate_id),
        )?;
        let committed = self
            .commit(state.revision, std::slice::from_ref(&event))
            .await?;
        committed
            .integration_candidates
            .get(&candidate_id)
            .cloned()
            .ok_or(IntegrationError::UnknownCandidate(candidate_id))
    }

    async fn commit(
        &self,
        expected_revision: u64,
        events: &[ExperienceEvent],
    ) -> Result<crate::core::CurrentState, IntegrationError> {
        match self
            .projector
            .record_batch(events, Some(expected_revision), None)
            .await
        {
            Ok(state) => Ok(state),
            Err(ProjectionError::StaleContext { expected, actual })
            | Err(ProjectionError::Storage(StorageError::StaleContext { expected, actual })) => {
                Err(IntegrationError::StaleRevision { expected, actual })
            }
            Err(error) => Err(IntegrationError::Projection(error)),
        }
    }
}

fn ensure_actor(
    state: &crate::core::CurrentState,
    actor_id: PrincipalId,
    cannot_be_model: bool,
) -> Result<(), IntegrationError> {
    let Some(principal) = state.principals.get(&actor_id) else {
        return Err(IntegrationError::UnknownActor(actor_id));
    };
    if cannot_be_model && matches!(principal.kind, PrincipalKind::Hekate) {
        return Err(IntegrationError::ModelCannotVerify);
    }
    Ok(())
}

fn validate_candidate(
    state: &crate::core::CurrentState,
    events: &[ExperienceEvent],
    candidate: &IntegrationCandidate,
    evidence_refs: &[EvidenceRef],
) -> Result<(), IntegrationError> {
    if !supported_kind(&candidate.kind) {
        return Err(IntegrationError::UnsupportedCandidateKind(
            candidate.kind.clone(),
        ));
    }
    if candidate.content.trim().is_empty() || candidate.rationale.trim().is_empty() {
        return Err(IntegrationError::MemoryMaterializationFailure(
            "candidate content and rationale cannot be empty".to_owned(),
        ));
    }
    if candidate.source_event_ids.is_empty() {
        return Err(IntegrationError::MissingEvidence);
    }
    if candidate.as_of_revision > state.revision {
        return Err(IntegrationError::FutureEvidenceSequence {
            sequence: candidate.as_of_revision,
            as_of: state.revision,
        });
    }
    if integration_candidate_fingerprint(
        &candidate.kind,
        &candidate.content,
        &candidate.source_event_ids,
        &candidate.counterevidence_event_ids,
    ) != candidate.fingerprint
    {
        return Err(IntegrationError::CandidateFingerprintMismatch);
    }
    for event_id in candidate
        .source_event_ids
        .iter()
        .chain(candidate.counterevidence_event_ids.iter())
    {
        let Some((sequence, event)) = events
            .iter()
            .enumerate()
            .find(|(_, event)| event.event_id == *event_id)
        else {
            return Err(IntegrationError::UnknownEvidenceEvent(*event_id));
        };
        let sequence = sequence as u64 + 1;
        if sequence > candidate.as_of_revision {
            return Err(IntegrationError::FutureEvidence {
                event: *event_id,
                as_of: candidate.as_of_revision,
            });
        }
        let valid = event
            .verify_integrity()
            .map_err(|_| IntegrationError::SourceEventIntegrity(*event_id))?;
        if !valid {
            return Err(IntegrationError::SourceEventIntegrity(*event_id));
        }
    }
    if evidence_refs.is_empty() {
        return Err(IntegrationError::MissingEvidence);
    }
    for evidence in evidence_refs {
        if !crate::core::valid_sha256_hex(&evidence.source_hash) {
            return Err(IntegrationError::InvalidEvidenceHash(
                evidence.source_hash.clone(),
            ));
        }
        if evidence.as_of_sequence > candidate.as_of_revision {
            return Err(IntegrationError::FutureEvidenceSequence {
                sequence: evidence.as_of_sequence,
                as_of: candidate.as_of_revision,
            });
        }
        if !candidate
            .source_event_ids
            .iter()
            .chain(candidate.counterevidence_event_ids.iter())
            .any(|event_id| event_id == &evidence.event_id)
        {
            return Err(IntegrationError::EvidenceOutsideCandidate(
                evidence.event_id,
            ));
        }
        let Some((sequence, event)) = events
            .iter()
            .enumerate()
            .find(|(_, event)| event.event_id == evidence.event_id)
        else {
            return Err(IntegrationError::UnknownEvidenceEvent(evidence.event_id));
        };
        let sequence = sequence as u64 + 1;
        if sequence > evidence.as_of_sequence || sequence > candidate.as_of_revision {
            return Err(IntegrationError::FutureEvidenceSequence {
                sequence,
                as_of: evidence.as_of_sequence.min(candidate.as_of_revision),
            });
        }
        let valid = event
            .verify_integrity()
            .map_err(|_| IntegrationError::SourceEventIntegrity(evidence.event_id))?;
        if !valid {
            return Err(IntegrationError::SourceEventIntegrity(evidence.event_id));
        }
        if let Some(artifact_id) = evidence.artifact_id {
            let Some(artifact) = state.artifacts.get(&artifact_id) else {
                return Err(IntegrationError::UnknownArtifactEvidence(artifact_id));
            };
            let artifact_sequence = events
                .iter()
                .enumerate()
                .find(|(_, item)| {
                    item.event_kind == EventKind::ArtifactCreated
                        && item.subject.as_ref().is_some_and(|subject| {
                            subject.kind == EntityKind::Artifact && subject.id == artifact_id.uuid()
                        })
                })
                .map(|(index, _)| index as u64 + 1)
                .ok_or(IntegrationError::UnknownArtifactEvidence(artifact_id))?;
            if artifact_sequence > evidence.as_of_sequence {
                return Err(IntegrationError::FutureEvidenceSequence {
                    sequence: artifact_sequence,
                    as_of: evidence.as_of_sequence,
                });
            }
            if artifact.provenance_event_id != evidence.event_id {
                return Err(IntegrationError::ArtifactProvenanceMismatch {
                    artifact: artifact_id,
                    event: evidence.event_id,
                });
            }
            if artifact.content_hash != evidence.source_hash {
                return Err(IntegrationError::ArtifactHashMismatch {
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
                return Err(IntegrationError::EventHashMismatch {
                    event: evidence.event_id,
                });
            }
        }
    }
    Ok(())
}

fn default_evidence(
    candidate: &IntegrationCandidate,
    events: &[ExperienceEvent],
) -> Result<Vec<EvidenceRef>, IntegrationError> {
    candidate
        .source_event_ids
        .iter()
        .chain(candidate.counterevidence_event_ids.iter())
        .map(|event_id| {
            let event = events
                .iter()
                .find(|event| event.event_id == *event_id)
                .ok_or(IntegrationError::UnknownEvidenceEvent(*event_id))?;
            let valid = event
                .verify_integrity()
                .map_err(|_| IntegrationError::SourceEventIntegrity(*event_id))?;
            if !valid {
                return Err(IntegrationError::SourceEventIntegrity(*event_id));
            }
            let source_hash = if event.integrity_hash.is_empty() {
                event.canonical_hash()?
            } else {
                event.integrity_hash.clone()
            };
            Ok(EvidenceRef {
                event_id: *event_id,
                artifact_id: None,
                source_hash,
                as_of_sequence: candidate.as_of_revision,
            })
        })
        .collect()
}

fn supported_kind(kind: &IntegrationCandidateKind) -> bool {
    matches!(
        kind,
        IntegrationCandidateKind::Memory
            | IntegrationCandidateKind::Association
            | IntegrationCandidateKind::Position
            | IntegrationCandidateKind::Conflict
            | IntegrationCandidateKind::Relationship
            | IntegrationCandidateKind::Identity
            | IntegrationCandidateKind::Goal
    )
}

fn parse_position_proposal(
    candidate: &IntegrationCandidate,
) -> Result<PositionIntegrationProposal, IntegrationError> {
    if !matches!(&candidate.kind, IntegrationCandidateKind::Position) {
        return Err(IntegrationError::UnsupportedCandidateKind(
            candidate.kind.clone(),
        ));
    }
    PositionIntegrationProposal::parse(&candidate.content)
        .map_err(IntegrationError::InvalidPositionProposal)
}

fn unique_hekate_principal(
    state: &crate::core::CurrentState,
) -> Result<PrincipalId, IntegrationError> {
    let mut principals = state
        .principals
        .values()
        .filter(|principal| matches!(principal.kind, PrincipalKind::Hekate))
        .map(|principal| principal.id);
    let Some(id) = principals.next() else {
        return Err(IntegrationError::InvalidPositionProposal(
            "HEKATE principal is missing".to_owned(),
        ));
    };
    if principals.next().is_some() {
        return Err(IntegrationError::InvalidPositionProposal(
            "HEKATE principal is ambiguous".to_owned(),
        ));
    }
    Ok(id)
}

fn position_plan(
    state: &crate::core::CurrentState,
    events: &[ExperienceEvent],
    candidate: &IntegrationCandidate,
    hekate_id: PrincipalId,
    proposal: &PositionIntegrationProposal,
) -> Result<(Position, PositionIntegrationActionKind, Option<Position>), IntegrationError> {
    let ledger_revision = u64::try_from(events.len())
        .map_err(|_| IntegrationError::InvalidPositionAsOf(candidate.as_of_revision))?;
    let as_of_len = usize::try_from(candidate.as_of_revision)
        .map_err(|_| IntegrationError::InvalidPositionAsOf(candidate.as_of_revision))?;
    if state.revision != ledger_revision
        || as_of_len > events.len()
        || state
            .sleep_runs
            .get(&candidate.sleep_run_id)
            .map_or(true, |run| {
                run.high_water_revision != candidate.as_of_revision
            })
    {
        return Err(IntegrationError::InvalidPositionAsOf(
            candidate.as_of_revision,
        ));
    }
    let as_of_state = Projector::replay(&events[..as_of_len])?;
    if as_of_state.revision != candidate.as_of_revision
        || !as_of_state
            .principals
            .get(&hekate_id)
            .is_some_and(|principal| matches!(principal.kind, PrincipalKind::Hekate))
    {
        return Err(IntegrationError::InvalidPositionAsOf(
            candidate.as_of_revision,
        ));
    }

    match &proposal.operation {
        PositionIntegrationOperation::Establish {
            subject,
            stance,
            reasons,
            reconsideration_conditions,
        } => {
            let subject = subject.trim().to_owned();
            if crate::core::position_integration_blocked(&as_of_state, hekate_id, &subject, None)
                || crate::core::position_integration_blocked(state, hekate_id, &subject, None)
            {
                return Err(IntegrationError::PositionConflict(subject));
            }
            let position = Position {
                id: PositionId::new(),
                principal_id: hekate_id,
                subject,
                stance: stance.clone(),
                version: 1,
                status: PositionStatus::Active,
                confidence: candidate.confidence,
                supersedes: None,
                reasons: normalized_position_texts(reasons),
                evidence_refs: candidate.source_event_ids.clone(),
                reconsideration_conditions: normalized_position_texts(reconsideration_conditions),
                created_at: now(),
            };
            validate_position_revision(state, &position)
                .map_err(|_| IntegrationError::InvalidPositionTransition)?;
            Ok((position, PositionIntegrationActionKind::Establish, None))
        }
        PositionIntegrationOperation::Revise {
            position_id,
            expected_version,
            stance,
            reasons,
            reconsideration_conditions,
        } => {
            let prior = prior_position(state, &as_of_state, *position_id)?;
            validate_position_base(&prior, hekate_id, *expected_version)?;
            if prior.status != PositionStatus::Active {
                return Err(IntegrationError::InvalidPositionTransition);
            }
            if crate::core::position_integration_blocked(
                &as_of_state,
                hekate_id,
                &prior.subject,
                Some(prior.id),
            ) || crate::core::position_integration_blocked(
                state,
                hekate_id,
                &prior.subject,
                Some(prior.id),
            ) {
                return Err(IntegrationError::PositionConflict(prior.subject));
            }
            let version = prior
                .version
                .checked_add(1)
                .ok_or(IntegrationError::InvalidPositionTransition)?;
            let position = Position {
                id: PositionId::new(),
                principal_id: hekate_id,
                subject: prior.subject.clone(),
                stance: stance.clone(),
                version,
                status: PositionStatus::Active,
                confidence: candidate.confidence,
                supersedes: Some(prior.id),
                reasons: normalized_position_texts(reasons),
                evidence_refs: candidate.source_event_ids.clone(),
                reconsideration_conditions: normalized_position_texts(reconsideration_conditions),
                created_at: now(),
            };
            validate_position_revision(state, &position)
                .map_err(|_| IntegrationError::InvalidPositionTransition)?;
            Ok((position, PositionIntegrationActionKind::Revise, Some(prior)))
        }
        PositionIntegrationOperation::Withdraw {
            position_id,
            expected_version,
            reason,
        } => {
            let mut prior = prior_position(state, &as_of_state, *position_id)?;
            validate_position_base(&prior, hekate_id, *expected_version)?;
            if prior.status != PositionStatus::Active {
                return Err(IntegrationError::InvalidPositionTransition);
            }
            if crate::core::position_integration_blocked(
                &as_of_state,
                hekate_id,
                &prior.subject,
                Some(prior.id),
            ) || crate::core::position_integration_blocked(
                state,
                hekate_id,
                &prior.subject,
                Some(prior.id),
            ) {
                return Err(IntegrationError::PositionConflict(prior.subject));
            }
            let previous = prior.clone();
            prior.status = PositionStatus::Retracted;
            prior.reasons.push(reason.trim().to_owned());
            prior
                .evidence_refs
                .extend(candidate.source_event_ids.iter().copied());
            prior.evidence_refs.sort();
            prior.evidence_refs.dedup();
            validate_position_revision(state, &prior)
                .map_err(|_| IntegrationError::InvalidPositionTransition)?;
            Ok((
                prior.clone(),
                PositionIntegrationActionKind::Withdraw,
                Some(previous),
            ))
        }
    }
}

fn prior_position(
    current: &crate::core::CurrentState,
    as_of: &crate::core::CurrentState,
    position_id: PositionId,
) -> Result<Position, IntegrationError> {
    let Some(prior) = as_of.positions.get(&position_id).cloned() else {
        return if current.positions.contains_key(&position_id) {
            Err(IntegrationError::PositionChanged(position_id))
        } else {
            Err(IntegrationError::UnknownPosition(position_id))
        };
    };
    if current.positions.get(&position_id) != Some(&prior) {
        return Err(IntegrationError::PositionChanged(position_id));
    }
    Ok(prior)
}

fn validate_position_base(
    prior: &Position,
    hekate_id: PrincipalId,
    expected_version: u32,
) -> Result<(), IntegrationError> {
    if prior.principal_id != hekate_id {
        return Err(IntegrationError::WrongPositionPrincipal(prior.id));
    }
    if prior.version != expected_version {
        return Err(IntegrationError::PositionVersionMismatch {
            position_id: prior.id,
            expected: expected_version,
            actual: prior.version,
        });
    }
    Ok(())
}

fn normalized_position_texts(values: &[String]) -> Vec<String> {
    values.iter().map(|value| value.trim().to_owned()).collect()
}

fn integration_event<T: Serialize>(
    actor_id: PrincipalId,
    event_kind: EventKind,
    entity_kind: EntityKind,
    entity_id: uuid::Uuid,
    payload: &T,
    candidate_id: IntegrationCandidateId,
    causation_id: Option<EventId>,
) -> Result<ExperienceEvent, IntegrationError> {
    Ok(ExperienceEvent::new(
        actor_id,
        event_kind,
        Some(EntityRef::new(entity_kind, entity_id)),
        serde_json::to_value(payload)?,
        EventSource::new("memory_integration", Some(candidate_id.to_string())),
        causation_id,
        Some(candidate_id.to_string()),
        None,
    )?)
}

fn candidate_event_id(
    events: &[ExperienceEvent],
    candidate_id: IntegrationCandidateId,
) -> Option<EventId> {
    integration_transition_event_id(events, candidate_id, EventKind::IntegrationCandidateCreated)
}

fn integration_transition_event_id(
    events: &[ExperienceEvent],
    candidate_id: IntegrationCandidateId,
    event_kind: EventKind,
) -> Option<EventId> {
    events.iter().rev().find_map(|event| {
        (event.event_kind == event_kind
            && event.subject.as_ref().is_some_and(|subject| {
                subject.kind == EntityKind::IntegrationCandidate
                    && subject.id == candidate_id.uuid()
            }))
        .then_some(event.event_id)
    })
}

fn non_empty(value: String) -> Option<String> {
    let value = value.trim().to_owned();
    (!value.is_empty()).then_some(value)
}
