use crate::core::model::{now, EventId, PrincipalId};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    PrincipalCreated,
    IdentityVersionCreated,
    RelationshipCreated,
    UserMessageReceived,
    ObservationRecorded,
    FocusResolved,
    GoalCreated,
    TaskCreated,
    TaskCompleted,
    RunStarted,
    RunSuspended,
    RunCompleted,
    WorkingStateUpdated,
    DecisionCreated,
    ResponseProduced,
    PositionEstablished,
    PositionMaintained,
    PositionRevised,
    PositionRetracted,
    PositionRecorded,
    ConflictOpened,
    ConflictUpdated,
    ConflictResolved,
    ConflictRecorded,
    CommitmentCreated,
    CommitmentFulfilled,
    ActionIntentCreated,
    OperationPlanned,
    OperationAuthorized,
    ApprovalRequested,
    ApprovalResolved,
    OperationStarted,
    OperationSucceeded,
    OperationFailed,
    OperationStateUnknown,
    ReceiptRecorded,
    VerificationRecorded,
    MemoryCandidateCreated,
    MemoryPromoted,
    MemoryRejected,
    MemorySuperseded,
    MemoryExpired,
    AttemptStarted,
    AttemptCompleted,
    ArtifactCreated,
    SleepRunStarted,
    IntegrationCandidateCreated,
    SleepRunCompleted,
    SleepRunInterrupted,
    SleepRunFailed,
    IntegrationCandidateVerified,
    IntegrationCandidateRejected,
    IntegrationCandidateMaterialized,
    MemoryRevisionMaterialized,
    CompletionCriterionDefined,
    CompletionClaimCreated,
    CompletionClaimVerified,
    CompletionClaimRejected,
    StateChanged,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityKind {
    Principal,
    IdentityVersion,
    Relationship,
    Observation,
    Goal,
    Task,
    Run,
    WorkingState,
    Decision,
    Position,
    Conflict,
    Commitment,
    ActionIntent,
    Operation,
    Approval,
    Receipt,
    Verification,
    MemoryCandidate,
    Memory,
    Attempt,
    Artifact,
    SleepRun,
    IntegrationCandidate,
    CompletionCriterion,
    CompletionClaim,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EntityRef {
    pub kind: EntityKind,
    pub id: Uuid,
}

impl EntityRef {
    pub fn new(kind: EntityKind, id: Uuid) -> Self {
        Self { kind, id }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EventSource {
    pub source_type: String,
    pub source_ref: Option<String>,
}

impl EventSource {
    pub fn new(source_type: impl Into<String>, source_ref: Option<String>) -> Self {
        Self {
            source_type: source_type.into(),
            source_ref,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ExperienceEvent {
    pub event_id: EventId,
    pub occurred_at: String,
    pub recorded_at: String,
    pub actor_id: PrincipalId,
    pub event_kind: EventKind,
    pub subject: Option<EntityRef>,
    pub payload: serde_json::Value,
    pub source: EventSource,
    pub causation_id: Option<EventId>,
    pub correlation_id: Option<String>,
    pub confidence: Option<f32>,
    pub integrity_hash: String,
}

#[derive(Debug, Error)]
pub enum EventError {
    #[error("could not serialize event for integrity hash: {0}")]
    Serialization(#[from] serde_json::Error),
}

impl ExperienceEvent {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        actor_id: PrincipalId,
        event_kind: EventKind,
        subject: Option<EntityRef>,
        payload: serde_json::Value,
        source: EventSource,
        causation_id: Option<EventId>,
        correlation_id: Option<String>,
        confidence: Option<f32>,
    ) -> Result<Self, EventError> {
        Self::new_with_id(
            EventId::new(),
            actor_id,
            event_kind,
            subject,
            payload,
            source,
            causation_id,
            correlation_id,
            confidence,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_id(
        event_id: EventId,
        actor_id: PrincipalId,
        event_kind: EventKind,
        subject: Option<EntityRef>,
        payload: serde_json::Value,
        source: EventSource,
        causation_id: Option<EventId>,
        correlation_id: Option<String>,
        confidence: Option<f32>,
    ) -> Result<Self, EventError> {
        let mut event = Self {
            event_id,
            occurred_at: now(),
            recorded_at: now(),
            actor_id,
            event_kind,
            subject,
            payload,
            source,
            causation_id,
            correlation_id,
            confidence,
            integrity_hash: String::new(),
        };
        event.integrity_hash = event.calculate_hash()?;
        Ok(event)
    }

    pub fn verify_integrity(&self) -> Result<bool, EventError> {
        Ok(self.integrity_hash == self.calculate_hash()?)
    }

    pub fn canonical_hash(&self) -> Result<String, EventError> {
        self.calculate_hash()
    }

    fn calculate_hash(&self) -> Result<String, EventError> {
        let mut unsigned = self.clone();
        unsigned.integrity_hash.clear();
        let bytes = serde_json::to_vec(&unsigned)?;
        Ok(super::evidence::sha256_hex(&bytes))
    }
}
