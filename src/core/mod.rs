pub mod embedding;
pub mod event;
pub mod evidence;
pub mod model;
pub mod recall;
pub mod transition;

pub use embedding::{
    EmbeddingDocument, EmbeddingEntityKind, EmbeddingIndexReport, EmbeddingMatch, EmbeddingRecord,
    EmbeddingSpace, EmbeddingStatus, EmbeddingValidationError, EmbeddingVector,
    DEFAULT_DOCUMENT_PREFIX, DEFAULT_QUERY_PREFIX,
};
pub use event::{EntityKind, EntityRef, EventKind, EventSource, ExperienceEvent};
pub use evidence::{
    completion_fingerprint, normalize_description, normalize_evidence_refs, sha256_hex,
    valid_sha256_hex, CompletionClaim, CompletionClaimTransition, CompletionCriterion, EvidenceRef,
    VerificationDisposition,
};
pub use model::{
    now, ActionIntent, ActionIntentId, ActionProposal, ActiveMemory, ActiveMemoryStatus, Approval,
    ApprovalId, ApprovalStatus, Artifact, ArtifactId, Attempt, AttemptId, AttemptStatus,
    CognitiveTrace, Commitment, CommitmentId, CommitmentStatus, CommittedJudgment,
    CompletionClaimId, CompletionCriterionId, Conflict, ConflictId, ConflictStatus,
    ContextSnapshot, CurrentState, Decision, DecisionId, DecisionKind, EventId, Focus, Goal,
    GoalId, GoalStatus, IdentityVersion, IdentityVersionId, InteractionResult, MemoryCandidate,
    MemoryCandidateId, MemoryCandidateStatus, MemoryId, MemoryKind, Observation, ObservationId,
    Operation, OperationId, OperationStatus, Position, PositionId, PositionStatus, Principal,
    PrincipalId, PrincipalKind, Receipt, ReceiptId, Relationship, RelationshipId, ResponseRecord,
    Run, RunId, RunStatus, SelfReview, Stance, Task, TaskId, TaskStatus, ThoughtContext,
    ThoughtCycle, ThoughtDraft, Verification, VerificationId, VerificationStatus, WorkingState,
    WorkingStateId,
};
pub use recall::{RecallBundle, RecallQuery, RecalledItem};
