pub mod context_snapshot;
pub mod embedding;
pub mod event;
pub mod evidence;
pub mod model;
pub mod recall;
pub mod response_profile;
pub mod sleep;
pub mod transition;

pub use context_snapshot::ContextBudgetReport as ContextSnapshotBudgetReport;
pub use context_snapshot::{
    ContextBudget, ContextBuildRequest, ContextItem, ContextItemKind, ContextSnapshot,
    ContextSourceRef, DEFAULT_CONTEXT_ACTIVE_BUDGET_BYTES, DEFAULT_CONTEXT_ANCHOR_BUDGET_BYTES,
    DEFAULT_CONTEXT_HARD_LIMIT_BYTES, DEFAULT_CONTEXT_MIDDLE_BUDGET_BYTES,
};
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
    CompletionClaimId, CompletionCriterionId, Conflict, ConflictId, ConflictStatus, CurrentState,
    Decision, DecisionId, DecisionKind, EventId, Focus, Goal, GoalId, GoalStatus, IdentityVersion,
    IdentityVersionId, IntegrationCandidateId, InteractionResult, MemoryCandidate,
    MemoryCandidateId, MemoryCandidateStatus, MemoryId, MemoryKind, Observation, ObservationId,
    Operation, OperationId, OperationStatus, Position, PositionId, PositionStatus, Principal,
    PrincipalId, PrincipalKind, Receipt, ReceiptId, Relationship, RelationshipId, ResponseRecord,
    Run, RunId, RunStatus, SelfReview, SleepRunId, Stance, Task, TaskId, TaskStatus,
    ThoughtContext, ThoughtCycle, ThoughtDraft, Verification, VerificationId, VerificationStatus,
    WorkingState, WorkingStateId,
};
pub use recall::{RecallBundle, RecallQuery, RecalledItem};
pub use response_profile::{
    ProgressVisibility, ResponseFormatPreference, ResponsePreferenceEvidence,
    ResponsePreferenceKey, ResponsePreferenceScope, ResponseProfile, ResponseProfileReport,
    ResponseProfileResolution, ResponseStepSize, ResponseVerbosity, TechnicalDepth,
};
pub use sleep::{
    integration_candidate_fingerprint, ContextBudgetReport, ExistingRevisionType,
    IntegrationCandidate, IntegrationCandidateDraft, IntegrationCandidateKind, SleepContext,
    SleepDeliberation, SleepRun, SleepRunStatus, SleepSeed, SleepSelfReview, MAX_CANDIDATE_SOURCES,
    MAX_SLEEP_CANDIDATES, MAX_SLEEP_RECALL, MAX_SLEEP_SEEDS, MAX_SLEEP_TEXT,
    SLEEP_ANCHOR_BUDGET_BYTES, SLEEP_CONTEXT_HARD_LIMIT_BYTES, SLEEP_RECALL_BUDGET_BYTES,
    SLEEP_SEED_BUDGET_BYTES,
};
