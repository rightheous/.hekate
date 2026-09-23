use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use uuid::Uuid;

use super::event::ExperienceEvent;
use super::recall::RecallBundle;

macro_rules! id_type {
    ($name:ident) => {
        #[derive(
            Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
        )]
        pub struct $name(Uuid);

        impl $name {
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }

            pub const fn from_uuid(value: Uuid) -> Self {
                Self(value)
            }

            pub const fn uuid(self) -> Uuid {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl From<Uuid> for $name {
            fn from(value: Uuid) -> Self {
                Self(value)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

id_type!(PrincipalId);
id_type!(IdentityVersionId);
id_type!(RelationshipId);
id_type!(GoalId);
id_type!(TaskId);
id_type!(RunId);
id_type!(AttemptId);
id_type!(WorkingStateId);
id_type!(PositionId);
id_type!(ConflictId);
id_type!(CommitmentId);
id_type!(ObservationId);
id_type!(DecisionId);
id_type!(ActionIntentId);
id_type!(OperationId);
id_type!(ArtifactId);
id_type!(EventId);
id_type!(MemoryCandidateId);
id_type!(MemoryId);
id_type!(ApprovalId);
id_type!(ReceiptId);
id_type!(VerificationId);
id_type!(SleepRunId);
id_type!(IntegrationCandidateId);
id_type!(CompletionCriterionId);
id_type!(CompletionClaimId);

pub fn now() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned())
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    User,
    Hekate,
    Unit,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Principal {
    pub id: PrincipalId,
    pub kind: PrincipalKind,
    pub name: String,
    pub identity_version_id: Option<IdentityVersionId>,
}

impl Principal {
    pub fn new(kind: PrincipalKind, name: impl Into<String>) -> Self {
        Self {
            id: PrincipalId::new(),
            kind,
            name: name.into(),
            identity_version_id: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct IdentityVersion {
    pub id: IdentityVersionId,
    pub principal_id: PrincipalId,
    pub version: u32,
    pub name: String,
    pub values: Vec<String>,
    pub boundaries: Vec<String>,
    #[serde(default = "now")]
    pub created_at: String,
    pub supersedes: Option<IdentityVersionId>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Relationship {
    pub id: RelationshipId,
    pub participants: Vec<PrincipalId>,
    pub shared_commitments: Vec<CommitmentId>,
    pub unresolved_conflicts: Vec<ConflictId>,
    pub trust_by_domain: BTreeMap<String, u8>,
    pub interaction_norms: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalStatus {
    Active,
    Paused,
    Completed,
    Abandoned,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Goal {
    pub id: GoalId,
    pub owner_principal_id: PrincipalId,
    pub participants: Vec<PrincipalId>,
    pub title: String,
    pub description: String,
    pub status: GoalStatus,
    pub created_at: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Planned,
    InProgress,
    Blocked,
    Completed,
    Cancelled,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Task {
    pub id: TaskId,
    pub goal_id: Option<GoalId>,
    pub title: String,
    pub status: TaskStatus,
    pub created_at: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Pending,
    Running,
    Suspended,
    Completed,
    Failed,
    Cancelled,
    NeedsAttention,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Run {
    pub id: RunId,
    pub task_id: Option<TaskId>,
    pub status: RunStatus,
    pub started_at: String,
    pub completed_at: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptStatus {
    Started,
    Succeeded,
    Failed,
    Unknown,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Attempt {
    pub id: AttemptId,
    pub run_id: RunId,
    pub status: AttemptStatus,
    pub started_at: String,
    pub finished_at: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkingState {
    pub id: WorkingStateId,
    pub run_id: RunId,
    pub revision: u64,
    pub completed_observations: Vec<ObservationId>,
    pub notes: Vec<String>,
    pub next_action: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Stance {
    Support,
    Oppose,
    Uncertain,
    Neutral,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionStatus {
    Active,
    Superseded,
    Retracted,
}

fn default_position_version() -> u32 {
    1
}

fn default_confidence() -> u8 {
    50
}

fn default_position_status() -> PositionStatus {
    PositionStatus::Active
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Position {
    pub id: PositionId,
    pub principal_id: PrincipalId,
    pub subject: String,
    pub stance: Stance,
    #[serde(default = "default_position_version")]
    pub version: u32,
    #[serde(default = "default_position_status")]
    pub status: PositionStatus,
    #[serde(default = "default_confidence")]
    pub confidence: u8,
    #[serde(default)]
    pub supersedes: Option<PositionId>,
    pub reasons: Vec<String>,
    pub evidence_refs: Vec<EventId>,
    pub reconsideration_conditions: Vec<String>,
    #[serde(default = "now")]
    pub created_at: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictStatus {
    Open,
    Negotiating,
    Resolved,
    AcceptedDisagreement,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Conflict {
    pub id: ConflictId,
    pub subject: String,
    pub participant_positions: Vec<PositionId>,
    pub status: ConflictStatus,
    #[serde(default = "default_position_version")]
    pub revision: u32,
    pub reasons: Vec<String>,
    pub evidence_refs: Vec<EventId>,
    pub alternatives: Vec<String>,
    pub reconsideration_conditions: Vec<String>,
    #[serde(default)]
    pub unresolved_questions: Vec<String>,
    #[serde(default)]
    pub resolution: Option<String>,
    #[serde(default)]
    pub resolved_at: Option<String>,
    #[serde(default = "now")]
    pub created_at: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitmentStatus {
    Open,
    Fulfilled,
    Broken,
    Cancelled,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Commitment {
    pub id: CommitmentId,
    pub debtor_principal_id: PrincipalId,
    pub creditor_principal_id: PrincipalId,
    pub promise: String,
    pub status: CommitmentStatus,
    pub source_event_id: Option<EventId>,
    pub created_at: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryKind {
    ExplicitPreference,
    InferredPreference,
    VerifiedFact,
    Episode,
    Lesson,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryCandidateStatus {
    Candidate,
    Promoted,
    Rejected,
    Superseded,
    Expired,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MemoryCandidate {
    pub id: MemoryCandidateId,
    pub kind: MemoryKind,
    pub content: String,
    pub subject_principal_id: Option<PrincipalId>,
    pub status: MemoryCandidateStatus,
    pub confidence: u8,
    pub source_event_ids: Vec<EventId>,
    pub valid_from: Option<String>,
    pub valid_until: Option<String>,
    pub supersedes: Option<MemoryCandidateId>,
    pub created_at: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ActiveMemoryStatus {
    Active,
    Superseded,
    Expired,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ActiveMemory {
    pub id: MemoryId,
    pub candidate_id: MemoryCandidateId,
    pub kind: MemoryKind,
    pub content: String,
    pub subject_principal_id: Option<PrincipalId>,
    pub status: ActiveMemoryStatus,
    pub confidence: u8,
    pub source_event_ids: Vec<EventId>,
    pub valid_from: Option<String>,
    pub valid_until: Option<String>,
    pub supersedes: Option<MemoryId>,
    pub last_verified_at: Option<String>,
    pub created_at: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Observation {
    pub id: ObservationId,
    pub actor_id: PrincipalId,
    pub content: String,
    pub source_type: String,
    pub source_ref: Option<String>,
    #[serde(default)]
    pub thread_id: Option<String>,
    #[serde(default)]
    pub message_id: Option<String>,
    pub received_at: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionKind {
    Respond,
    Agree,
    AskWhy,
    Challenge,
    CounterPropose,
    Negotiate,
    Refuse,
    ObserveMore,
    ProposeAction,
    CreateOrUpdateGoal,
    CreateOrUpdateTask,
    RequestClarification,
    RequestApproval,
    Suspend,
    Complete,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ThoughtDraft {
    pub interpretation: String,
    pub initial_judgment: DecisionKind,
    pub reasons: Vec<String>,
    pub uncertainties: Vec<String>,
    pub initial_intent: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SelfReview {
    pub strongest_counterargument: String,
    pub value_conflicts: Vec<String>,
    pub unsupported_claims: Vec<String>,
    pub revision_direction: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ActionIntent {
    pub id: ActionIntentId,
    pub capability: String,
    pub operation: String,
    pub target: String,
    pub arguments: serde_json::Value,
    pub expected_effect: String,
    pub preconditions: Vec<String>,
    #[serde(default)]
    pub proposed_by_event: Option<EventId>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ActionProposal {
    pub capability: String,
    pub operation: String,
    pub target: String,
    pub arguments: serde_json::Value,
    pub expected_effect: String,
    pub preconditions: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationStatus {
    Planned,
    Authorized,
    Started,
    Succeeded,
    Failed,
    Unknown,
    Verified,
    Disputed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Operation {
    pub id: OperationId,
    pub intent_id: ActionIntentId,
    pub status: OperationStatus,
    pub idempotency_key: String,
    #[serde(default)]
    pub approval_id: Option<ApprovalId>,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalStatus {
    Pending,
    Approved,
    Denied,
    Expired,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Approval {
    pub id: ApprovalId,
    pub intent_id: ActionIntentId,
    pub operation_id: OperationId,
    pub requested_by: PrincipalId,
    pub status: ApprovalStatus,
    pub reason: String,
    pub resolved_by: Option<PrincipalId>,
    pub resolved_at: Option<String>,
    pub created_at: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Receipt {
    pub id: ReceiptId,
    pub operation_id: OperationId,
    pub status: OperationStatus,
    pub external_reference: Option<String>,
    pub output: serde_json::Value,
    pub recorded_at: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationStatus {
    Verified,
    Failed,
    Disputed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Verification {
    pub id: VerificationId,
    pub operation_id: OperationId,
    pub status: VerificationStatus,
    pub evidence: Vec<String>,
    pub checked_at: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Artifact {
    pub id: ArtifactId,
    pub path: String,
    pub content_hash: String,
    pub size: u64,
    pub media_type: String,
    pub provenance_event_id: EventId,
    pub created_at: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Decision {
    pub id: DecisionId,
    pub kind: DecisionKind,
    pub message: String,
    #[serde(default)]
    pub target_principal_id: Option<PrincipalId>,
    #[serde(default)]
    pub request: String,
    pub context_hash: String,
    #[serde(default = "default_confidence")]
    pub confidence: u8,
    pub reasons: Vec<String>,
    pub evidence_refs: Vec<EventId>,
    pub alternatives: Vec<String>,
    pub reconsideration_conditions: Vec<String>,
    #[serde(default)]
    pub unresolved_questions: Vec<String>,
    #[serde(default)]
    pub related_position_ids: Vec<PositionId>,
    #[serde(default)]
    pub user_position: Option<Position>,
    #[serde(default)]
    pub cognitive_trace_id: Option<String>,
    pub position: Option<Position>,
    pub conflict: Option<Conflict>,
    pub action: Option<ActionIntent>,
    pub created_at: String,
}

impl Decision {
    pub fn respond(message: impl Into<String>) -> Self {
        Self {
            id: DecisionId::new(),
            kind: DecisionKind::Respond,
            message: message.into(),
            target_principal_id: None,
            request: String::new(),
            context_hash: String::new(),
            confidence: default_confidence(),
            reasons: Vec::new(),
            evidence_refs: Vec::new(),
            alternatives: Vec::new(),
            reconsideration_conditions: Vec::new(),
            unresolved_questions: Vec::new(),
            related_position_ids: Vec::new(),
            user_position: None,
            cognitive_trace_id: None,
            position: None,
            conflict: None,
            action: None,
            created_at: now(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommittedJudgment {
    pub final_act: DecisionKind,
    pub reasons: Vec<String>,
    pub response: String,
    pub confidence: u8,
    pub unresolved_questions: Vec<String>,
    pub alternatives: Vec<String>,
    pub reconsideration_conditions: Vec<String>,
    pub evidence_refs: Vec<EventId>,
    pub related_position_ids: Vec<PositionId>,
    pub position: Option<Position>,
    pub user_position: Option<Position>,
    pub conflict: Option<Conflict>,
    pub action: Option<ActionProposal>,
}

#[derive(Clone, Debug)]
pub struct ThoughtCycle {
    pub draft: ThoughtDraft,
    pub review: SelfReview,
    pub commitment: CommittedJudgment,
    pub trace: CognitiveTrace,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CognitiveTrace {
    pub trace_id: String,
    pub outcome: String,
    pub provider: String,
    pub model: String,
    pub schema_version: String,
    pub context_sequence: u64,
    pub context_hash: String,
    pub referenced_event_ids: Vec<EventId>,
    pub draft: Option<ThoughtDraft>,
    pub review: Option<SelfReview>,
    pub commitment: Option<CommittedJudgment>,
    pub parse_errors: Vec<String>,
    pub retries: u8,
    pub elapsed_ms: u64,
    pub raw_response_hash: Option<String>,
    pub error_kind: Option<String>,
    pub created_at: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResponseRecord {
    pub decision_id: DecisionId,
    pub content: String,
    pub created_at: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CurrentState {
    pub revision: u64,
    pub principals: BTreeMap<PrincipalId, Principal>,
    pub identity_versions: BTreeMap<IdentityVersionId, IdentityVersion>,
    pub relationships: BTreeMap<RelationshipId, Relationship>,
    pub goals: BTreeMap<GoalId, Goal>,
    pub tasks: BTreeMap<TaskId, Task>,
    pub runs: BTreeMap<RunId, Run>,
    #[serde(default)]
    pub attempts: BTreeMap<AttemptId, Attempt>,
    pub working_states: BTreeMap<RunId, WorkingState>,
    pub positions: BTreeMap<PositionId, Position>,
    pub conflicts: BTreeMap<ConflictId, Conflict>,
    pub commitments: BTreeMap<CommitmentId, Commitment>,
    pub observations: BTreeMap<ObservationId, Observation>,
    pub decisions: BTreeMap<DecisionId, Decision>,
    pub action_intents: BTreeMap<ActionIntentId, ActionIntent>,
    pub operations: BTreeMap<OperationId, Operation>,
    pub artifacts: BTreeMap<ArtifactId, Artifact>,
    #[serde(default)]
    pub memory_candidates: BTreeMap<MemoryCandidateId, MemoryCandidate>,
    #[serde(default)]
    pub active_memories: BTreeMap<MemoryId, ActiveMemory>,
    #[serde(default)]
    pub approvals: BTreeMap<ApprovalId, Approval>,
    #[serde(default)]
    pub receipts: BTreeMap<ReceiptId, Receipt>,
    #[serde(default)]
    pub verifications: BTreeMap<VerificationId, Verification>,
    #[serde(default)]
    pub sleep_runs: BTreeMap<SleepRunId, crate::core::sleep::SleepRun>,
    #[serde(default)]
    pub integration_candidates:
        BTreeMap<IntegrationCandidateId, crate::core::sleep::IntegrationCandidate>,
    #[serde(default)]
    pub integration_verifications:
        BTreeMap<IntegrationCandidateId, crate::core::IntegrationVerification>,
    #[serde(default)]
    pub integration_materializations:
        BTreeMap<IntegrationCandidateId, crate::core::IntegrationMaterialization>,
    #[serde(default)]
    pub position_integration_materializations:
        BTreeMap<IntegrationCandidateId, crate::core::PositionIntegrationMaterialization>,
    #[serde(default)]
    pub sleep_cursor: u64,
    #[serde(default)]
    pub completion_criteria:
        BTreeMap<CompletionCriterionId, crate::core::evidence::CompletionCriterion>,
    #[serde(default)]
    pub completion_claims: BTreeMap<CompletionClaimId, crate::core::evidence::CompletionClaim>,
    pub applied_events: Vec<EventId>,
}

impl Default for CurrentState {
    fn default() -> Self {
        Self {
            revision: 0,
            principals: BTreeMap::new(),
            identity_versions: BTreeMap::new(),
            relationships: BTreeMap::new(),
            goals: BTreeMap::new(),
            tasks: BTreeMap::new(),
            runs: BTreeMap::new(),
            attempts: BTreeMap::new(),
            working_states: BTreeMap::new(),
            positions: BTreeMap::new(),
            conflicts: BTreeMap::new(),
            commitments: BTreeMap::new(),
            observations: BTreeMap::new(),
            decisions: BTreeMap::new(),
            action_intents: BTreeMap::new(),
            operations: BTreeMap::new(),
            artifacts: BTreeMap::new(),
            memory_candidates: BTreeMap::new(),
            active_memories: BTreeMap::new(),
            approvals: BTreeMap::new(),
            receipts: BTreeMap::new(),
            verifications: BTreeMap::new(),
            sleep_runs: BTreeMap::new(),
            integration_candidates: BTreeMap::new(),
            integration_verifications: BTreeMap::new(),
            integration_materializations: BTreeMap::new(),
            position_integration_materializations: BTreeMap::new(),
            sleep_cursor: 0,
            completion_criteria: BTreeMap::new(),
            completion_claims: BTreeMap::new(),
            applied_events: Vec::new(),
        }
    }
}

impl CurrentState {
    pub fn active_run(&self) -> Option<&Run> {
        self.runs.values().find(|run| {
            matches!(
                run.status,
                RunStatus::Pending | RunStatus::Running | RunStatus::Suspended
            )
        })
    }

    pub fn hekate_identity(&self) -> Option<&IdentityVersion> {
        self.principals
            .values()
            .find(|principal| matches!(principal.kind, PrincipalKind::Hekate))
            .and_then(|principal| principal.identity_version_id)
            .and_then(|id| self.identity_versions.get(&id))
    }

    pub fn active_positions(&self, principal_id: PrincipalId) -> Vec<&Position> {
        self.positions
            .values()
            .filter(|position| {
                position.principal_id == principal_id
                    && matches!(position.status, PositionStatus::Active)
            })
            .collect()
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ThoughtContext {
    pub event_sequence: u64,
    pub observation: Observation,
    pub focus: Focus,
    pub identity: Option<IdentityVersion>,
    pub relationship: Option<Relationship>,
    pub goal: Option<Goal>,
    pub task: Option<Task>,
    pub run: Option<Run>,
    pub working_state: Option<WorkingState>,
    pub positions: Vec<Position>,
    pub user_positions: Vec<Position>,
    pub conflicts: Vec<Conflict>,
    pub commitments: Vec<Commitment>,
    #[serde(default)]
    pub memories: Vec<ActiveMemory>,
    #[serde(default)]
    pub memory_candidates: Vec<MemoryCandidate>,
    #[serde(default)]
    pub pending_approvals: Vec<Approval>,
    #[serde(default)]
    pub artifacts: Vec<Artifact>,
    #[serde(default)]
    pub recall: RecallBundle,
    #[serde(default)]
    pub context_snapshot: Option<serde_json::Value>,
    pub recent_event_ids: Vec<EventId>,
    pub relevant_events: Vec<ExperienceEvent>,
    pub available_capabilities: Vec<String>,
    pub output_schema: String,
    pub snapshot_hash: String,
}

pub type ContextSnapshot = ThoughtContext;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Focus {
    pub goal_id: Option<GoalId>,
    pub task_id: Option<TaskId>,
    pub run_id: Option<RunId>,
}

impl Focus {
    pub fn unattached() -> Self {
        Self {
            goal_id: None,
            task_id: None,
            run_id: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InteractionResult {
    pub observation_id: ObservationId,
    pub focus: Focus,
    pub decision: Decision,
    pub revision: u64,
    #[serde(default)]
    pub operation_id: Option<OperationId>,
    #[serde(default)]
    pub approval_id: Option<ApprovalId>,
}
