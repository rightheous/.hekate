use serde::{Deserialize, Serialize};

use super::{
    EntityRef, EventId, ObservationId, PrincipalId, RecallBundle, RelationshipId, ResponseProfile,
    RunId, TaskId,
};

pub const DEFAULT_CONTEXT_ANCHOR_BUDGET_BYTES: usize = 24 * 1024;
pub const DEFAULT_CONTEXT_MIDDLE_BUDGET_BYTES: usize = 32 * 1024;
pub const DEFAULT_CONTEXT_ACTIVE_BUDGET_BYTES: usize = 48 * 1024;
pub const DEFAULT_CONTEXT_HARD_LIMIT_BYTES: usize = 104 * 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextBudget {
    pub anchor_bytes: usize,
    pub middle_bytes: usize,
    pub active_bytes: usize,
    pub hard_limit_bytes: usize,
}

impl Default for ContextBudget {
    fn default() -> Self {
        Self {
            anchor_bytes: DEFAULT_CONTEXT_ANCHOR_BUDGET_BYTES,
            middle_bytes: DEFAULT_CONTEXT_MIDDLE_BUDGET_BYTES,
            active_bytes: DEFAULT_CONTEXT_ACTIVE_BUDGET_BYTES,
            hard_limit_bytes: DEFAULT_CONTEXT_HARD_LIMIT_BYTES,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextBudgetReport {
    pub anchor_bytes: usize,
    pub middle_bytes: usize,
    pub active_bytes: usize,
    pub total_bytes: usize,
    pub included_items: usize,
    pub dropped_middle_items: usize,
    pub dropped_active_items: usize,
    pub hard_limit_bytes: usize,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextItemKind {
    Identity,
    Policy,
    Focus,
    Relationship,
    Memory,
    Position,
    Conflict,
    Goal,
    Task,
    Run,
    WorkingState,
    Artifact,
    RecentObservation,
    RecentEvent,
    Recall,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ContextItem {
    pub kind: ContextItemKind,
    pub text: String,
    pub source_event_ids: Vec<EventId>,
    pub entity: Option<EntityRef>,
    pub as_of_revision: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ContextSourceRef {
    pub event_id: EventId,
    pub entity: Option<EntityRef>,
    pub source_hash: String,
    pub as_of_revision: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ContextBuildRequest {
    pub principal_id: PrincipalId,
    #[serde(default)]
    pub current_observation_id: Option<ObservationId>,
    pub relationship_id: Option<RelationshipId>,
    pub task_id: Option<TaskId>,
    pub run_id: Option<RunId>,
    pub as_of_revision: u64,
    pub recalled: RecallBundle,
    pub budget: ContextBudget,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ContextSnapshot {
    pub as_of_revision: u64,
    pub anchors: Vec<ContextItem>,
    pub compressed_middle: Vec<ContextItem>,
    pub active_recent: Vec<ContextItem>,
    pub response_profile: ResponseProfile,
    pub budget_report: ContextBudgetReport,
    pub source_refs: Vec<ContextSourceRef>,
    pub snapshot_hash: String,
}
