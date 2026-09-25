use serde::{Deserialize, Serialize};

use super::model::{EventId, Focus, GoalId, ObservationId, RunId, TaskId, TaskStatus};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FocusOutcome {
    Continue,
    NewWork,
    Conversation,
    Clarification,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FocusBasis {
    ExplicitNewWork,
    ExplicitTask,
    ExplicitGoal,
    ExplicitRun,
    ExplicitContinuation,
    SameThreadContinuation,
    StrongTextMatch,
    GeneralConversation,
    AmbiguousCandidates,
    ClosedWorkReference,
    InvalidReference,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FocusCandidate {
    pub goal_id: GoalId,
    pub goal_title: String,
    pub task_id: TaskId,
    pub task_title: String,
    pub task_status: TaskStatus,
    pub run_id: Option<RunId>,
    #[serde(default)]
    pub evidence_event_ids: Vec<EventId>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FocusResolution {
    pub observation_id: ObservationId,
    pub outcome: FocusOutcome,
    pub focus: Focus,
    pub as_of_revision: u64,
    pub basis: FocusBasis,
    #[serde(default)]
    pub candidates: Vec<FocusCandidate>,
    #[serde(default)]
    pub clarification: Option<String>,
}
