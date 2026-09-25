use serde::{Deserialize, Serialize};

use super::model::EventId;
use uuid::Uuid;

/// Future outbound intent. This type does not authorize or deliver messages.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InitiativeStatus {
    Proposed,
    AwaitingApproval,
    Ready,
    Delivered,
    Dismissed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InitiativeProposal {
    pub id: Uuid,
    pub content: String,
    pub rationale: String,
    pub source_event_ids: Vec<EventId>,
    pub status: InitiativeStatus,
    pub created_at: String,
}
