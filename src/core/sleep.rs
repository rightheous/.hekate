use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::evidence::VerificationDisposition;
use super::{
    CognitiveTrace, Conflict, EventId, IdentityVersion, IntegrationCandidateId, Observation,
    Position, Relationship, SleepRunId,
};

pub const MAX_SLEEP_SEEDS: usize = 12;
pub const MAX_SLEEP_RECALL: usize = 24;
pub const MAX_SLEEP_CANDIDATES: usize = 8;
pub const MAX_CANDIDATE_SOURCES: usize = 16;
pub const MAX_SLEEP_TEXT: usize = 2_000;
pub const SLEEP_ANCHOR_BUDGET_BYTES: usize = 16 * 1024;
pub const SLEEP_SEED_BUDGET_BYTES: usize = 32 * 1024;
pub const SLEEP_RECALL_BUDGET_BYTES: usize = 32 * 1024;
pub const SLEEP_CONTEXT_HARD_LIMIT_BYTES: usize = 80 * 1024;

pub type ExistingRevisionType = u64;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SleepRunStatus {
    Running,
    Completed,
    Interrupted,
    Failed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SleepRun {
    pub id: SleepRunId,
    pub status: SleepRunStatus,
    pub high_water_revision: u64,
    pub cursor_before: u64,
    pub cursor_after: Option<u64>,
    pub seed_event_ids: Vec<EventId>,
    pub processed_observation_count: u32,
    pub created_candidate_count: u32,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub error_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_budget_report: Option<ContextBudgetReport>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextBudgetReport {
    pub anchor_bytes: usize,
    pub seed_bytes: usize,
    pub recalled_bytes: usize,
    pub total_bytes: usize,
    pub included_seed_count: usize,
    pub deferred_seed_count: usize,
    pub included_recall_count: usize,
    pub dropped_recall_count: usize,
    pub hard_limit_bytes: usize,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationCandidateKind {
    Memory,
    MemoryRevision,
    Position,
    Conflict,
    Relationship,
    Identity,
    Goal,
    Association,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct IntegrationCandidate {
    pub id: IntegrationCandidateId,
    pub sleep_run_id: SleepRunId,
    pub kind: IntegrationCandidateKind,
    #[serde(default)]
    pub disposition: VerificationDisposition,
    #[serde(default)]
    pub as_of_revision: ExistingRevisionType,
    pub content: String,
    pub rationale: String,
    pub source_event_ids: Vec<EventId>,
    pub counterevidence_event_ids: Vec<EventId>,
    pub confidence: u8,
    pub fingerprint: String,
    pub created_at: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SleepSeed {
    pub event_id: EventId,
    pub sequence: u64,
    pub observation: Observation,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SleepContext {
    pub sleep_run_id: SleepRunId,
    pub high_water_revision: u64,
    pub seed_observations: Vec<SleepSeed>,
    pub recalled_experiences: Vec<crate::core::RecalledItem>,
    pub identity: Option<IdentityVersion>,
    pub active_positions: Vec<Position>,
    pub active_conflicts: Vec<Conflict>,
    pub relationship: Option<Relationship>,
    #[serde(default)]
    pub snapshot_hash: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct IntegrationCandidateDraft {
    pub kind: IntegrationCandidateKind,
    pub content: String,
    pub rationale: String,
    pub source_event_ids: Vec<EventId>,
    pub counterevidence_event_ids: Vec<EventId>,
    pub confidence: u8,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SleepSelfReview {
    pub weak_points: Vec<String>,
    pub possible_counterevidence_event_ids: Vec<EventId>,
    pub revised: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SleepDeliberation {
    pub draft_summary: String,
    pub self_review: SleepSelfReview,
    pub candidates: Vec<IntegrationCandidateDraft>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace: Option<CognitiveTrace>,
}

pub fn integration_candidate_fingerprint(
    kind: &IntegrationCandidateKind,
    content: &str,
    source_event_ids: &[EventId],
    counterevidence_event_ids: &[EventId],
) -> String {
    let mut source = source_event_ids.to_vec();
    source.sort();
    let mut counterevidence = counterevidence_event_ids.to_vec();
    counterevidence.sort();
    let kind = serde_json::to_string(kind).unwrap_or_default();
    let mut hash = Sha256::new();
    for value in [kind, content.trim().to_owned()] {
        hash.update(value.as_bytes());
        hash.update([0]);
    }
    for event_id in &source {
        hash.update(event_id.to_string().as_bytes());
        hash.update([0]);
    }
    hash.update([1]);
    for event_id in &counterevidence {
        hash.update(event_id.to_string().as_bytes());
        hash.update([0]);
    }
    hash.finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
