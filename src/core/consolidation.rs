use serde::{Deserialize, Serialize};

use super::model::{EventId, IntegrationCandidateId, SleepRunId};

/// Future consolidation provenance. Sleep and MemoryIntegration remain the
/// durable owners until a separate consolidation workflow is implemented.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ConsolidationBoundary {
    pub as_of_revision: u64,
    pub sleep_run_ids: Vec<SleepRunId>,
    pub candidate_ids: Vec<IntegrationCandidateId>,
    pub source_event_ids: Vec<EventId>,
}
