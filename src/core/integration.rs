use serde::{Deserialize, Serialize};

use super::{
    EvidenceRef, ExistingRevisionType, IntegrationCandidateId, MemoryCandidateId, MemoryId,
    PrincipalId, VerificationDisposition,
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct IntegrationVerification {
    pub candidate_id: IntegrationCandidateId,
    pub previous_disposition: VerificationDisposition,
    pub new_disposition: VerificationDisposition,
    pub actor_id: PrincipalId,
    pub reason: String,
    pub evidence_refs: Vec<EvidenceRef>,
    pub as_of_revision: ExistingRevisionType,
    pub created_at: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct IntegrationMaterialization {
    pub candidate_id: IntegrationCandidateId,
    pub memory_candidate_id: MemoryCandidateId,
    pub memory_id: MemoryId,
    pub source_event_ids: Vec<super::EventId>,
    pub counterevidence_event_ids: Vec<super::EventId>,
    pub evidence_refs: Vec<EvidenceRef>,
    pub fingerprint: String,
    pub as_of_revision: ExistingRevisionType,
    pub created_at: String,
}
