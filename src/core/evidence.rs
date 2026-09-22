use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::model::{ArtifactId, EventId, TaskId};

pub use super::model::{CompletionClaimId, CompletionCriterionId};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Ord, PartialOrd, Serialize)]
pub struct EvidenceRef {
    pub event_id: EventId,
    pub artifact_id: Option<ArtifactId>,
    pub source_hash: String,
    pub as_of_sequence: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationDisposition {
    NeedsValidation,
    Verified,
    Rejected,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CompletionCriterion {
    pub id: CompletionCriterionId,
    pub task_id: TaskId,
    pub description: String,
    pub required: bool,
    pub created_at: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CompletionClaim {
    pub id: CompletionClaimId,
    pub task_id: TaskId,
    pub criterion_id: CompletionCriterionId,
    pub disposition: VerificationDisposition,
    pub confidence: u8,
    pub evidence_refs: Vec<EvidenceRef>,
    pub blocker: Option<String>,
    pub fingerprint: String,
    pub as_of_sequence: u64,
    pub supersedes: Option<CompletionClaimId>,
    pub created_at: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CompletionClaimTransition {
    pub claim_id: CompletionClaimId,
    pub task_id: TaskId,
    pub criterion_id: CompletionCriterionId,
    pub previous_disposition: VerificationDisposition,
    pub disposition: VerificationDisposition,
    pub reason: String,
    pub evidence_refs: Vec<EvidenceRef>,
    pub transitioned_at: String,
}

pub fn normalize_description(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

pub fn normalize_evidence_refs(mut refs: Vec<EvidenceRef>) -> Vec<EvidenceRef> {
    refs.sort_by(|left, right| {
        left.event_id
            .cmp(&right.event_id)
            .then_with(|| left.artifact_id.cmp(&right.artifact_id))
            .then_with(|| left.source_hash.cmp(&right.source_hash))
            .then_with(|| left.as_of_sequence.cmp(&right.as_of_sequence))
    });
    refs.dedup_by(|left, right| {
        left.event_id == right.event_id
            && left.artifact_id == right.artifact_id
            && left.source_hash == right.source_hash
    });
    refs
}

pub fn completion_fingerprint(
    task_id: TaskId,
    criterion_id: CompletionCriterionId,
    evidence_refs: &[EvidenceRef],
) -> String {
    let mut event_ids = evidence_refs
        .iter()
        .map(|evidence| evidence.event_id.to_string())
        .collect::<Vec<_>>();
    let mut artifact_ids = evidence_refs
        .iter()
        .filter_map(|evidence| evidence.artifact_id.map(|id| id.to_string()))
        .collect::<Vec<_>>();
    let mut source_hashes = evidence_refs
        .iter()
        .map(|evidence| evidence.source_hash.clone())
        .collect::<Vec<_>>();
    event_ids.sort();
    artifact_ids.sort();
    source_hashes.sort();
    let input = format!(
        "completion\n{task_id}\n{criterion_id}\n{}\n{}\n{}",
        event_ids.join(","),
        artifact_ids.join(","),
        source_hashes.join(","),
    );
    sha256_hex(input.as_bytes())
}

pub fn valid_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
