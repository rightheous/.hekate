use serde::{Deserialize, Serialize};

use super::{
    ConflictStatus, CurrentState, EventId, EvidenceRef, ExistingRevisionType,
    IntegrationCandidateId, MemoryCandidateId, MemoryId, Position, PositionId, PositionStatus,
    PrincipalId, Stance, VerificationDisposition, MAX_CANDIDATE_SOURCES, MAX_SLEEP_TEXT,
};

pub const POSITION_INTEGRATION_SCHEMA: &str = "hekate.position_integration.v1";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum PositionIntegrationOperation {
    Establish {
        subject: String,
        stance: Stance,
        reasons: Vec<String>,
        reconsideration_conditions: Vec<String>,
    },
    Revise {
        position_id: PositionId,
        expected_version: u32,
        stance: Stance,
        reasons: Vec<String>,
        reconsideration_conditions: Vec<String>,
    },
    Withdraw {
        position_id: PositionId,
        expected_version: u32,
        reason: String,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PositionIntegrationProposal {
    pub schema: String,
    pub operation: PositionIntegrationOperation,
}

impl PositionIntegrationProposal {
    pub fn parse(content: &str) -> Result<Self, String> {
        let proposal: Self = serde_json::from_str(content).map_err(|error| error.to_string())?;
        if proposal.schema != POSITION_INTEGRATION_SCHEMA {
            return Err("schema must be hekate.position_integration.v1".to_owned());
        }
        let validate_text = |value: &str, field: &str| {
            if value.trim().is_empty() || value.trim().len() > MAX_SLEEP_TEXT {
                Err(format!(
                    "{field} must be non-empty and at most {MAX_SLEEP_TEXT} bytes"
                ))
            } else {
                Ok(())
            }
        };
        let validate_texts = |values: &[String], field: &str| {
            if values.is_empty() || values.len() > MAX_CANDIDATE_SOURCES {
                return Err(format!(
                    "{field} must contain between 1 and {MAX_CANDIDATE_SOURCES} entries"
                ));
            }
            for value in values {
                validate_text(value, field)?;
            }
            Ok(())
        };
        match &proposal.operation {
            PositionIntegrationOperation::Establish {
                subject,
                reasons,
                reconsideration_conditions,
                ..
            } => {
                validate_text(subject, "subject")?;
                validate_texts(reasons, "reasons")?;
                validate_texts(reconsideration_conditions, "reconsideration_conditions")?;
            }
            PositionIntegrationOperation::Revise {
                expected_version,
                reasons,
                reconsideration_conditions,
                ..
            } => {
                if *expected_version == 0 {
                    return Err("expected_version must be positive".to_owned());
                }
                validate_texts(reasons, "reasons")?;
                validate_texts(reconsideration_conditions, "reconsideration_conditions")?;
            }
            PositionIntegrationOperation::Withdraw {
                expected_version,
                reason,
                ..
            } => {
                if *expected_version == 0 {
                    return Err("expected_version must be positive".to_owned());
                }
                validate_text(reason, "reason")?;
            }
        }
        Ok(proposal)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionIntegrationActionKind {
    Establish,
    Revise,
    Withdraw,
}

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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PositionIntegrationMaterialization {
    pub candidate_id: IntegrationCandidateId,
    pub position_id: PositionId,
    pub position_event_id: EventId,
    pub verification_event_id: EventId,
    pub action: PositionIntegrationActionKind,
    pub prior_position: Option<Position>,
    pub source_event_ids: Vec<EventId>,
    pub counterevidence_event_ids: Vec<EventId>,
    pub evidence_refs: Vec<EvidenceRef>,
    pub fingerprint: String,
    pub as_of_revision: ExistingRevisionType,
    pub created_at: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PositionIntegrationEventPayload {
    #[serde(flatten)]
    pub position: Position,
    pub position_integration: PositionIntegrationMaterialization,
}

pub fn position_integration_blocked(
    state: &CurrentState,
    principal_id: PrincipalId,
    subject: &str,
    target_position_id: Option<PositionId>,
) -> bool {
    let subject = super::normalize_description(subject);
    state.positions.values().any(|position| {
        position.principal_id == principal_id
            && position.status == PositionStatus::Active
            && Some(position.id) != target_position_id
            && super::normalize_description(&position.subject) == subject
    }) || state.conflicts.values().any(|conflict| {
        matches!(
            conflict.status,
            ConflictStatus::Open | ConflictStatus::Negotiating
        ) && (super::normalize_description(&conflict.subject) == subject
            || target_position_id.is_some_and(|id| conflict.participant_positions.contains(&id)))
    })
}
