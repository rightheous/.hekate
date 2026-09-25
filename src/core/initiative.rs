use serde::{Deserialize, Serialize};

use super::event::EntityRef;
use super::evidence::sha256_hex;
use super::model::{EventId, PrincipalId};
use uuid::Uuid;

/// Kind of initiative proposed from existing evidence.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InitiativeKind {
    Question,
    Challenge,
    FollowUp,
}

/// An agenda item with enough provenance to be reviewed and deduplicated.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AgendaCandidate {
    pub kind: InitiativeKind,
    pub content: String,
    pub rationale: String,
    pub source_entity: EntityRef,
    pub source_version: u64,
    pub source_event_ids: Vec<EventId>,
    pub target_principal_id: PrincipalId,
    pub as_of_revision: u64,
    pub fingerprint: String,
}

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
    pub kind: InitiativeKind,
    pub content: String,
    pub rationale: String,
    pub source_entity: EntityRef,
    pub source_version: u64,
    pub source_event_ids: Vec<EventId>,
    pub target_principal_id: PrincipalId,
    pub as_of_revision: u64,
    pub fingerprint: String,
    pub status: InitiativeStatus,
    pub created_at: String,
}

/// Fingerprint an initiative's semantic source and target for stable deduping.
/// Evidence IDs are treated as a set, so ordering and repeated IDs do not matter.
pub fn initiative_fingerprint(
    kind: InitiativeKind,
    source_entity: &EntityRef,
    source_version: u64,
    target_principal_id: PrincipalId,
    source_event_ids: &[EventId],
) -> String {
    let mut evidence_ids = source_event_ids.to_vec();
    evidence_ids.sort_unstable();
    evidence_ids.dedup();

    let canonical = serde_json::to_vec(&(
        "initiative-v1",
        kind,
        source_entity,
        source_version,
        target_principal_id,
        evidence_ids,
    ))
    .expect("serializing initiative fingerprint input");
    sha256_hex(&canonical)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::event::EntityKind;

    fn entity(id: u128) -> EntityRef {
        EntityRef::new(EntityKind::Observation, Uuid::from_u128(id))
    }

    fn principal(id: u128) -> PrincipalId {
        PrincipalId::from_uuid(Uuid::from_u128(id))
    }

    fn event(id: u128) -> EventId {
        EventId::from_uuid(Uuid::from_u128(id))
    }

    #[test]
    fn initiative_fingerprint_is_stable_for_same_provenance() {
        let source = entity(1);
        let target = principal(2);
        let first = [event(3), event(4)];
        let reordered_with_duplicate = [event(4), event(3), event(3)];

        assert_eq!(
            initiative_fingerprint(InitiativeKind::Question, &source, 5, target, &first,),
            initiative_fingerprint(
                InitiativeKind::Question,
                &source,
                5,
                target,
                &reordered_with_duplicate,
            )
        );
    }

    #[test]
    fn initiative_fingerprint_changes_with_provenance() {
        let source = entity(1);
        let target = principal(2);
        let evidence = [event(3), event(4)];
        let fingerprint =
            initiative_fingerprint(InitiativeKind::Question, &source, 5, target, &evidence);

        assert_ne!(
            fingerprint,
            initiative_fingerprint(InitiativeKind::Challenge, &source, 5, target, &evidence)
        );
        assert_ne!(
            fingerprint,
            initiative_fingerprint(InitiativeKind::Question, &entity(6), 5, target, &evidence)
        );
        assert_ne!(
            fingerprint,
            initiative_fingerprint(InitiativeKind::Question, &source, 7, target, &evidence)
        );
        assert_ne!(
            fingerprint,
            initiative_fingerprint(
                InitiativeKind::Question,
                &source,
                5,
                principal(8),
                &evidence,
            )
        );
        assert_ne!(
            fingerprint,
            initiative_fingerprint(
                InitiativeKind::Question,
                &source,
                5,
                target,
                &[event(3), event(9)],
            )
        );
    }
}
