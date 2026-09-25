use std::sync::Arc;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::core::event::{
    EntityKind, EntityRef, EventError, EventKind, EventSource, ExperienceEvent,
};
use crate::core::{
    initiative_fingerprint, now, AgendaCandidate, Commitment, CurrentState, EventId,
    InitiativeProposal, InitiativeStatus, PrincipalId, PrincipalKind,
};
use crate::ports::{Storage, StorageError};
use crate::runtime::projector::{ProjectionError, Projector};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum InitiativeRunResult {
    NoCandidate,
    Duplicate { fingerprint: String },
    Deferred,
    Proposed { proposal: InitiativeProposal },
    Dismissed { proposal: InitiativeProposal },
}

#[derive(Debug, Error)]
pub enum InitiativeError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Projection(#[from] ProjectionError),
    #[error(transparent)]
    Event(#[from] EventError),
    #[error("initiative candidate is invalid: {0}")]
    InvalidCandidate(String),
    #[error("initiative {0} does not exist")]
    NotFound(Uuid),
    #[error("initiative ledger revision changed while loading a snapshot")]
    StaleSnapshot,
    #[error("initiative projection does not match replayed ledger state")]
    ProjectionMismatch,
}

#[derive(Clone)]
pub struct InitiativeService {
    storage: Arc<dyn Storage>,
    hekate_id: PrincipalId,
    user_id: PrincipalId,
}

impl InitiativeService {
    pub fn new(storage: Arc<dyn Storage>, hekate_id: PrincipalId, user_id: PrincipalId) -> Self {
        Self {
            storage,
            hekate_id,
            user_id,
        }
    }

    pub async fn list(&self) -> Result<Vec<InitiativeProposal>, InitiativeError> {
        Ok(self
            .storage
            .load_state()
            .await?
            .initiatives
            .into_values()
            .filter(|proposal| proposal.target_principal_id == self.user_id)
            .collect())
    }

    pub async fn foreground_active(&self) -> Result<bool, InitiativeError> {
        Ok(self.storage.has_active_foreground_lease().await?)
    }

    pub async fn run_once(&self) -> Result<InitiativeRunResult, InitiativeError> {
        if self.storage.has_active_foreground_lease().await? {
            return Ok(InitiativeRunResult::Deferred);
        }
        let (state, events) = match self.snapshot().await {
            Ok(snapshot) => snapshot,
            Err(InitiativeError::StaleSnapshot) => return Ok(InitiativeRunResult::Deferred),
            Err(error) => return Err(error),
        };
        let candidates = super::agenda::select(&state, &events, self.hekate_id, self.user_id);
        let Some(candidate) = candidates.iter().find(|candidate| {
            !state
                .initiatives
                .values()
                .any(|proposal| proposal.fingerprint == candidate.fingerprint)
        }) else {
            if let Some(candidate) = candidates.first() {
                return Ok(InitiativeRunResult::Duplicate {
                    fingerprint: candidate.fingerprint.clone(),
                });
            }
            return Ok(InitiativeRunResult::NoCandidate);
        };
        self.propose_candidate(candidate.clone()).await
    }

    /// Revalidates a selector result immediately before its event batch is committed.
    pub async fn propose_candidate(
        &self,
        candidate: AgendaCandidate,
    ) -> Result<InitiativeRunResult, InitiativeError> {
        let (state, events) = match self.snapshot().await {
            Ok(snapshot) => snapshot,
            Err(InitiativeError::StaleSnapshot) => return Ok(InitiativeRunResult::Deferred),
            Err(error) => return Err(error),
        };
        if state
            .initiatives
            .values()
            .any(|proposal| proposal.fingerprint == candidate.fingerprint)
        {
            return Ok(InitiativeRunResult::Duplicate {
                fingerprint: candidate.fingerprint,
            });
        }
        if candidate.as_of_revision != state.revision {
            return Ok(InitiativeRunResult::Deferred);
        }
        validate_candidate(&state, &events, &candidate, self.hekate_id, self.user_id)?;
        if self.storage.has_active_foreground_lease().await? {
            return Ok(InitiativeRunResult::Deferred);
        }

        let mut proposal = InitiativeProposal {
            id: Uuid::new_v4(),
            kind: candidate.kind,
            content: candidate.content,
            rationale: candidate.rationale,
            source_entity: candidate.source_entity,
            source_version: candidate.source_version,
            source_event_ids: candidate.source_event_ids,
            target_principal_id: candidate.target_principal_id,
            as_of_revision: candidate.as_of_revision,
            fingerprint: candidate.fingerprint,
            status: InitiativeStatus::Proposed,
            created_at: now(),
        };
        let proposed = initiative_event(
            self.hekate_id,
            EventKind::InitiativeProposed,
            &proposal,
            None,
        )?;
        proposal.status = InitiativeStatus::Ready;
        let readied = initiative_event(
            self.hekate_id,
            EventKind::InitiativeReadied,
            &proposal,
            Some(proposed.event_id),
        )?;
        match Projector::new(self.storage.clone())
            .record_batch(&[proposed, readied], Some(state.revision), None)
            .await
        {
            Ok(_) => Ok(InitiativeRunResult::Proposed { proposal }),
            Err(error) if revision_conflict(&error) => Ok(InitiativeRunResult::Deferred),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn dismiss(&self, id: Uuid) -> Result<InitiativeRunResult, InitiativeError> {
        let (state, _) = self.snapshot().await?;
        let Some(current) = state.initiatives.get(&id) else {
            return Err(InitiativeError::NotFound(id));
        };
        if current.target_principal_id != self.user_id {
            return Err(InitiativeError::InvalidCandidate(
                "initiative is addressed to another principal".to_owned(),
            ));
        }
        if current.status == InitiativeStatus::Dismissed {
            return Ok(InitiativeRunResult::Dismissed {
                proposal: current.clone(),
            });
        }
        let mut proposal = current.clone();
        proposal.status = InitiativeStatus::Dismissed;
        let event = initiative_event(
            self.user_id,
            EventKind::InitiativeDismissed,
            &proposal,
            None,
        )?;
        match Projector::new(self.storage.clone())
            .record_batch(&[event], Some(state.revision), None)
            .await
        {
            Ok(_) => Ok(InitiativeRunResult::Dismissed { proposal }),
            Err(error) if revision_conflict(&error) => Ok(InitiativeRunResult::Deferred),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn ready_for_display(&self) -> Result<Option<InitiativeProposal>, InitiativeError> {
        Ok(self
            .list()
            .await?
            .into_iter()
            .find(|proposal| proposal.status == InitiativeStatus::Ready))
    }

    async fn snapshot(&self) -> Result<(CurrentState, Vec<ExperienceEvent>), InitiativeError> {
        let state = self.storage.load_state().await?;
        let events = self.storage.load_events().await?;
        let event_ids = events
            .iter()
            .map(|event| event.event_id)
            .collect::<Vec<_>>();
        if state.revision != events.len() as u64 || state.applied_events != event_ids {
            return Err(InitiativeError::StaleSnapshot);
        }
        if Projector::replay(&events)? != state {
            return Err(InitiativeError::ProjectionMismatch);
        }
        Ok((state, events))
    }
}

fn validate_candidate(
    state: &CurrentState,
    events: &[ExperienceEvent],
    candidate: &AgendaCandidate,
    hekate_id: PrincipalId,
    user_id: PrincipalId,
) -> Result<(), InitiativeError> {
    let reject = |message: &str| InitiativeError::InvalidCandidate(message.to_owned());
    if !state
        .principals
        .get(&hekate_id)
        .is_some_and(|principal| matches!(principal.kind, PrincipalKind::Hekate))
    {
        return Err(reject("configured HEKATE principal is missing or invalid"));
    }
    if !state
        .principals
        .get(&user_id)
        .is_some_and(|principal| matches!(principal.kind, PrincipalKind::User))
    {
        return Err(reject("configured target principal is missing or invalid"));
    }
    if candidate.target_principal_id != user_id {
        return Err(reject("candidate target is not the configured user"));
    }
    if candidate.content.trim().is_empty() || candidate.rationale.trim().is_empty() {
        return Err(reject("content and rationale must be non-empty"));
    }
    if candidate.source_event_ids.is_empty() {
        return Err(reject("candidate has no evidence events"));
    }
    if candidate.fingerprint
        != initiative_fingerprint(
            candidate.kind,
            &candidate.source_entity,
            candidate.source_version,
            candidate.target_principal_id,
            &candidate.source_event_ids,
        )
    {
        return Err(reject(
            "candidate fingerprint does not match its provenance",
        ));
    }
    for event in events {
        if !event.verify_integrity()? {
            return Err(reject("ledger event integrity check failed"));
        }
    }
    for event_id in &candidate.source_event_ids {
        if !events.iter().any(|event| event.event_id == *event_id) {
            return Err(reject("evidence event is not in the current ledger"));
        }
    }
    if !source_owned_by(state, &candidate.source_entity, user_id, hekate_id) {
        return Err(reject(
            "source entity is not owned by or addressed to the target",
        ));
    }
    let Some(version) = source_version(state, events, &candidate.source_entity) else {
        return Err(reject("source entity is unsupported or no longer exists"));
    };
    if candidate.source_version != version {
        return Err(reject("source entity version changed"));
    }
    if !super::agenda::select(state, events, hekate_id, user_id).contains(candidate) {
        return Err(reject(
            "candidate does not match the current verified Agenda selection",
        ));
    }
    Ok(())
}

fn source_version(
    state: &CurrentState,
    events: &[ExperienceEvent],
    source: &EntityRef,
) -> Option<u64> {
    if source.kind == EntityKind::Commitment {
        let commitment = state.commitments.get(&source.id.into())?;
        let (sequence, event, projected) = events
            .iter()
            .enumerate()
            .filter(|(_, event)| {
                event.subject.as_ref() == Some(source)
                    && matches!(
                        event.event_kind,
                        EventKind::CommitmentCreated | EventKind::CommitmentFulfilled
                    )
            })
            .filter_map(|(index, event)| {
                let projected = serde_json::from_value::<Commitment>(event.payload.clone()).ok()?;
                (projected.id.uuid() == source.id).then_some((index as u64 + 1, event, projected))
            })
            .max_by_key(|(sequence, _, _)| *sequence)?;
        if event.verify_integrity().ok() != Some(true)
            || projected != *commitment
            || event.event_kind != EventKind::CommitmentCreated
        {
            return None;
        }
        return Some(sequence);
    }

    let explicit = match source.kind {
        EntityKind::IdentityVersion => state
            .identity_versions
            .get(&source.id.into())
            .map(|item| item.version as u64),
        EntityKind::Position => state
            .positions
            .get(&source.id.into())
            .map(|item| item.version as u64),
        EntityKind::Conflict => state
            .conflicts
            .get(&source.id.into())
            .map(|item| item.revision as u64),
        _ => None,
    };
    explicit.or_else(|| {
        // ponytail: direct-event counts stand in for versions on unversioned entities; replace with explicit versions when those entities gain updates.
        let versions = events
            .iter()
            .filter(|event| event.subject.as_ref() == Some(source))
            .count() as u64;
        (versions > 0).then_some(versions)
    })
}

fn source_owned_by(
    state: &CurrentState,
    source: &EntityRef,
    target: PrincipalId,
    hekate: PrincipalId,
) -> bool {
    match source.kind {
        EntityKind::Principal => {
            source.id == target.uuid()
                && state
                    .principals
                    .get(&target)
                    .is_some_and(|principal| matches!(principal.kind, PrincipalKind::User))
        }
        EntityKind::IdentityVersion => state
            .identity_versions
            .get(&source.id.into())
            .is_some_and(|item| item.principal_id == target),
        EntityKind::Relationship => state
            .relationships
            .get(&source.id.into())
            .is_some_and(|item| item.participants.contains(&target)),
        EntityKind::Observation => state
            .observations
            .get(&source.id.into())
            .is_some_and(|item| item.actor_id == target),
        EntityKind::Goal => state.goals.get(&source.id.into()).is_some_and(|goal| {
            goal.owner_principal_id == target || goal.participants.contains(&target)
        }),
        EntityKind::Task => state
            .tasks
            .get(&source.id.into())
            .and_then(|task| task.goal_id)
            .and_then(|goal_id| state.goals.get(&goal_id))
            .is_some_and(|goal| {
                goal.owner_principal_id == target || goal.participants.contains(&target)
            }),
        EntityKind::Run => state
            .runs
            .get(&source.id.into())
            .and_then(|run| run.task_id)
            .and_then(|task_id| state.tasks.get(&task_id))
            .and_then(|task| task.goal_id)
            .and_then(|goal_id| state.goals.get(&goal_id))
            .is_some_and(|goal| {
                goal.owner_principal_id == target || goal.participants.contains(&target)
            }),
        EntityKind::WorkingState => state
            .working_states
            .values()
            .find(|item| item.id.uuid() == source.id)
            .and_then(|item| state.runs.get(&item.run_id))
            .and_then(|run| run.task_id)
            .and_then(|task_id| state.tasks.get(&task_id))
            .and_then(|task| task.goal_id)
            .and_then(|goal_id| state.goals.get(&goal_id))
            .is_some_and(|goal| {
                goal.owner_principal_id == target || goal.participants.contains(&target)
            }),
        EntityKind::Decision => state
            .decisions
            .get(&source.id.into())
            .is_some_and(|item| item.target_principal_id == Some(target)),
        EntityKind::Position => state
            .positions
            .get(&source.id.into())
            .is_some_and(|item| item.principal_id == target || item.principal_id == hekate),
        EntityKind::Conflict => state
            .conflicts
            .get(&source.id.into())
            .is_some_and(|conflict| {
                conflict.participant_positions.iter().any(|id| {
                    state
                        .positions
                        .get(id)
                        .is_some_and(|position| position.principal_id == target)
                })
            }),
        EntityKind::Commitment => state
            .commitments
            .get(&source.id.into())
            .is_some_and(|item| {
                item.debtor_principal_id == target || item.creditor_principal_id == target
            }),
        EntityKind::MemoryCandidate => state
            .memory_candidates
            .get(&source.id.into())
            .is_some_and(|item| item.subject_principal_id == Some(target)),
        EntityKind::Memory => state
            .active_memories
            .get(&source.id.into())
            .is_some_and(|item| item.subject_principal_id == Some(target)),
        // ponytail: reject unmodeled owner kinds; add a case when Agenda can select one safely.
        _ => false,
    }
}

fn initiative_event(
    actor: PrincipalId,
    kind: EventKind,
    proposal: &InitiativeProposal,
    causation_id: Option<EventId>,
) -> Result<ExperienceEvent, EventError> {
    let id = proposal.id;
    ExperienceEvent::new(
        actor,
        kind,
        Some(EntityRef::new(EntityKind::Initiative, id)),
        serde_json::to_value(proposal)?,
        EventSource::new("initiative", Some(id.to_string())),
        causation_id,
        Some(id.to_string()),
        None,
    )
}

fn revision_conflict(error: &ProjectionError) -> bool {
    matches!(
        error,
        ProjectionError::StaleContext { .. }
            | ProjectionError::Storage(
                StorageError::StaleContext { .. } | StorageError::RevisionConflict { .. }
            )
    )
}
