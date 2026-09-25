//! Deterministic resolution of an observation to an existing task, new work,
//! ordinary conversation, or a persisted clarification question.

mod candidates;
mod classify;

use crate::core::{Attempt, CurrentState, EventKind, ExperienceEvent, Observation};
use crate::runtime::focus::focus_for_run;

pub(crate) use classify::{resolve, FocusPlan};

pub use crate::core::continuity::{FocusBasis, FocusCandidate, FocusOutcome, FocusResolution};

pub(crate) fn legacy_resolution(
    state: &CurrentState,
    events: &[ExperienceEvent],
    observation: &Observation,
) -> Result<Option<FocusResolution>, serde_json::Error> {
    let correlation_id = observation.id.to_string();
    let attempt = events
        .iter()
        .find(|event| {
            event.event_kind == EventKind::AttemptStarted
                && event.correlation_id.as_deref() == Some(correlation_id.as_str())
        })
        .map(|event| serde_json::from_value::<Attempt>(event.payload.clone()))
        .transpose()?;
    let run_id = if let Some(attempt) = attempt {
        Some(attempt.run_id)
    } else {
        events
            .iter()
            .find(|event| {
                event.event_kind == EventKind::RunStarted
                    && event.correlation_id.as_deref() == Some(correlation_id.as_str())
            })
            .map(|event| serde_json::from_value::<crate::core::Run>(event.payload.clone()))
            .transpose()?
            .map(|run| run.id)
    };
    let Some(focus) = run_id.and_then(|run_id| focus_for_run(state, run_id)) else {
        return Ok(None);
    };
    let created_work = [
        EventKind::GoalCreated,
        EventKind::TaskCreated,
        EventKind::RunStarted,
    ]
    .iter()
    .all(|kind| {
        events.iter().any(|event| {
            &event.event_kind == kind
                && event.correlation_id.as_deref() == Some(correlation_id.as_str())
        })
    });
    Ok(Some(FocusResolution {
        observation_id: observation.id,
        outcome: if focus.task_id.is_some() {
            if created_work {
                FocusOutcome::NewWork
            } else {
                FocusOutcome::Continue
            }
        } else {
            FocusOutcome::Conversation
        },
        focus,
        as_of_revision: state.revision,
        basis: if created_work {
            crate::core::FocusBasis::ExplicitNewWork
        } else {
            crate::core::FocusBasis::ExplicitContinuation
        },
        candidates: Vec::new(),
        clarification: None,
    }))
}
