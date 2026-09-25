use crate::core::initiative::AgendaCandidate;
use crate::core::{CurrentState, ExperienceEvent, PrincipalId};

/// Select initiative candidates from the current projection and event ledger.
/// The v1 contract defines the signature; candidate selection is not active yet.
pub fn select(
    state: &CurrentState,
    events: &[ExperienceEvent],
    hekate_id: PrincipalId,
    user_id: PrincipalId,
) -> Vec<AgendaCandidate> {
    let _ = (state, events, hekate_id, user_id);
    Vec::new()
}
