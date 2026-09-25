//! Future proposal and outbox boundary. Delivery and capability authorization
//! remain outside this module until a separate initiative workflow is approved.

pub mod agenda;
pub mod service;
pub mod worker;

pub use crate::core::initiative::{
    initiative_fingerprint, AgendaCandidate, InitiativeKind, InitiativeProposal, InitiativeStatus,
};
