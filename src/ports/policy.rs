use thiserror::Error;

use crate::core::{ActionIntent, PrincipalId};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PolicyDecision {
    pub allowed: bool,
    pub requires_approval: bool,
    pub reason: String,
}

#[derive(Debug, Error)]
pub enum PolicyError {
    #[error("policy evaluation failed: {0}")]
    Evaluation(String),
}

pub trait Policy: Send + Sync {
    fn evaluate(
        &self,
        actor_id: PrincipalId,
        intent: &ActionIntent,
    ) -> Result<PolicyDecision, PolicyError>;
}
