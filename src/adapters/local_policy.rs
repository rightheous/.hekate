use crate::core::{ActionIntent, PrincipalId};
use crate::ports::{Policy, PolicyDecision, PolicyError};

#[derive(Clone, Default)]
pub struct LocalPolicy;

impl Policy for LocalPolicy {
    fn evaluate(
        &self,
        _actor_id: PrincipalId,
        intent: &ActionIntent,
    ) -> Result<PolicyDecision, PolicyError> {
        if intent.capability == "workspace_read"
            && matches!(
                intent.operation.as_str(),
                "list" | "read_text" | "search" | "metadata"
            )
        {
            return Ok(PolicyDecision {
                allowed: true,
                requires_approval: false,
                reason: "v1 permits read-only workspace access".to_owned(),
            });
        }
        if intent.capability == "workspace_write"
            && matches!(intent.operation.as_str(), "write_text" | "create")
        {
            return Ok(PolicyDecision {
                allowed: true,
                requires_approval: true,
                reason: "workspace writes are permitted only after explicit approval".to_owned(),
            });
        }
        Ok(PolicyDecision {
            allowed: false,
            requires_approval: false,
            reason: "the local Phase 0 policy permits no external mutation".to_owned(),
        })
    }
}
