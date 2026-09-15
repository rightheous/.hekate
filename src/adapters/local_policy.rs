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
        if intent.capability == "computer" {
            return Ok(match computer_use_risk(&intent.operation) {
                Some(ComputerUseRisk::Observe) => PolicyDecision {
                    allowed: true,
                    requires_approval: false,
                    reason: "desktop observation is read-only".to_owned(),
                },
                Some(ComputerUseRisk::Interact) => PolicyDecision {
                    allowed: true,
                    requires_approval: false,
                    reason: "non-sensitive desktop interaction is permitted".to_owned(),
                },
                Some(ComputerUseRisk::SensitiveInteraction) => PolicyDecision {
                    allowed: true,
                    requires_approval: true,
                    reason: "desktop input may trigger sensitive external effects".to_owned(),
                },
                None => PolicyDecision {
                    allowed: false,
                    requires_approval: false,
                    reason: "unknown computer-use operation".to_owned(),
                },
            });
        }
        if intent.capability == "workspace_read"
            && matches!(
                intent.operation.as_str(),
                "list" | "read" | "read_text" | "search" | "metadata" | "hash" | "diff"
            )
        {
            return Ok(PolicyDecision {
                allowed: true,
                requires_approval: false,
                reason: "v1 permits read-only workspace access".to_owned(),
            });
        }
        if intent.capability == "workspace_write"
            && matches!(intent.operation.as_str(), "write" | "write_text" | "create")
        {
            return Ok(PolicyDecision {
                allowed: true,
                requires_approval: true,
                reason: "workspace writes are permitted only after explicit approval".to_owned(),
            });
        }
        if intent.capability == "document_reader" && intent.operation == "read" {
            return Ok(PolicyDecision {
                allowed: true,
                requires_approval: false,
                reason: "v1 permits read-only document conversion".to_owned(),
            });
        }
        if intent.capability == "git_read"
            && matches!(
                intent.operation.as_str(),
                "status" | "diff" | "log" | "show" | "branch_list"
            )
        {
            return Ok(PolicyDecision {
                allowed: true,
                requires_approval: false,
                reason: "Git inspection is read-only".to_owned(),
            });
        }
        if intent.capability == "git_write"
            && matches!(
                intent.operation.as_str(),
                "add" | "commit" | "branch_create" | "checkout"
            )
        {
            return Ok(PolicyDecision {
                allowed: true,
                requires_approval: true,
                reason: "Git mutation requires explicit approval".to_owned(),
            });
        }
        if intent.capability.starts_with("browser.")
            && matches!(
                intent.operation.as_str(),
                "open"
                    | "snapshot"
                    | "find"
                    | "click"
                    | "type"
                    | "scroll"
                    | "back"
                    | "forward"
                    | "reload"
                    | "screenshot"
                    | "download"
                    | "current_url"
            )
        {
            let requires_approval = !matches!(
                intent.operation.as_str(),
                "open" | "snapshot" | "find" | "current_url"
            );
            return Ok(PolicyDecision {
                allowed: true,
                requires_approval,
                reason: if requires_approval {
                    "browser mutation requires explicit approval"
                } else {
                    "browser observation is read-only"
                }
                .to_owned(),
            });
        }
        Ok(PolicyDecision {
            allowed: false,
            requires_approval: false,
            reason: "the local Phase 0 policy permits no external mutation".to_owned(),
        })
    }
}

#[derive(Clone, Copy)]
enum ComputerUseRisk {
    Observe,
    Interact,
    SensitiveInteraction,
}

fn computer_use_risk(operation: &str) -> Option<ComputerUseRisk> {
    match operation {
        "list_windows" | "screenshot" | "inspect_accessibility" => Some(ComputerUseRisk::Observe),
        "focus_window" | "scroll" | "move_pointer" => Some(ComputerUseRisk::Interact),
        "click" | "double_click" | "type_text" | "key" | "hotkey" => {
            Some(ComputerUseRisk::SensitiveInteraction)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{computer_use_risk, ComputerUseRisk};

    #[test]
    fn computer_risk_comes_from_the_operation() {
        assert!(matches!(
            computer_use_risk("screenshot"),
            Some(ComputerUseRisk::Observe)
        ));
        assert!(matches!(
            computer_use_risk("scroll"),
            Some(ComputerUseRisk::Interact)
        ));
        assert!(matches!(
            computer_use_risk("type_text"),
            Some(ComputerUseRisk::SensitiveInteraction)
        ));
        assert!(computer_use_risk("purchase").is_none());
    }
}
