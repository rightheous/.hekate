use crate::core::model::{Operation, OperationStatus, Run, RunStatus, Task, TaskStatus};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TransitionError {
    #[error("invalid {entity} transition from {from:?} to {to:?}")]
    Invalid {
        entity: &'static str,
        from: String,
        to: String,
    },
}

pub fn transition_task(task: &mut Task, next: TaskStatus) -> Result<(), TransitionError> {
    let allowed = matches!(
        (&task.status, &next),
        (
            TaskStatus::Planned,
            TaskStatus::InProgress | TaskStatus::Cancelled
        ) | (
            TaskStatus::InProgress,
            TaskStatus::Blocked | TaskStatus::Completed | TaskStatus::Cancelled
        ) | (
            TaskStatus::Blocked,
            TaskStatus::InProgress | TaskStatus::Cancelled
        )
    );
    if !allowed {
        return Err(TransitionError::Invalid {
            entity: "task",
            from: format!("{:?}", task.status),
            to: format!("{next:?}"),
        });
    }
    task.status = next;
    Ok(())
}

pub fn transition_run(run: &mut Run, next: RunStatus) -> Result<(), TransitionError> {
    let allowed = matches!(
        (&run.status, &next),
        (
            RunStatus::Pending,
            RunStatus::Running | RunStatus::Cancelled | RunStatus::NeedsAttention
        ) | (
            RunStatus::Running,
            RunStatus::Suspended
                | RunStatus::Completed
                | RunStatus::Failed
                | RunStatus::NeedsAttention
        ) | (
            RunStatus::Suspended,
            RunStatus::Running
                | RunStatus::Completed
                | RunStatus::Cancelled
                | RunStatus::NeedsAttention
        ) | (
            RunStatus::NeedsAttention,
            RunStatus::Running | RunStatus::Cancelled
        )
    );
    if !allowed {
        return Err(TransitionError::Invalid {
            entity: "run",
            from: format!("{:?}", run.status),
            to: format!("{next:?}"),
        });
    }
    run.status = next;
    Ok(())
}

pub fn transition_operation(
    operation: &mut Operation,
    next: OperationStatus,
) -> Result<(), TransitionError> {
    let allowed = matches!(
        (&operation.status, &next),
        (
            OperationStatus::Planned,
            OperationStatus::Authorized | OperationStatus::Failed
        ) | (
            OperationStatus::Authorized,
            OperationStatus::Started | OperationStatus::Failed
        ) | (
            OperationStatus::Started,
            OperationStatus::Succeeded | OperationStatus::Failed | OperationStatus::Unknown
        ) | (
            OperationStatus::Unknown,
            OperationStatus::Succeeded | OperationStatus::Failed | OperationStatus::Disputed
        ) | (
            OperationStatus::Succeeded,
            OperationStatus::Verified | OperationStatus::Disputed
        ) | (OperationStatus::Failed, OperationStatus::Disputed)
    );
    if !allowed {
        return Err(TransitionError::Invalid {
            entity: "operation",
            from: format!("{:?}", operation.status),
            to: format!("{next:?}"),
        });
    }
    operation.status = next;
    Ok(())
}
