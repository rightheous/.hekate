use crate::core::{CurrentState, Focus, Observation, RunStatus};

pub fn resolve_focus(state: &CurrentState, observation: &Observation) -> Focus {
    let content = observation.content.trim().to_ascii_lowercase();
    if content.starts_with("new task:")
        || content.starts_with("new run:")
        || content.starts_with("새 작업:")
        || content.starts_with("새 실행:")
    {
        return Focus::unattached();
    }

    state
        .runs
        .values()
        .filter(|run| {
            matches!(
                run.status,
                RunStatus::Pending | RunStatus::Running | RunStatus::Suspended
            )
        })
        .max_by_key(|run| run.started_at.clone())
        .map(|run| {
            let task_id = run.task_id;
            let goal_id = task_id
                .and_then(|id| state.tasks.get(&id))
                .and_then(|task| task.goal_id);
            Focus {
                goal_id,
                task_id,
                run_id: Some(run.id),
            }
        })
        .unwrap_or_else(Focus::unattached)
}
