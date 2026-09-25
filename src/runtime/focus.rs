use crate::core::{CurrentState, Focus, RunId};

pub fn focus_for_run(state: &CurrentState, run_id: RunId) -> Option<Focus> {
    let run = state.runs.get(&run_id)?;
    let task_id = run.task_id;
    let goal_id = task_id
        .and_then(|id| state.tasks.get(&id))
        .and_then(|task| task.goal_id);
    Some(Focus {
        goal_id,
        task_id,
        run_id: Some(run.id),
    })
}
