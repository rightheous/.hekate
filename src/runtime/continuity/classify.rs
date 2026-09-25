use uuid::Uuid;

use crate::core::{
    now, CurrentState, ExperienceEvent, Focus, FocusBasis, FocusCandidate, FocusOutcome,
    FocusResolution, Goal, GoalId, GoalStatus, Observation, PrincipalId, Run, RunId, RunStatus,
    Task, TaskId, TaskStatus,
};

use super::candidates::{
    active_run, active_tasks, has_distinctive_shared_word, owned_goal, owned_run_task,
    shared_word_count, task_keywords, task_reference, words, TaskCandidate,
};

pub(crate) struct FocusPlan {
    pub resolution: FocusResolution,
    pub goal: Option<Goal>,
    pub task: Option<Task>,
    pub run: Option<Run>,
}

#[derive(Clone, Copy)]
enum WorkReference {
    Goal(GoalId),
    Task(TaskId),
    Run(RunId),
}

enum TextMatch {
    None,
    Unique(usize),
    Uncertain(Vec<usize>),
}

pub(crate) fn resolve(
    state: &CurrentState,
    events: &[ExperienceEvent],
    observation: &Observation,
    user_id: PrincipalId,
    hekate_id: PrincipalId,
) -> FocusPlan {
    let candidates = active_tasks(state, events, observation, user_id);
    if has_new_work_intent(&observation.content) {
        return new_work(state, observation, user_id, hekate_id, candidates);
    }
    if let Some(reference) = parse_reference(&observation.content) {
        return resolve_reference(state, events, observation, user_id, candidates, reference);
    }
    if is_social_message(&observation.content) {
        return conversation(state, observation, candidates);
    }
    if has_continuation_intent(&observation.content) {
        return choose_from_pool(
            state,
            observation,
            candidates.clone(),
            FocusBasis::ExplicitContinuation,
            true,
        );
    }

    match match_text(&observation.content, &candidates) {
        TextMatch::Unique(index) => continue_task(
            state,
            observation,
            &candidates[index],
            FocusBasis::StrongTextMatch,
            None,
        ),
        TextMatch::Uncertain(indices) => clarification(
            state,
            observation,
            FocusBasis::AmbiguousCandidates,
            indices
                .into_iter()
                .map(|index| candidates[index].clone())
                .collect(),
            None,
        ),
        TextMatch::None => conversation(state, observation, candidates),
    }
}

fn resolve_reference(
    state: &CurrentState,
    events: &[ExperienceEvent],
    observation: &Observation,
    user_id: PrincipalId,
    candidates: Vec<TaskCandidate>,
    reference: WorkReference,
) -> FocusPlan {
    match reference {
        WorkReference::Task(task_id) => {
            let Some(task) = state.tasks.get(&task_id) else {
                return clarification(
                    state,
                    observation,
                    FocusBasis::InvalidReference,
                    Vec::new(),
                    Some(
                        "I can't verify that task ID. Which active task should I continue?"
                            .to_owned(),
                    ),
                );
            };
            let Some(candidate) = task_reference(state, events, observation, user_id, task) else {
                return clarification(
                    state,
                    observation,
                    FocusBasis::InvalidReference,
                    Vec::new(),
                    Some("I can't verify that task belongs to you. Which active task should I continue?".to_owned()),
                );
            };
            match task.status {
                TaskStatus::InProgress
                    if state
                        .goals
                        .get(&candidate.record.goal_id)
                        .is_some_and(|goal| matches!(goal.status, GoalStatus::Active)) =>
                {
                    continue_task(
                        state,
                        observation,
                        &candidate,
                        FocusBasis::ExplicitTask,
                        None,
                    )
                }
                TaskStatus::Completed | TaskStatus::Cancelled => clarification(
                    state,
                    observation,
                    FocusBasis::ClosedWorkReference,
                    vec![candidate],
                    Some(format!(
                        "Task '{}' is {:?}. Should it be reopened, or should I start a new task?",
                        task.title, task.status
                    )),
                ),
                _ => clarification(
                    state,
                    observation,
                    FocusBasis::InvalidReference,
                    vec![candidate],
                    Some(format!(
                        "Task '{}' is not active yet. Should I start it, or choose an active task?",
                        task.title
                    )),
                ),
            }
        }
        WorkReference::Goal(goal_id) => {
            if !owned_goal(state, user_id, goal_id)
                || !state
                    .goals
                    .get(&goal_id)
                    .is_some_and(|goal| matches!(goal.status, GoalStatus::Active))
            {
                return clarification(
                    state,
                    observation,
                    FocusBasis::InvalidReference,
                    Vec::new(),
                    Some(
                        "I can't verify an active goal with that ID. Which task should I continue?"
                            .to_owned(),
                    ),
                );
            }
            let goal_candidates = candidates
                .into_iter()
                .filter(|candidate| candidate.record.goal_id == goal_id)
                .collect::<Vec<_>>();
            if goal_candidates.is_empty() {
                return clarification(
                    state,
                    observation,
                    FocusBasis::ClosedWorkReference,
                    Vec::new(),
                    Some("That goal has no active task. Which task should I continue, or should I start a new one?".to_owned()),
                );
            }
            choose_from_pool(
                state,
                observation,
                goal_candidates,
                FocusBasis::ExplicitGoal,
                false,
            )
        }
        WorkReference::Run(run_id) => {
            let Some(run) = state.runs.get(&run_id) else {
                return clarification(
                    state,
                    observation,
                    FocusBasis::InvalidReference,
                    Vec::new(),
                    Some(
                        "I can't verify that run ID. Which active task should I continue?"
                            .to_owned(),
                    ),
                );
            };
            let Some(task) = owned_run_task(state, user_id, run) else {
                return clarification(
                    state,
                    observation,
                    FocusBasis::InvalidReference,
                    Vec::new(),
                    Some("That run is not attached to an active task I can continue. Which task did you mean?".to_owned()),
                );
            };
            let Some(candidate) = candidates
                .iter()
                .find(|candidate| candidate.record.task_id == task.id)
                .cloned()
            else {
                let candidate = task_reference(state, events, observation, user_id, &task);
                return clarification(
                    state,
                    observation,
                    FocusBasis::ClosedWorkReference,
                    candidate.into_iter().collect(),
                    Some("That run or its task is not active. Should I reopen the work, or choose another task?".to_owned()),
                );
            };
            if !matches!(run.status, RunStatus::Pending | RunStatus::Running) {
                return clarification(
                    state,
                    observation,
                    FocusBasis::ClosedWorkReference,
                    vec![candidate],
                    Some(format!(
                        "Run {run_id} is {:?}. Should I start a new run for task '{}'?",
                        run.status, task.title
                    )),
                );
            }
            continue_task(
                state,
                observation,
                &candidate,
                FocusBasis::ExplicitRun,
                Some(run.clone()),
            )
        }
    }
}

fn choose_from_pool(
    state: &CurrentState,
    observation: &Observation,
    candidates: Vec<TaskCandidate>,
    single_candidate_basis: FocusBasis,
    continuation_request: bool,
) -> FocusPlan {
    if candidates.is_empty() {
        return clarification(
            state,
            observation,
            FocusBasis::ClosedWorkReference,
            Vec::new(),
            Some("I couldn't identify an active task to continue. Name its task ID or say 'new task: ...'.".to_owned()),
        );
    }
    let same_thread = candidates
        .iter()
        .enumerate()
        .filter(|(_, candidate)| candidate.same_thread)
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    if same_thread.len() == 1 {
        return continue_task(
            state,
            observation,
            &candidates[same_thread[0]],
            FocusBasis::SameThreadContinuation,
            None,
        );
    }
    match match_text(&observation.content, &candidates) {
        TextMatch::Unique(index) => continue_task(
            state,
            observation,
            &candidates[index],
            if continuation_request {
                FocusBasis::ExplicitContinuation
            } else {
                single_candidate_basis
            },
            None,
        ),
        TextMatch::Uncertain(indices) if candidates.len() > 1 => clarification(
            state,
            observation,
            FocusBasis::AmbiguousCandidates,
            indices
                .into_iter()
                .map(|index| candidates[index].clone())
                .collect(),
            None,
        ),
        _ if candidates.len() == 1 => continue_task(
            state,
            observation,
            &candidates[0],
            if continuation_request {
                FocusBasis::ExplicitContinuation
            } else {
                single_candidate_basis
            },
            None,
        ),
        _ => clarification(
            state,
            observation,
            FocusBasis::AmbiguousCandidates,
            candidates,
            None,
        ),
    }
}

fn continue_task(
    state: &CurrentState,
    observation: &Observation,
    candidate: &TaskCandidate,
    basis: FocusBasis,
    forced_run: Option<Run>,
) -> FocusPlan {
    let run = forced_run.or_else(|| active_run(state, candidate.record.task_id).cloned());
    let (run_id, new_run) = match run {
        Some(run) => (run.id, None),
        None => {
            let run = Run {
                id: RunId::new(),
                task_id: Some(candidate.record.task_id),
                status: RunStatus::Running,
                started_at: now(),
                completed_at: None,
            };
            (run.id, Some(run))
        }
    };
    let focus = Focus {
        goal_id: Some(candidate.record.goal_id),
        task_id: Some(candidate.record.task_id),
        run_id: Some(run_id),
    };
    FocusPlan {
        resolution: FocusResolution {
            observation_id: observation.id,
            outcome: FocusOutcome::Continue,
            focus,
            as_of_revision: state.revision,
            basis,
            candidates: vec![candidate.record.clone()],
            clarification: None,
        },
        goal: None,
        task: None,
        run: new_run,
    }
}

fn new_work(
    state: &CurrentState,
    observation: &Observation,
    user_id: PrincipalId,
    hekate_id: PrincipalId,
    candidates: Vec<TaskCandidate>,
) -> FocusPlan {
    let title = task_title(&observation.content);
    let goal = Goal {
        id: GoalId::new(),
        owner_principal_id: user_id,
        participants: vec![user_id, hekate_id],
        title: title.clone(),
        description: observation.content.clone(),
        status: GoalStatus::Active,
        created_at: now(),
    };
    let task = Task {
        id: TaskId::new(),
        goal_id: Some(goal.id),
        title,
        status: TaskStatus::InProgress,
        created_at: now(),
    };
    let run = Run {
        id: RunId::new(),
        task_id: Some(task.id),
        status: RunStatus::Running,
        started_at: now(),
        completed_at: None,
    };
    FocusPlan {
        resolution: FocusResolution {
            observation_id: observation.id,
            outcome: FocusOutcome::NewWork,
            focus: Focus {
                goal_id: Some(goal.id),
                task_id: Some(task.id),
                run_id: Some(run.id),
            },
            as_of_revision: state.revision,
            basis: FocusBasis::ExplicitNewWork,
            candidates: records(&candidates),
            clarification: None,
        },
        goal: Some(goal),
        task: Some(task),
        run: Some(run),
    }
}

fn conversation(
    state: &CurrentState,
    observation: &Observation,
    candidates: Vec<TaskCandidate>,
) -> FocusPlan {
    let run = Run {
        id: RunId::new(),
        task_id: None,
        status: RunStatus::Running,
        started_at: now(),
        completed_at: None,
    };
    FocusPlan {
        resolution: FocusResolution {
            observation_id: observation.id,
            outcome: FocusOutcome::Conversation,
            focus: Focus {
                goal_id: None,
                task_id: None,
                run_id: Some(run.id),
            },
            as_of_revision: state.revision,
            basis: FocusBasis::GeneralConversation,
            candidates: records(&candidates),
            clarification: None,
        },
        goal: None,
        task: None,
        run: Some(run),
    }
}

fn clarification(
    state: &CurrentState,
    observation: &Observation,
    basis: FocusBasis,
    candidates: Vec<TaskCandidate>,
    question: Option<String>,
) -> FocusPlan {
    let candidates = records(&candidates);
    let clarification = question.or_else(|| Some(candidate_question(&candidates)));
    FocusPlan {
        resolution: FocusResolution {
            observation_id: observation.id,
            outcome: FocusOutcome::Clarification,
            focus: Focus::unattached(),
            as_of_revision: state.revision,
            basis,
            candidates,
            clarification,
        },
        goal: None,
        task: None,
        run: None,
    }
}

fn records(candidates: &[TaskCandidate]) -> Vec<FocusCandidate> {
    candidates
        .iter()
        .map(|candidate| candidate.record.clone())
        .collect()
}

fn candidate_question(candidates: &[FocusCandidate]) -> String {
    if candidates.is_empty() {
        return "Which task do you want to continue? Give its task ID or say 'new task: ...'."
            .to_owned();
    }
    let choices = candidates
        .iter()
        .take(5)
        .map(|candidate| format!("'{}' ({})", candidate.task_title, candidate.task_id))
        .collect::<Vec<_>>()
        .join(", ");
    format!("Which task should I continue: {choices}? Reply with a task ID or say 'new task: ...'.")
}

fn match_text(text: &str, candidates: &[TaskCandidate]) -> TextMatch {
    let query = words(text);
    if query.is_empty() {
        return TextMatch::None;
    }
    let scores = candidates
        .iter()
        .map(|candidate| {
            let keywords = task_keywords(candidate);
            (
                shared_word_count(&query, keywords),
                has_distinctive_shared_word(&query, keywords),
            )
        })
        .collect::<Vec<_>>();
    let best_score = scores.iter().map(|(score, _)| *score).max().unwrap_or(0);
    if best_score == 0 {
        return TextMatch::None;
    }
    let matching = scores
        .iter()
        .enumerate()
        .filter_map(|(index, (score, _))| (*score == best_score).then_some(index))
        .collect::<Vec<_>>();
    let strong = best_score >= 2 || matching.iter().any(|index| scores[*index].1);
    if strong && matching.len() == 1 {
        TextMatch::Unique(matching[0])
    } else {
        TextMatch::Uncertain(matching)
    }
}

fn has_new_work_intent(text: &str) -> bool {
    let text = text.trim().to_lowercase();
    [
        "new task:",
        "new goal:",
        "new run:",
        "start a new task",
        "create a new task",
        "start a new project",
        "새 작업:",
        "새 목표:",
        "새 실행:",
        "새 프로젝트:",
        "새 작업 시작",
        "새로 시작:",
        "다른 작업 시작",
    ]
    .iter()
    .any(|marker| text.starts_with(marker))
}

fn has_continuation_intent(text: &str) -> bool {
    let text = text.to_lowercase();
    [
        "continue",
        "resume",
        "pick up",
        "carry on",
        "back to",
        "return to",
        "keep working",
        "아까 하던",
        "이전 작업",
        "지난 작업",
        "그 작업 계속",
        "계속하",
        "이어가",
        "이어서",
        "재개",
    ]
    .iter()
    .any(|marker| text.contains(marker))
}

fn is_social_message(text: &str) -> bool {
    let text = text
        .trim()
        .trim_matches(|character: char| !character.is_alphanumeric())
        .to_lowercase();
    matches!(
        text.as_str(),
        "hi" | "hello" | "hey" | "good morning" | "good evening" | "안녕" | "안녕하세요"
    )
}

fn parse_reference(text: &str) -> Option<WorkReference> {
    let tokens = text
        .split(|character: char| !character.is_alphanumeric() && character != '-')
        .filter(|token| !token.is_empty())
        .collect::<Vec<_>>();
    tokens.windows(2).find_map(|pair| {
        let id = Uuid::parse_str(pair[1]).ok()?;
        match pair[0].to_lowercase().as_str() {
            "task" | "작업" | "태스크" => Some(WorkReference::Task(TaskId::from_uuid(id))),
            "goal" | "목표" => Some(WorkReference::Goal(GoalId::from_uuid(id))),
            "run" | "실행" => Some(WorkReference::Run(RunId::from_uuid(id))),
            _ => None,
        }
    })
}

fn task_title(text: &str) -> String {
    let lower = text.to_lowercase();
    let title = [
        "new task:",
        "new goal:",
        "new run:",
        "새 작업:",
        "새 목표:",
        "새 실행:",
        "새 프로젝트:",
        "새로 시작:",
    ]
    .iter()
    .find_map(|prefix| {
        lower
            .strip_prefix(prefix)
            .map(|_| text[prefix.len()..].trim())
    })
    .filter(|title| !title.is_empty())
    .unwrap_or_else(|| text.trim());
    let title = title.chars().take(80).collect::<String>();
    if title.is_empty() {
        "Untitled task".to_owned()
    } else {
        title
    }
}
