use std::collections::BTreeSet;

use uuid::Uuid;

use crate::core::{
    CurrentState, EventKind, ExperienceEvent, FocusCandidate, FocusOutcome, GoalStatus,
    Observation, PrincipalId, Run, RunStatus, Task, TaskStatus,
};

#[derive(Clone, Debug)]
pub(crate) struct TaskCandidate {
    pub record: FocusCandidate,
    pub keywords: BTreeSet<String>,
    pub same_thread: bool,
}

pub(crate) fn active_tasks(
    state: &CurrentState,
    events: &[ExperienceEvent],
    observation: &Observation,
    actor_id: PrincipalId,
) -> Vec<TaskCandidate> {
    state
        .tasks
        .values()
        .filter_map(|task| candidate(state, events, observation, actor_id, task))
        .filter(|candidate| {
            candidate.record.task_status == TaskStatus::InProgress
                && state
                    .goals
                    .get(&candidate.record.goal_id)
                    .is_some_and(|goal| matches!(goal.status, GoalStatus::Active))
        })
        .collect()
}

pub(crate) fn task_reference(
    state: &CurrentState,
    events: &[ExperienceEvent],
    observation: &Observation,
    actor_id: PrincipalId,
    task: &Task,
) -> Option<TaskCandidate> {
    candidate(state, events, observation, actor_id, task)
}

pub(crate) fn owned_goal(
    state: &CurrentState,
    actor_id: PrincipalId,
    goal_id: crate::core::GoalId,
) -> bool {
    state.goals.get(&goal_id).is_some_and(|goal| {
        goal.owner_principal_id == actor_id || goal.participants.contains(&actor_id)
    })
}

pub(crate) fn owned_run_task(
    state: &CurrentState,
    actor_id: PrincipalId,
    run: &Run,
) -> Option<Task> {
    let task = state.tasks.get(&run.task_id?)?.clone();
    owned_goal(state, actor_id, task.goal_id?).then_some(task)
}

pub(crate) fn active_run(state: &CurrentState, task_id: crate::core::TaskId) -> Option<&Run> {
    state
        .runs
        .values()
        .filter(|run| {
            run.task_id == Some(task_id)
                && matches!(run.status, RunStatus::Pending | RunStatus::Running)
        })
        .max_by(|left, right| left.started_at.cmp(&right.started_at))
}

pub(crate) fn task_keywords(candidate: &TaskCandidate) -> &BTreeSet<String> {
    &candidate.keywords
}

pub(crate) fn words(text: &str) -> BTreeSet<String> {
    text.split(|character: char| !character.is_alphanumeric())
        .map(str::to_lowercase)
        .filter(|word| word.chars().count() > 1 && !is_stop_word(word))
        .collect()
}

pub(crate) fn shared_word_count(left: &BTreeSet<String>, right: &BTreeSet<String>) -> usize {
    left.intersection(right).count()
}

pub(crate) fn has_distinctive_shared_word(
    left: &BTreeSet<String>,
    right: &BTreeSet<String>,
) -> bool {
    left.intersection(right)
        .any(|word| word.chars().count() >= 8)
}

fn candidate(
    state: &CurrentState,
    events: &[ExperienceEvent],
    observation: &Observation,
    actor_id: PrincipalId,
    task: &Task,
) -> Option<TaskCandidate> {
    let goal_id = task.goal_id?;
    let goal = state.goals.get(&goal_id)?;
    if goal.owner_principal_id != actor_id && !goal.participants.contains(&actor_id) {
        return None;
    }
    let mut evidence_event_ids = Vec::new();
    append_subject_event(
        events,
        EventKind::GoalCreated,
        goal_id.uuid(),
        &mut evidence_event_ids,
    );
    append_subject_event(
        events,
        EventKind::TaskCreated,
        task.id.uuid(),
        &mut evidence_event_ids,
    );

    let run = active_run(state, task.id).cloned();
    if let Some(run) = &run {
        append_subject_event(
            events,
            EventKind::RunStarted,
            run.id.uuid(),
            &mut evidence_event_ids,
        );
    }

    let same_thread = observation.thread_id.as_deref().is_some_and(|thread_id| {
        state.observations.values().any(|prior| {
            prior.id != observation.id
                && prior.thread_id.as_deref() == Some(thread_id)
                && state
                    .focus_resolutions
                    .get(&prior.id)
                    .is_some_and(|resolution| {
                        matches!(
                            resolution.outcome,
                            FocusOutcome::Continue | FocusOutcome::NewWork
                        ) && resolution.focus.task_id == Some(task.id)
                    })
        })
    });
    if same_thread {
        for prior in state.observations.values().filter(|prior| {
            prior.id != observation.id
                && prior.thread_id == observation.thread_id
                && state
                    .focus_resolutions
                    .get(&prior.id)
                    .is_some_and(|resolution| resolution.focus.task_id == Some(task.id))
        }) {
            append_observation_event(events, prior.id, &mut evidence_event_ids);
            append_correlation_event(
                events,
                EventKind::FocusResolved,
                prior.id.to_string().as_str(),
                &mut evidence_event_ids,
            );
        }
    }

    let keywords = words(&format!("{} {}", goal.title, task.title));
    Some(TaskCandidate {
        record: FocusCandidate {
            goal_id,
            goal_title: goal.title.clone(),
            task_id: task.id,
            task_title: task.title.clone(),
            task_status: task.status.clone(),
            run_id: run.map(|run| run.id),
            evidence_event_ids,
        },
        keywords,
        same_thread,
    })
}

fn append_subject_event(
    events: &[ExperienceEvent],
    kind: EventKind,
    id: Uuid,
    output: &mut Vec<crate::core::EventId>,
) {
    if let Some(event) = events.iter().find(|event| {
        event.event_kind == kind
            && event
                .subject
                .as_ref()
                .is_some_and(|subject| subject.id == id)
    }) {
        append_unique(output, event.event_id);
    }
}

fn append_observation_event(
    events: &[ExperienceEvent],
    observation_id: crate::core::ObservationId,
    output: &mut Vec<crate::core::EventId>,
) {
    if let Some(event) = events.iter().find(|event| {
        matches!(
            event.event_kind,
            EventKind::ObservationRecorded | EventKind::UserMessageReceived
        ) && event
            .subject
            .as_ref()
            .is_some_and(|subject| subject.id == observation_id.uuid())
    }) {
        append_unique(output, event.event_id);
    }
}

fn append_correlation_event(
    events: &[ExperienceEvent],
    kind: EventKind,
    correlation_id: &str,
    output: &mut Vec<crate::core::EventId>,
) {
    if let Some(event) = events.iter().find(|event| {
        event.event_kind == kind && event.correlation_id.as_deref() == Some(correlation_id)
    }) {
        append_unique(output, event.event_id);
    }
}

fn append_unique(output: &mut Vec<crate::core::EventId>, event_id: crate::core::EventId) {
    if !output.contains(&event_id) {
        output.push(event_id);
    }
}

fn is_stop_word(word: &str) -> bool {
    matches!(
        word,
        "a" | "an"
            | "and"
            | "are"
            | "back"
            | "can"
            | "continue"
            | "could"
            | "for"
            | "from"
            | "goal"
            | "i"
            | "in"
            | "is"
            | "it"
            | "keep"
            | "me"
            | "new"
            | "of"
            | "on"
            | "our"
            | "pick"
            | "previous"
            | "resume"
            | "run"
            | "task"
            | "the"
            | "this"
            | "to"
            | "up"
            | "we"
            | "with"
            | "작업"
            | "계속"
            | "하던"
            | "아까"
            | "이전"
            | "그"
            | "다시"
            | "이어"
    )
}
