use std::sync::Arc;

use hekate::adapters::sqlite::SqliteStore;
use hekate::core::{
    now, sha256_hex, ActionIntent, ActionIntentId, Approval, ApprovalId, ApprovalStatus, Artifact,
    ArtifactId, EntityKind, EntityRef, EventKind, EventSource, EvidenceRef, ExperienceEvent,
    Observation, ObservationId, Operation, OperationId, OperationStatus, PrincipalId, Task, TaskId,
    TaskStatus, VerificationDisposition,
};
use hekate::runtime::recovery::recover;
use hekate::runtime::{CompletionError, CompletionGate, CompletionGateResult, Projector};

fn database_url(label: &str) -> String {
    format!(
        "sqlite://{}",
        std::env::temp_dir()
            .join(format!(
                "hekate-completion-{label}-{}.db",
                uuid::Uuid::new_v4()
            ))
            .display()
    )
}

fn event<T: serde::Serialize>(
    actor_id: PrincipalId,
    event_kind: EventKind,
    entity_kind: EntityKind,
    entity_id: uuid::Uuid,
    payload: &T,
) -> ExperienceEvent {
    ExperienceEvent::new(
        actor_id,
        event_kind,
        Some(EntityRef::new(entity_kind, entity_id)),
        serde_json::to_value(payload).expect("payload"),
        EventSource::new("test", None),
        None,
        None,
        None,
    )
    .expect("event")
}

async fn task_and_source(store: &Arc<SqliteStore>) -> (PrincipalId, Task, ExperienceEvent, u64) {
    let actor = PrincipalId::new();
    let task = Task {
        id: TaskId::new(),
        goal_id: None,
        title: "completion test".to_owned(),
        status: TaskStatus::InProgress,
        created_at: now(),
    };
    let projector = Projector::new(store.clone());
    projector
        .record(event(
            actor,
            EventKind::TaskCreated,
            EntityKind::Task,
            task.id.uuid(),
            &task,
        ))
        .await
        .expect("task");
    let observation = Observation {
        id: ObservationId::new(),
        actor_id: actor,
        content: "source evidence".to_owned(),
        source_type: "test".to_owned(),
        source_ref: None,
        thread_id: None,
        message_id: None,
        received_at: now(),
    };
    let source_event = event(
        actor,
        EventKind::ObservationRecorded,
        EntityKind::Observation,
        observation.id.uuid(),
        &observation,
    );
    let state = projector
        .record(source_event.clone())
        .await
        .expect("source");
    (actor, task, source_event, state.revision)
}

#[tokio::test]
async fn claim_lifecycle_requires_provenance_and_replays() -> Result<(), Box<dyn std::error::Error>>
{
    let store = Arc::new(SqliteStore::open(&database_url("lifecycle")).await?);
    let (actor, task, source_event, source_sequence) = task_and_source(&store).await;
    let artifact = Artifact {
        id: ArtifactId::new(),
        path: "evidence.txt".to_owned(),
        content_hash: sha256_hex(b"artifact"),
        size: 8,
        media_type: "text/plain".to_owned(),
        provenance_event_id: source_event.event_id,
        created_at: now(),
    };
    Projector::new(store.clone())
        .record(event(
            actor,
            EventKind::ArtifactCreated,
            EntityKind::Artifact,
            artifact.id.uuid(),
            &artifact,
        ))
        .await
        .expect("artifact");

    let gate = CompletionGate::new(store.clone());
    let criterion = gate
        .define_criterion(actor, task.id, "  artifact exists  ", true)
        .await?;
    let claim = gate
        .create_claim(
            actor,
            task.id,
            criterion.id,
            100,
            vec![EvidenceRef {
                event_id: source_event.event_id,
                artifact_id: Some(artifact.id),
                source_hash: artifact.content_hash.clone(),
                as_of_sequence: source_sequence + 1,
            }],
            None,
            None,
        )
        .await?;
    assert_eq!(claim.disposition, VerificationDisposition::NeedsValidation);
    assert!(matches!(
        gate.evaluate_task_completion(task.id, store.state().await?.revision)
            .await?,
        CompletionGateResult::Blocked {
            needs_validation_claim_ids,
            ..
        } if needs_validation_claim_ids == vec![claim.id]
    ));

    let verified = gate
        .verify_claim(actor, claim.id, "human checked artifact")
        .await?;
    assert_eq!(verified.disposition, VerificationDisposition::Verified);
    let state = store.state().await?;
    assert!(matches!(
        gate.evaluate_task_completion(task.id, state.revision).await?,
        CompletionGateResult::Ready { verified_claim_ids } if verified_claim_ids == vec![claim.id]
    ));
    let events = store.events().await?;
    assert_eq!(state, Projector::replay(&events)?);
    assert!(recover(store.as_ref()).await?.projection_verified);
    Ok(())
}

#[tokio::test]
async fn structural_completion_blockers_prevent_verified_or_completed_state(
) -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(SqliteStore::open(&database_url("blockers")).await?);
    let (actor, task, source_event, _) = task_and_source(&store).await;
    let gate = CompletionGate::new(store.clone());
    let criterion = gate
        .define_criterion(actor, task.id, "a checked source", true)
        .await?;
    let current = store.state().await?;
    let valid_source = EvidenceRef {
        event_id: source_event.event_id,
        artifact_id: None,
        source_hash: store
            .events()
            .await?
            .iter()
            .find(|event| event.event_id == source_event.event_id)
            .expect("source event")
            .integrity_hash
            .clone(),
        as_of_sequence: current.revision,
    };
    let future = EvidenceRef {
        as_of_sequence: current.revision + 1,
        ..valid_source.clone()
    };
    assert!(matches!(
        gate.create_claim(actor, task.id, criterion.id, 100, vec![future], None, None)
            .await,
        Err(CompletionError::FutureEvidenceSequence { .. })
    ));

    let empty_claim = gate
        .create_claim(actor, task.id, criterion.id, 100, Vec::new(), None, None)
        .await?;
    assert!(matches!(
        gate.verify_claim(actor, empty_claim.id, "attempted verification")
            .await,
        Err(CompletionError::EvidenceRequired)
    ));
    assert!(matches!(
        gate.create_claim(actor, task.id, criterion.id, 100, Vec::new(), None, None)
            .await,
        Err(CompletionError::DuplicateFingerprint(_))
    ));

    let intent = ActionIntent {
        id: ActionIntentId::new(),
        capability: "test".to_owned(),
        operation: "write".to_owned(),
        target: "test".to_owned(),
        arguments: serde_json::json!({}),
        expected_effect: "write".to_owned(),
        preconditions: Vec::new(),
        proposed_by_event: None,
    };
    let operation = Operation {
        id: OperationId::new(),
        intent_id: intent.id,
        status: OperationStatus::Unknown,
        idempotency_key: "completion-test".to_owned(),
        approval_id: Some(ApprovalId::new()),
        started_at: None,
        finished_at: None,
    };
    let approval = Approval {
        id: operation.approval_id.expect("approval"),
        intent_id: intent.id,
        operation_id: operation.id,
        requested_by: actor,
        status: ApprovalStatus::Pending,
        reason: "test approval".to_owned(),
        resolved_by: None,
        resolved_at: None,
        created_at: now(),
    };
    let projector = Projector::new(store.clone());
    projector
        .record(event(
            actor,
            EventKind::ActionIntentCreated,
            EntityKind::ActionIntent,
            intent.id.uuid(),
            &intent,
        ))
        .await?;
    projector
        .record(event(
            actor,
            EventKind::OperationStateUnknown,
            EntityKind::Operation,
            operation.id.uuid(),
            &operation,
        ))
        .await?;
    projector
        .record(event(
            actor,
            EventKind::ApprovalRequested,
            EntityKind::Approval,
            approval.id.uuid(),
            &approval,
        ))
        .await?;
    let state = store.state().await?;
    let result = gate
        .evaluate_task_completion(task.id, state.revision)
        .await?;
    assert!(matches!(
        result,
        CompletionGateResult::Blocked {
            pending_approval_ids,
            unknown_operation_ids,
            ..
        } if pending_approval_ids == vec![approval.id] && unknown_operation_ids == vec![operation.id]
    ));
    assert_eq!(
        store.state().await?.tasks[&task.id].status,
        TaskStatus::InProgress
    );
    let _ = valid_source;
    Ok(())
}
