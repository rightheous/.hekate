use async_trait::async_trait;
use hekate::{
    adapters::{local_policy::LocalPolicy, primary_model::PrimaryModel, sqlite::SqliteStore},
    config::Config,
    core::*,
    ports::*,
    runtime::{engine::Engine, projector::Projector, recovery::recover},
};
use std::sync::Arc;

struct Disconnected;
#[async_trait]
impl CapabilityCatalog for Disconnected {
    fn names(&self) -> Vec<String> {
        vec!["workspace_read".into()]
    }
    async fn execute(
        &self,
        _: &str,
        _: serde_json::Value,
    ) -> Result<CapabilityResult, CapabilityError> {
        Err(CapabilityError::OutcomeUnknown(
            "CDP disconnected after dispatch".into(),
        ))
    }
}

#[tokio::test]
async fn uncertain_execution_is_recorded_and_cannot_be_retried(
) -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(SqliteStore::open("sqlite::memory:").await?);
    let config = Config::default();
    let engine = Engine::new(
        store.clone(),
        Arc::new(PrimaryModel::from_config(&config)?),
        Arc::new(LocalPolicy),
        Arc::new(Disconnected),
        config.hekate_principal_id,
        config.user_principal_id,
    );
    let intent = ActionIntent {
        id: ActionIntentId::new(),
        capability: "workspace_read".into(),
        operation: "read_text".into(),
        target: "fixture".into(),
        arguments: serde_json::json!({}),
        expected_effect: "observe".into(),
        preconditions: vec![],
        proposed_by_event: None,
    };
    let operation = Operation {
        id: OperationId::new(),
        intent_id: intent.id,
        status: OperationStatus::Planned,
        idempotency_key: "uncertain-fixture".into(),
        approval_id: None,
        started_at: None,
        finished_at: None,
    };
    let projector = Projector::new(store.clone());
    for (kind, payload) in [
        (
            EventKind::ActionIntentCreated,
            serde_json::to_value(intent)?,
        ),
        (
            EventKind::OperationPlanned,
            serde_json::to_value(&operation)?,
        ),
    ] {
        projector
            .record(ExperienceEvent::new(
                config.hekate_principal_id,
                kind,
                None,
                payload,
                EventSource::new("test", None),
                None,
                None,
                None,
            )?)
            .await?;
    }
    let receipt = engine.execute_operation(operation.id).await?;
    assert_eq!(receipt.status, OperationStatus::Unknown);
    assert_eq!(
        store.state().await?.operations[&operation.id].status,
        OperationStatus::Unknown
    );
    assert!(store.state().await?.verifications.is_empty());
    assert!(engine.execute_operation(operation.id).await.is_err());
    assert_eq!(
        recover(store.as_ref()).await?.unknown_operations,
        vec![operation.id]
    );
    engine.shutdown().await?;
    Ok(())
}
