use std::sync::Arc;
use std::time::Duration;

use hekate::adapters::sqlite::SqliteStore;
use hekate::core::PrincipalId;
use hekate::ports::Storage;
use hekate::runtime::initiative::service::InitiativeService;
use hekate::runtime::initiative::worker::{
    InitiativeWorker, InitiativeWorkerConfig, InitiativeWorkerOutcome,
};

#[tokio::test]
async fn worker_yields_to_foreground_without_recording_or_executing_anything() {
    let store = Arc::new(SqliteStore::open("sqlite::memory:").await.expect("store"));
    assert!(store
        .acquire_foreground_lease("foreground", Duration::from_secs(30))
        .await
        .expect("foreground lease"));
    let service = InitiativeService::new(store.clone(), PrincipalId::new(), PrincipalId::new());
    let worker = InitiativeWorker::new(service, InitiativeWorkerConfig::default()).expect("worker");
    let before = store.state().await.expect("before state");
    let before_events = store.events().await.expect("before events");

    assert_eq!(
        worker.run_cycle().await,
        InitiativeWorkerOutcome::DeferredForeground
    );
    assert_eq!(store.state().await.expect("after state"), before);
    assert_eq!(store.events().await.expect("after events"), before_events);
    assert!(before.operations.is_empty());
    assert!(before.approvals.is_empty());
    assert!(before.receipts.is_empty());
}
