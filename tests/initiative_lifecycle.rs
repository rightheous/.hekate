use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use hekate::adapters::sqlite::SqliteStore;
use hekate::core::event::{EntityKind, EntityRef, EventKind, EventSource, ExperienceEvent};
use hekate::core::{
    initiative_fingerprint, CognitiveTrace, Conflict, ConflictId, ConflictStatus, CurrentState,
    InitiativeStatus, Observation, ObservationId, Position, PositionId, PositionStatus, Principal,
    PrincipalId, PrincipalKind, Stance,
};
use hekate::ports::{Storage, StorageError};
use hekate::runtime::initiative::service::{
    InitiativeError, InitiativeRunResult, InitiativeService,
};
use hekate::runtime::projector::Projector;
use hekate::runtime::recovery::recover;
use uuid::Uuid;

fn database_url() -> String {
    format!(
        "sqlite://{}",
        std::env::temp_dir()
            .join(format!("hekate-initiative-{}.db", Uuid::new_v4()))
            .display()
    )
}

fn event(
    actor_id: PrincipalId,
    kind: EventKind,
    entity_kind: EntityKind,
    entity_id: Uuid,
    payload: serde_json::Value,
) -> ExperienceEvent {
    ExperienceEvent::new(
        actor_id,
        kind,
        Some(EntityRef::new(entity_kind, entity_id)),
        payload,
        EventSource::new("initiative_test", None),
        None,
        None,
        None,
    )
    .expect("event")
}

async fn seed_agenda_conflict(
    store: &Arc<SqliteStore>,
) -> (PrincipalId, PrincipalId, Conflict, hekate::core::EventId) {
    let hekate = PrincipalId::new();
    let user = PrincipalId::new();
    for principal in [
        Principal {
            id: hekate,
            kind: PrincipalKind::Hekate,
            name: "HEKATE".to_owned(),
            identity_version_id: None,
        },
        Principal {
            id: user,
            kind: PrincipalKind::User,
            name: "User".to_owned(),
            identity_version_id: None,
        },
    ] {
        Projector::new(store.clone())
            .record(event(
                hekate,
                EventKind::PrincipalCreated,
                EntityKind::Principal,
                principal.id.uuid(),
                serde_json::to_value(principal).expect("principal payload"),
            ))
            .await
            .expect("seed principal");
    }
    let observation = Observation {
        id: ObservationId::new(),
        actor_id: user,
        content: "The migration has an unresolved assumption".to_owned(),
        source_type: "test".to_owned(),
        source_ref: None,
        thread_id: None,
        message_id: None,
        received_at: hekate::core::model::now(),
    };
    let source = event(
        user,
        EventKind::ObservationRecorded,
        EntityKind::Observation,
        observation.id.uuid(),
        serde_json::to_value(observation).expect("observation payload"),
    );
    let source_event_id = source.event_id;
    Projector::new(store.clone())
        .record(source)
        .await
        .expect("seed observation");
    let hekate_position = Position {
        id: PositionId::new(),
        principal_id: hekate,
        subject: "workspace retention".to_owned(),
        stance: Stance::Support,
        version: 1,
        status: PositionStatus::Active,
        confidence: 80,
        supersedes: None,
        reasons: vec!["the migration evidence is unresolved".to_owned()],
        evidence_refs: vec![source_event_id],
        reconsideration_conditions: vec!["new retention evidence".to_owned()],
        created_at: "2026-01-01T00:00:00Z".to_owned(),
    };
    let user_position = Position {
        id: PositionId::new(),
        principal_id: user,
        stance: Stance::Oppose,
        ..hekate_position.clone()
    };
    for (actor, position) in [(hekate, &hekate_position), (user, &user_position)] {
        Projector::new(store.clone())
            .record(event(
                actor,
                EventKind::PositionEstablished,
                EntityKind::Position,
                position.id.uuid(),
                serde_json::to_value(position).expect("position payload"),
            ))
            .await
            .expect("seed position");
    }
    let conflict = Conflict {
        id: ConflictId::new(),
        subject: "workspace retention".to_owned(),
        participant_positions: vec![hekate_position.id, user_position.id],
        status: ConflictStatus::Open,
        revision: 1,
        reasons: vec!["the participants disagree on retention".to_owned()],
        evidence_refs: vec![source_event_id],
        alternatives: Vec::new(),
        reconsideration_conditions: vec!["new retention evidence".to_owned()],
        unresolved_questions: vec!["Should HEKATE retain the current workspace?".to_owned()],
        resolution: None,
        resolved_at: None,
        created_at: "2026-01-01T00:00:00Z".to_owned(),
    };
    Projector::new(store.clone())
        .record(event(
            hekate,
            EventKind::ConflictOpened,
            EntityKind::Conflict,
            conflict.id.uuid(),
            serde_json::to_value(&conflict).expect("conflict payload"),
        ))
        .await
        .expect("seed conflict");
    (hekate, user, conflict, source_event_id)
}

struct RevisionBumpingStorage {
    inner: Arc<SqliteStore>,
    user_id: PrincipalId,
}

#[async_trait]
impl Storage for RevisionBumpingStorage {
    async fn load_state(&self) -> Result<CurrentState, StorageError> {
        self.inner.load_state().await
    }

    async fn load_events(&self) -> Result<Vec<ExperienceEvent>, StorageError> {
        self.inner.load_events().await
    }

    async fn commit_batch(
        &self,
        events: &[ExperienceEvent],
        state: &CurrentState,
        expected_base_revision: Option<u64>,
        cognitive_trace: Option<&CognitiveTrace>,
    ) -> Result<(), StorageError> {
        if events
            .first()
            .is_some_and(|event| event.event_kind == EventKind::InitiativeProposed)
        {
            let unrelated = Observation {
                id: ObservationId::new(),
                actor_id: self.user_id,
                content: "Concurrent observation advances the ledger revision".to_owned(),
                source_type: "revision_race_test".to_owned(),
                source_ref: None,
                thread_id: None,
                message_id: None,
                received_at: hekate::core::model::now(),
            };
            Projector::new(self.inner.clone())
                .record(event(
                    self.user_id,
                    EventKind::ObservationRecorded,
                    EntityKind::Observation,
                    unrelated.id.uuid(),
                    serde_json::to_value(unrelated)
                        .map_err(|error| StorageError::Backend(error.to_string()))?,
                ))
                .await
                .map_err(|error| StorageError::Backend(error.to_string()))?;
        }
        self.inner
            .commit_batch(events, state, expected_base_revision, cognitive_trace)
            .await
    }

    async fn record_cognitive_trace(&self, trace: &CognitiveTrace) -> Result<(), StorageError> {
        self.inner.record_cognitive_trace(trace).await
    }

    async fn acquire_foreground_lease(
        &self,
        owner: &str,
        ttl: Duration,
    ) -> Result<bool, StorageError> {
        self.inner.acquire_foreground_lease(owner, ttl).await
    }

    async fn renew_foreground_lease(
        &self,
        owner: &str,
        ttl: Duration,
    ) -> Result<bool, StorageError> {
        self.inner.renew_foreground_lease(owner, ttl).await
    }

    async fn release_foreground_lease(&self, owner: &str) -> Result<(), StorageError> {
        self.inner.release_foreground_lease(owner).await
    }

    async fn has_active_foreground_lease(&self) -> Result<bool, StorageError> {
        self.inner.has_active_foreground_lease().await
    }
}

#[test]
fn old_projection_snapshots_default_to_no_initiatives() {
    let mut snapshot = serde_json::to_value(hekate::core::CurrentState::default()).expect("state");
    snapshot
        .as_object_mut()
        .expect("state object")
        .remove("initiatives");
    let restored: hekate::core::CurrentState =
        serde_json::from_value(snapshot).expect("legacy snapshot");
    assert!(restored.initiatives.is_empty());
}

#[tokio::test]
async fn proposal_lifecycle_revalidates_deduplicates_dismisses_and_replays() {
    let url = database_url();
    let store = Arc::new(SqliteStore::open(&url).await.expect("store"));
    let (hekate, user, _, source_event_id) = seed_agenda_conflict(&store).await;
    let service = InitiativeService::new(store.clone(), hekate, user);
    let state = store.state().await.expect("state");
    let events = store.events().await.expect("seed events");
    let valid = hekate::runtime::initiative::agenda::select(&state, &events, hekate, user)
        .into_iter()
        .next()
        .expect("selected candidate");
    let mut stale = valid.clone();
    stale.as_of_revision = state.revision - 1;
    assert_eq!(
        service
            .propose_candidate(stale)
            .await
            .expect("stale result"),
        InitiativeRunResult::Deferred
    );
    let mut unrelated_evidence = valid.clone();
    unrelated_evidence.source_event_ids = vec![events[0].event_id];
    unrelated_evidence.fingerprint = initiative_fingerprint(
        unrelated_evidence.kind,
        &unrelated_evidence.source_entity,
        unrelated_evidence.source_version,
        unrelated_evidence.target_principal_id,
        &unrelated_evidence.source_event_ids,
    );
    assert!(matches!(
        service.propose_candidate(unrelated_evidence).await,
        Err(InitiativeError::InvalidCandidate(_))
    ));
    let mut wrong_version = valid.clone();
    wrong_version.source_version += 1;
    wrong_version.fingerprint = initiative_fingerprint(
        wrong_version.kind,
        &wrong_version.source_entity,
        wrong_version.source_version,
        wrong_version.target_principal_id,
        &wrong_version.source_event_ids,
    );
    assert!(matches!(
        service.propose_candidate(wrong_version).await,
        Err(InitiativeError::InvalidCandidate(_))
    ));
    assert_eq!(store.events().await.expect("events").len(), events.len());
    let proposal = match service
        .propose_candidate(valid.clone())
        .await
        .expect("proposal")
    {
        InitiativeRunResult::Proposed { proposal } => proposal,
        other => panic!("unexpected result: {other:?}"),
    };
    assert_eq!(proposal.status, InitiativeStatus::Ready);
    let proposal_events = store.events().await.expect("proposal events");
    assert_eq!(
        proposal_events[events.len()].event_kind,
        EventKind::InitiativeProposed
    );
    assert_eq!(
        proposal_events[events.len() + 1].event_kind,
        EventKind::InitiativeReadied
    );
    let projected = store.state().await.expect("projected state");
    assert_eq!(projected.initiatives.get(&proposal.id), Some(&proposal));
    let before_display = store.events().await.expect("events before display");
    assert_eq!(
        service.ready_for_display().await.expect("local display"),
        Some(proposal.clone())
    );
    let other_user = InitiativeService::new(store.clone(), hekate, PrincipalId::new());
    assert!(other_user
        .ready_for_display()
        .await
        .expect("other target display")
        .is_none());
    assert_eq!(
        store.events().await.expect("events after display"),
        before_display,
        "display must remain a local read"
    );
    assert_eq!(
        Projector::replay(&store.events().await.expect("events")).expect("replay"),
        projected
    );

    drop(service);
    drop(store);
    let restarted_store = Arc::new(SqliteStore::open(&url).await.expect("restart"));
    let restarted = InitiativeService::new(restarted_store.clone(), hekate, user);
    assert_eq!(
        restarted
            .propose_candidate(valid.clone())
            .await
            .expect("dedupe after restart"),
        InitiativeRunResult::Duplicate {
            fingerprint: proposal.fingerprint.clone()
        }
    );
    assert_eq!(
        restarted.dismiss(proposal.id).await.expect("dismiss"),
        InitiativeRunResult::Dismissed {
            proposal: hekate::core::InitiativeProposal {
                status: InitiativeStatus::Dismissed,
                ..proposal.clone()
            }
        }
    );
    assert!(restarted
        .ready_for_display()
        .await
        .expect("ready display")
        .is_none());
    assert_eq!(
        restarted
            .propose_candidate(valid.clone())
            .await
            .expect("dismissed fingerprint stays deduped"),
        InitiativeRunResult::Duplicate {
            fingerprint: proposal.fingerprint
        }
    );
    let replayed = Projector::replay(&restarted_store.events().await.expect("events"))
        .expect("replay after dismiss");
    assert_eq!(
        replayed,
        restarted_store.state().await.expect("state after dismiss")
    );
    assert_eq!(
        replayed
            .initiatives
            .get(&proposal.id)
            .expect("initiative")
            .status,
        InitiativeStatus::Dismissed
    );
    assert_eq!(
        restarted_store
            .events()
            .await
            .expect("dismiss event")
            .last()
            .expect("last event")
            .event_kind,
        EventKind::InitiativeDismissed
    );
    assert!(
        recover(restarted_store.as_ref())
            .await
            .expect("recovery verification")
            .projection_verified
    );
    let pool = sqlx::SqlitePool::connect(&url)
        .await
        .expect("integrity pool");
    sqlx::query("UPDATE events SET integrity_hash = 'invalid' WHERE event_id = ?")
        .bind(source_event_id.to_string())
        .execute(&pool)
        .await
        .expect("corrupt source event hash");
    assert!(matches!(
        restarted.run_once().await,
        Err(InitiativeError::Storage(_))
    ));
    pool.close().await;
}

#[tokio::test]
async fn revision_race_defers_without_partially_recording_a_proposal() {
    let inner = Arc::new(SqliteStore::open("sqlite::memory:").await.expect("store"));
    let (hekate, user, _, _) = seed_agenda_conflict(&inner).await;
    let racing_storage = Arc::new(RevisionBumpingStorage {
        inner: inner.clone(),
        user_id: user,
    });
    let service = InitiativeService::new(racing_storage, hekate, user);

    assert_eq!(
        service.run_once().await.expect("stale write result"),
        InitiativeRunResult::Deferred
    );
    let events = inner.events().await.expect("events after race");
    let projected = inner.state().await.expect("state after race");
    assert_eq!(events.len(), 7, "only the competing event may be committed");
    assert!(projected.initiatives.is_empty());
    assert!(!events.iter().any(|event| {
        matches!(
            event.event_kind,
            EventKind::InitiativeProposed | EventKind::InitiativeReadied
        )
    }));
    assert_eq!(Projector::replay(&events).expect("replay"), projected);
}

#[tokio::test]
async fn run_once_skips_a_duplicate_candidate_and_reaches_the_next_one() {
    let store = Arc::new(SqliteStore::open("sqlite::memory:").await.expect("store"));
    let (hekate, user, mut second_conflict, _) = seed_agenda_conflict(&store).await;
    second_conflict.id = ConflictId::new();
    second_conflict.unresolved_questions =
        vec!["What evidence would change the retention choice?".to_owned()];
    Projector::new(store.clone())
        .record(event(
            hekate,
            EventKind::ConflictOpened,
            EntityKind::Conflict,
            second_conflict.id.uuid(),
            serde_json::to_value(second_conflict).expect("second conflict payload"),
        ))
        .await
        .expect("seed second conflict");
    let service = InitiativeService::new(store.clone(), hekate, user);

    let first = service.run_once().await.expect("first candidate");
    let second = service.run_once().await.expect("next candidate");
    assert!(matches!(first, InitiativeRunResult::Proposed { .. }));
    assert!(matches!(second, InitiativeRunResult::Proposed { .. }));
    assert_eq!(service.list().await.expect("user proposals").len(), 2);
    assert_eq!(
        Projector::replay(&store.events().await.expect("events")).expect("replay"),
        store.state().await.expect("state")
    );
}
