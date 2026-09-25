use std::sync::Arc;

use hekate::adapters::sqlite::SqliteStore;
use hekate::core::event::{EntityKind, EntityRef, EventKind, EventSource, ExperienceEvent};
use hekate::core::{
    initiative_fingerprint, AgendaCandidate, InitiativeKind, InitiativeStatus, Observation,
    ObservationId, Principal, PrincipalId, PrincipalKind,
};
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

fn candidate(
    source_entity: EntityRef,
    source_event_ids: Vec<hekate::core::EventId>,
    target: PrincipalId,
    version: u64,
    revision: u64,
    kind: InitiativeKind,
) -> AgendaCandidate {
    AgendaCandidate {
        kind,
        content: "Review the unresolved assumption".to_owned(),
        rationale: "The latest source event leaves it open".to_owned(),
        fingerprint: initiative_fingerprint(
            kind,
            &source_entity,
            version,
            target,
            &source_event_ids,
        ),
        source_entity,
        source_version: version,
        source_event_ids,
        target_principal_id: target,
        as_of_revision: revision,
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
    let hekate = PrincipalId::new();
    let user = PrincipalId::new();
    let projector = Projector::new(store.clone());
    let hekate_principal = Principal {
        id: hekate,
        kind: PrincipalKind::Hekate,
        name: "HEKATE".to_owned(),
        identity_version_id: None,
    };
    projector
        .record(event(
            hekate,
            EventKind::PrincipalCreated,
            EntityKind::Principal,
            hekate.uuid(),
            serde_json::to_value(hekate_principal).expect("hekate payload"),
        ))
        .await
        .expect("seed hekate");
    let user_principal = Principal {
        id: user,
        kind: PrincipalKind::User,
        name: "User".to_owned(),
        identity_version_id: None,
    };
    projector
        .record(event(
            hekate,
            EventKind::PrincipalCreated,
            EntityKind::Principal,
            user.uuid(),
            serde_json::to_value(user_principal).expect("user payload"),
        ))
        .await
        .expect("seed user");
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
    let source_event = event(
        user,
        EventKind::ObservationRecorded,
        EntityKind::Observation,
        observation.id.uuid(),
        serde_json::to_value(&observation).expect("observation payload"),
    );
    let source_event_id = source_event.event_id;
    projector
        .record(source_event)
        .await
        .expect("seed observation");

    let service = InitiativeService::new(store.clone(), hekate, user);
    let state = store.state().await.expect("state");
    let source = EntityRef::new(EntityKind::Observation, observation.id.uuid());
    let stale = candidate(
        source.clone(),
        vec![source_event_id],
        user,
        1,
        state.revision - 1,
        InitiativeKind::Challenge,
    );
    assert_eq!(
        service
            .propose_candidate(stale)
            .await
            .expect("stale result"),
        InitiativeRunResult::Deferred
    );
    let wrong_version = candidate(
        source.clone(),
        vec![source_event_id],
        user,
        2,
        state.revision,
        InitiativeKind::Challenge,
    );
    assert!(matches!(
        service.propose_candidate(wrong_version).await,
        Err(InitiativeError::InvalidCandidate(_))
    ));
    assert_eq!(store.events().await.expect("events").len(), 3);

    let valid = candidate(
        source,
        vec![source_event_id],
        user,
        1,
        state.revision,
        InitiativeKind::Question,
    );
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
    assert_eq!(proposal_events[3].event_kind, EventKind::InitiativeProposed);
    assert_eq!(proposal_events[4].event_kind, EventKind::InitiativeReadied);
    let projected = store.state().await.expect("projected state");
    assert_eq!(projected.initiatives.get(&proposal.id), Some(&proposal));
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
            .propose_candidate(valid)
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
            .propose_candidate(candidate(
                EntityRef::new(EntityKind::Observation, observation.id.uuid()),
                vec![source_event_id],
                user,
                1,
                state.revision,
                InitiativeKind::Question,
            ))
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
        restarted_store.events().await.expect("dismiss event")[5].event_kind,
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
