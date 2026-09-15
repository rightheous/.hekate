use std::sync::Arc;

use hekate::adapters::sqlite::SqliteStore;
use hekate::core::{
    EntityKind, EntityRef, EventKind, EventSource, ExperienceEvent, Position, PositionId,
    PositionStatus, PrincipalId, Stance,
};
use hekate::ports::Storage;
use hekate::runtime::Projector;
use uuid::Uuid;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let database_url = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("database URL is required"))?;

    let store = SqliteStore::open(&database_url).await?;
    let state = store.state().await?;

    let hekate_id = PrincipalId::from_uuid(Uuid::from_u128(1));
    let user_id = PrincipalId::from_uuid(Uuid::from_u128(2));

    let hekate_position = Position {
        id: PositionId::from_uuid(Uuid::from_u128(0x201)),
        principal_id: hekate_id,
        subject: "claiming false arithmetic as true".to_owned(),
        stance: Stance::Oppose,
        version: 1,
        status: PositionStatus::Active,
        confidence: 100,
        supersedes: None,
        reasons: vec!["Do not present a known false arithmetic statement as true.".to_owned()],
        evidence_refs: Vec::new(),
        reconsideration_conditions: vec![
            "Valid mathematical evidence demonstrates that the claim is true.".to_owned(),
        ],
        created_at: hekate::core::model::now(),
    };

    let user_position = Position {
        id: PositionId::from_uuid(Uuid::from_u128(0x202)),
        principal_id: user_id,
        subject: "claiming false arithmetic as true".to_owned(),
        stance: Stance::Support,
        version: 1,
        status: PositionStatus::Active,
        confidence: 100,
        supersedes: None,
        reasons: vec!["The user insists that 2+2=5 be accepted as true.".to_owned()],
        evidence_refs: Vec::new(),
        reconsideration_conditions: vec![
            "The user withdraws the false arithmetic claim.".to_owned()
        ],
        created_at: hekate::core::model::now(),
    };

    let first = ExperienceEvent::new(
        hekate_id,
        EventKind::PositionEstablished,
        Some(EntityRef::new(
            EntityKind::Position,
            hekate_position.id.uuid(),
        )),
        serde_json::to_value(&hekate_position)?,
        EventSource::new("safe_live_scenario", None),
        None,
        None,
        Some(1.0),
    )?;

    let second = ExperienceEvent::new(
        user_id,
        EventKind::PositionEstablished,
        Some(EntityRef::new(
            EntityKind::Position,
            user_position.id.uuid(),
        )),
        serde_json::to_value(&user_position)?,
        EventSource::new("safe_live_scenario", None),
        Some(first.event_id),
        None,
        Some(1.0),
    )?;

    Projector::new(Arc::new(store.clone()))
        .record_batch(&[first, second], Some(state.revision), None)
        .await?;

    store.shutdown().await?;

    println!("hekate_position={}", hekate_position.id);
    println!("user_position={}", user_position.id);

    Ok(())
}
