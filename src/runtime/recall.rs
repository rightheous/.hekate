use std::collections::HashSet;
use std::sync::Arc;

use crate::core::{
    ActiveMemoryStatus, CurrentState, EmbeddingEntityKind, EntityKind, EntityRef, EventKind,
    ExperienceEvent, MemoryId, Observation, ObservationId, PositionId, PositionStatus,
    RecallBundle, RecallQuery, RecalledItem,
};
use crate::runtime::embedding_indexer::{position_text, EmbeddingIndexError, EmbeddingIndexer};

pub const DEFAULT_RECALL_LIMIT: usize = 6;
const MAX_RECALL_CANDIDATES: usize = 32;

pub struct SemanticRecall {
    indexer: Arc<EmbeddingIndexer>,
    limit: usize,
}

impl SemanticRecall {
    pub fn new(indexer: Arc<EmbeddingIndexer>) -> Self {
        Self {
            indexer,
            limit: DEFAULT_RECALL_LIMIT,
        }
    }

    pub fn with_limit(indexer: Arc<EmbeddingIndexer>, limit: usize) -> Self {
        Self {
            indexer,
            limit: limit.min(MAX_RECALL_CANDIDATES),
        }
    }

    pub fn embedding_space_id(&self) -> &str {
        &self.indexer.space().id
    }

    pub async fn recall(
        &self,
        query: &RecallQuery,
        state: &CurrentState,
        events: &[ExperienceEvent],
    ) -> Result<RecallBundle, EmbeddingIndexError> {
        let limit = query.limit.min(self.limit).min(MAX_RECALL_CANDIDATES);
        let mut bundle = RecallBundle::empty(query.query_hash(), self.embedding_space_id());
        if limit == 0 {
            return Ok(bundle);
        }
        let candidate_limit = limit.saturating_mul(2).min(MAX_RECALL_CANDIDATES);
        let mut matches = self
            .indexer
            .search_excluding(&query.text, &query.exclude_event_ids, candidate_limit)
            .await?;
        matches.retain(|item| item.score.is_finite());
        matches.sort_by(|left, right| {
            right.score.total_cmp(&left.score).then_with(|| {
                left.source_event_id
                    .to_string()
                    .cmp(&right.source_event_id.to_string())
            })
        });

        let mut source_event_ids = HashSet::new();
        for item in matches {
            if !source_event_ids.insert(item.source_event_id) {
                continue;
            }
            let Some(item) = restore_item(&item, state, events) else {
                continue;
            };
            bundle.items.push(item);
            if bundle.items.len() == limit {
                break;
            }
        }
        Ok(bundle)
    }

    pub async fn index_events(
        &self,
        state: &CurrentState,
        events: &[ExperienceEvent],
    ) -> Result<crate::core::EmbeddingIndexReport, EmbeddingIndexError> {
        self.indexer.index_events(state, events).await
    }
}

fn restore_item(
    item: &crate::core::EmbeddingMatch,
    state: &CurrentState,
    events: &[ExperienceEvent],
) -> Option<RecalledItem> {
    let source_event = events
        .iter()
        .find(|event| event.event_id == item.source_event_id)?;
    let entity_id = uuid::Uuid::parse_str(&item.entity_id).ok()?;
    let (entity, text, created_at) = match item.entity_kind {
        EmbeddingEntityKind::Observation => {
            if !matches!(
                source_event.event_kind,
                EventKind::ObservationRecorded | EventKind::UserMessageReceived
            ) || !matches!(
                source_event.subject.as_ref(),
                Some(subject)
                    if subject.kind == EntityKind::Observation && subject.id == entity_id
            ) {
                return None;
            }
            let observation: Observation =
                serde_json::from_value(source_event.payload.clone()).ok()?;
            if observation.id != ObservationId::from(entity_id)
                || observation.content.trim().is_empty()
            {
                return None;
            }
            (
                EntityRef::new(EntityKind::Observation, entity_id),
                observation.content,
                observation.received_at,
            )
        }
        EmbeddingEntityKind::Memory => {
            if !matches!(
                source_event.subject.as_ref(),
                Some(subject) if subject.kind == EntityKind::Memory && subject.id == entity_id
            ) {
                return None;
            }
            let memory = state
                .active_memories
                .get(&MemoryId::from(entity_id))
                .filter(|memory| memory.status == ActiveMemoryStatus::Active)?;
            (
                EntityRef::new(EntityKind::Memory, entity_id),
                memory.content.clone(),
                memory.created_at.clone(),
            )
        }
        EmbeddingEntityKind::Position => {
            if !matches!(
                source_event.subject.as_ref(),
                Some(subject) if subject.kind == EntityKind::Position && subject.id == entity_id
            ) {
                return None;
            }
            let position = state
                .positions
                .get(&PositionId::from(entity_id))
                .filter(|position| position.status == PositionStatus::Active)?;
            (
                EntityRef::new(EntityKind::Position, entity_id),
                position_text(position),
                position.created_at.clone(),
            )
        }
    };
    if text.trim().is_empty() {
        return None;
    }
    Some(RecalledItem {
        entity,
        source_event_id: item.source_event_id,
        text,
        score: item.score,
        created_at,
    })
}
