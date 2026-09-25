use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::time::timeout;

use crate::core::{
    ActiveMemoryStatus, CurrentState, EmbeddingEntityKind, EntityKind, EntityRef, EventKind,
    ExperienceEvent, MemoryId, Observation, ObservationId, PositionId, PositionStatus,
    RecallBundle, RecallQuery, RecalledItem,
};
use crate::runtime::embedding_indexer::{position_text, EmbeddingIndexError, EmbeddingIndexer};

pub const DEFAULT_RECALL_LIMIT: usize = 6;
pub const FOREGROUND_RECALL_BUDGET: Duration = Duration::from_millis(150);
pub const BACKGROUND_RECALL_BUDGET: Duration = Duration::from_secs(3);
const MAX_RECALL_CANDIDATES: usize = 32;
const MAX_LOCAL_EVENTS: usize = 1024;
const MAX_LOCAL_FACTS: usize = 512;
const MAX_LOCAL_TEXT_CHARS: usize = 2048;
const MAX_LOCAL_QUERY_TOKENS: usize = 64;

pub struct SemanticRecall {
    indexer: std::sync::Arc<EmbeddingIndexer>,
    limit: usize,
}

impl SemanticRecall {
    pub fn new(indexer: std::sync::Arc<EmbeddingIndexer>) -> Self {
        Self {
            indexer,
            limit: DEFAULT_RECALL_LIMIT,
        }
    }

    pub fn with_limit(indexer: std::sync::Arc<EmbeddingIndexer>, limit: usize) -> Self {
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
        self.recall_with_budget(query, state, events, BACKGROUND_RECALL_BUDGET)
            .await
    }

    pub async fn recall_with_budget(
        &self,
        query: &RecallQuery,
        state: &CurrentState,
        events: &[ExperienceEvent],
        budget: Duration,
    ) -> Result<RecallBundle, EmbeddingIndexError> {
        let limit = query.limit.min(self.limit).min(MAX_RECALL_CANDIDATES);
        let mut bundle = RecallBundle::empty(query.query_hash(), self.embedding_space_id());
        if limit == 0 {
            return Ok(bundle);
        }

        let candidate_limit = limit.saturating_mul(2).min(MAX_RECALL_CANDIDATES);
        let semantic = timeout(
            budget,
            self.indexer
                .search_excluding(&query.text, &query.exclude_event_ids, candidate_limit),
        )
        .await;
        match semantic {
            Ok(Ok(mut matches)) => {
                matches.retain(|item| item.score.is_finite());
                matches.sort_by(|left, right| {
                    right.score.total_cmp(&left.score).then_with(|| {
                        left.source_event_id
                            .to_string()
                            .cmp(&right.source_event_id.to_string())
                    })
                });
                for matched in matches {
                    if bundle
                        .items
                        .iter()
                        .any(|item| item.source_event_id == matched.source_event_id)
                    {
                        continue;
                    }
                    if let Some(item) = restore_item(&matched, query, state, events) {
                        bundle.items.push(item);
                        if bundle.items.len() == limit {
                            return Ok(bundle);
                        }
                    }
                }
            }
            Ok(Err(error)) => tracing::debug!(%error, "using local recall after embedding failure"),
            Err(_) => tracing::debug!(?budget, "using local recall after embedding timeout"),
        }

        let local = recall_local(query, state, events);
        for item in local.items {
            if !bundle
                .items
                .iter()
                .any(|existing| existing.source_event_id == item.source_event_id)
            {
                bundle.items.push(item);
                if bundle.items.len() == limit {
                    break;
                }
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

    pub async fn index_once(
        &self,
        state: &CurrentState,
        events: &[ExperienceEvent],
    ) -> Result<crate::core::EmbeddingIndexReport, EmbeddingIndexError> {
        self.indexer.index_once(state, events).await
    }
}

/// Deterministic fallback over a bounded tail of the ledger and active facts.
/// ponytail: scan 1024 recent events and 512 active facts; move to SQLite FTS5
/// if that ceiling stops meeting the recall budget.
pub fn recall_local(
    query: &RecallQuery,
    state: &CurrentState,
    events: &[ExperienceEvent],
) -> RecallBundle {
    let mut bundle = RecallBundle::empty(query.query_hash(), "local-lexical-v1");
    let limit = query.limit.min(MAX_RECALL_CANDIDATES);
    if limit == 0 {
        return bundle;
    }
    let Some(query_tokens) = lexical_tokens(&query.text) else {
        return bundle;
    };
    let as_of = query
        .as_of_sequence
        .unwrap_or(state.revision)
        .min(events.len() as u64);
    let event_start = (as_of as usize).saturating_sub(MAX_LOCAL_EVENTS);
    let excluded = query
        .exclude_event_ids
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    let mut candidates = Vec::new();

    for event in events.iter().take(as_of as usize).skip(event_start) {
        if excluded.contains(&event.event_id) || !valid_event(event) {
            continue;
        }
        if event.event_kind != EventKind::ObservationRecorded
            && event.event_kind != EventKind::UserMessageReceived
        {
            continue;
        }
        let Some(subject) = event
            .subject
            .as_ref()
            .filter(|subject| subject.kind == EntityKind::Observation)
        else {
            continue;
        };
        let Ok(observation) = serde_json::from_value::<Observation>(event.payload.clone()) else {
            continue;
        };
        if observation.id != ObservationId::from(subject.id) {
            continue;
        }
        if let Some(score) = lexical_score(&query.text, &query_tokens, &observation.content) {
            candidates.push(recalled_item(
                event,
                as_of,
                EntityRef::new(EntityKind::Observation, subject.id),
                observation.content,
                score,
            ));
        }
    }

    if as_of == state.revision {
        let memory_events = latest_entity_events(
            events,
            as_of,
            &EntityKind::Memory,
            &[EventKind::MemoryPromoted],
        );
        for memory in state
            .active_memories
            .values()
            .rev()
            .take(MAX_LOCAL_FACTS)
            .filter(|memory| memory.status == ActiveMemoryStatus::Active)
        {
            let Some((_, event)) = memory_events.get(&memory.id.uuid()).copied() else {
                continue;
            };
            if excluded.contains(&event.event_id) {
                continue;
            }
            if let Some(score) = lexical_score(&query.text, &query_tokens, &memory.content) {
                candidates.push(recalled_item(
                    event,
                    as_of,
                    EntityRef::new(EntityKind::Memory, memory.id.uuid()),
                    memory.content.clone(),
                    score,
                ));
            }
        }

        let position_events = latest_entity_events(
            events,
            as_of,
            &EntityKind::Position,
            &[
                EventKind::PositionEstablished,
                EventKind::PositionMaintained,
                EventKind::PositionRevised,
                EventKind::PositionRecorded,
            ],
        );
        for position in state
            .positions
            .values()
            .rev()
            .take(MAX_LOCAL_FACTS)
            .filter(|position| position.status == PositionStatus::Active)
        {
            let Some((_, event)) = position_events.get(&position.id.uuid()).copied() else {
                continue;
            };
            if excluded.contains(&event.event_id) {
                continue;
            }
            if let Some(score) = lexical_score(&query.text, &query_tokens, &position_text(position))
            {
                candidates.push(recalled_item(
                    event,
                    as_of,
                    EntityRef::new(EntityKind::Position, position.id.uuid()),
                    position_text(position),
                    score,
                ));
            }
        }
    }

    candidates.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| right.created_at.cmp(&left.created_at))
            .then_with(|| left.source_event_id.cmp(&right.source_event_id))
    });
    let mut source_ids = HashSet::new();
    bundle.items = candidates
        .into_iter()
        .filter(|item| source_ids.insert(item.source_event_id))
        .take(limit)
        .collect();
    bundle
}

fn restore_item(
    item: &crate::core::EmbeddingMatch,
    query: &RecallQuery,
    state: &CurrentState,
    events: &[ExperienceEvent],
) -> Option<RecalledItem> {
    let (index, source_event) = events
        .iter()
        .enumerate()
        .find(|(_, event)| event.event_id == item.source_event_id)?;
    let sequence = index as u64 + 1;
    if query.as_of_sequence.is_some_and(|as_of| sequence > as_of)
        || query.exclude_event_ids.contains(&item.source_event_id)
        || !valid_event(source_event)
    {
        return None;
    }
    let entity_id = uuid::Uuid::parse_str(&item.entity_id).ok()?;
    let (entity, text) = match item.entity_kind {
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
            )
        }
        EmbeddingEntityKind::Memory => {
            let as_of = query.as_of_sequence.unwrap_or(state.revision);
            let latest = latest_entity_events(
                events,
                as_of,
                &EntityKind::Memory,
                &[EventKind::MemoryPromoted],
            );
            if !matches!(
                source_event.subject.as_ref(),
                Some(subject) if subject.kind == EntityKind::Memory && subject.id == entity_id
            ) || as_of != state.revision
                || latest.get(&entity_id).map(|(_, event)| event.event_id)
                    != Some(source_event.event_id)
            {
                return None;
            }
            let memory = state
                .active_memories
                .get(&MemoryId::from(entity_id))
                .filter(|memory| memory.status == ActiveMemoryStatus::Active)?;
            (
                EntityRef::new(EntityKind::Memory, entity_id),
                memory.content.clone(),
            )
        }
        EmbeddingEntityKind::Position => {
            let as_of = query.as_of_sequence.unwrap_or(state.revision);
            let latest = latest_entity_events(
                events,
                as_of,
                &EntityKind::Position,
                &[
                    EventKind::PositionEstablished,
                    EventKind::PositionMaintained,
                    EventKind::PositionRevised,
                    EventKind::PositionRecorded,
                ],
            );
            if !matches!(
                source_event.subject.as_ref(),
                Some(subject) if subject.kind == EntityKind::Position && subject.id == entity_id
            ) || as_of != state.revision
                || latest.get(&entity_id).map(|(_, event)| event.event_id)
                    != Some(source_event.event_id)
            {
                return None;
            }
            let position = state
                .positions
                .get(&PositionId::from(entity_id))
                .filter(|position| position.status == PositionStatus::Active)?;
            (
                EntityRef::new(EntityKind::Position, entity_id),
                position_text(position),
            )
        }
    };
    if text.trim().is_empty() {
        return None;
    }
    Some(recalled_item(
        source_event,
        query.as_of_sequence.unwrap_or(state.revision),
        entity,
        text,
        item.score,
    ))
}

fn latest_entity_events<'a>(
    events: &'a [ExperienceEvent],
    as_of: u64,
    entity_kind: &EntityKind,
    accepted_kinds: &[EventKind],
) -> HashMap<uuid::Uuid, (u64, &'a ExperienceEvent)> {
    let mut latest = HashMap::new();
    for (index, event) in events.iter().enumerate().take(as_of as usize) {
        if !accepted_kinds.contains(&event.event_kind) || !valid_event(event) {
            continue;
        }
        if let Some(subject) = event
            .subject
            .as_ref()
            .filter(|subject| &subject.kind == entity_kind)
        {
            latest.insert(subject.id, (index as u64 + 1, event));
        }
    }
    latest
}

fn recalled_item(
    event: &ExperienceEvent,
    sequence: u64,
    entity: EntityRef,
    text: String,
    score: f32,
) -> RecalledItem {
    RecalledItem {
        entity,
        source_event_id: event.event_id,
        source_hash: if event.integrity_hash.is_empty() {
            event.canonical_hash().unwrap_or_default()
        } else {
            event.integrity_hash.clone()
        },
        as_of_sequence: sequence,
        text,
        score,
        created_at: event.occurred_at.clone(),
    }
}

fn valid_event(event: &ExperienceEvent) -> bool {
    event.verify_integrity().unwrap_or(false)
}

fn lexical_tokens(text: &str) -> Option<HashSet<String>> {
    let tokens = text
        .chars()
        .take(MAX_LOCAL_TEXT_CHARS)
        .collect::<String>()
        .split(|character: char| !character.is_alphanumeric())
        .filter(|token| token.chars().count() > 1)
        .take(MAX_LOCAL_QUERY_TOKENS)
        .map(str::to_lowercase)
        .collect::<HashSet<_>>();
    (!tokens.is_empty()).then_some(tokens)
}

fn lexical_score(query: &str, query_tokens: &HashSet<String>, text: &str) -> Option<f32> {
    let bounded_query = query.chars().take(MAX_LOCAL_TEXT_CHARS).collect::<String>();
    let normalized_query = bounded_query.to_lowercase();
    let normalized = text
        .chars()
        .take(MAX_LOCAL_TEXT_CHARS)
        .collect::<String>()
        .to_lowercase();
    let text_tokens = normalized
        .split(|character: char| !character.is_alphanumeric())
        .filter(|token| token.chars().count() > 1)
        .map(str::to_owned)
        .collect::<HashSet<_>>();
    let overlap = query_tokens.intersection(&text_tokens).count();
    let phrase_match = normalized.contains(&normalized_query);
    if overlap == 0 && !phrase_match {
        return None;
    }
    Some(overlap as f32 / query_tokens.len() as f32 + if phrase_match { 0.5 } else { 0.0 })
}
