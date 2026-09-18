use std::sync::Arc;

use thiserror::Error;

use crate::core::{
    ActiveMemoryStatus, CurrentState, EmbeddingDocument, EmbeddingEntityKind, EmbeddingIndexReport,
    EmbeddingMatch, EntityKind, EventId, ExperienceEvent, ObservationId, Position, PositionId,
    PositionStatus, Stance,
};
use crate::ports::{
    EmbeddingProvider, EmbeddingProviderError, EmbeddingStore, EmbeddingStoreError,
};

#[derive(Debug, Error)]
pub enum EmbeddingIndexError {
    #[error(transparent)]
    Provider(#[from] EmbeddingProviderError),
    #[error(transparent)]
    Store(#[from] EmbeddingStoreError),
    #[error("embedding provenance is missing for {kind} {entity_id}")]
    MissingProvenance {
        kind: &'static str,
        entity_id: String,
    },
}

pub struct EmbeddingIndexer {
    provider: Arc<dyn EmbeddingProvider>,
    store: Arc<dyn EmbeddingStore>,
    batch_size: usize,
}

impl EmbeddingIndexer {
    pub fn new(
        provider: Arc<dyn EmbeddingProvider>,
        store: Arc<dyn EmbeddingStore>,
        batch_size: usize,
    ) -> Self {
        Self {
            provider,
            store,
            batch_size: batch_size.max(1),
        }
    }

    pub async fn index_once(
        &self,
        state: &CurrentState,
        events: &[ExperienceEvent],
    ) -> Result<EmbeddingIndexReport, EmbeddingIndexError> {
        let documents = embedding_documents(state, events)?;
        let report = self.index_documents(&documents).await?;
        let space = self.provider.space();
        if let Err(error) = self
            .store
            .deactivate_missing_documents(space, &documents)
            .await
        {
            let _ = self.store.record_failure(space, "storage").await;
            return Err(error.into());
        }
        Ok(report)
    }

    pub async fn index_events(
        &self,
        state: &CurrentState,
        events: &[ExperienceEvent],
    ) -> Result<EmbeddingIndexReport, EmbeddingIndexError> {
        let documents = embedding_documents_for_events(state, events)?;
        let inactive_entities = embedding_targets(events)
            .into_iter()
            .filter(|(kind, entity_id)| {
                !documents.iter().any(|document| {
                    &document.entity_kind == kind && &document.entity_id == entity_id
                })
            })
            .collect::<Vec<_>>();
        let report = self.index_documents(&documents).await?;
        if let Err(error) = self
            .store
            .deactivate_entities(self.provider.space(), &inactive_entities)
            .await
        {
            let _ = self
                .store
                .record_failure(self.provider.space(), "storage")
                .await;
            return Err(error.into());
        }
        Ok(report)
    }

    pub async fn index_documents(
        &self,
        documents: &[EmbeddingDocument],
    ) -> Result<EmbeddingIndexReport, EmbeddingIndexError> {
        let space = self.provider.space();
        self.store.register_space(space).await?;
        let missing = self
            .store
            .discover_missing_documents(space, documents)
            .await?;
        let mut report = EmbeddingIndexReport {
            discovered: documents.len() as u64,
            skipped: (documents.len() - missing.len()) as u64,
            ..EmbeddingIndexReport::default()
        };
        for batch in missing.chunks(self.batch_size) {
            let vectors = match self.provider.embed_documents(batch).await {
                Ok(vectors) => vectors,
                Err(error) => {
                    let _ = self.store.record_failure(space, error.kind()).await;
                    return Err(error.into());
                }
            };
            if let Err(error) = self.store.store_embeddings(space, batch, &vectors).await {
                let _ = self.store.record_failure(space, "storage").await;
                return Err(error.into());
            }
            report.embedded += batch.len() as u64;
        }
        if missing.is_empty() {
            self.store.store_embeddings(space, &[], &[]).await?;
        }
        Ok(report)
    }

    pub fn space(&self) -> &crate::core::EmbeddingSpace {
        self.provider.space()
    }

    pub async fn search(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<EmbeddingMatch>, EmbeddingIndexError> {
        let space = self.provider.space();
        let query = match self.provider.embed_query(query).await {
            Ok(query) => query,
            Err(error) => {
                let _ = self.store.record_failure(space, error.kind()).await;
                return Err(error.into());
            }
        };
        Ok(self.store.search_embeddings(space, &query, limit).await?)
    }
}

pub fn embedding_documents(
    state: &CurrentState,
    events: &[ExperienceEvent],
) -> Result<Vec<EmbeddingDocument>, EmbeddingIndexError> {
    let mut documents: Vec<EmbeddingDocument> = Vec::new();
    for observation in state.observations.values() {
        documents.push(EmbeddingDocument::new(
            EmbeddingEntityKind::Observation,
            observation.id.to_string(),
            provenance(events, EntityKind::Observation, observation.id.uuid())?,
            observation.content.clone(),
            0,
        ));
    }
    for memory in state.active_memories.values() {
        if memory.status != ActiveMemoryStatus::Active {
            continue;
        }
        documents.push(EmbeddingDocument::new(
            EmbeddingEntityKind::Memory,
            memory.id.to_string(),
            provenance(events, EntityKind::Memory, memory.id.uuid())?,
            memory.content.clone(),
            0,
        ));
    }
    for position in state.positions.values() {
        if position.status != PositionStatus::Active {
            continue;
        }
        documents.push(EmbeddingDocument::new(
            EmbeddingEntityKind::Position,
            position.id.to_string(),
            provenance(events, EntityKind::Position, position.id.uuid())?,
            position_text(position),
            0,
        ));
    }
    Ok(documents)
}

pub fn embedding_documents_for_events(
    state: &CurrentState,
    events: &[ExperienceEvent],
) -> Result<Vec<EmbeddingDocument>, EmbeddingIndexError> {
    let mut documents: Vec<EmbeddingDocument> = Vec::new();
    for event in events {
        let Some(subject) = event.subject.as_ref() else {
            continue;
        };
        let document = match subject.kind {
            EntityKind::Observation => state
                .observations
                .get(&ObservationId::from(subject.id))
                .map(|observation| {
                    EmbeddingDocument::new(
                        EmbeddingEntityKind::Observation,
                        observation.id.to_string(),
                        event.event_id,
                        observation.content.clone(),
                        0,
                    )
                }),
            EntityKind::Memory => state
                .active_memories
                .get(&crate::core::MemoryId::from(subject.id))
                .filter(|memory| memory.status == ActiveMemoryStatus::Active)
                .map(|memory| {
                    EmbeddingDocument::new(
                        EmbeddingEntityKind::Memory,
                        memory.id.to_string(),
                        event.event_id,
                        memory.content.clone(),
                        0,
                    )
                }),
            EntityKind::Position => state
                .positions
                .get(&PositionId::from(subject.id))
                .filter(|position| position.status == PositionStatus::Active)
                .map(|position| {
                    EmbeddingDocument::new(
                        EmbeddingEntityKind::Position,
                        position.id.to_string(),
                        event.event_id,
                        position_text(position),
                        0,
                    )
                }),
            _ => None,
        };
        if let Some(document) = document {
            if let Some(index) = documents.iter().position(|existing| {
                existing.entity_kind == document.entity_kind
                    && existing.entity_id == document.entity_id
            }) {
                documents[index] = document;
            } else {
                documents.push(document);
            }
        }
    }
    Ok(documents)
}

fn embedding_targets(events: &[ExperienceEvent]) -> Vec<(EmbeddingEntityKind, String)> {
    let mut targets = Vec::new();
    for event in events {
        let Some(subject) = event.subject.as_ref() else {
            continue;
        };
        let Some(kind) = (match &subject.kind {
            EntityKind::Observation => Some(EmbeddingEntityKind::Observation),
            EntityKind::Memory => Some(EmbeddingEntityKind::Memory),
            EntityKind::Position => Some(EmbeddingEntityKind::Position),
            _ => None,
        }) else {
            continue;
        };
        let target = (kind, subject.id.to_string());
        if !targets.contains(&target) {
            targets.push(target);
        }
    }
    targets
}

fn provenance(
    events: &[ExperienceEvent],
    kind: EntityKind,
    id: uuid::Uuid,
) -> Result<EventId, EmbeddingIndexError> {
    events
        .iter()
        .rev()
        .find(|event| {
            event
                .subject
                .as_ref()
                .is_some_and(|subject| subject.kind == kind && subject.id == id)
        })
        .map(|event| event.event_id)
        .ok_or(EmbeddingIndexError::MissingProvenance {
            kind: match kind {
                EntityKind::Observation => "observation",
                EntityKind::Memory => "memory",
                EntityKind::Position => "position",
                _ => "entity",
            },
            entity_id: id.to_string(),
        })
}

fn stance(value: &Stance) -> &'static str {
    match value {
        Stance::Support => "support",
        Stance::Oppose => "oppose",
        Stance::Uncertain => "uncertain",
        Stance::Neutral => "neutral",
    }
}

pub(crate) fn position_text(position: &Position) -> String {
    let reasons = position
        .reasons
        .iter()
        .map(|reason| format!("- {reason}"))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "subject: {}\nstance: {}\nreasons:\n{}",
        position.subject,
        stance(&position.stance),
        reasons
    )
}
