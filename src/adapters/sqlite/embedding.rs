use std::collections::HashSet;

use async_trait::async_trait;
use sqlx::Row;

use crate::adapters::sqlite::database::{Database, DatabaseError};
use crate::core::{
    now, EmbeddingDocument, EmbeddingEntityKind, EmbeddingMatch, EmbeddingSpace, EmbeddingStatus,
    EmbeddingVector, EventId,
};
use crate::ports::{EmbeddingStore, EmbeddingStoreError};

#[derive(Clone)]
pub struct SqliteEmbeddingStore {
    database: Database,
}

impl SqliteEmbeddingStore {
    pub async fn open(url: &str) -> Result<Self, DatabaseError> {
        Ok(Self {
            database: Database::connect(url).await?,
        })
    }
}

#[async_trait]
impl EmbeddingStore for SqliteEmbeddingStore {
    async fn register_space(&self, space: &EmbeddingSpace) -> Result<(), EmbeddingStoreError> {
        sqlx::query(
            "INSERT INTO embedding_spaces
             (embedding_space_id, provider, model, revision, dimensions, normalized, query_prefix, document_prefix, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(embedding_space_id) DO UPDATE SET
                 provider = excluded.provider, model = excluded.model, revision = excluded.revision,
                 dimensions = excluded.dimensions, normalized = excluded.normalized,
                 query_prefix = excluded.query_prefix, document_prefix = excluded.document_prefix",
        )
        .bind(&space.id)
        .bind(&space.provider)
        .bind(&space.model)
        .bind(&space.revision)
        .bind(space.dimensions as i64)
        .bind(space.normalized)
        .bind(&space.query_prefix)
        .bind(&space.document_prefix)
        .bind(&space.created_at)
        .execute(self.database.pool())
        .await
        .map_err(backend)?;
        Ok(())
    }

    async fn discover_missing_documents(
        &self,
        space: &EmbeddingSpace,
        documents: &[EmbeddingDocument],
    ) -> Result<Vec<EmbeddingDocument>, EmbeddingStoreError> {
        let mut missing = Vec::new();
        for document in documents {
            let row = sqlx::query(
                "SELECT source_event_id FROM embedding_records
                 WHERE embedding_space_id = ? AND entity_kind = ? AND entity_id = ?
                   AND chunk_index = ? AND source_text_hash = ? AND active = 1",
            )
            .bind(&space.id)
            .bind(entity_kind(&document.entity_kind))
            .bind(&document.entity_id)
            .bind(document.chunk_index as i64)
            .bind(&document.source_text_hash)
            .fetch_optional(self.database.pool())
            .await
            .map_err(backend)?;
            let current = row
                .map(|row| row.try_get::<String, _>("source_event_id").map_err(backend))
                .transpose()?;
            if current.as_deref() != Some(&document.source_event_id.to_string()) {
                missing.push(document.clone());
            }
        }
        Ok(missing)
    }

    async fn store_embeddings(
        &self,
        space: &EmbeddingSpace,
        documents: &[EmbeddingDocument],
        vectors: &[EmbeddingVector],
    ) -> Result<(), EmbeddingStoreError> {
        if documents.len() != vectors.len() {
            return Err(EmbeddingStoreError::InvalidInput(
                "document and vector counts differ".to_owned(),
            ));
        }
        let mut transaction = self.database.pool().begin().await.map_err(backend)?;
        for (document, vector) in documents.iter().zip(vectors) {
            sqlx::query(
                "UPDATE embedding_records SET active = 0
                 WHERE embedding_space_id = ? AND entity_kind = ? AND entity_id = ?
                   AND chunk_index = ? AND source_text_hash <> ? AND active = 1",
            )
            .bind(&space.id)
            .bind(entity_kind(&document.entity_kind))
            .bind(&document.entity_id)
            .bind(document.chunk_index as i64)
            .bind(&document.source_text_hash)
            .execute(&mut *transaction)
            .await
            .map_err(backend)?;
            sqlx::query(
                "INSERT INTO embedding_records
                 (id, embedding_space_id, entity_kind, entity_id, source_event_id, source_text_hash,
                  chunk_index, vector_blob, created_at, active)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                 ON CONFLICT(embedding_space_id, entity_kind, entity_id, chunk_index, source_text_hash)
                 DO UPDATE SET source_event_id = excluded.source_event_id, vector_blob = excluded.vector_blob,
                               created_at = excluded.created_at, active = 1",
            )
            .bind(uuid::Uuid::new_v4().to_string())
            .bind(&space.id)
            .bind(entity_kind(&document.entity_kind))
            .bind(&document.entity_id)
            .bind(document.source_event_id.to_string())
            .bind(&document.source_text_hash)
            .bind(document.chunk_index as i64)
            .bind(vector.to_blob())
            .bind(now())
            .execute(&mut *transaction)
            .await
            .map_err(backend)?;
        }
        sqlx::query(
            "INSERT INTO embedding_index_state (embedding_space_id, last_success_at, last_error_kind, updated_at)
             VALUES (?, ?, NULL, ?)
             ON CONFLICT(embedding_space_id) DO UPDATE SET
                 last_success_at = excluded.last_success_at, last_error_kind = NULL, updated_at = excluded.updated_at",
        )
        .bind(&space.id)
        .bind(now())
        .bind(now())
        .execute(&mut *transaction)
        .await
        .map_err(backend)?;
        transaction.commit().await.map_err(backend)?;
        Ok(())
    }

    async fn deactivate_missing_documents(
        &self,
        space: &EmbeddingSpace,
        documents: &[EmbeddingDocument],
    ) -> Result<(), EmbeddingStoreError> {
        let rows = sqlx::query(
            "SELECT id, entity_kind, entity_id, chunk_index FROM embedding_records
             WHERE embedding_space_id = ? AND active = 1",
        )
        .bind(&space.id)
        .fetch_all(self.database.pool())
        .await
        .map_err(backend)?;
        let mut transaction = self.database.pool().begin().await.map_err(backend)?;
        for row in rows {
            let kind: String = row.try_get("entity_kind").map_err(backend)?;
            let entity_id: String = row.try_get("entity_id").map_err(backend)?;
            let chunk_index: i64 = row.try_get("chunk_index").map_err(backend)?;
            let current = documents.iter().any(|document| {
                entity_kind(&document.entity_kind) == kind
                    && document.entity_id == entity_id
                    && i64::from(document.chunk_index) == chunk_index
            });
            if !current {
                sqlx::query("UPDATE embedding_records SET active = 0 WHERE id = ?")
                    .bind(row.try_get::<String, _>("id").map_err(backend)?)
                    .execute(&mut *transaction)
                    .await
                    .map_err(backend)?;
            }
        }
        transaction.commit().await.map_err(backend)?;
        Ok(())
    }

    async fn deactivate_entities(
        &self,
        space: &EmbeddingSpace,
        entities: &[(EmbeddingEntityKind, String)],
    ) -> Result<(), EmbeddingStoreError> {
        if entities.is_empty() {
            return Ok(());
        }
        let mut transaction = self.database.pool().begin().await.map_err(backend)?;
        for (kind, entity_id) in entities {
            sqlx::query(
                "UPDATE embedding_records SET active = 0
                 WHERE embedding_space_id = ? AND entity_kind = ? AND entity_id = ? AND active = 1",
            )
            .bind(&space.id)
            .bind(entity_kind(kind))
            .bind(entity_id)
            .execute(&mut *transaction)
            .await
            .map_err(backend)?;
        }
        transaction.commit().await.map_err(backend)?;
        Ok(())
    }

    async fn search_embeddings(
        &self,
        space: &EmbeddingSpace,
        query: &EmbeddingVector,
        exclude_event_ids: &[EventId],
        limit: usize,
    ) -> Result<Vec<EmbeddingMatch>, EmbeddingStoreError> {
        let rows = sqlx::query(
            "SELECT id, entity_kind, entity_id, source_event_id, source_text_hash, vector_blob
             FROM embedding_records WHERE embedding_space_id = ? AND active = 1",
        )
        .bind(&space.id)
        .fetch_all(self.database.pool())
        .await
        .map_err(backend)?;
        let mut matches = Vec::with_capacity(rows.len());
        let excluded = exclude_event_ids.iter().copied().collect::<HashSet<_>>();
        for row in rows {
            let record_id: String = row.try_get("id").map_err(backend)?;
            let source_event_id = uuid::Uuid::parse_str(
                &row.try_get::<String, _>("source_event_id")
                    .map_err(backend)?,
            )
            .map(crate::core::EventId::from)
            .map_err(|_| EmbeddingStoreError::Backend("invalid source event ID".to_owned()))?;
            if excluded.contains(&source_event_id) {
                continue;
            }
            let kind: String = row.try_get("entity_kind").map_err(backend)?;
            let vector_blob: Vec<u8> = row.try_get("vector_blob").map_err(backend)?;
            let vector = EmbeddingVector::from_blob(&vector_blob, space).map_err(|reason| {
                EmbeddingStoreError::CorruptVector {
                    record_id: record_id.clone(),
                    reason,
                }
            })?;
            matches.push(EmbeddingMatch {
                entity_kind: parse_entity_kind(&kind)?,
                entity_id: row.try_get("entity_id").map_err(backend)?,
                source_event_id,
                source_text_hash: row.try_get("source_text_hash").map_err(backend)?,
                score: query.dot(&vector),
            });
        }
        matches.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.source_event_id.cmp(&right.source_event_id))
                .then_with(|| entity_kind(&left.entity_kind).cmp(entity_kind(&right.entity_kind)))
                .then_with(|| left.entity_id.cmp(&right.entity_id))
                .then_with(|| left.source_text_hash.cmp(&right.source_text_hash))
        });
        matches.truncate(limit);
        Ok(matches)
    }

    async fn embedding_status(
        &self,
        space: &EmbeddingSpace,
    ) -> Result<EmbeddingStatus, EmbeddingStoreError> {
        let count: i64 = sqlx::query("SELECT COUNT(*) AS count FROM embedding_records WHERE embedding_space_id = ? AND active = 1")
            .bind(&space.id)
            .fetch_one(self.database.pool())
            .await
            .map_err(backend)?
            .try_get("count")
            .map_err(backend)?;
        let state = sqlx::query(
            "SELECT last_success_at, last_error_kind FROM embedding_index_state WHERE embedding_space_id = ?",
        )
        .bind(&space.id)
        .fetch_optional(self.database.pool())
        .await
        .map_err(backend)?;
        Ok(EmbeddingStatus {
            enabled: true,
            space_id: space.id.clone(),
            model: space.model.clone(),
            revision: space.revision.clone(),
            dimensions: space.dimensions,
            indexed_record_count: count.max(0) as u64,
            missing_record_count: 0,
            last_success: state
                .as_ref()
                .map(|row| row.try_get("last_success_at").map_err(backend))
                .transpose()?,
            last_error_kind: state
                .as_ref()
                .map(|row| row.try_get("last_error_kind").map_err(backend))
                .transpose()?,
        })
    }

    async fn record_failure(
        &self,
        space: &EmbeddingSpace,
        kind: &str,
    ) -> Result<(), EmbeddingStoreError> {
        sqlx::query(
            "INSERT INTO embedding_index_state (embedding_space_id, last_success_at, last_error_kind, updated_at)
             VALUES (?, NULL, ?, ?)
             ON CONFLICT(embedding_space_id) DO UPDATE SET
                 last_error_kind = excluded.last_error_kind, updated_at = excluded.updated_at",
        )
        .bind(&space.id)
        .bind(kind)
        .bind(now())
        .execute(self.database.pool())
        .await
        .map_err(backend)?;
        Ok(())
    }
}

fn entity_kind(kind: &EmbeddingEntityKind) -> &'static str {
    match kind {
        EmbeddingEntityKind::Observation => "observation",
        EmbeddingEntityKind::Memory => "memory",
        EmbeddingEntityKind::Position => "position",
    }
}

fn parse_entity_kind(value: &str) -> Result<EmbeddingEntityKind, EmbeddingStoreError> {
    match value {
        "observation" => Ok(EmbeddingEntityKind::Observation),
        "memory" => Ok(EmbeddingEntityKind::Memory),
        "position" => Ok(EmbeddingEntityKind::Position),
        _ => Err(EmbeddingStoreError::Backend(
            "invalid embedding entity kind".to_owned(),
        )),
    }
}

fn backend(error: impl std::fmt::Display) -> EmbeddingStoreError {
    EmbeddingStoreError::Backend(error.to_string())
}
