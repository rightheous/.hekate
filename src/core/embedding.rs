use std::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use super::{now, EventId};

pub const DEFAULT_QUERY_PREFIX: &str = "task: search result | query: ";
pub const DEFAULT_DOCUMENT_PREFIX: &str = "title: none | text: ";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EmbeddingSpace {
    pub id: String,
    pub provider: String,
    pub model: String,
    pub revision: String,
    pub dimensions: usize,
    pub normalized: bool,
    pub query_prefix: String,
    pub document_prefix: String,
    pub created_at: String,
}

impl EmbeddingSpace {
    pub fn new(
        provider: impl Into<String>,
        model: impl Into<String>,
        revision: impl Into<String>,
        dimensions: usize,
        normalized: bool,
        query_prefix: impl Into<String>,
        document_prefix: impl Into<String>,
    ) -> Self {
        let provider = provider.into();
        let model = model.into();
        let revision = revision.into();
        let query_prefix = query_prefix.into();
        let document_prefix = document_prefix.into();
        let mut hash = Sha256::new();
        for value in [
            provider.as_str(),
            model.as_str(),
            revision.as_str(),
            &dimensions.to_string(),
            if normalized {
                "normalized"
            } else {
                "unnormalized"
            },
            query_prefix.as_str(),
            document_prefix.as_str(),
        ] {
            hash.update(value.as_bytes());
            hash.update([0]);
        }
        let id = hash
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        Self {
            id: format!("embedding-{id}"),
            provider,
            model,
            revision,
            dimensions,
            normalized,
            query_prefix,
            document_prefix,
            created_at: now(),
        }
    }
}

#[derive(Clone, PartialEq)]
pub struct EmbeddingVector(Vec<f32>);

impl fmt::Debug for EmbeddingVector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EmbeddingVector")
            .field("dimensions", &self.0.len())
            .finish()
    }
}

impl EmbeddingVector {
    pub fn new(values: Vec<f32>, space: &EmbeddingSpace) -> Result<Self, EmbeddingValidationError> {
        if values.len() != space.dimensions {
            return Err(EmbeddingValidationError::Dimensions {
                expected: space.dimensions,
                actual: values.len(),
            });
        }
        if values.iter().any(|value| !value.is_finite()) {
            return Err(EmbeddingValidationError::NonFinite);
        }
        let norm = values
            .iter()
            .map(|value| f64::from(*value) * f64::from(*value))
            .sum::<f64>()
            .sqrt();
        if !norm.is_finite() {
            return Err(EmbeddingValidationError::NonFinite);
        }
        if norm == 0.0 {
            return Err(EmbeddingValidationError::Zero);
        }
        let values = if space.normalized {
            values
                .into_iter()
                .map(|value| (f64::from(value) / norm) as f32)
                .collect()
        } else {
            values
        };
        Ok(Self(values))
    }

    pub fn values(&self) -> &[f32] {
        &self.0
    }

    pub(crate) fn to_blob(&self) -> Vec<u8> {
        self.0
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect()
    }

    pub(crate) fn from_blob(
        blob: &[u8],
        space: &EmbeddingSpace,
    ) -> Result<Self, EmbeddingValidationError> {
        let expected = space.dimensions.saturating_mul(std::mem::size_of::<f32>());
        if blob.len() != expected {
            return Err(EmbeddingValidationError::BlobLength {
                expected,
                actual: blob.len(),
            });
        }
        let values = blob
            .chunks_exact(std::mem::size_of::<f32>())
            .map(|bytes| f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
            .collect();
        Self::new(values, space)
    }

    pub(crate) fn dot(&self, other: &Self) -> f32 {
        self.0
            .iter()
            .zip(&other.0)
            .map(|(left, right)| left * right)
            .sum()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingEntityKind {
    Observation,
    Memory,
    Position,
}

#[derive(Clone, Eq, PartialEq)]
pub struct EmbeddingDocument {
    pub entity_kind: EmbeddingEntityKind,
    pub entity_id: String,
    pub source_event_id: EventId,
    pub source_text: String,
    pub source_text_hash: String,
    pub chunk_index: u32,
}

impl fmt::Debug for EmbeddingDocument {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EmbeddingDocument")
            .field("entity_kind", &self.entity_kind)
            .field("entity_id", &self.entity_id)
            .field("source_event_id", &self.source_event_id)
            .field("source_text_hash", &self.source_text_hash)
            .field("chunk_index", &self.chunk_index)
            .finish()
    }
}

impl EmbeddingDocument {
    pub fn new(
        entity_kind: EmbeddingEntityKind,
        entity_id: impl Into<String>,
        source_event_id: EventId,
        source_text: impl Into<String>,
        chunk_index: u32,
    ) -> Self {
        let source_text = source_text.into();
        let source_text_hash = Sha256::digest(source_text.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        Self {
            entity_kind,
            entity_id: entity_id.into(),
            source_event_id,
            source_text,
            source_text_hash,
            chunk_index,
        }
    }
}

#[derive(Clone, PartialEq)]
pub struct EmbeddingRecord {
    pub id: String,
    pub embedding_space_id: String,
    pub entity_kind: EmbeddingEntityKind,
    pub entity_id: String,
    pub source_event_id: EventId,
    pub source_text_hash: String,
    pub chunk_index: u32,
    pub vector: EmbeddingVector,
    pub created_at: String,
    pub active: bool,
}

impl fmt::Debug for EmbeddingRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EmbeddingRecord")
            .field("id", &self.id)
            .field("embedding_space_id", &self.embedding_space_id)
            .field("entity_kind", &self.entity_kind)
            .field("entity_id", &self.entity_id)
            .field("source_event_id", &self.source_event_id)
            .field("source_text_hash", &self.source_text_hash)
            .field("chunk_index", &self.chunk_index)
            .field("created_at", &self.created_at)
            .field("active", &self.active)
            .finish()
    }
}

impl EmbeddingRecord {
    pub fn new(
        space: &EmbeddingSpace,
        document: &EmbeddingDocument,
        vector: EmbeddingVector,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            embedding_space_id: space.id.clone(),
            entity_kind: document.entity_kind.clone(),
            entity_id: document.entity_id.clone(),
            source_event_id: document.source_event_id,
            source_text_hash: document.source_text_hash.clone(),
            chunk_index: document.chunk_index,
            vector,
            created_at: now(),
            active: true,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct EmbeddingMatch {
    pub entity_kind: EmbeddingEntityKind,
    pub entity_id: String,
    pub source_event_id: EventId,
    pub source_text_hash: String,
    pub score: f32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EmbeddingStatus {
    pub enabled: bool,
    pub space_id: String,
    pub model: String,
    pub revision: String,
    pub dimensions: usize,
    pub indexed_record_count: u64,
    pub missing_record_count: u64,
    pub last_success: Option<String>,
    pub last_error_kind: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct EmbeddingIndexReport {
    pub discovered: u64,
    pub embedded: u64,
    pub skipped: u64,
    pub failed: u64,
}

#[derive(Debug, Error)]
pub enum EmbeddingValidationError {
    #[error("embedding dimensions mismatch: expected {expected}, received {actual}")]
    Dimensions { expected: usize, actual: usize },
    #[error("embedding contains a non-finite value")]
    NonFinite,
    #[error("embedding vector is zero")]
    Zero,
    #[error("embedding BLOB length mismatch: expected {expected}, received {actual}")]
    BlobLength { expected: usize, actual: usize },
}
