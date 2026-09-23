use async_trait::async_trait;
use thiserror::Error;

use crate::core::{
    EmbeddingDocument, EmbeddingMatch, EmbeddingSpace, EmbeddingStatus, EmbeddingValidationError,
    EmbeddingVector, EventId,
};

#[derive(Debug, Error)]
pub enum EmbeddingProviderError {
    #[error("embedding provider is disabled")]
    Disabled,
    #[error("embedding provider configuration error: {0}")]
    Configuration(String),
    #[error("embedding provider request timed out")]
    Timeout,
    #[error("embedding provider request failed")]
    Request,
    #[error("embedding provider returned HTTP {status} (response {response_hash})")]
    Http { status: u16, response_hash: String },
    #[error("embedding provider returned invalid {kind} (response {response_hash})")]
    InvalidResponse { kind: String, response_hash: String },
    #[error(transparent)]
    Validation(#[from] EmbeddingValidationError),
}

impl EmbeddingProviderError {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Configuration(_) => "configuration",
            Self::Timeout => "timeout",
            Self::Request => "request",
            Self::Http { .. } => "http",
            Self::InvalidResponse { .. } => "invalid_response",
            Self::Validation(_) => "invalid_vector",
        }
    }
}

#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    fn space(&self) -> &EmbeddingSpace;
    async fn embed_query(&self, query: &str) -> Result<EmbeddingVector, EmbeddingProviderError>;
    async fn embed_documents(
        &self,
        documents: &[EmbeddingDocument],
    ) -> Result<Vec<EmbeddingVector>, EmbeddingProviderError>;
}

#[derive(Debug, Error)]
pub enum EmbeddingStoreError {
    #[error("embedding storage error: {0}")]
    Backend(String),
    #[error("embedding vector corruption for record {record_id}: {reason}")]
    CorruptVector {
        record_id: String,
        reason: EmbeddingValidationError,
    },
    #[error("embedding storage input error: {0}")]
    InvalidInput(String),
}

#[async_trait]
pub trait EmbeddingStore: Send + Sync {
    async fn register_space(&self, space: &EmbeddingSpace) -> Result<(), EmbeddingStoreError>;
    async fn discover_missing_documents(
        &self,
        space: &EmbeddingSpace,
        documents: &[EmbeddingDocument],
    ) -> Result<Vec<EmbeddingDocument>, EmbeddingStoreError>;
    async fn store_embeddings(
        &self,
        space: &EmbeddingSpace,
        documents: &[EmbeddingDocument],
        vectors: &[EmbeddingVector],
    ) -> Result<(), EmbeddingStoreError>;
    async fn deactivate_missing_documents(
        &self,
        space: &EmbeddingSpace,
        documents: &[EmbeddingDocument],
    ) -> Result<(), EmbeddingStoreError>;
    async fn deactivate_entities(
        &self,
        space: &EmbeddingSpace,
        entities: &[(crate::core::EmbeddingEntityKind, String)],
    ) -> Result<(), EmbeddingStoreError> {
        let _ = (space, entities);
        Ok(())
    }
    async fn search_embeddings(
        &self,
        space: &EmbeddingSpace,
        query: &EmbeddingVector,
        exclude_event_ids: &[EventId],
        limit: usize,
    ) -> Result<Vec<EmbeddingMatch>, EmbeddingStoreError>;
    async fn embedding_status(
        &self,
        space: &EmbeddingSpace,
    ) -> Result<EmbeddingStatus, EmbeddingStoreError>;
    async fn record_failure(
        &self,
        space: &EmbeddingSpace,
        kind: &str,
    ) -> Result<(), EmbeddingStoreError>;
}
