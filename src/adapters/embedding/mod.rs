use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::Config;
use crate::core::{EmbeddingDocument, EmbeddingSpace, EmbeddingVector};
use crate::ports::{EmbeddingProvider, EmbeddingProviderError};

#[derive(Clone)]
pub struct OpenAiEmbeddingAdapter {
    client: reqwest::Client,
    url: String,
    space: EmbeddingSpace,
}

impl OpenAiEmbeddingAdapter {
    pub fn from_config(config: &Config, space: EmbeddingSpace) -> Result<Self, String> {
        Self::new(
            config.embedding_url.clone(),
            space,
            config.embedding_timeout_seconds,
        )
    }

    pub fn new(url: String, space: EmbeddingSpace, timeout_seconds: u64) -> Result<Self, String> {
        if url.trim().is_empty() {
            return Err("embedding URL is empty".to_owned());
        }
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(timeout_seconds.max(1)))
            .build()
            .map_err(|error| format!("could not create embedding HTTP client: {error}"))?;
        Ok(Self { client, url, space })
    }

    async fn embed(
        &self,
        input: Vec<String>,
    ) -> Result<Vec<EmbeddingVector>, EmbeddingProviderError> {
        if input.is_empty() {
            return Ok(Vec::new());
        }
        let request = EmbeddingRequest {
            model: &self.space.model,
            input,
        };
        let response = self
            .client
            .post(&self.url)
            .json(&request)
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() {
                    EmbeddingProviderError::Timeout
                } else {
                    EmbeddingProviderError::Request
                }
            })?;
        let status = response.status();
        let body = response.text().await.map_err(|error| {
            if error.is_timeout() {
                EmbeddingProviderError::Timeout
            } else {
                EmbeddingProviderError::Request
            }
        })?;
        let response_hash = hash(&body);
        if !status.is_success() {
            return Err(EmbeddingProviderError::Http {
                status: status.as_u16(),
                response_hash,
            });
        }
        let parsed: EmbeddingResponse =
            serde_json::from_str(&body).map_err(|_| EmbeddingProviderError::InvalidResponse {
                kind: "JSON envelope".to_owned(),
                response_hash: response_hash.clone(),
            })?;
        let expected = request.input.len();
        if parsed.data.len() != expected {
            return Err(EmbeddingProviderError::InvalidResponse {
                kind: "embedding count".to_owned(),
                response_hash,
            });
        }
        let mut ordered = (0..expected).map(|_| None).collect::<Vec<_>>();
        for item in parsed.data {
            let Some(slot) = ordered.get_mut(item.index) else {
                return Err(EmbeddingProviderError::InvalidResponse {
                    kind: "embedding index".to_owned(),
                    response_hash,
                });
            };
            if slot.is_some() {
                return Err(EmbeddingProviderError::InvalidResponse {
                    kind: "duplicate embedding index".to_owned(),
                    response_hash,
                });
            }
            *slot = Some(
                EmbeddingVector::new(item.embedding, &self.space).map_err(|error| {
                    EmbeddingProviderError::InvalidResponse {
                        kind: error.to_string(),
                        response_hash: response_hash.clone(),
                    }
                })?,
            );
        }
        ordered.into_iter().collect::<Option<Vec<_>>>().ok_or(
            EmbeddingProviderError::InvalidResponse {
                kind: "missing embedding index".to_owned(),
                response_hash,
            },
        )
    }
}

#[async_trait]
impl EmbeddingProvider for OpenAiEmbeddingAdapter {
    fn space(&self) -> &EmbeddingSpace {
        &self.space
    }

    async fn embed_query(&self, query: &str) -> Result<EmbeddingVector, EmbeddingProviderError> {
        let mut vectors = self
            .embed(vec![format!("{}{}", self.space.query_prefix, query)])
            .await?;
        vectors
            .pop()
            .ok_or(EmbeddingProviderError::InvalidResponse {
                kind: "missing query embedding".to_owned(),
                response_hash: "none".to_owned(),
            })
    }

    async fn embed_documents(
        &self,
        documents: &[EmbeddingDocument],
    ) -> Result<Vec<EmbeddingVector>, EmbeddingProviderError> {
        self.embed(
            documents
                .iter()
                .map(|document| format!("{}{}", self.space.document_prefix, document.source_text))
                .collect(),
        )
        .await
    }
}

#[derive(Serialize)]
struct EmbeddingRequest<'a> {
    model: &'a str,
    input: Vec<String>,
}

#[derive(Deserialize)]
struct EmbeddingResponse {
    data: Vec<EmbeddingResponseItem>,
}

#[derive(Deserialize)]
struct EmbeddingResponseItem {
    index: usize,
    embedding: Vec<f32>,
}

fn hash(value: &str) -> String {
    Sha256::digest(value.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
