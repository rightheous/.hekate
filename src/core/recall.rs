use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{EntityRef, EventId};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RecallQuery {
    pub text: String,
    pub limit: usize,
    pub exclude_event_ids: Vec<EventId>,
}

impl RecallQuery {
    pub fn query_hash(&self) -> String {
        hash(&self.text)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RecalledItem {
    pub entity: EntityRef,
    pub source_event_id: EventId,
    pub text: String,
    pub score: f32,
    pub created_at: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct RecallBundle {
    pub query_hash: String,
    pub embedding_space_id: String,
    pub items: Vec<RecalledItem>,
}

impl RecallBundle {
    pub fn empty(query_hash: impl Into<String>, embedding_space_id: impl Into<String>) -> Self {
        Self {
            query_hash: query_hash.into(),
            embedding_space_id: embedding_space_id.into(),
            items: Vec::new(),
        }
    }

    pub fn empty_for(text: &str, embedding_space_id: impl Into<String>) -> Self {
        Self::empty(hash(text), embedding_space_id)
    }
}

fn hash(value: &str) -> String {
    Sha256::digest(value.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
