//! Model execution settings and local, content-free diagnostics.
use crate::core::{ConflictId, EventId, PositionId};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TransportKind {
    #[default]
    OpenaiCompatible,
    Ollama,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RequestPurpose {
    Foreground,
    Sleep,
}
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(untagged)]
pub enum Think {
    Enabled(bool),
    Level(String),
}
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default)]
pub struct ModelIoConfig {
    pub transport: TransportKind,
    pub context_tokens: Option<u32>,
    pub foreground_max_tokens: u32,
    pub sleep_max_tokens: u32,
    pub safety_margin: u32,
    pub foreground_reasoning_effort: String,
    pub sleep_reasoning_effort: String,
    pub foreground_think: Option<Think>,
    pub sleep_think: Option<Think>,
}
impl Default for ModelIoConfig {
    fn default() -> Self {
        Self {
            transport: TransportKind::OpenaiCompatible,
            context_tokens: None,
            foreground_max_tokens: 8192,
            sleep_max_tokens: 4096,
            safety_margin: 512,
            foreground_reasoning_effort: "low".into(),
            sleep_reasoning_effort: "none".into(),
            foreground_think: None,
            sleep_think: None,
        }
    }
}
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct ModelMessage {
    pub role: String,
    pub content: String,
}
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct BudgetReport {
    pub context_tokens: u32,
    pub context_source: String,
    pub estimated_input_tokens: u64,
    pub estimation_method: String,
    pub reserved_output_tokens: u32,
    pub safety_margin: u32,
    pub excluded_items: usize,
}
#[derive(Clone, Debug)]
pub struct PreparedModelRequest {
    pub messages: Vec<ModelMessage>,
    pub purpose: RequestPurpose,
    pub budget: BudgetReport,
    pub evidence_ids: Vec<EventId>,
    pub position_ids: Vec<PositionId>,
    pub conflict_ids: Vec<ConflictId>,
    pub context_revision: u64,
    pub context_hash: String,
    pub reasoning_effort: String,
    pub think: Option<Think>,
}
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct ModelIoDiagnostic {
    pub transport: TransportKind,
    pub purpose: RequestPurpose,
    pub budget: BudgetReport,
    pub observed_context_tokens: Option<u32>,
    pub prompt_tokens: Option<u64>,
    pub completion_tokens: Option<u64>,
    pub finish_reason: Option<String>,
    pub content_type: String,
    pub content_bytes: Option<usize>,
    pub request_hash: String,
    pub response_hash: Option<String>,
    pub elapsed_ms: u64,
    pub error_kind: Option<String>,
}
#[derive(Clone, Debug)]
pub struct PromptItem {
    pub value: Value,
    pub evidence_ids: Vec<EventId>,
}
#[derive(Clone, Debug)]
pub struct RenderedPrompt {
    pub system: String,
    pub required: PromptItem,
    pub optional: Vec<PromptItem>,
    pub position_ids: Vec<PositionId>,
    pub conflict_ids: Vec<ConflictId>,
    pub revision: u64,
    pub hash: String,
}
#[derive(Debug, thiserror::Error)]
pub enum PreparationError {
    #[error("model configuration: {0}")]
    Configuration(String),
    #[error("context_budget_exceeded: estimated input {0:?}")]
    Budget(BudgetReport),
}

impl ModelIoConfig {
    pub fn load_env(&mut self) -> Result<(), crate::config::ConfigError> {
        // A single JSON value parser handles integers, enum names and typed think.
        fn value<T: serde::de::DeserializeOwned>(
            key: &str,
        ) -> Result<Option<T>, crate::config::ConfigError> {
            let Ok(raw) = std::env::var(key) else {
                return Ok(None);
            };
            let parsed = serde_json::from_str(&raw)
                .or_else(|_| serde_json::from_value(serde_json::Value::String(raw.clone())));
            parsed
                .map(Some)
                .map_err(|_| crate::config::ConfigError::InvalidEnvironment {
                    name: key.into(),
                    value: "invalid model setting".into(),
                })
        }
        if let Some(v) = value("HEKATE_MODEL_TRANSPORT")? {
            self.transport = v;
        }
        if let Some(v) = value("HEKATE_MODEL_CONTEXT_TOKENS")? {
            self.context_tokens = Some(v);
        }
        if let Some(v) = value("HEKATE_MODEL_FOREGROUND_MAX_TOKENS")? {
            self.foreground_max_tokens = v;
        }
        if let Some(v) = value("HEKATE_MODEL_SLEEP_MAX_TOKENS")? {
            self.sleep_max_tokens = v;
        }
        if let Some(v) = value("HEKATE_MODEL_SAFETY_MARGIN")? {
            self.safety_margin = v;
        }
        if let Some(v) = value("HEKATE_MODEL_FOREGROUND_REASONING_EFFORT")? {
            self.foreground_reasoning_effort = v;
        }
        if let Some(v) = value("HEKATE_MODEL_SLEEP_REASONING_EFFORT")? {
            self.sleep_reasoning_effort = v;
        }
        if let Some(v) = value("HEKATE_MODEL_FOREGROUND_THINK")? {
            self.foreground_think = Some(v);
        }
        if let Some(v) = value("HEKATE_MODEL_SLEEP_THINK")? {
            self.sleep_think = Some(v);
        }
        Ok(())
    }
}
