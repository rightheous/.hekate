use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::core::PrincipalId;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    #[serde(default = "default_database_url")]
    pub database_url: String,
    #[serde(default = "default_workspace_root")]
    pub workspace_root: PathBuf,
    #[serde(default = "default_hekate_id")]
    pub hekate_principal_id: PrincipalId,
    #[serde(default = "default_user_id")]
    pub user_principal_id: PrincipalId,
    #[serde(default = "default_model_base_url")]
    pub model_base_url: String,
    #[serde(default, skip_serializing)]
    pub model_api_key: Option<String>,
    #[serde(default = "default_model_name")]
    pub model_name: String,
    #[serde(default = "default_model_timeout_seconds")]
    pub model_timeout_seconds: u64,
    #[serde(default = "default_docling_binary")]
    pub docling_binary: PathBuf,
    #[serde(default = "default_document_timeout_seconds")]
    pub document_timeout_seconds: u64,
    #[serde(default)]
    pub browser_cdp_endpoint: Option<String>,
    #[serde(default = "default_embedding_enabled")]
    pub embedding_enabled: bool,
    #[serde(default = "default_embedding_url")]
    pub embedding_url: String,
    #[serde(default = "default_embedding_model")]
    pub embedding_model: String,
    #[serde(default = "default_embedding_revision")]
    pub embedding_revision: String,
    #[serde(default = "default_embedding_dimensions")]
    pub embedding_dimensions: usize,
    #[serde(default = "default_embedding_timeout_seconds")]
    pub embedding_timeout_seconds: u64,
    #[serde(default = "default_embedding_batch_size")]
    pub embedding_batch_size: usize,
    #[serde(default = "default_sleep_worker_idle_seconds")]
    pub sleep_worker_idle_seconds: u64,
    #[serde(default = "default_sleep_worker_completed_seconds")]
    pub sleep_worker_completed_seconds: u64,
    #[serde(default = "default_sleep_worker_deferred_seconds")]
    pub sleep_worker_deferred_seconds: u64,
    #[serde(default = "default_sleep_worker_initial_backoff_seconds")]
    pub sleep_worker_initial_backoff_seconds: u64,
    #[serde(default = "default_sleep_worker_max_backoff_seconds")]
    pub sleep_worker_max_backoff_seconds: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            database_url: default_database_url(),
            workspace_root: default_workspace_root(),
            hekate_principal_id: default_hekate_id(),
            user_principal_id: default_user_id(),
            model_base_url: default_model_base_url(),
            model_api_key: None,
            model_name: default_model_name(),
            model_timeout_seconds: default_model_timeout_seconds(),
            docling_binary: default_docling_binary(),
            document_timeout_seconds: default_document_timeout_seconds(),
            browser_cdp_endpoint: None,
            embedding_enabled: default_embedding_enabled(),
            embedding_url: default_embedding_url(),
            embedding_model: default_embedding_model(),
            embedding_revision: default_embedding_revision(),
            embedding_dimensions: default_embedding_dimensions(),
            embedding_timeout_seconds: default_embedding_timeout_seconds(),
            embedding_batch_size: default_embedding_batch_size(),
            sleep_worker_idle_seconds: default_sleep_worker_idle_seconds(),
            sleep_worker_completed_seconds: default_sleep_worker_completed_seconds(),
            sleep_worker_deferred_seconds: default_sleep_worker_deferred_seconds(),
            sleep_worker_initial_backoff_seconds: default_sleep_worker_initial_backoff_seconds(),
            sleep_worker_max_backoff_seconds: default_sleep_worker_max_backoff_seconds(),
        }
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("could not read config {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("could not parse config: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("invalid environment variable {name}: {value}")]
    InvalidEnvironment { name: String, value: String },
    #[error("invalid config: {0}")]
    Invalid(String),
}

impl Config {
    pub fn load(path: Option<&Path>) -> Result<Self, ConfigError> {
        let mut config = match path {
            Some(path) => {
                let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
                    path: path.to_owned(),
                    source,
                })?;
                toml::from_str(&text)?
            }
            None => Self::default(),
        };
        if let Ok(value) = std::env::var("HEKATE_DATABASE_URL") {
            config.database_url = value;
        }
        if let Ok(value) = std::env::var("HEKATE_WORKSPACE_ROOT") {
            config.workspace_root = PathBuf::from(value);
        }
        if let Ok(value) = std::env::var("HEKATE_MODEL_BASE_URL") {
            config.model_base_url = value;
        }
        if let Ok(value) = std::env::var("HEKATE_MODEL_API_KEY") {
            config.model_api_key = Some(value);
        }
        if let Ok(value) = std::env::var("HEKATE_MODEL_NAME") {
            config.model_name = value;
        }
        if let Ok(value) = std::env::var("HEKATE_MODEL_TIMEOUT_SECONDS") {
            config.model_timeout_seconds =
                value.parse().map_err(|_| ConfigError::InvalidEnvironment {
                    name: "HEKATE_MODEL_TIMEOUT_SECONDS".to_owned(),
                    value,
                })?;
        }
        if let Ok(value) = std::env::var("HEKATE_DOCLING_BINARY") {
            config.docling_binary = PathBuf::from(value);
        }
        if let Ok(value) = std::env::var("HEKATE_DOCUMENT_TIMEOUT_SECONDS") {
            config.document_timeout_seconds =
                value.parse().map_err(|_| ConfigError::InvalidEnvironment {
                    name: "HEKATE_DOCUMENT_TIMEOUT_SECONDS".to_owned(),
                    value,
                })?;
        }
        if let Ok(value) = std::env::var("HEKATE_BROWSER_CDP_ENDPOINT") {
            config.browser_cdp_endpoint = Some(value);
        }
        if let Ok(value) = std::env::var("HEKATE_EMBEDDING_ENABLED") {
            config.embedding_enabled =
                value.parse().map_err(|_| ConfigError::InvalidEnvironment {
                    name: "HEKATE_EMBEDDING_ENABLED".to_owned(),
                    value,
                })?;
        }
        if let Ok(value) = std::env::var("HEKATE_EMBEDDING_URL") {
            config.embedding_url = value;
        }
        if let Ok(value) = std::env::var("HEKATE_EMBEDDING_MODEL") {
            config.embedding_model = value;
        }
        if let Ok(value) = std::env::var("HEKATE_EMBEDDING_REVISION") {
            config.embedding_revision = value;
        }
        if let Ok(value) = std::env::var("HEKATE_EMBEDDING_DIMENSIONS") {
            config.embedding_dimensions =
                value.parse().map_err(|_| ConfigError::InvalidEnvironment {
                    name: "HEKATE_EMBEDDING_DIMENSIONS".to_owned(),
                    value,
                })?;
        }
        if let Ok(value) = std::env::var("HEKATE_EMBEDDING_TIMEOUT_SECONDS") {
            config.embedding_timeout_seconds =
                value.parse().map_err(|_| ConfigError::InvalidEnvironment {
                    name: "HEKATE_EMBEDDING_TIMEOUT_SECONDS".to_owned(),
                    value,
                })?;
        }
        if let Ok(value) = std::env::var("HEKATE_EMBEDDING_BATCH_SIZE") {
            config.embedding_batch_size =
                value.parse().map_err(|_| ConfigError::InvalidEnvironment {
                    name: "HEKATE_EMBEDDING_BATCH_SIZE".to_owned(),
                    value,
                })?;
        }
        if let Ok(value) = std::env::var("HEKATE_SLEEP_WORKER_IDLE_SECONDS") {
            config.sleep_worker_idle_seconds =
                value.parse().map_err(|_| ConfigError::InvalidEnvironment {
                    name: "HEKATE_SLEEP_WORKER_IDLE_SECONDS".to_owned(),
                    value,
                })?;
        }
        if let Ok(value) = std::env::var("HEKATE_SLEEP_WORKER_COMPLETED_SECONDS") {
            config.sleep_worker_completed_seconds =
                value.parse().map_err(|_| ConfigError::InvalidEnvironment {
                    name: "HEKATE_SLEEP_WORKER_COMPLETED_SECONDS".to_owned(),
                    value,
                })?;
        }
        if let Ok(value) = std::env::var("HEKATE_SLEEP_WORKER_DEFERRED_SECONDS") {
            config.sleep_worker_deferred_seconds =
                value.parse().map_err(|_| ConfigError::InvalidEnvironment {
                    name: "HEKATE_SLEEP_WORKER_DEFERRED_SECONDS".to_owned(),
                    value,
                })?;
        }
        if let Ok(value) = std::env::var("HEKATE_SLEEP_WORKER_INITIAL_BACKOFF_SECONDS") {
            config.sleep_worker_initial_backoff_seconds =
                value.parse().map_err(|_| ConfigError::InvalidEnvironment {
                    name: "HEKATE_SLEEP_WORKER_INITIAL_BACKOFF_SECONDS".to_owned(),
                    value,
                })?;
        }
        if let Ok(value) = std::env::var("HEKATE_SLEEP_WORKER_MAX_BACKOFF_SECONDS") {
            config.sleep_worker_max_backoff_seconds =
                value.parse().map_err(|_| ConfigError::InvalidEnvironment {
                    name: "HEKATE_SLEEP_WORKER_MAX_BACKOFF_SECONDS".to_owned(),
                    value,
                })?;
        }
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.sleep_worker_idle_seconds == 0 {
            return Err(ConfigError::Invalid(
                "sleep_worker_idle_seconds must be greater than zero".to_owned(),
            ));
        }
        if self.sleep_worker_completed_seconds == 0 {
            return Err(ConfigError::Invalid(
                "sleep_worker_completed_seconds must be greater than zero".to_owned(),
            ));
        }
        if self.sleep_worker_deferred_seconds == 0 {
            return Err(ConfigError::Invalid(
                "sleep_worker_deferred_seconds must be greater than zero".to_owned(),
            ));
        }
        if self.sleep_worker_initial_backoff_seconds == 0 {
            return Err(ConfigError::Invalid(
                "sleep_worker_initial_backoff_seconds must be greater than zero".to_owned(),
            ));
        }
        if self.sleep_worker_max_backoff_seconds == 0 {
            return Err(ConfigError::Invalid(
                "sleep_worker_max_backoff_seconds must be greater than zero".to_owned(),
            ));
        }
        if self.sleep_worker_max_backoff_seconds < self.sleep_worker_initial_backoff_seconds {
            return Err(ConfigError::Invalid(
                "sleep_worker_max_backoff_seconds must be at least the initial backoff".to_owned(),
            ));
        }
        Ok(())
    }
}

fn default_database_url() -> String {
    "sqlite://var/hekate.db".to_owned()
}

fn default_workspace_root() -> PathBuf {
    PathBuf::from(".")
}

fn default_model_timeout_seconds() -> u64 {
    300
}

fn default_docling_binary() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/home/hekate"))
        .join(".local/bin/docling-rs")
}

fn default_document_timeout_seconds() -> u64 {
    300
}

fn default_model_base_url() -> String {
    "http://127.0.0.1:19191/v1".to_owned()
}

fn default_model_name() -> String {
    "orcarouter/Qwen3.8-27B-Uncensored:iq4_xs".to_owned()
}

fn default_embedding_enabled() -> bool {
    true
}

fn default_embedding_url() -> String {
    "http://127.0.0.1:19082/v1/embeddings".to_owned()
}

fn default_embedding_model() -> String {
    "embeddinggemma".to_owned()
}

fn default_embedding_revision() -> String {
    "embeddinggemma-q4_0-768-v1".to_owned()
}

fn default_embedding_dimensions() -> usize {
    768
}

fn default_embedding_timeout_seconds() -> u64 {
    30
}

fn default_embedding_batch_size() -> usize {
    16
}

fn default_sleep_worker_idle_seconds() -> u64 {
    300
}

fn default_sleep_worker_completed_seconds() -> u64 {
    30
}

fn default_sleep_worker_deferred_seconds() -> u64 {
    60
}

fn default_sleep_worker_initial_backoff_seconds() -> u64 {
    30
}

fn default_sleep_worker_max_backoff_seconds() -> u64 {
    900
}

fn default_hekate_id() -> PrincipalId {
    PrincipalId::from_uuid(Uuid::from_u128(1))
}

fn default_user_id() -> PrincipalId {
    PrincipalId::from_uuid(Uuid::from_u128(2))
}
