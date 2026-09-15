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
        Ok(config)
    }
}

fn default_database_url() -> String {
    "sqlite://var/hekate.db".to_owned()
}

fn default_workspace_root() -> PathBuf {
    PathBuf::from(".")
}

fn default_model_timeout_seconds() -> u64 {
    120
}

fn default_model_base_url() -> String {
    "http://127.0.0.1:19190/v1".to_owned()
}

fn default_model_name() -> String {
    "hekate-qwen".to_owned()
}

fn default_hekate_id() -> PrincipalId {
    PrincipalId::from_uuid(Uuid::from_u128(1))
}

fn default_user_id() -> PrincipalId {
    PrincipalId::from_uuid(Uuid::from_u128(2))
}
