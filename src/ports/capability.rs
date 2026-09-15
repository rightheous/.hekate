use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CapabilityManifest {
    pub name: String,
    pub description: String,
    pub permission: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CapabilityResult {
    pub data: serde_json::Value,
    pub evidence: Vec<String>,
}

#[derive(Debug, Error)]
pub enum CapabilityError {
    #[error("capability input is invalid: {0}")]
    InvalidInput(String),
    #[error("capability failed: {0}")]
    Execution(String),
}

#[async_trait]
pub trait Capability: Send + Sync {
    fn manifest(&self) -> CapabilityManifest;
    async fn execute(&self, input: serde_json::Value) -> Result<CapabilityResult, CapabilityError>;
}

#[async_trait]
pub trait CapabilityCatalog: Send + Sync {
    fn names(&self) -> Vec<String>;
    async fn execute(
        &self,
        name: &str,
        input: serde_json::Value,
    ) -> Result<CapabilityResult, CapabilityError>;
}
