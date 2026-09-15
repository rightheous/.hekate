use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::adapters::git::{GitError, LocalGit};
use crate::ports::{Capability, CapabilityError, CapabilityManifest, CapabilityResult};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GitReadOperation {
    Status,
    Diff,
    Log,
    Show,
    BranchList,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GitReadRequest {
    pub operation: GitReadOperation,
    #[serde(default)]
    pub revision: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GitWriteOperation {
    Add,
    Commit,
    BranchCreate,
    Checkout,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GitWriteRequest {
    pub operation: GitWriteOperation,
    #[serde(default)]
    pub paths: Vec<String>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub branch: Option<String>,
}

pub struct GitReadCapability {
    git: LocalGit,
}

pub struct GitWriteCapability {
    git: LocalGit,
}

impl GitReadCapability {
    pub fn new(git: LocalGit) -> Self {
        Self { git }
    }
}

impl GitWriteCapability {
    pub fn new(git: LocalGit) -> Self {
        Self { git }
    }
}

#[async_trait]
impl Capability for GitReadCapability {
    fn manifest(&self) -> CapabilityManifest {
        CapabilityManifest {
            name: "git_read".to_owned(),
            description: "read Git state from the configured repository root".to_owned(),
            permission: "git:read".to_owned(),
        }
    }

    async fn execute(&self, input: serde_json::Value) -> Result<CapabilityResult, CapabilityError> {
        let request: GitReadRequest = serde_json::from_value(input)
            .map_err(|error| CapabilityError::InvalidInput(error.to_string()))?;
        let output = match request.operation {
            GitReadOperation::Status => self.git.status().await,
            GitReadOperation::Diff => self.git.diff().await,
            GitReadOperation::Log => self.git.log(request.limit.unwrap_or(20)).await,
            GitReadOperation::Show => {
                self.git
                    .show(request.revision.as_deref().unwrap_or("HEAD"))
                    .await
            }
            GitReadOperation::BranchList => self.git.branch_list().await,
        }
        .map_err(map_error)?;
        Ok(CapabilityResult {
            data: serde_json::to_value(output)
                .map_err(|error| CapabilityError::Execution(error.to_string()))?,
            evidence: vec!["git".to_owned()],
        })
    }
}

#[async_trait]
impl Capability for GitWriteCapability {
    fn manifest(&self) -> CapabilityManifest {
        CapabilityManifest {
            name: "git_write".to_owned(),
            description: "mutate Git state after runtime authorization".to_owned(),
            permission: "git:write".to_owned(),
        }
    }

    async fn execute(&self, input: serde_json::Value) -> Result<CapabilityResult, CapabilityError> {
        let request: GitWriteRequest = serde_json::from_value(input)
            .map_err(|error| CapabilityError::InvalidInput(error.to_string()))?;
        let output = match request.operation {
            GitWriteOperation::Add => self.git.add(&request.paths).await,
            GitWriteOperation::Commit => {
                self.git
                    .commit(required(&request.message, "message")?)
                    .await
            }
            GitWriteOperation::BranchCreate => {
                self.git
                    .branch_create(required(&request.branch, "branch")?)
                    .await
            }
            GitWriteOperation::Checkout => {
                self.git
                    .checkout(required(&request.branch, "branch")?)
                    .await
            }
        }
        .map_err(map_error)?;
        Ok(CapabilityResult {
            data: serde_json::to_value(output)
                .map_err(|error| CapabilityError::Execution(error.to_string()))?,
            evidence: vec!["git".to_owned()],
        })
    }
}

fn required<'a>(value: &'a Option<String>, name: &str) -> Result<&'a str, CapabilityError> {
    value
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| CapabilityError::InvalidInput(format!("{name} is required")))
}

fn map_error(error: GitError) -> CapabilityError {
    CapabilityError::Execution(error.to_string())
}
