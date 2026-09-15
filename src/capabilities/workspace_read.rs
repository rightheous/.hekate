use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::adapters::local_workspace::LocalWorkspace;
use crate::ports::{Capability, CapabilityError, CapabilityManifest, CapabilityResult};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceReadOperation {
    List,
    ReadText,
    Search,
    Metadata,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WorkspaceReadRequest {
    pub operation: WorkspaceReadOperation,
    #[serde(default = "current_directory")]
    pub path: String,
    #[serde(default)]
    pub needle: Option<String>,
}

pub struct WorkspaceReadCapability {
    workspace: LocalWorkspace,
}

impl WorkspaceReadCapability {
    pub fn new(workspace: LocalWorkspace) -> Self {
        Self { workspace }
    }
}

#[async_trait]
impl Capability for WorkspaceReadCapability {
    fn manifest(&self) -> CapabilityManifest {
        CapabilityManifest {
            name: "workspace_read".to_owned(),
            description: "read UTF-8 files and metadata below the configured workspace root"
                .to_owned(),
            permission: "filesystem:read".to_owned(),
        }
    }

    async fn execute(&self, input: serde_json::Value) -> Result<CapabilityResult, CapabilityError> {
        let request: WorkspaceReadRequest = serde_json::from_value(input)
            .map_err(|error| CapabilityError::InvalidInput(error.to_string()))?;
        match request.operation {
            WorkspaceReadOperation::List => Ok(CapabilityResult {
                data: serde_json::json!({
                    "files": self.workspace.list(&request.path).await.map_err(map_error)?
                }),
                evidence: vec![request.path],
                verified: true,
            }),
            WorkspaceReadOperation::ReadText => {
                let file = self
                    .workspace
                    .read_text(&request.path)
                    .await
                    .map_err(map_error)?;
                Ok(CapabilityResult {
                    data: serde_json::to_value(&file)
                        .map_err(|error| CapabilityError::Execution(error.to_string()))?,
                    evidence: vec![file.path],
                    verified: true,
                })
            }
            WorkspaceReadOperation::Metadata => {
                let metadata = self
                    .workspace
                    .metadata(&request.path)
                    .await
                    .map_err(map_error)?;
                Ok(CapabilityResult {
                    data: serde_json::to_value(&metadata)
                        .map_err(|error| CapabilityError::Execution(error.to_string()))?,
                    evidence: vec![metadata.path.clone()],
                    verified: true,
                })
            }
            WorkspaceReadOperation::Search => {
                let needle = request
                    .needle
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        CapabilityError::InvalidInput("needle is required".to_owned())
                    })?;
                let matches = self
                    .workspace
                    .search(&request.path, &needle)
                    .await
                    .map_err(map_error)?;
                Ok(CapabilityResult {
                    data: serde_json::json!({ "matches": matches }),
                    evidence: vec![request.path],
                    verified: true,
                })
            }
        }
    }
}

fn current_directory() -> String {
    ".".to_owned()
}

fn map_error(error: crate::adapters::local_workspace::WorkspaceError) -> CapabilityError {
    CapabilityError::Execution(error.to_string())
}
