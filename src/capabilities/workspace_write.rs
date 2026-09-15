use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::adapters::local_workspace::LocalWorkspace;
use crate::ports::{Capability, CapabilityError, CapabilityManifest, CapabilityResult};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceWriteOperation {
    WriteText,
    Create,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WorkspaceWriteRequest {
    pub operation: WorkspaceWriteOperation,
    pub path: String,
    pub content: String,
}

pub struct WorkspaceWriteCapability {
    workspace: LocalWorkspace,
}

impl WorkspaceWriteCapability {
    pub fn new(workspace: LocalWorkspace) -> Self {
        Self { workspace }
    }
}

#[async_trait]
impl Capability for WorkspaceWriteCapability {
    fn manifest(&self) -> CapabilityManifest {
        CapabilityManifest {
            name: "workspace_write".to_owned(),
            description: "write one UTF-8 file below the configured workspace root".to_owned(),
            permission: "filesystem:write".to_owned(),
        }
    }

    async fn execute(&self, input: serde_json::Value) -> Result<CapabilityResult, CapabilityError> {
        let request: WorkspaceWriteRequest = serde_json::from_value(input)
            .map_err(|error| CapabilityError::InvalidInput(error.to_string()))?;
        let file = self
            .workspace
            .write_text(&request.path, &request.content)
            .await
            .map_err(|error| CapabilityError::Execution(error.to_string()))?;
        Ok(CapabilityResult {
            data: serde_json::to_value(&file)
                .map_err(|error| CapabilityError::Execution(error.to_string()))?,
            evidence: vec![file.path],
            verified: true,
        })
    }
}
