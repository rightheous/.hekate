use std::sync::Arc;

use thiserror::Error;

use crate::adapters::browser::BrowserAdapter;
use crate::adapters::computer_use::ComputerUseAdapter;
use crate::adapters::docling::Docling;
use crate::adapters::git::LocalGit;
use crate::adapters::local_policy::LocalPolicy;
use crate::adapters::local_workspace::LocalWorkspace;
use crate::adapters::primary_model::PrimaryModel;
use crate::adapters::sqlite::{SqliteStore, StoreError};
use crate::capabilities::{
    BrowserCapability, BrowserOperation, CapabilityRegistry, ComputerUseCapability,
    DocumentReaderCapability, GitReadCapability, GitWriteCapability, UnavailableBrowserCapability,
    WorkspaceReadCapability, WorkspaceWriteCapability,
};
use crate::config::Config;
use crate::runtime::engine::Engine;
use crate::runtime::recovery::{recover, RecoveryError};

#[derive(Debug, Error)]
pub enum BootstrapError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Recovery(#[from] RecoveryError),
    #[error("workspace setup failed: {0}")]
    Workspace(String),
    #[error("model setup failed: {0}")]
    Model(String),
    #[error("document reader setup failed: {0}")]
    Document(String),
}

pub async fn build_engine(config: &Config) -> Result<Engine, BootstrapError> {
    let store = Arc::new(SqliteStore::open(&config.database_url).await?);
    let _ = recover(store.as_ref()).await?;

    let workspace = LocalWorkspace::new(&config.workspace_root)
        .map_err(|error| BootstrapError::Workspace(error.to_string()))?;
    let model = PrimaryModel::from_config(config).map_err(BootstrapError::Model)?;
    let docling = Docling::new(&config.docling_binary, config.document_timeout_seconds)
        .map_err(|error| BootstrapError::Document(error.to_string()))?;
    let mut registry = CapabilityRegistry::new();
    registry.register(Arc::new(WorkspaceReadCapability::new(workspace.clone())));
    registry.register(Arc::new(DocumentReaderCapability::new(
        workspace.clone(),
        docling,
    )));
    let git = LocalGit::new(&config.workspace_root)
        .await
        .map_err(|error| BootstrapError::Workspace(error.to_string()))?;
    registry.register(Arc::new(GitReadCapability::new(git.clone())));
    registry.register(Arc::new(GitWriteCapability::new(git)));
    registry.register(Arc::new(WorkspaceWriteCapability::new(workspace)));
    registry.register(Arc::new(ComputerUseCapability::new(
        ComputerUseAdapter::new(&config.workspace_root),
    )));
    let browser_root = config.workspace_root.join("var/browser");
    std::fs::create_dir_all(&browser_root)
        .map_err(|error| BootstrapError::Workspace(error.to_string()))?;
    let browser = match config.browser_cdp_endpoint.as_deref() {
        Some(endpoint) => BrowserAdapter::connect(endpoint, &browser_root)
            .await
            .map_err(|error| error.to_string()),
        None => Err("no trusted CDP endpoint configured".to_owned()),
    };
    match browser {
        Ok(browser) => {
            let browser = Arc::new(tokio::sync::Mutex::new(browser));
            for operation in BrowserOperation::ALL {
                registry.register(Arc::new(BrowserCapability::new(browser.clone(), operation)));
            }
        }
        Err(error) => {
            for operation in BrowserOperation::ALL {
                registry.register(Arc::new(UnavailableBrowserCapability::new(
                    operation,
                    error.to_string(),
                )));
            }
        }
    }

    Ok(Engine::new(
        store,
        Arc::new(model),
        Arc::new(LocalPolicy),
        Arc::new(registry),
        config.hekate_principal_id,
        config.user_principal_id,
    ))
}
