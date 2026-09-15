use std::sync::Arc;

use thiserror::Error;

use crate::adapters::local_policy::LocalPolicy;
use crate::adapters::local_workspace::LocalWorkspace;
use crate::adapters::primary_model::PrimaryModel;
use crate::adapters::sqlite::{SqliteStore, StoreError};
use crate::capabilities::{CapabilityRegistry, WorkspaceReadCapability, WorkspaceWriteCapability};
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
}

pub async fn build_engine(config: &Config) -> Result<Engine, BootstrapError> {
    let store = Arc::new(SqliteStore::open(&config.database_url).await?);
    let _ = recover(store.as_ref()).await?;

    let workspace = LocalWorkspace::new(&config.workspace_root)
        .map_err(|error| BootstrapError::Workspace(error.to_string()))?;
    let model = PrimaryModel::from_config(config).map_err(BootstrapError::Model)?;
    let mut registry = CapabilityRegistry::new();
    registry.register(Arc::new(WorkspaceReadCapability::new(workspace.clone())));
    registry.register(Arc::new(WorkspaceWriteCapability::new(workspace)));

    Ok(Engine::new(
        store,
        Arc::new(model),
        Arc::new(LocalPolicy),
        Arc::new(registry),
        config.hekate_principal_id,
        config.user_principal_id,
    ))
}
