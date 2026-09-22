use std::sync::Arc;

use thiserror::Error;

use crate::adapters::browser::BrowserAdapter;
use crate::adapters::computer_use::ComputerUseAdapter;
use crate::adapters::docling::Docling;
use crate::adapters::embedding::OpenAiEmbeddingAdapter;
use crate::adapters::git::LocalGit;
use crate::adapters::local_policy::LocalPolicy;
use crate::adapters::local_workspace::LocalWorkspace;
use crate::adapters::primary_model::PrimaryModel;
use crate::adapters::sqlite::{SqliteEmbeddingStore, SqliteStore, StoreError};
use crate::capabilities::{
    BrowserCapability, BrowserOperation, CapabilityRegistry, ComputerUseCapability,
    DocumentReaderCapability, GitReadCapability, GitWriteCapability, UnavailableBrowserCapability,
    WorkspaceReadCapability, WorkspaceWriteCapability,
};
use crate::config::Config;
use crate::core::{
    EmbeddingSpace, ResponseProfileResolution, DEFAULT_DOCUMENT_PREFIX, DEFAULT_QUERY_PREFIX,
};
use crate::ports::StorageError;
use crate::runtime::embedding_indexer::EmbeddingIndexer;
use crate::runtime::engine::Engine;
use crate::runtime::recall::SemanticRecall;
use crate::runtime::recovery::{recover, RecoveryError};
use crate::runtime::response_profile::resolve_response_profile_with_report;

#[derive(Debug, Error)]
pub enum BootstrapError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Recovery(#[from] RecoveryError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("workspace setup failed: {0}")]
    Workspace(String),
    #[error("model setup failed: {0}")]
    Model(String),
    #[error("document reader setup failed: {0}")]
    Document(String),
    #[error("embedding setup failed: {0}")]
    Embedding(String),
}

pub fn embedding_space(config: &Config) -> EmbeddingSpace {
    EmbeddingSpace::new(
        "openai-compatible",
        &config.embedding_model,
        &config.embedding_revision,
        config.embedding_dimensions,
        true,
        DEFAULT_QUERY_PREFIX,
        DEFAULT_DOCUMENT_PREFIX,
    )
}

pub fn build_embedding_provider(
    config: &Config,
) -> Result<Option<OpenAiEmbeddingAdapter>, BootstrapError> {
    if !config.embedding_enabled {
        return Ok(None);
    }
    OpenAiEmbeddingAdapter::from_config(config, embedding_space(config))
        .map(Some)
        .map_err(BootstrapError::Embedding)
}

pub async fn build_engine(config: &Config) -> Result<Engine, BootstrapError> {
    let store = Arc::new(SqliteStore::open(&config.database_url).await?);
    let _ = recover(store.as_ref()).await?;

    let workspace = LocalWorkspace::new(&config.workspace_root)
        .map_err(|error| BootstrapError::Workspace(error.to_string()))?;
    let model = Arc::new(PrimaryModel::from_config(config).map_err(BootstrapError::Model)?);
    let recall = if let Some(provider) = build_embedding_provider(config)? {
        let embedding_store = Arc::new(
            SqliteEmbeddingStore::open(&config.database_url)
                .await
                .map_err(|error| BootstrapError::Embedding(error.to_string()))?,
        );
        let indexer = Arc::new(EmbeddingIndexer::new(
            Arc::new(provider),
            embedding_store,
            config.embedding_batch_size,
        ));
        Some(Arc::new(SemanticRecall::new(indexer)))
    } else {
        None
    };
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

    let engine = Engine::new(
        store,
        model.clone(),
        Arc::new(LocalPolicy),
        Arc::new(registry),
        config.hekate_principal_id,
        config.user_principal_id,
    )
    .with_sleep_model(model);
    Ok(match recall {
        Some(recall) => engine.with_semantic_recall(recall),
        None => engine,
    })
}

pub async fn build_response_profile(
    config: &Config,
) -> Result<ResponseProfileResolution, BootstrapError> {
    let store = SqliteStore::open(&config.database_url).await?;
    let state = store.state().await?;
    let events = store.events().await?;
    Ok(resolve_response_profile_with_report(
        &state,
        &events,
        config.user_principal_id,
        None,
        None,
        state.revision,
    ))
}
