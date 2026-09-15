use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::adapters::docling::{Docling, DoclingError};
use crate::adapters::local_workspace::{LocalWorkspace, WorkspaceError};
use crate::ports::{Capability, CapabilityError, CapabilityManifest, CapabilityResult};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DocumentReaderRequest {
    pub path: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DocumentProjection {
    pub source_path: String,
    pub source_content_hash: String,
    pub media_type: String,
    pub parser_backend: String,
    pub parser_version: String,
    pub conversion_timestamp: String,
    pub untrusted: bool,
    pub anchors: Vec<String>,
    pub derived_artifact_hash: String,
    pub markdown: String,
}

pub struct DocumentReaderCapability {
    workspace: LocalWorkspace,
    docling: Docling,
}

impl DocumentReaderCapability {
    pub fn new(workspace: LocalWorkspace, docling: Docling) -> Self {
        Self { workspace, docling }
    }

    async fn read(&self, relative: &str) -> Result<DocumentProjection, DocumentReaderError> {
        let source = self.workspace.file_path(relative)?;
        let bytes = tokio::fs::read(&source)
            .await
            .map_err(|error| WorkspaceError::Io(error.to_string()))?;
        let media_type = media_type(&source)?;
        let (markdown, parser_backend, parser_version) = match media_type {
            "text/markdown" | "text/plain" => (
                String::from_utf8(bytes.clone())
                    .map_err(|error| WorkspaceError::Utf8(error.to_string()))?,
                "native_utf8".to_owned(),
                env!("CARGO_PKG_VERSION").to_owned(),
            ),
            _ => {
                let output = self.docling.convert(&source).await?;
                (output.markdown, "docling-rs".to_owned(), output.version)
            }
        };
        Ok(DocumentProjection {
            source_path: relative.to_owned(),
            source_content_hash: hash(&bytes),
            media_type: media_type.to_owned(),
            parser_backend,
            parser_version,
            conversion_timestamp: crate::core::model::now(),
            untrusted: true,
            anchors: Vec::new(),
            derived_artifact_hash: hash(markdown.as_bytes()),
            markdown,
        })
    }
}

#[async_trait]
impl Capability for DocumentReaderCapability {
    fn manifest(&self) -> CapabilityManifest {
        CapabilityManifest {
            name: "document_reader".to_owned(),
            description: "read workspace documents into an untrusted Markdown projection"
                .to_owned(),
            permission: "filesystem:read".to_owned(),
        }
    }

    async fn execute(&self, input: serde_json::Value) -> Result<CapabilityResult, CapabilityError> {
        let request: DocumentReaderRequest = serde_json::from_value(input)
            .map_err(|error| CapabilityError::InvalidInput(error.to_string()))?;
        let projection = self.read(&request.path).await.map_err(map_error)?;
        Ok(CapabilityResult {
            data: serde_json::to_value(&projection)
                .map_err(|error| CapabilityError::Execution(error.to_string()))?,
            evidence: vec![
                projection.source_path.clone(),
                projection.source_content_hash.clone(),
            ],
        })
    }
}

#[derive(Debug, thiserror::Error)]
enum DocumentReaderError {
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    #[error(transparent)]
    Docling(#[from] DoclingError),
    #[error("unsupported document media type")]
    UnsupportedMediaType,
}

fn media_type(path: &std::path::Path) -> Result<&'static str, DocumentReaderError> {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("pdf") => Ok("application/pdf"),
        Some("docx") => {
            Ok("application/vnd.openxmlformats-officedocument.wordprocessingml.document")
        }
        Some("pptx") => {
            Ok("application/vnd.openxmlformats-officedocument.presentationml.presentation")
        }
        Some("xlsx") => Ok("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
        Some("html" | "htm") => Ok("text/html"),
        Some("epub") => Ok("application/epub+zip"),
        Some("md" | "markdown") => Ok("text/markdown"),
        Some("txt") => Ok("text/plain"),
        _ => Err(DocumentReaderError::UnsupportedMediaType),
    }
}

fn map_error(error: DocumentReaderError) -> CapabilityError {
    CapabilityError::Execution(error.to_string())
}

fn hash(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
