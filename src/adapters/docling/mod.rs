use std::path::{Path, PathBuf};
use std::time::Duration;

use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::process::Command;

#[derive(Clone)]
pub struct Docling {
    binary: PathBuf,
    timeout: Duration,
}

#[derive(Debug, Error)]
pub enum DoclingError {
    #[error("docling binary path must be absolute")]
    InvalidBinaryPath,
    #[error("docling timed out")]
    Timeout,
    #[error("could not start docling: {0}")]
    Spawn(String),
    #[error("docling exited unsuccessfully (code {exit_code:?}, stderr SHA-256 {stderr_hash})")]
    Failed {
        exit_code: Option<i32>,
        stderr_hash: String,
    },
    #[error("docling returned non-UTF-8 output")]
    NonUtf8Output,
}

#[derive(Clone, Debug)]
pub struct DoclingOutput {
    pub markdown: String,
    pub version: String,
}

impl Docling {
    pub fn new(binary: impl Into<PathBuf>, timeout_seconds: u64) -> Result<Self, DoclingError> {
        let binary = binary.into();
        if !binary.is_absolute() {
            return Err(DoclingError::InvalidBinaryPath);
        }
        Ok(Self {
            binary,
            timeout: Duration::from_secs(timeout_seconds.max(1)),
        })
    }

    pub async fn convert(&self, source: &Path) -> Result<DoclingOutput, DoclingError> {
        let output = self.run(["--to", "md"], Some(source)).await?;
        let markdown = String::from_utf8(output.stdout).map_err(|_| DoclingError::NonUtf8Output)?;
        let version = String::from_utf8(self.run(["--version"], None).await?.stdout)
            .map_err(|_| DoclingError::NonUtf8Output)?
            .trim()
            .to_owned();
        Ok(DoclingOutput { markdown, version })
    }

    async fn run(
        &self,
        arguments: impl IntoIterator<Item = &'static str>,
        source: Option<&Path>,
    ) -> Result<std::process::Output, DoclingError> {
        let mut command = Command::new(&self.binary);
        command.args(arguments);
        if let Some(source) = source {
            command.arg(source);
        }
        command.kill_on_drop(true);
        let output = tokio::time::timeout(self.timeout, command.output())
            .await
            .map_err(|_| DoclingError::Timeout)?
            .map_err(|error| DoclingError::Spawn(error.to_string()))?;
        if output.status.success() {
            Ok(output)
        } else {
            output_or_error(output)
        }
    }
}

fn output_or_error(output: std::process::Output) -> Result<std::process::Output, DoclingError> {
    Err(DoclingError::Failed {
        exit_code: output.status.code(),
        stderr_hash: hash(&output.stderr),
    })
}

fn hash(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
