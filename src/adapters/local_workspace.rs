use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde::Serialize;
use sha2::{Digest, Sha256};
use similar::TextDiff;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum WorkspaceError {
    #[error("workspace I/O failed: {0}")]
    Io(String),
    #[error("workspace file is not UTF-8: {0}")]
    Utf8(String),
    #[error("path escapes the workspace root")]
    Escape,
    #[error("path is not a file")]
    NotFile,
}

#[derive(Clone)]
pub struct LocalWorkspace {
    root: PathBuf,
}

#[derive(Clone, Debug, Serialize)]
pub struct WorkspaceFile {
    pub path: String,
    pub content: String,
    pub content_hash: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct WorkspaceMetadata {
    pub path: String,
    pub size: u64,
    pub is_file: bool,
    pub is_directory: bool,
    pub content_hash: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct WorkspaceMatch {
    pub path: String,
    pub line: usize,
    pub text: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct WorkspaceDiff {
    pub path: String,
    pub diff: String,
}

impl LocalWorkspace {
    pub fn new(root: impl AsRef<Path>) -> Result<Self, WorkspaceError> {
        let root =
            std::fs::canonicalize(root).map_err(|error| WorkspaceError::Io(error.to_string()))?;
        Ok(Self { root })
    }

    pub async fn list(&self, relative: &str) -> Result<Vec<String>, WorkspaceError> {
        let start = self.allowed_path(relative)?;
        let mut pending = vec![start];
        let mut visited = BTreeSet::new();
        let mut files = Vec::new();
        while let Some(directory) = pending.pop() {
            if !visited.insert(directory.clone()) {
                continue;
            }
            let mut entries = tokio::fs::read_dir(&directory)
                .await
                .map_err(|error| WorkspaceError::Io(error.to_string()))?;
            while let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|error| WorkspaceError::Io(error.to_string()))?
            {
                let path = self.allowed_absolute_path(&entry.path())?;
                let metadata = tokio::fs::metadata(&path)
                    .await
                    .map_err(|error| WorkspaceError::Io(error.to_string()))?;
                if metadata.is_dir() {
                    pending.push(path);
                } else if metadata.is_file() {
                    files.push(self.relative(&path));
                }
            }
        }
        files.sort();
        Ok(files)
    }

    pub async fn read_text(&self, relative: &str) -> Result<WorkspaceFile, WorkspaceError> {
        let path = self.allowed_path(relative)?;
        let bytes = tokio::fs::read(&path)
            .await
            .map_err(|error| WorkspaceError::Io(error.to_string()))?;
        let content = String::from_utf8(bytes.clone())
            .map_err(|error| WorkspaceError::Utf8(error.to_string()))?;
        Ok(WorkspaceFile {
            path: self.relative(&path),
            content,
            content_hash: hash(&bytes),
        })
    }

    pub async fn metadata(&self, relative: &str) -> Result<WorkspaceMetadata, WorkspaceError> {
        let path = self.allowed_path(relative)?;
        let metadata = tokio::fs::metadata(&path)
            .await
            .map_err(|error| WorkspaceError::Io(error.to_string()))?;
        let content_hash = if metadata.is_file() {
            let bytes = tokio::fs::read(&path)
                .await
                .map_err(|error| WorkspaceError::Io(error.to_string()))?;
            hash(&bytes)
        } else {
            String::new()
        };
        Ok(WorkspaceMetadata {
            path: self.relative(&path),
            size: metadata.len(),
            is_file: metadata.is_file(),
            is_directory: metadata.is_dir(),
            content_hash,
        })
    }

    pub async fn hash(&self, relative: &str) -> Result<String, WorkspaceError> {
        Ok(self.read_text(relative).await?.content_hash)
    }

    pub async fn diff_text(
        &self,
        relative: &str,
        original: &str,
    ) -> Result<WorkspaceDiff, WorkspaceError> {
        let file = self.read_text(relative).await?;
        Ok(WorkspaceDiff {
            path: file.path.clone(),
            diff: TextDiff::from_lines(original, &file.content)
                .unified_diff()
                .header("provided", &file.path)
                .to_string(),
        })
    }

    pub async fn write_text(
        &self,
        relative: &str,
        content: &str,
    ) -> Result<WorkspaceFile, WorkspaceError> {
        let path = self.allowed_write_path(relative)?;
        tokio::fs::write(&path, content)
            .await
            .map_err(|error| WorkspaceError::Io(error.to_string()))?;
        self.read_text(relative).await
    }

    pub async fn search(
        &self,
        relative: &str,
        needle: &str,
    ) -> Result<Vec<WorkspaceMatch>, WorkspaceError> {
        let path = self.allowed_path(relative)?;
        let files = if tokio::fs::metadata(&path)
            .await
            .map_err(|error| WorkspaceError::Io(error.to_string()))?
            .is_dir()
        {
            self.list(relative).await?
        } else {
            vec![self.relative(&path)]
        };
        let mut matches = Vec::new();
        for file in files {
            let content = self.read_text(&file).await?;
            for (line_number, line) in content.content.lines().enumerate() {
                if line.contains(needle) {
                    matches.push(WorkspaceMatch {
                        path: file.clone(),
                        line: line_number + 1,
                        text: line.to_owned(),
                    });
                }
            }
        }
        Ok(matches)
    }

    pub(crate) fn file_path(&self, relative: &str) -> Result<PathBuf, WorkspaceError> {
        let path = self.allowed_path(relative)?;
        if !path.is_file() {
            return Err(WorkspaceError::NotFile);
        }
        Ok(path)
    }

    fn allowed_path(&self, relative: &str) -> Result<PathBuf, WorkspaceError> {
        let relative = Path::new(relative);
        if relative.is_absolute() {
            return Err(WorkspaceError::Escape);
        }
        self.allowed_absolute_path(&self.root.join(relative))
    }

    fn allowed_absolute_path(&self, candidate: &Path) -> Result<PathBuf, WorkspaceError> {
        let canonical = std::fs::canonicalize(candidate)
            .map_err(|error| WorkspaceError::Io(error.to_string()))?;
        if !canonical.starts_with(&self.root) {
            return Err(WorkspaceError::Escape);
        }
        Ok(canonical)
    }

    fn allowed_write_path(&self, relative: &str) -> Result<PathBuf, WorkspaceError> {
        let relative_path = Path::new(relative);
        if relative.is_empty() || relative_path.is_absolute() {
            return Err(WorkspaceError::Escape);
        }
        let candidate = self.root.join(relative_path);
        match std::fs::symlink_metadata(&candidate) {
            Ok(_) => {
                let canonical = std::fs::canonicalize(candidate)
                    .map_err(|error| WorkspaceError::Io(error.to_string()))?;
                if !canonical.starts_with(&self.root) || !canonical.is_file() {
                    return Err(WorkspaceError::Escape);
                }
                return Ok(canonical);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(WorkspaceError::Io(error.to_string())),
        }
        let Some(parent) = candidate.parent() else {
            return Err(WorkspaceError::Escape);
        };
        let canonical_parent =
            std::fs::canonicalize(parent).map_err(|error| WorkspaceError::Io(error.to_string()))?;
        if !canonical_parent.starts_with(&self.root) {
            return Err(WorkspaceError::Escape);
        }
        let Some(name) = candidate.file_name() else {
            return Err(WorkspaceError::Escape);
        };
        Ok(canonical_parent.join(name))
    }

    fn relative(&self, path: &Path) -> String {
        path.strip_prefix(&self.root)
            .unwrap_or(path)
            .to_string_lossy()
            .into_owned()
    }
}

fn hash(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}
