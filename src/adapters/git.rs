use std::path::{Path, PathBuf};

use serde::Serialize;
use thiserror::Error;
use tokio::process::Command;

const GIT_BINARY: &str = "/usr/bin/git";

#[derive(Debug, Error)]
pub enum GitError {
    #[error("git I/O failed: {0}")]
    Io(String),
    #[error("configured repository root is not a Git repository")]
    NotRepository,
    #[error("path escapes the repository root")]
    Escape,
    #[error("invalid Git reference")]
    InvalidReference,
    #[error("invalid Git branch name")]
    InvalidBranch,
    #[error("git {operation} failed ({status}): {stderr}")]
    CommandFailed {
        operation: &'static str,
        status: String,
        stderr: String,
    },
}

#[derive(Clone, Debug, Serialize)]
pub struct GitOutput {
    pub output: String,
}

#[derive(Clone)]
pub struct LocalGit {
    root: PathBuf,
}

impl LocalGit {
    pub async fn new(root: impl AsRef<Path>) -> Result<Self, GitError> {
        let root = std::fs::canonicalize(root).map_err(|error| GitError::Io(error.to_string()))?;
        let git = Self { root };
        let repository_root = git
            .run(
                "rev-parse",
                args(&["--no-pager", "rev-parse", "--show-toplevel"]),
            )
            .await
            .map_err(|error| match error {
                GitError::CommandFailed { .. } => GitError::NotRepository,
                error => error,
            })?;
        let repository_root = std::fs::canonicalize(repository_root.output.trim())
            .map_err(|error| GitError::Io(error.to_string()))?;
        if repository_root != git.root {
            return Err(GitError::NotRepository);
        }
        Ok(git)
    }

    pub async fn status(&self) -> Result<GitOutput, GitError> {
        self.run(
            "status",
            args(&["--no-pager", "status", "--short", "--branch"]),
        )
        .await
    }

    pub async fn diff(&self) -> Result<GitOutput, GitError> {
        self.run("diff", args(&["--no-pager", "diff", "--no-ext-diff"]))
            .await
    }

    pub async fn log(&self, limit: usize) -> Result<GitOutput, GitError> {
        self.run(
            "log",
            vec![
                "--no-pager".to_owned(),
                "log".to_owned(),
                format!("--max-count={}", limit.min(100)),
                "--format=%H%x09%s".to_owned(),
            ],
        )
        .await
    }

    pub async fn show(&self, revision: &str) -> Result<GitOutput, GitError> {
        valid_reference(revision)?;
        self.run(
            "show",
            vec![
                "--no-pager".to_owned(),
                "show".to_owned(),
                "--no-ext-diff".to_owned(),
                "--format=medium".to_owned(),
                revision.to_owned(),
            ],
        )
        .await
    }

    pub async fn branch_list(&self) -> Result<GitOutput, GitError> {
        self.run(
            "branch_list",
            args(&["--no-pager", "branch", "--format=%(refname:short)"]),
        )
        .await
    }

    pub async fn add(&self, paths: &[String]) -> Result<GitOutput, GitError> {
        if paths.is_empty() {
            return Err(GitError::Escape);
        }
        let mut args = args(&["--no-pager", "add", "--"]);
        for path in paths {
            args.push(self.repository_path(path)?);
        }
        self.run("add", args).await
    }

    pub async fn commit(&self, message: &str) -> Result<GitOutput, GitError> {
        if message.trim().is_empty() {
            return Err(GitError::Io("commit message is required".to_owned()));
        }
        self.run(
            "commit",
            vec![
                "--no-pager".to_owned(),
                "commit".to_owned(),
                "-m".to_owned(),
                message.to_owned(),
            ],
        )
        .await
    }

    pub async fn branch_create(&self, branch: &str) -> Result<GitOutput, GitError> {
        valid_branch(branch)?;
        self.run(
            "branch_create",
            vec![
                "--no-pager".to_owned(),
                "branch".to_owned(),
                branch.to_owned(),
            ],
        )
        .await
    }

    pub async fn checkout(&self, branch: &str) -> Result<GitOutput, GitError> {
        valid_reference(branch)?;
        self.run(
            "checkout",
            vec![
                "--no-pager".to_owned(),
                "checkout".to_owned(),
                "--quiet".to_owned(),
                branch.to_owned(),
            ],
        )
        .await
    }

    async fn run(&self, operation: &'static str, args: Vec<String>) -> Result<GitOutput, GitError> {
        let output = Command::new(GIT_BINARY)
            .args(args)
            .current_dir(&self.root)
            .env("GIT_TERMINAL_PROMPT", "0")
            .output()
            .await
            .map_err(|error| GitError::Io(error.to_string()))?;
        if !output.status.success() {
            return Err(GitError::CommandFailed {
                operation,
                status: output.status.to_string(),
                stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            });
        }
        Ok(GitOutput {
            output: String::from_utf8_lossy(&output.stdout).into_owned(),
        })
    }

    fn repository_path(&self, path: &str) -> Result<String, GitError> {
        let path = Path::new(path);
        if path.is_absolute() {
            return Err(GitError::Escape);
        }
        let canonical = std::fs::canonicalize(self.root.join(path))
            .map_err(|error| GitError::Io(error.to_string()))?;
        let relative = canonical
            .strip_prefix(&self.root)
            .map_err(|_| GitError::Escape)?;
        if relative.as_os_str().is_empty() {
            return Ok(".".to_owned());
        }
        relative
            .to_str()
            .map(str::to_owned)
            .ok_or_else(|| GitError::Io("repository path is not UTF-8".to_owned()))
    }
}

fn args(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn valid_reference(value: &str) -> Result<(), GitError> {
    if value.is_empty()
        || value.starts_with('-')
        || value.chars().any(|character| {
            matches!(
                character,
                '\0' | ' ' | '~' | '^' | ':' | '?' | '*' | '[' | '\\'
            )
        })
        || value.contains("..")
        || value.contains("@{")
    {
        return Err(GitError::InvalidReference);
    }
    Ok(())
}

fn valid_branch(value: &str) -> Result<(), GitError> {
    valid_reference(value).map_err(|_| GitError::InvalidBranch)?;
    if value.starts_with('/') || value.ends_with('/') || value.ends_with('.') {
        return Err(GitError::InvalidBranch);
    }
    Ok(())
}
