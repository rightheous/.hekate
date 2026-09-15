//! CDP execution only. No model, shell, workspace, Git, or credential-store access.
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::Duration,
};

use chromiumoxide::cdp::browser_protocol::{
    accessibility::GetFullAxTreeParams,
    browser::{BrowserContextId, SetDownloadBehaviorBehavior, SetDownloadBehaviorParams},
    dom::{
        BackendNodeId, DescribeNodeParams, FocusParams, GetBoxModelParams,
        ScrollIntoViewIfNeededParams,
    },
    input::{DispatchMouseEventParams, DispatchMouseEventType, InsertTextParams},
    page::{GetNavigationHistoryParams, NavigateToHistoryEntryParams},
    target::{CreateBrowserContextParams, CreateTargetParams},
};
use chromiumoxide::{Browser, BrowserConfig, Page};
use futures_util::StreamExt;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::{io::AsyncWriteExt, task::JoinHandle};
use url::Url;
use uuid::Uuid;

use crate::capabilities::browser::{BrowserRequest, BrowserTarget};
use crate::ports::{CapabilityError, CapabilityResult};

pub struct BrowserAdapter {
    browser: Browser,
    page: Page,
    context: BrowserContextId,
    handler: JoinHandle<()>,
    launched: bool,
    artifact_root: PathBuf,
    references: HashMap<String, BackendNodeId>,
}

// Read only the AX fields we use. Chrome adds ignored-reason enum values independently
// of this client's release; those unused fields must not break the entire snapshot.
#[derive(serde::Serialize)]
struct AccessibleTree {}

impl chromiumoxide::Method for AccessibleTree {
    fn identifier(&self) -> chromiumoxide::types::MethodId {
        GetFullAxTreeParams::IDENTIFIER.into()
    }
}

impl chromiumoxide::Command for AccessibleTree {
    type Response = AccessibleNodes;
}

#[derive(Debug, serde::Deserialize)]
struct AccessibleNodes {
    nodes: Vec<AccessibleNode>,
}

#[derive(Debug, serde::Deserialize)]
struct AccessibleNode {
    ignored: bool,
    role: Option<AccessibleValue>,
    name: Option<AccessibleValue>,
    #[serde(rename = "backendDOMNodeId")]
    backend_dom_node_id: Option<BackendNodeId>,
}

#[derive(Debug, serde::Deserialize)]
struct AccessibleValue {
    value: Option<Value>,
}

impl BrowserAdapter {
    /// Use a dedicated browser, never the user's personal profile.
    pub async fn launch(
        config: BrowserConfig,
        artifact_root: impl AsRef<Path>,
    ) -> Result<Self, CapabilityError> {
        if config.user_data_dir.is_none() {
            return Err(invalid("launch requires a dedicated user_data_dir"));
        }
        let (browser, handler) = Browser::launch(config).await.map_err(failed)?;
        Self::attach(browser, handler, artifact_root.as_ref(), true).await
    }

    /// Connect only to a trusted CDP endpoint. A fresh context isolates cookies and downloads.
    pub async fn connect(
        endpoint: &str,
        artifact_root: impl AsRef<Path>,
    ) -> Result<Self, CapabilityError> {
        let endpoint_url = Url::parse(endpoint).map_err(invalid)?;
        if !matches!(endpoint_url.scheme(), "http" | "https" | "ws" | "wss")
            || endpoint_url.host_str().is_none()
        {
            return Err(invalid("invalid CDP endpoint"));
        }
        let (browser, handler) = Browser::connect(endpoint).await.map_err(failed)?;
        Self::attach(browser, handler, artifact_root.as_ref(), false).await
    }

    async fn attach(
        mut browser: Browser,
        mut handler: chromiumoxide::Handler,
        root: &Path,
        launched: bool,
    ) -> Result<Self, CapabilityError> {
        let task = tokio::spawn(async move {
            while let Some(event) = handler.next().await {
                if let Err(error) = event {
                    // New Chrome versions can emit events absent from the pinned CDP schema.
                    // Keep polling; unanswered commands still time out and fail closed.
                    if matches!(error, chromiumoxide::error::CdpError::Serde(_)) {
                        tracing::warn!("browser received an unsupported CDP message");
                        continue;
                    }
                    tracing::warn!(%error, "browser CDP handler stopped");
                    break;
                }
            }
        });
        let setup = async {
            let artifact_root = tokio::fs::canonicalize(root).await.map_err(failed)?;
            if !tokio::fs::metadata(&artifact_root)
                .await
                .map_err(failed)?
                .is_dir()
            {
                return Err(invalid("artifact root must be an existing directory"));
            }
            let context = browser
                .create_browser_context(
                    CreateBrowserContextParams::builder()
                        .dispose_on_detach(true)
                        .build(),
                )
                .await
                .map_err(failed)?;
            let initialized = async {
                browser
                    .execute(
                        SetDownloadBehaviorParams::builder()
                            .behavior(SetDownloadBehaviorBehavior::Deny)
                            .browser_context_id(context.clone())
                            .build()
                            .map_err(invalid)?,
                    )
                    .await
                    .map_err(failed)?;
                let page = browser
                    .new_page(
                        CreateTargetParams::builder()
                            .url("about:blank")
                            .browser_context_id(context.clone())
                            .build()
                            .map_err(invalid)?,
                    )
                    .await
                    .map_err(failed)?;
                Ok::<_, CapabilityError>(page)
            }
            .await;
            match initialized {
                Ok(page) => Ok((artifact_root, context, page)),
                Err(error) => {
                    let _ = browser.dispose_browser_context(context).await;
                    Err(error)
                }
            }
        }
        .await;
        match setup {
            Ok((artifact_root, context, page)) => Ok(Self {
                browser,
                page,
                context,
                handler: task,
                launched,
                artifact_root,
                references: HashMap::new(),
            }),
            Err(error) => {
                if launched {
                    let _ = browser.kill().await;
                }
                task.abort();
                Err(error)
            }
        }
    }

    pub async fn shutdown(&mut self) -> Result<(), CapabilityError> {
        let result = self
            .browser
            .dispose_browser_context(self.context.clone())
            .await
            .map_err(failed);
        if self.launched {
            if self.browser.close().await.is_ok() {
                self.browser.wait().await.map_err(failed)?;
            } else {
                let _ = self.browser.kill().await;
            }
        }
        self.handler.abort();
        result
    }

    pub(crate) async fn execute(
        &mut self,
        request: BrowserRequest,
    ) -> Result<CapabilityResult, CapabilityError> {
        // A lost CDP response after dispatch cannot establish the external outcome.
        let verified = matches!(
            request,
            BrowserRequest::Open { .. }
                | BrowserRequest::Snapshot
                | BrowserRequest::Find { .. }
                | BrowserRequest::CurrentUrl
                | BrowserRequest::Screenshot
        );
        let data = match request {
            BrowserRequest::Open { url } => {
                let url = web_url(&url)?;
                self.references.clear();
                self.page.goto(url.as_str()).await.map_err(unknown)?;
                json!({"url": self.page.url().await.map_err(unknown)?})
            }
            BrowserRequest::Snapshot => self.snapshot(None).await?,
            BrowserRequest::Find { text } => {
                if text.is_empty() || text.len() > 1024 {
                    return Err(invalid("find text must be 1..1024 bytes"));
                }
                self.snapshot(Some(&text)).await?
            }
            BrowserRequest::CurrentUrl => json!({"url": self.page.url().await.map_err(failed)?}),
            BrowserRequest::Click { target } => {
                let node = self.resolve(target).await?;
                self.references.clear();
                self.page
                    .execute(
                        ScrollIntoViewIfNeededParams::builder()
                            .backend_node_id(node)
                            .build(),
                    )
                    .await
                    .map_err(unknown)?;
                let model = self
                    .page
                    .execute(GetBoxModelParams::builder().backend_node_id(node).build())
                    .await
                    .map_err(failed)?
                    .result
                    .model;
                let quad = model.content.inner();
                if quad.len() != 8 {
                    return Err(invalid("element has no clickable box"));
                }
                let x = (quad[0] + quad[2] + quad[4] + quad[6]) / 4.0;
                let y = (quad[1] + quad[3] + quad[5] + quad[7]) / 4.0;
                self.page
                    .click(chromiumoxide::layout::Point { x, y })
                    .await
                    .map_err(unknown)?;
                json!({"dispatched": true, "external_effect": "unverified"})
            }
            BrowserRequest::Type { target, text } => {
                if text.len() > 16384 {
                    return Err(invalid("text exceeds 16384 bytes"));
                }
                let node = self.resolve(target).await?;
                let description = self
                    .page
                    .execute(DescribeNodeParams::builder().backend_node_id(node).build())
                    .await
                    .map_err(failed)?
                    .result
                    .node;
                if description.node_name != "INPUT" && description.node_name != "TEXTAREA" {
                    return Err(invalid("typing requires an input or textarea"));
                }
                let attrs = description.attributes.unwrap_or_default();
                if attrs
                    .chunks_exact(2)
                    .any(|a| a[0].eq_ignore_ascii_case("type") && a[1].eq_ignore_ascii_case("file"))
                {
                    return Err(invalid("file uploads are not supported"));
                }
                self.references.clear();
                self.page
                    .execute(FocusParams::builder().backend_node_id(node).build())
                    .await
                    .map_err(unknown)?;
                self.page
                    .execute(InsertTextParams::new(text))
                    .await
                    .map_err(unknown)?;
                json!({"dispatched": true, "external_effect": "unverified"})
            }
            BrowserRequest::Scroll { x, y } => {
                if x.unsigned_abs() > 10000 || y.unsigned_abs() > 10000 {
                    return Err(invalid("scroll exceeds 10000 pixels"));
                }
                self.references.clear();
                self.page
                    .execute(
                        DispatchMouseEventParams::builder()
                            .r#type(DispatchMouseEventType::MouseWheel)
                            .x(1.0)
                            .y(1.0)
                            .delta_x(x as f64)
                            .delta_y(y as f64)
                            .build()
                            .map_err(invalid)?,
                    )
                    .await
                    .map_err(unknown)?;
                json!({"dispatched": true})
            }
            BrowserRequest::Back | BrowserRequest::Forward => {
                let delta = if matches!(request, BrowserRequest::Back) {
                    -1
                } else {
                    1
                };
                let history = self
                    .page
                    .execute(GetNavigationHistoryParams::default())
                    .await
                    .map_err(failed)?
                    .result;
                let index = history.current_index + delta;
                let entry = usize::try_from(index)
                    .ok()
                    .and_then(|i| history.entries.get(i))
                    .ok_or_else(|| invalid("no history entry in that direction"))?;
                web_url(&entry.url)?;
                self.references.clear();
                self.page
                    .execute(NavigateToHistoryEntryParams::new(entry.id))
                    .await
                    .map_err(unknown)?;
                tokio::time::timeout(Duration::from_secs(10), async {
                    loop {
                        let current = self
                            .page
                            .execute(GetNavigationHistoryParams::default())
                            .await
                            .map_err(unknown)?
                            .result;
                        if current
                            .entries
                            .get(current.current_index as usize)
                            .is_some_and(|item| item.id == entry.id)
                        {
                            return Ok::<_, CapabilityError>(());
                        }
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                })
                .await
                .map_err(unknown)??;
                json!({"dispatched": true})
            }
            BrowserRequest::Reload => {
                self.references.clear();
                self.page.reload().await.map_err(unknown)?;
                json!({"dispatched": true})
            }
            BrowserRequest::Screenshot => {
                let bytes = self
                    .page
                    .screenshot(chromiumoxide::page::ScreenshotParams::default())
                    .await
                    .map_err(failed)?;
                self.save_artifact(&bytes, "png").await?
            }
            BrowserRequest::Download { url } => self.download(&url).await?,
        };
        Ok(CapabilityResult {
            data: json!({"trust": "untrusted_external_data", "observation": data}),
            evidence: vec![],
            verified,
        })
    }

    async fn snapshot(&mut self, filter: Option<&str>) -> Result<Value, CapabilityError> {
        self.references.clear();
        let tree = self
            .page
            .execute(AccessibleTree {})
            .await
            .map_err(failed)?
            .result;
        let filter = filter.map(str::to_lowercase);
        let mut elements = Vec::new();
        let mut truncated = false;
        for node in tree.nodes {
            if node.ignored {
                continue;
            }
            let role = node
                .role
                .and_then(|v| v.value)
                .and_then(|v| v.as_str().map(str::to_owned))
                .unwrap_or_default();
            let name = node
                .name
                .and_then(|v| v.value)
                .and_then(|v| v.as_str().map(str::to_owned))
                .unwrap_or_default();
            if name.is_empty() && matches!(role.as_str(), "generic" | "none") {
                continue;
            }
            if filter
                .as_ref()
                .is_some_and(|f| !name.to_lowercase().contains(f))
            {
                continue;
            }
            if elements.len() == 200 {
                truncated = true;
                break;
            }
            let reference = node.backend_dom_node_id.map(|id| {
                let reference = Uuid::new_v4().to_string();
                self.references.insert(reference.clone(), id);
                reference
            });
            elements.push(json!({"ref": reference, "role": role,
                "name": name.chars().take(300).collect::<String>()}));
        }
        Ok(json!({"url": self.page.url().await.map_err(failed)?,
            "title": self.page.get_title().await.map_err(failed)?, "elements": elements, "truncated": truncated}))
    }

    async fn resolve(&self, target: BrowserTarget) -> Result<BackendNodeId, CapabilityError> {
        match target {
            BrowserTarget::Reference(reference) => self
                .references
                .get(&reference)
                .copied()
                .ok_or_else(|| invalid("expired or unknown reference; take a new snapshot")),
            BrowserTarget::Selector(selector) => {
                if selector.is_empty() || selector.len() > 1024 {
                    return Err(invalid("selector must be 1..1024 bytes"));
                }
                let elements = self.page.find_elements(selector).await.map_err(invalid)?;
                if elements.len() != 1 {
                    return Err(invalid("selector must match exactly one element"));
                }
                Ok(elements[0].backend_node_id)
            }
        }
    }

    /// Anonymous HTTP download: no browser cookies, secrets, caller paths, or suggested filenames.
    async fn download(&self, url: &str) -> Result<Value, CapabilityError> {
        let url = web_url(url)?;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::custom(|attempt| {
                if attempt.previous().len() >= 10 || web_url(attempt.url().as_str()).is_err() {
                    attempt.error("download redirect rejected")
                } else {
                    attempt.follow()
                }
            }))
            .build()
            .map_err(failed)?;
        let mut response = client
            .get(url)
            .send()
            .await
            .map_err(unknown)?
            .error_for_status()
            .map_err(failed)?;
        const LIMIT: usize = 32 * 1024 * 1024;
        if response.content_length().is_some_and(|n| n > LIMIT as u64) {
            return Err(failed("download exceeds 32 MiB"));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(unknown)? {
            if bytes.len() + chunk.len() > LIMIT {
                return Err(failed("download exceeds 32 MiB"));
            }
            bytes.extend_from_slice(&chunk);
        }
        self.save_artifact(&bytes, "download").await
    }

    async fn save_artifact(&self, bytes: &[u8], extension: &str) -> Result<Value, CapabilityError> {
        // The root and its ancestors must be runtime-owned, not writable by page content or other tenants.
        if tokio::fs::canonicalize(&self.artifact_root)
            .await
            .map_err(failed)?
            != self.artifact_root
        {
            return Err(invalid("artifact root changed"));
        }
        let path = self
            .artifact_root
            .join(format!("{}.{}", Uuid::new_v4(), extension));
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
            .map_err(failed)?;
        file.write_all(bytes)
            .await
            .map_err(|error| unknown(format!("{}: {error}", path.display())))?;
        file.sync_all()
            .await
            .map_err(|error| unknown(format!("{}: {error}", path.display())))?;
        Ok(
            json!({"path": path, "size": bytes.len(), "content_hash": format!("{:x}", Sha256::digest(bytes))}),
        )
    }
}

impl Drop for BrowserAdapter {
    fn drop(&mut self) {
        self.handler.abort();
    }
}

fn web_url(value: &str) -> Result<Url, CapabilityError> {
    let url = Url::parse(value).map_err(invalid)?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(invalid(
            "only HTTP(S) URLs without embedded credentials are allowed",
        ));
    }
    Ok(url)
}

fn invalid(error: impl std::fmt::Display) -> CapabilityError {
    CapabilityError::InvalidInput(error.to_string())
}
fn failed(error: impl std::fmt::Display) -> CapabilityError {
    CapabilityError::Execution(error.to_string())
}
fn unknown(error: impl std::fmt::Display) -> CapabilityError {
    CapabilityError::OutcomeUnknown(error.to_string())
}

#[cfg(test)]
mod tests;
