use async_trait::async_trait;
use serde::Deserialize;

use crate::adapters::computer_use::{ComputerUseAdapter, ComputerUseError, InteractionResult};
use crate::ports::{Capability, CapabilityError, CapabilityManifest, CapabilityResult};

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum ComputerUseRequest {
    ListWindows,
    FocusWindow {
        window_id: u32,
    },
    Screenshot,
    InspectAccessibility {
        #[serde(default = "default_max_nodes")]
        max_nodes: usize,
    },
    Click {
        x: i32,
        y: i32,
    },
    DoubleClick {
        x: i32,
        y: i32,
    },
    TypeText {
        text: String,
    },
    Key {
        key: String,
    },
    Hotkey {
        keys: Vec<String>,
    },
    Scroll {
        amount: i32,
        #[serde(default)]
        horizontal: bool,
    },
    MovePointer {
        x: i32,
        y: i32,
    },
}

pub struct ComputerUseCapability {
    adapter: ComputerUseAdapter,
}

impl ComputerUseCapability {
    pub fn new(adapter: ComputerUseAdapter) -> Self {
        Self { adapter }
    }
}

#[async_trait]
impl Capability for ComputerUseCapability {
    fn manifest(&self) -> CapabilityManifest {
        CapabilityManifest {
            name: "computer".to_owned(),
            description: "observe and operate a Linux/X11 desktop as a last-resort fallback"
                .to_owned(),
            permission: "desktop:observe|interact|sensitive".to_owned(),
        }
    }

    async fn execute(&self, input: serde_json::Value) -> Result<CapabilityResult, CapabilityError> {
        let request: ComputerUseRequest = serde_json::from_value(input)
            .map_err(|error| CapabilityError::InvalidInput(error.to_string()))?;
        match request {
            ComputerUseRequest::ListWindows => {
                let windows = self.adapter.list_windows().map_err(map_error)?;
                result(serde_json::json!({ "windows": windows }), Vec::new(), true)
            }
            ComputerUseRequest::Screenshot => {
                let screenshot = self.adapter.screenshot().map_err(map_error)?;
                let evidence = vec![screenshot.path.clone(), screenshot.content_hash.clone()];
                result(serde_json::to_value(screenshot)?, evidence, true)
            }
            ComputerUseRequest::InspectAccessibility { max_nodes } => {
                let nodes = self
                    .adapter
                    .inspect_accessibility(max_nodes)
                    .await
                    .map_err(map_error)?;
                result(serde_json::json!({ "nodes": nodes }), Vec::new(), true)
            }
            ComputerUseRequest::FocusWindow { window_id } => {
                interaction(self.adapter.focus_window(window_id).map_err(map_error)?)
            }
            ComputerUseRequest::Click { x, y } => {
                interaction(self.adapter.click(x, y, false).map_err(map_error)?)
            }
            ComputerUseRequest::DoubleClick { x, y } => {
                interaction(self.adapter.click(x, y, true).map_err(map_error)?)
            }
            ComputerUseRequest::TypeText { text } => {
                interaction(self.adapter.type_text(&text).map_err(map_error)?)
            }
            ComputerUseRequest::Key { key } => {
                interaction(self.adapter.key(&key).map_err(map_error)?)
            }
            ComputerUseRequest::Hotkey { keys } => {
                interaction(self.adapter.hotkey(&keys).map_err(map_error)?)
            }
            ComputerUseRequest::Scroll { amount, horizontal } => {
                interaction(self.adapter.scroll(amount, horizontal).map_err(map_error)?)
            }
            ComputerUseRequest::MovePointer { x, y } => {
                interaction(self.adapter.move_pointer(x, y).map_err(map_error)?)
            }
        }
    }
}

fn result(
    data: serde_json::Value,
    evidence: Vec<String>,
    verified: bool,
) -> Result<CapabilityResult, CapabilityError> {
    Ok(CapabilityResult {
        data,
        evidence,
        verified,
    })
}

fn interaction(value: InteractionResult) -> Result<CapabilityResult, CapabilityError> {
    let evidence = vec![
        format!("before:sha256:{}", value.before.screen_hash),
        format!("after:sha256:{}", value.after.screen_hash),
    ];
    result(serde_json::to_value(&value)?, evidence, value.verified)
}

fn default_max_nodes() -> usize {
    200
}

fn map_error(error: ComputerUseError) -> CapabilityError {
    match error {
        ComputerUseError::Invalid(message) => CapabilityError::InvalidInput(message),
        error => CapabilityError::Execution(error.to_string()),
    }
}

impl From<serde_json::Error> for CapabilityError {
    fn from(error: serde_json::Error) -> Self {
        Self::Execution(error.to_string())
    }
}
