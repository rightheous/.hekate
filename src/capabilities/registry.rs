use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::ports::{
    Capability, CapabilityCatalog, CapabilityError, CapabilityManifest, CapabilityResult,
};

#[derive(Clone, Default)]
pub struct CapabilityRegistry {
    capabilities: BTreeMap<String, Arc<dyn Capability>>,
}

impl CapabilityRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, capability: Arc<dyn Capability>) {
        let name = capability.manifest().name;
        self.capabilities.insert(name, capability);
    }

    pub fn manifests(&self) -> Vec<CapabilityManifest> {
        self.capabilities
            .values()
            .map(|capability| capability.manifest())
            .collect()
    }

    pub fn names(&self) -> Vec<String> {
        self.capabilities.keys().cloned().collect()
    }

    pub async fn execute(
        &self,
        name: &str,
        input: serde_json::Value,
    ) -> Result<CapabilityResult, CapabilityError> {
        self.capabilities
            .get(name)
            .ok_or_else(|| CapabilityError::Execution(format!("unknown capability: {name}")))?
            .execute(input)
            .await
    }
}

#[async_trait]
impl CapabilityCatalog for CapabilityRegistry {
    fn names(&self) -> Vec<String> {
        CapabilityRegistry::names(self)
    }

    async fn execute(
        &self,
        name: &str,
        input: serde_json::Value,
    ) -> Result<CapabilityResult, CapabilityError> {
        CapabilityRegistry::execute(self, name, input).await
    }
}
