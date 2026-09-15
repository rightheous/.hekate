use async_trait::async_trait;
use thiserror::Error;

use crate::core::{CognitiveTrace, ThoughtContext, ThoughtCycle};

#[derive(Debug, Error)]
pub enum CognitiveError {
    #[error("model configuration error: {message}")]
    Configuration {
        message: String,
        trace: CognitiveTrace,
    },
    #[error("model request timed out")]
    Timeout { trace: CognitiveTrace },
    #[error("model provider failed: {message}")]
    Provider {
        message: String,
        trace: CognitiveTrace,
    },
    #[error("model returned malformed thought cycle: {message}")]
    Malformed {
        message: String,
        trace: CognitiveTrace,
    },
}

impl CognitiveError {
    pub fn trace(&self) -> &CognitiveTrace {
        match self {
            Self::Configuration { trace, .. }
            | Self::Timeout { trace }
            | Self::Provider { trace, .. }
            | Self::Malformed { trace, .. } => trace,
        }
    }
}

#[async_trait]
pub trait CognitiveModel: Send + Sync {
    async fn think(&self, context: &ThoughtContext) -> Result<ThoughtCycle, CognitiveError>;
}
