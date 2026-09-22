use async_trait::async_trait;
use thiserror::Error;

use crate::core::{CognitiveTrace, SleepContext, SleepDeliberation, ThoughtContext, ThoughtCycle};

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

#[derive(Debug, Error)]
pub enum SleepCognitiveError {
    #[error("sleep model configuration error: {message}")]
    Configuration {
        message: String,
        trace: CognitiveTrace,
    },
    #[error("sleep model request timed out")]
    Timeout { trace: CognitiveTrace },
    #[error("sleep model provider failed: {message}")]
    Provider {
        message: String,
        trace: CognitiveTrace,
    },
    #[error("sleep model returned malformed deliberation: {message}")]
    Malformed {
        message: String,
        trace: CognitiveTrace,
    },
}

impl SleepCognitiveError {
    pub fn trace(&self) -> &CognitiveTrace {
        match self {
            Self::Configuration { trace, .. }
            | Self::Timeout { trace }
            | Self::Provider { trace, .. }
            | Self::Malformed { trace, .. } => trace,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Configuration { .. } => "configuration",
            Self::Timeout { .. } => "timeout",
            Self::Provider { .. } => "provider_error",
            Self::Malformed { .. } => "malformed_model_output",
        }
    }
}

#[async_trait]
pub trait SleepCognitiveModel: Send + Sync {
    async fn deliberate_sleep(
        &self,
        context: &SleepContext,
    ) -> Result<SleepDeliberation, SleepCognitiveError>;
}
