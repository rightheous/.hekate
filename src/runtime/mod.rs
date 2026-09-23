pub mod completion;
pub mod contest;
pub mod context;
pub mod context_builder;
pub mod deliberation;
pub mod embedding_indexer;
pub mod engine;
pub mod focus;
pub mod projector;
pub mod recall;
pub mod recovery;
pub mod response_profile;
pub mod sleep;

pub use completion::{
    CompletionCriterionStatus, CompletionError, CompletionGate, CompletionGateResult,
    CompletionStatusReport,
};
pub use context_builder::{
    build_context_snapshot, build_context_snapshot_with_profile, ContextBuildError,
    ContextBuilderError,
};
pub use engine::{Engine, EngineError};
pub use projector::{ProjectionError, Projector};
pub use sleep::{SleepOnceResult, SleepOnceStatus, SleepStatus};
