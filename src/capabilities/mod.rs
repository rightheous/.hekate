pub mod git;
pub mod registry;
pub mod workspace_read;
pub mod workspace_write;

pub use git::{
    GitReadCapability, GitReadOperation, GitReadRequest, GitWriteCapability, GitWriteOperation,
    GitWriteRequest,
};
pub use registry::CapabilityRegistry;
pub use workspace_read::{WorkspaceReadCapability, WorkspaceReadOperation, WorkspaceReadRequest};
pub use workspace_write::{
    WorkspaceWriteCapability, WorkspaceWriteOperation, WorkspaceWriteRequest,
};
