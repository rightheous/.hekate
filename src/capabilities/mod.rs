pub mod computer_use;
pub mod document_reader;
pub mod git;
pub mod registry;
pub mod workspace_read;
pub mod workspace_write;

pub use browser::{BrowserCapability, BrowserOperation, UnavailableBrowserCapability};
pub use computer_use::{ComputerUseCapability, ComputerUseRequest};
pub use document_reader::{DocumentProjection, DocumentReaderCapability, DocumentReaderRequest};
pub use git::{
    GitReadCapability, GitReadOperation, GitReadRequest, GitWriteCapability, GitWriteOperation,
    GitWriteRequest,
};
pub use registry::CapabilityRegistry;
pub use workspace_read::{WorkspaceReadCapability, WorkspaceReadOperation, WorkspaceReadRequest};
pub use workspace_write::{
    WorkspaceWriteCapability, WorkspaceWriteOperation, WorkspaceWriteRequest,
};
pub mod browser;
