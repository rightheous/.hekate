//! HTTP only: no ledger writes or cognitive-state changes.
pub mod ollama;
pub mod openai_compatible;
use crate::core::model_io::*;
#[derive(Debug)]
pub struct ModelResponse {
    pub content: String,
    pub diagnostic: ModelIoDiagnostic,
}
#[derive(Debug)]
pub struct TransportError {
    pub kind: &'static str,
    pub diagnostic: ModelIoDiagnostic,
    pub envelope: Option<String>,
}
pub async fn send(
    _: &reqwest::Client,
    _: &str,
    _: Option<&str>,
    _: &str,
    _: TransportKind,
    _: &PreparedModelRequest,
) -> Result<ModelResponse, TransportError> {
    // Kept unreachable until request preparation is implemented; never return a fake success.
    unimplemented!("transport skeleton is not connected to PrimaryModel")
}
