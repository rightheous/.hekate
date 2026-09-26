use crate::core::model_io::*;
pub fn prepare(
    _: RenderedPrompt,
    _: &ModelIoConfig,
    _: RequestPurpose,
    _: Option<&str>,
) -> Result<PreparedModelRequest, PreparationError> {
    Err(PreparationError::Configuration(
        "model request preparation is not wired yet".into(),
    ))
}
