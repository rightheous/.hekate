use crate::core::{model_io::*, SleepContext, ThoughtContext};
pub fn foreground(_: &ThoughtContext) -> Result<RenderedPrompt, PreparationError> {
    Err(PreparationError::Configuration(
        "foreground renderer is not wired yet".into(),
    ))
}
pub fn sleep(_: &SleepContext) -> Result<RenderedPrompt, PreparationError> {
    Err(PreparationError::Configuration(
        "Sleep renderer is not wired yet".into(),
    ))
}
