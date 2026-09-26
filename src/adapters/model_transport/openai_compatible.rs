use super::*;
use serde::Deserialize;
use serde_json::json;

pub fn body(model: &str, request: &PreparedModelRequest) -> Value {
    let mut value = json!({"model": model, "temperature": 0,
        "max_tokens": request.budget.reserved_output_tokens, "messages": request.messages});
    match request.purpose {
        RequestPurpose::Foreground => value["reasoning_effort"] = json!(request.reasoning_effort),
        RequestPurpose::Sleep => {
            value["model_options"] = json!({"reasoning_effort":request.reasoning_effort})
        }
    }
    value
}
pub(super) fn metadata(value: &Value, diagnostic: &mut ModelIoDiagnostic) {
    diagnostic.prompt_tokens = value
        .pointer("/usage/prompt_tokens")
        .and_then(Value::as_u64);
    diagnostic.completion_tokens = value
        .pointer("/usage/completion_tokens")
        .and_then(Value::as_u64);
    diagnostic.finish_reason = value
        .pointer("/choices/0/finish_reason")
        .and_then(Value::as_str)
        .map(|s| diagnostic_token(s, 32));
    content_metadata(value.pointer("/choices/0/message/content"), diagnostic);
}
#[derive(Deserialize)]
struct Envelope {
    choices: Vec<Choice>,
}
#[derive(Deserialize)]
struct Choice {
    finish_reason: Option<String>,
    message: Message,
}
#[derive(Deserialize)]
struct Message {
    content: Option<String>,
    #[serde(default)]
    tool_calls: Vec<Value>,
}
pub(super) fn decode(raw: &str) -> DecodeResult {
    let value: Value =
        serde_json::from_str(raw).map_err(|e| ("malformed_response", Some(e.classify())))?;
    if value
        .pointer("/choices/0/finish_reason")
        .and_then(Value::as_str)
        == Some("length")
    {
        return Err(("truncated_response", None));
    }
    let envelope: Envelope =
        serde_json::from_value(value).map_err(|e| ("malformed_response", Some(e.classify())))?;
    let choice = envelope
        .choices
        .first()
        .ok_or(("malformed_response", None))?;
    if choice.finish_reason.as_deref() != Some("stop") || !choice.message.tool_calls.is_empty() {
        return Err(("incomplete_response", None));
    }
    choice
        .message
        .content
        .clone()
        .filter(|v| !v.trim().is_empty())
        .ok_or(("empty_content", None))
}
