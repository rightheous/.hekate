use super::*;
use serde::Deserialize;
use serde_json::json;

pub fn body(model: &str, request: &PreparedModelRequest) -> Value {
    let mut value = json!({"model": model, "messages": request.messages, "stream": false,
        "options": {"num_ctx": request.budget.context_tokens, "num_predict": request.budget.reserved_output_tokens, "temperature": 0}});
    if let Some(think) = &request.think {
        value["think"] = json!(think);
    }
    value
}
pub(super) fn metadata(value: &Value, diagnostic: &mut ModelIoDiagnostic) {
    diagnostic.prompt_tokens = value.get("prompt_eval_count").and_then(Value::as_u64);
    diagnostic.completion_tokens = value.get("eval_count").and_then(Value::as_u64);
    diagnostic.finish_reason = value
        .get("done_reason")
        .and_then(Value::as_str)
        .map(|s| diagnostic_token(s, 32));
    content_metadata(value.pointer("/message/content"), diagnostic);
}
#[derive(Deserialize)]
struct Envelope {
    done: bool,
    done_reason: Option<String>,
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
    if value.get("done_reason").and_then(Value::as_str) == Some("length") {
        return Err(("truncated_response", None));
    }
    let envelope: Envelope =
        serde_json::from_value(value).map_err(|e| ("malformed_response", Some(e.classify())))?;
    if !envelope.done
        || envelope.done_reason.as_deref() != Some("stop")
        || !envelope.message.tool_calls.is_empty()
    {
        return Err(("incomplete_response", None));
    }
    envelope
        .message
        .content
        .filter(|v| !v.trim().is_empty())
        .ok_or(("empty_content", None))
}
pub(super) fn envelope(raw: &str, category: Option<serde_json::error::Category>) -> String {
    // Preserve bounded top-level diagnostics for native envelopes too.
    response_diagnostic(raw, category)
}
