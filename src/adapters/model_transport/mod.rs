//! HTTP only: no ledger writes or cognitive-state changes.
pub mod ollama;
pub mod openai_compatible;
use crate::core::model_io::*;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::time::Instant;

#[derive(Debug)]
pub struct ModelResponse {
    pub content: String,
    pub diagnostic: ModelIoDiagnostic,
}
#[derive(Clone, Debug)]
pub struct TransportError {
    pub kind: &'static str,
    pub diagnostic: ModelIoDiagnostic,
    pub envelope: Option<String>,
}

pub fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub fn endpoint(base: &str, transport: TransportKind) -> Result<url::Url, &'static str> {
    let mut url = url::Url::parse(base).map_err(|_| "invalid model URL")?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("model URL must be HTTP(S) without embedded credentials, query or fragment");
    }
    let path = url.path().trim_end_matches('/');
    match transport {
        TransportKind::Ollama => {
            if !matches!(path, "" | "/v1" | "/api") {
                return Err("native Ollama URL supports only root, /api or /v1 suffix");
            }
            url.set_path("/api/chat");
        }
        TransportKind::OpenaiCompatible => {
            let path = if path.ends_with("/chat/completions") {
                path.to_owned()
            } else if path.is_empty() {
                "/v1/chat/completions".into()
            } else {
                format!("{path}/chat/completions")
            };
            url.set_path(&path);
        }
    }
    Ok(url)
}
impl ModelIoDiagnostic {
    pub fn prepared(transport: TransportKind, request: &PreparedModelRequest) -> Self {
        Self {
            transport,
            purpose: request.purpose,
            budget: request.budget.clone(),
            observed_context_tokens: None,
            prompt_tokens: None,
            completion_tokens: None,
            finish_reason: None,
            content_type: "missing".into(),
            content_bytes: None,
            request_hash: String::new(),
            response_hash: None,
            elapsed_ms: 0,
            error_kind: None,
        }
    }
}

pub async fn send(
    client: &reqwest::Client,
    base: &str,
    api_key: Option<&str>,
    model: &str,
    transport: TransportKind,
    prepared: &PreparedModelRequest,
) -> Result<ModelResponse, TransportError> {
    let mut diagnostic = ModelIoDiagnostic::prepared(transport, prepared);
    let fail = |kind: &'static str, mut diagnostic: ModelIoDiagnostic, envelope| {
        diagnostic.error_kind = Some(kind.into());
        TransportError {
            kind,
            diagnostic,
            envelope,
        }
    };
    if !crate::runtime::prompt_budget::check_prepared(prepared) {
        return Err(fail("context_budget_exceeded", diagnostic, None));
    }
    let url =
        endpoint(base, transport).map_err(|_| fail("configuration", diagnostic.clone(), None))?;
    if model.trim().is_empty() {
        return Err(fail("configuration", diagnostic, None));
    }
    let body = match transport {
        TransportKind::OpenaiCompatible => openai_compatible::body(model, prepared),
        TransportKind::Ollama => ollama::body(model, prepared),
    };
    let bytes = serde_json::to_vec(&body).expect("JSON value serializes");
    diagnostic.request_hash = hash(&bytes);
    let started = Instant::now();
    let mut request = client
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(bytes);
    if let Some(key) = api_key {
        request = request.bearer_auth(key);
    }
    let response = request.send().await.map_err(|e| {
        diagnostic.elapsed_ms = started.elapsed().as_millis() as u64;
        fail(
            if e.is_timeout() {
                "timeout"
            } else {
                "provider_error"
            },
            diagnostic.clone(),
            None,
        )
    })?;
    let status = response.status();
    let raw = response.bytes().await.map_err(|e| {
        diagnostic.elapsed_ms = started.elapsed().as_millis() as u64;
        fail(
            if e.is_timeout() {
                "timeout"
            } else {
                "provider_error"
            },
            diagnostic.clone(),
            None,
        )
    })?;
    diagnostic.elapsed_ms = started.elapsed().as_millis() as u64;
    diagnostic.response_hash = Some(hash(&raw));
    if !status.is_success() {
        return Err(fail(
            "provider_error",
            diagnostic,
            Some(format!("http_status={}", status.as_u16())),
        ));
    }
    let text = match std::str::from_utf8(&raw) {
        Ok(text) => text,
        Err(_) => {
            return Err(fail(
                "malformed_response",
                diagnostic,
                Some("json_error=syntax;content_encoding=invalid_utf8".into()),
            ))
        }
    };
    let parsed = serde_json::from_slice::<Value>(&raw);
    if let Ok(value) = &parsed {
        match transport {
            TransportKind::OpenaiCompatible => openai_compatible::metadata(value, &mut diagnostic),
            TransportKind::Ollama => ollama::metadata(value, &mut diagnostic),
        }
    }
    let result = match transport {
        TransportKind::OpenaiCompatible => openai_compatible::decode(&text),
        TransportKind::Ollama => ollama::decode(&text),
    };
    match result {
        Ok(content) => Ok(ModelResponse {
            content,
            diagnostic,
        }),
        Err((kind, category)) => {
            let envelope = match transport {
                TransportKind::OpenaiCompatible => response_diagnostic(&text, category),
                TransportKind::Ollama => ollama::envelope(&text, category),
            };
            Err(fail(kind, diagnostic, Some(envelope)))
        }
    }
}

pub(super) fn content_metadata(value: Option<&Value>, diagnostic: &mut ModelIoDiagnostic) {
    diagnostic.content_type = match value {
        None => "missing",
        Some(Value::Null) => "null",
        Some(Value::String(_)) => "string",
        Some(Value::Object(_)) => "object",
        Some(Value::Array(_)) => "array",
        Some(Value::Bool(_)) => "boolean",
        Some(Value::Number(_)) => "number",
    }
    .into();
    diagnostic.content_bytes = value.and_then(Value::as_str).map(str::len);
}
pub(super) type DecodeResult = Result<String, (&'static str, Option<serde_json::error::Category>)>;
const MAX_ENVELOPE_DIAGNOSTIC_BYTES: usize = 512;
const MAX_DIAGNOSTIC_FIELD_NAMES: usize = 8;
const MAX_DIAGNOSTIC_FIELD_NAME_CHARS: usize = 24;
const MAX_DIAGNOSTIC_FINISH_REASON_CHARS: usize = 32;

fn response_diagnostic(raw: &str, category: Option<serde_json::error::Category>) -> String {
    let parsed = serde_json::from_str::<Value>(raw).ok();
    let value = parsed.as_ref();
    let json_error = match category {
        Some(serde_json::error::Category::Io) => "io",
        Some(serde_json::error::Category::Syntax) => "syntax",
        Some(serde_json::error::Category::Data) => "data",
        Some(serde_json::error::Category::Eof) => "eof",
        None if parsed.is_some() => "none",
        None => "unknown",
    };
    let top_level_fields = value
        .and_then(Value::as_object)
        .map(|fields| {
            let mut names = fields
                .keys()
                .take(MAX_DIAGNOSTIC_FIELD_NAMES)
                .map(|name| diagnostic_token(name, MAX_DIAGNOSTIC_FIELD_NAME_CHARS))
                .collect::<Vec<_>>();
            if fields.len() > MAX_DIAGNOSTIC_FIELD_NAMES {
                names.push("+more".to_owned());
            }
            format!("[{}]", names.join(","))
        })
        .unwrap_or_else(|| "unavailable".to_owned());
    let choices = value
        .and_then(|value| value.get("choices"))
        .and_then(Value::as_array);
    let first_choice = choices.and_then(|choices| choices.first());
    let message = first_choice
        .and_then(|choice| choice.get("message"))
        .and_then(Value::as_object);
    let shape = |value: Option<&Value>| match value {
        None => "missing:na".to_owned(),
        Some(Value::Null) => "null:0".to_owned(),
        Some(Value::Bool(_)) => "boolean:1".to_owned(),
        Some(Value::Number(_)) => "number:1".to_owned(),
        Some(Value::String(value)) => format!("string:{}b", value.len()),
        Some(Value::Array(value)) => format!("array:{}", value.len()),
        Some(Value::Object(value)) => format!("object:{}", value.len()),
    };
    let choices_count = choices.map_or_else(|| "unavailable".to_owned(), |v| v.len().to_string());
    let finish_reason = match first_choice.and_then(|choice| choice.get("finish_reason")) {
        None => "missing".to_owned(),
        Some(Value::Null) => "null".to_owned(),
        Some(Value::String(reason)) => diagnostic_token(reason, MAX_DIAGNOSTIC_FINISH_REASON_CHARS),
        Some(_) => "other".to_owned(),
    };
    let completion_tokens = value
        .and_then(|value| value.get("usage"))
        .and_then(|usage| usage.get("completion_tokens"))
        .and_then(Value::as_u64)
        .map_or_else(|| "unavailable".to_owned(), |tokens| tokens.to_string());
    let prompt_tokens = value
        .and_then(|value| value.get("usage"))
        .and_then(|usage| usage.get("prompt_tokens"))
        .and_then(Value::as_u64)
        .map_or_else(|| "unavailable".to_owned(), |tokens| tokens.to_string());
    let mut diagnostic = format!(
        "json_error={json_error};top_level_fields={};choices_count={choices_count};first_finish_reason={finish_reason};content={};reasoning_content={};completion_tokens={completion_tokens};prompt_tokens={prompt_tokens}",
        top_level_fields,
        shape(message.and_then(|message| message.get("content"))),
        shape(message.and_then(|message| message.get("reasoning_content"))),
    );
    if diagnostic.len() > MAX_ENVELOPE_DIAGNOSTIC_BYTES {
        let mut end = MAX_ENVELOPE_DIAGNOSTIC_BYTES;
        while !diagnostic.is_char_boundary(end) {
            end -= 1;
        }
        diagnostic.truncate(end);
    }
    diagnostic
}

fn diagnostic_token(value: &str, max_chars: usize) -> String {
    let token = value
        .chars()
        .take(max_chars)
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.') {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    if token.is_empty() {
        "_".to_owned()
    } else {
        token
    }
}
