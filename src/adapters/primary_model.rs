use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::{de, de::Deserializer, Deserialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::config::Config;
use crate::core::{
    CognitiveTrace, CommittedJudgment, Conflict, ConflictId, ConflictStatus, DecisionKind, EventId,
    Position, PositionId, PositionStatus, SelfReview, Stance, ThoughtContext, ThoughtCycle,
    ThoughtDraft,
};
use crate::ports::{CognitiveError, CognitiveModel};

const SCHEMA_VERSION: &str = "thought-cycle.v1";
const PROVIDER: &str = "openai_compatible";

#[derive(Clone)]
pub struct PrimaryModel {
    client: reqwest::Client,
    base_url: String,
    api_key: Option<String>,
    model: String,
}

impl PrimaryModel {
    pub fn from_config(config: &Config) -> Result<Self, String> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.model_timeout_seconds.max(1)))
            .build()
            .map_err(|error| format!("could not create model HTTP client: {error}"))?;
        Ok(Self {
            client,
            base_url: config.model_base_url.clone(),
            api_key: config.model_api_key.clone(),
            model: config.model_name.clone(),
        })
    }

    fn trace(&self, context: &ThoughtContext) -> CognitiveTrace {
        CognitiveTrace {
            trace_id: Uuid::new_v4().to_string(),
            outcome: "failed".to_owned(),
            provider: PROVIDER.to_owned(),
            model: self.model.clone(),
            schema_version: SCHEMA_VERSION.to_owned(),
            context_sequence: context.event_sequence,
            context_hash: context.snapshot_hash.clone(),
            referenced_event_ids: context.recent_event_ids.clone(),
            draft: None,
            review: None,
            commitment: None,
            parse_errors: Vec::new(),
            retries: 0,
            elapsed_ms: 0,
            raw_response_hash: None,
            error_kind: None,
            created_at: crate::core::model::now(),
        }
    }

    async fn request(
        &self,
        context: &ThoughtContext,
        correction: Option<&str>,
    ) -> Result<RawResponse, RequestError> {
        if self.base_url.trim().is_empty() {
            return Err(RequestError::Configuration(
                "model base URL is empty".to_owned(),
            ));
        }
        if self.model.trim().is_empty() {
            return Err(RequestError::Configuration(
                "model name is empty".to_owned(),
            ));
        }
        let context_json = serde_json::to_string(context)
            .map_err(|error| RequestError::Configuration(error.to_string()))?;
        let allowed_evidence_refs = json_string_ids(context.recent_event_ids.iter());
        let allowed_conflict_ids =
            json_string_ids(context.conflicts.iter().map(|conflict| conflict.id));
        let allowed_position_ids = json_string_ids(
            context
                .positions
                .iter()
                .chain(context.user_positions.iter())
                .map(|position| position.id),
        );
        let correction = correction.unwrap_or("");
        let system = format!(
            "{SYSTEM_PROMPT}\nThe only allowed evidence_refs for this response are exactly {allowed_evidence_refs}; use [] when no supplied event is needed. The only existing conflict IDs are {allowed_conflict_ids}; use conflict_change.id:null for a new conflict. The only existing position IDs are {allowed_position_ids}.\n{correction}\nReturn only one JSON object; do not include markdown fences."
        );
        let body = serde_json::json!({
            "model": self.model,
            "temperature": 0,
            "max_tokens": 8192,
            "model_options": {"reasoning_effort": "none"},
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": context_json}
            ]
        });
        let started = Instant::now();
        let mut request = self.client.post(format!(
            "{}/chat/completions",
            self.base_url.trim_end_matches('/')
        ));
        if let Some(api_key) = self.api_key.as_deref() {
            request = request.bearer_auth(api_key);
        }
        let response = request.json(&body).send().await.map_err(|error| {
            if error.is_timeout() {
                RequestError::Timeout
            } else {
                RequestError::Provider
            }
        })?;
        let status = response.status();
        let text = response.text().await.map_err(|_| RequestError::Provider)?;
        let elapsed_ms = started.elapsed().as_millis() as u64;
        let response_hash = hash(&text);
        if !status.is_success() {
            return Err(RequestError::Http(status.as_u16(), response_hash));
        }
        let envelope: ChatResponse = serde_json::from_str(&text)
            .map_err(|_| RequestError::Malformed(response_hash.clone()))?;
        let content = envelope
            .choices
            .first()
            .and_then(|choice| {
                choice
                    .message
                    .content
                    .clone()
                    .filter(|content| !content.trim().is_empty())
                    .or_else(|| choice.message.reasoning_content.clone())
            })
            .ok_or(RequestError::Malformed(response_hash.clone()))?;
        Ok(RawResponse {
            content,
            response_hash,
            elapsed_ms,
        })
    }
}

#[async_trait]
impl CognitiveModel for PrimaryModel {
    async fn think(&self, context: &ThoughtContext) -> Result<ThoughtCycle, CognitiveError> {
        let started = Instant::now();
        let mut trace = self.trace(context);
        let first = self.request(context, None).await;
        let first = match first {
            Ok(response) => response,
            Err(error) => {
                return Err(self.request_error(error, trace, started.elapsed().as_millis()))
            }
        };
        trace.raw_response_hash = Some(first.response_hash.clone());
        trace.elapsed_ms = first.elapsed_ms;

        let cycle = match parse_cycle_for_context(&first.content, context) {
            Ok(cycle) => cycle,
            Err(error) => {
                let correction = error.correction;
                trace.parse_errors.push(error.message);
                trace.retries = 1;
                let retry = self.request(context, Some(&correction)).await;
                let retry = match retry {
                    Ok(response) => response,
                    Err(error) => {
                        return Err(self.request_error(error, trace, started.elapsed().as_millis()))
                    }
                };
                trace.raw_response_hash = Some(retry.response_hash.clone());
                trace.elapsed_ms = started.elapsed().as_millis() as u64;
                match parse_cycle_for_context(&retry.content, context) {
                    Ok(cycle) => cycle,
                    Err(error) => {
                        trace.parse_errors.push(error.message);
                        trace.error_kind = Some("malformed_model_output".to_owned());
                        return Err(CognitiveError::Malformed {
                            message: "malformed model output after one correction attempt"
                                .to_owned(),
                            trace,
                        });
                    }
                }
            }
        };
        trace.elapsed_ms = started.elapsed().as_millis() as u64;
        trace.outcome = "succeeded".to_owned();
        trace.draft = Some(cycle.draft.clone());
        trace.review = Some(cycle.review.clone());
        trace.commitment = Some(cycle.commitment.clone());
        Ok(ThoughtCycle { trace, ..cycle })
    }
}

impl PrimaryModel {
    fn request_error(
        &self,
        error: RequestError,
        mut trace: CognitiveTrace,
        elapsed_ms: u128,
    ) -> CognitiveError {
        trace.elapsed_ms = elapsed_ms as u64;
        let (message, error_kind) = match error {
            RequestError::Configuration(message) => {
                trace.error_kind = Some("configuration".to_owned());
                return CognitiveError::Configuration { message, trace };
            }
            RequestError::Timeout => ("model request timed out".to_owned(), "timeout"),
            RequestError::Provider => {
                ("model provider request failed".to_owned(), "provider_error")
            }
            RequestError::Http(status, response_hash) => {
                if trace.raw_response_hash.is_none() {
                    trace.raw_response_hash = Some(response_hash);
                }
                (
                    format!("provider returned HTTP status {status}"),
                    "provider_error",
                )
            }
            RequestError::Malformed(response_hash) => {
                if trace.raw_response_hash.is_none() {
                    trace.raw_response_hash = Some(response_hash);
                }
                (
                    "provider response envelope was malformed".to_owned(),
                    "malformed_response",
                )
            }
        };
        trace.error_kind = Some(error_kind.to_owned());
        if error_kind == "timeout" {
            CognitiveError::Timeout { trace }
        } else if error_kind == "malformed_response" {
            CognitiveError::Malformed { message, trace }
        } else {
            CognitiveError::Provider { message, trace }
        }
    }
}

const SYSTEM_PROMPT: &str = r#"
You are HEKATE's cognitive model. Deliberate once in three explicit phases.
Return one JSON object only. Do not include prose or markdown fences. Do not call tools or execute any operation; this is a cognitive evaluation only.
The top-level fields are flat and required unless marked nullable:
{"draft_interpretation":"string","draft_initial_judgment":"agree","draft_reasons":["string"],"draft_doubts":["string"],"review_strongest_objection":"string","review_identity_conflicts":["string"],"review_unsupported_claims":["string"],"review_suggested_revision":null,"act":"agree","rationale":"string","response":"string","confidence":50,"evidence_refs":["event-id"],"position_change":null,"conflict_change":null}
Both draft_initial_judgment and act must be exactly one of: agree, ask_why, challenge, counter_propose, negotiate, refuse, observe_more, request_clarification. Do not use any other act.
Use only event IDs present in the supplied context in evidence_refs. Never invent an event ID. If no supplied event is needed, evidence_refs must be []. Use null for absent position_change or conflict_change. A non-null position_change or conflict_change must contain complete data and must use IDs from the supplied context where an existing ID is required.
The response field is the user-facing answer. Rationale, response, and all required string fields must be present; do not replace them with null.
String fields that are arrays contain strings. confidence is an integer from 0 to 100. At the top level, null is allowed only for review_suggested_revision, position_change, and conflict_change; conflict_change.id may also be null for a new conflict.
When act is challenge, counter_propose, negotiate, or refuse, conflict_change is required and must be non-null. Use this exact flat object shape: {"id":null,"subject":"string","participant_positions":["position-id"],"status":"open","revision":1,"reasons":["string"],"evidence_refs":["event-id"],"alternatives":["string"],"reconsideration_conditions":["string"],"unresolved_questions":["string"],"resolution":null,"resolved_at":null,"created_at":null}. Its complete object keys are id, subject, participant_positions, status, revision, reasons, evidence_refs, alternatives, reconsideration_conditions, unresolved_questions, resolution, resolved_at, and created_at. Set id to null for a new conflict; the runtime supplies its UUID. For an update or resolution, use only an existing conflict ID supplied in context and never invent a UUID. participant_positions must be an array of position ID strings, and status must be exactly open, negotiating, resolved, or accepted_disagreement. It must include participant_positions referring to existing context positions or supplied evidence, plus reasons, reconsideration_conditions, and unresolved_questions. Keep the chosen act and rationale consistent with that conflict. When act is agree, request_clarification, or observe_more, conflict_change may be null.
When the supplied context contains an opposing HEKATE Position about unverified deletion and the user requests deletion without verification, treat that relationship as a conflict and return the semantically appropriate conflict act with its complete conflict_change. Do not execute the requested deletion.
"#;

#[derive(Debug)]
struct RawResponse {
    content: String,
    response_hash: String,
    elapsed_ms: u64,
}

#[derive(Debug)]
enum RequestError {
    Configuration(String),
    Timeout,
    Provider,
    Http(u16, String),
    Malformed(String),
}

#[derive(Debug, Deserialize)]
struct ChatResponse {
    choices: Vec<ChatChoice>,
}

#[derive(Debug, Deserialize)]
struct ChatChoice {
    message: ChatMessage,
}

#[derive(Debug, Deserialize)]
struct ChatMessage {
    content: Option<String>,
    reasoning_content: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WireThoughtCycle {
    draft_interpretation: String,
    draft_initial_judgment: String,
    #[serde(deserialize_with = "deserialize_string_list")]
    draft_reasons: Vec<String>,
    #[serde(deserialize_with = "deserialize_string_list")]
    draft_doubts: Vec<String>,
    review_strongest_objection: String,
    #[serde(deserialize_with = "deserialize_string_list")]
    review_identity_conflicts: Vec<String>,
    #[serde(deserialize_with = "deserialize_string_list")]
    review_unsupported_claims: Vec<String>,
    review_suggested_revision: Option<String>,
    act: String,
    rationale: String,
    response: String,
    confidence: u8,
    #[serde(deserialize_with = "deserialize_string_list")]
    evidence_refs: Vec<String>,
    #[serde(default)]
    position_change: Option<WirePosition>,
    #[serde(default)]
    conflict_change: Option<WireConflict>,
}

#[derive(Debug, Deserialize)]
struct WirePosition {
    id: String,
    principal_id: String,
    subject: String,
    stance: String,
    version: u32,
    status: String,
    confidence: u8,
    supersedes: Option<String>,
    #[serde(deserialize_with = "deserialize_string_list")]
    reasons: Vec<String>,
    #[serde(deserialize_with = "deserialize_string_list")]
    evidence_refs: Vec<String>,
    #[serde(deserialize_with = "deserialize_string_list")]
    reconsideration_conditions: Vec<String>,
    #[serde(default)]
    created_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WireConflict {
    id: Option<String>,
    subject: String,
    #[serde(deserialize_with = "deserialize_string_list")]
    participant_positions: Vec<String>,
    status: String,
    revision: u32,
    #[serde(deserialize_with = "deserialize_string_list")]
    reasons: Vec<String>,
    #[serde(deserialize_with = "deserialize_string_list")]
    evidence_refs: Vec<String>,
    #[serde(deserialize_with = "deserialize_string_list")]
    alternatives: Vec<String>,
    #[serde(deserialize_with = "deserialize_string_list")]
    reconsideration_conditions: Vec<String>,
    #[serde(deserialize_with = "deserialize_string_list")]
    unresolved_questions: Vec<String>,
    resolution: Option<String>,
    resolved_at: Option<String>,
    #[serde(default)]
    created_at: Option<String>,
}

#[derive(Debug)]
struct ParseFailure {
    message: String,
    correction: String,
}

fn json_string_ids<I>(ids: I) -> String
where
    I: IntoIterator,
    I::Item: std::fmt::Display,
{
    let ids = ids.into_iter().map(|id| id.to_string()).collect::<Vec<_>>();
    serde_json::to_string(&ids).unwrap_or_else(|_| "[]".to_owned())
}

#[cfg(test)]
fn parse_cycle(value: &str) -> Result<ThoughtCycle, String> {
    parse_cycle_with_context(value, None)
}

fn json_candidate(value: &str) -> &str {
    let candidate = value.trim();
    candidate
        .strip_prefix("```json")
        .or_else(|| candidate.strip_prefix("```JSON"))
        .or_else(|| candidate.strip_prefix("```"))
        .and_then(|value| value.strip_suffix("```"))
        .map(str::trim)
        .unwrap_or(candidate)
}

fn parse_cycle_with_context(
    value: &str,
    context: Option<&ThoughtContext>,
) -> Result<ThoughtCycle, String> {
    let candidate = json_candidate(value);
    let wire: WireThoughtCycle = serde_json::from_str(candidate)
        .map_err(|error| format!("thought cycle JSON parse failed: {error}"))?;
    let draft_initial_judgment =
        parse_decision_kind(&wire.draft_initial_judgment, "draft_initial_judgment")?;
    let final_act = parse_decision_kind(&wire.act, "act")?;
    if wire.draft_interpretation.trim().is_empty() {
        return Err("thought cycle field draft_interpretation is empty".to_owned());
    }
    if wire.review_strongest_objection.trim().is_empty() {
        return Err("thought cycle field review_strongest_objection is empty".to_owned());
    }
    if wire.rationale.trim().is_empty() {
        return Err("thought cycle field rationale is empty".to_owned());
    }
    if wire.response.trim().is_empty() {
        return Err("thought cycle field response is empty".to_owned());
    }
    let evidence_refs = parse_event_ids(&wire.evidence_refs, "evidence_refs")?;
    let position = wire.position_change.map(position_from_wire).transpose()?;
    let conflict = wire
        .conflict_change
        .map(|value| conflict_from_wire(value, context))
        .transpose()?;
    let draft_interpretation = wire.draft_interpretation;
    let rationale = wire.rationale;
    let response = wire.response;
    Ok(ThoughtCycle {
        draft: ThoughtDraft {
            interpretation: draft_interpretation.clone(),
            initial_judgment: draft_initial_judgment,
            reasons: wire.draft_reasons,
            uncertainties: wire.draft_doubts.clone(),
            initial_intent: draft_interpretation,
        },
        review: SelfReview {
            strongest_counterargument: wire.review_strongest_objection,
            value_conflicts: wire.review_identity_conflicts,
            unsupported_claims: wire.review_unsupported_claims,
            revision_direction: wire.review_suggested_revision,
        },
        commitment: CommittedJudgment {
            final_act,
            reasons: vec![rationale],
            response,
            confidence: wire.confidence,
            unresolved_questions: wire.draft_doubts,
            alternatives: Vec::new(),
            reconsideration_conditions: Vec::new(),
            evidence_refs,
            related_position_ids: Vec::new(),
            position,
            user_position: None,
            conflict,
            action: None,
        },
        trace: empty_trace(),
    })
}

fn parse_cycle_for_context(
    value: &str,
    context: &ThoughtContext,
) -> Result<ThoughtCycle, ParseFailure> {
    let cycle = parse_cycle_with_context(value, Some(context))
        .map_err(|message| parse_failure(message, context, preserve_raw_fields(value)))?;
    let references = cycle.commitment.evidence_refs.iter();
    let position_references = cycle
        .commitment
        .position
        .iter()
        .flat_map(|position| position.evidence_refs.iter());
    let conflict_references = cycle
        .commitment
        .conflict
        .iter()
        .flat_map(|conflict| conflict.evidence_refs.iter());
    if let Some(event_id) = references
        .chain(position_references)
        .chain(conflict_references)
        .find(|event_id| !context.recent_event_ids.contains(event_id))
    {
        return Err(parse_failure(
            format!("thought cycle evidence_refs ID {event_id} is outside the supplied context"),
            context,
            preserve_cycle_fields(&cycle),
        ));
    }
    validate_conflict_structure(&cycle, context)?;
    Ok(cycle)
}

fn validate_conflict_structure(
    cycle: &ThoughtCycle,
    context: &ThoughtContext,
) -> Result<(), ParseFailure> {
    let act = decision_kind_token(&cycle.commitment.final_act);
    if !matches!(
        cycle.commitment.final_act,
        DecisionKind::Challenge
            | DecisionKind::CounterPropose
            | DecisionKind::Negotiate
            | DecisionKind::Refuse
    ) {
        return Ok(());
    }
    let Some(conflict) = cycle.commitment.conflict.as_ref() else {
        return Err(conflict_repair_failure(cycle, act, context));
    };
    let references_existing_position = conflict.participant_positions.iter().any(|id| {
        context
            .positions
            .iter()
            .chain(context.user_positions.iter())
            .any(|position| position.id == *id)
    });
    let has_evidence =
        !cycle.commitment.evidence_refs.is_empty() || !conflict.evidence_refs.is_empty();
    if conflict.participant_positions.is_empty() || (!references_existing_position && !has_evidence)
    {
        return Err(conflict_repair_failure(cycle, act, context));
    }
    Ok(())
}

fn preserve_raw_fields(value: &str) -> String {
    let Ok(value) = serde_json::from_str::<Value>(json_candidate(value)) else {
        return String::new();
    };
    let Some(act) = value.get("act").and_then(Value::as_str) else {
        return String::new();
    };
    let Some(rationale) = value.get("rationale").and_then(Value::as_str) else {
        return String::new();
    };
    format!("Keep act `{act}` and the original rationale {rationale:?} unchanged. ")
}

fn preserve_cycle_fields(cycle: &ThoughtCycle) -> String {
    format!(
        "Keep act `{}` and the original rationale {:?} unchanged. ",
        decision_kind_token(&cycle.commitment.final_act),
        cycle
            .commitment
            .reasons
            .first()
            .map(String::as_str)
            .unwrap_or("")
    )
}

fn parse_failure(message: String, context: &ThoughtContext, preserved: String) -> ParseFailure {
    let allowed_evidence_refs = json_string_ids(context.recent_event_ids.iter());
    let allowed_conflict_ids =
        json_string_ids(context.conflicts.iter().map(|conflict| conflict.id));
    let allowed_position_ids = json_string_ids(
        context
            .positions
            .iter()
            .chain(context.user_positions.iter())
            .map(|position| position.id),
    );
    ParseFailure {
        message: message.clone(),
        correction: format!(
            "The previous response failed validation: {message}. {preserved}Return one complete flat JSON object. Use exactly one act token from agree, ask_why, challenge, counter_propose, negotiate, refuse, observe_more, request_clarification. Use evidence_refs only from {allowed_evidence_refs}; if no supplied event is needed, use []. Existing conflict IDs are {allowed_conflict_ids}: set conflict_change.id to null for a new conflict, and use one of those IDs only for an update or resolution. Existing position IDs are {allowed_position_ids}; participant_positions must use only those IDs. Never invent UUIDs, Event IDs, Position IDs, or Conflict IDs. If the selected act is challenge, counter_propose, negotiate, or refuse, conflict_change must be non-null and complete with id, subject, participant_positions, status, revision, reasons, evidence_refs, alternatives, reconsideration_conditions, unresolved_questions, resolution, resolved_at, and created_at. Preserve the original meaning and return only JSON without markdown fences."
        ),
    }
}

fn conflict_repair_failure(
    cycle: &ThoughtCycle,
    act: &str,
    context: &ThoughtContext,
) -> ParseFailure {
    parse_failure(
        format!("act {act} requires a non-null conflict_change with a supplied reference"),
        context,
        preserve_cycle_fields(cycle),
    )
}

fn decision_kind_token(kind: &DecisionKind) -> &'static str {
    match kind {
        DecisionKind::Respond => "respond",
        DecisionKind::Agree => "agree",
        DecisionKind::AskWhy => "ask_why",
        DecisionKind::Challenge => "challenge",
        DecisionKind::CounterPropose => "counter_propose",
        DecisionKind::Negotiate => "negotiate",
        DecisionKind::Refuse => "refuse",
        DecisionKind::ObserveMore => "observe_more",
        DecisionKind::ProposeAction => "propose_action",
        DecisionKind::CreateOrUpdateGoal => "create_or_update_goal",
        DecisionKind::CreateOrUpdateTask => "create_or_update_task",
        DecisionKind::RequestClarification => "request_clarification",
        DecisionKind::RequestApproval => "request_approval",
        DecisionKind::Suspend => "suspend",
        DecisionKind::Complete => "complete",
    }
}

fn empty_trace() -> CognitiveTrace {
    CognitiveTrace {
        trace_id: String::new(),
        outcome: String::new(),
        provider: String::new(),
        model: String::new(),
        schema_version: String::new(),
        context_sequence: 0,
        context_hash: String::new(),
        referenced_event_ids: Vec::new(),
        draft: None,
        review: None,
        commitment: None,
        parse_errors: Vec::new(),
        retries: 0,
        elapsed_ms: 0,
        raw_response_hash: None,
        error_kind: None,
        created_at: String::new(),
    }
}

fn deserialize_string_list<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    match Value::deserialize(deserializer)? {
        Value::String(value) => Ok(vec![value]),
        Value::Array(values) => values
            .into_iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| de::Error::custom("array field must contain only strings"))
            })
            .collect(),
        _ => Err(de::Error::custom(
            "expected a string or an array of strings",
        )),
    }
}

fn parse_decision_kind(value: &str, field: &str) -> Result<DecisionKind, String> {
    match normalize_known_token(value).as_str() {
        "agree" => Ok(DecisionKind::Agree),
        "ask_why" => Ok(DecisionKind::AskWhy),
        "challenge" => Ok(DecisionKind::Challenge),
        "counter_propose" => Ok(DecisionKind::CounterPropose),
        "negotiate" => Ok(DecisionKind::Negotiate),
        "refuse" => Ok(DecisionKind::Refuse),
        "observe_more" => Ok(DecisionKind::ObserveMore),
        "request_clarification" => Ok(DecisionKind::RequestClarification),
        _ => Err(format!(
            "thought cycle field {field} has an unsupported act"
        )),
    }
}

fn normalize_known_token(value: &str) -> String {
    let mut result = String::new();
    let mut previous = None;
    for character in value.trim().chars() {
        if character == '-' || character.is_ascii_whitespace() {
            if !result.ends_with('_') {
                result.push('_');
            }
        } else if character.is_ascii_uppercase() {
            if previous.is_some_and(|item: char| item.is_ascii_lowercase())
                && !result.ends_with('_')
            {
                result.push('_');
            }
            result.push(character.to_ascii_lowercase());
        } else {
            result.push(character.to_ascii_lowercase());
        }
        previous = Some(character);
    }
    result.trim_matches('_').to_owned()
}

fn parse_event_ids(values: &[String], field: &str) -> Result<Vec<EventId>, String> {
    values
        .iter()
        .map(|value| {
            Uuid::parse_str(value.trim())
                .map(EventId::from)
                .map_err(|_| format!("thought cycle field {field} contains an invalid event ID"))
        })
        .collect()
}

fn parse_id<T>(value: &str, field: &str) -> Result<T, String>
where
    T: From<Uuid>,
{
    Uuid::parse_str(value.trim())
        .map(T::from)
        .map_err(|_| format!("thought cycle field {field} contains an invalid ID"))
}

fn parse_stance(value: &str) -> Result<Stance, String> {
    match normalize_known_token(value).as_str() {
        "support" => Ok(Stance::Support),
        "oppose" => Ok(Stance::Oppose),
        "uncertain" => Ok(Stance::Uncertain),
        "neutral" => Ok(Stance::Neutral),
        _ => Err("thought cycle position_change has an unsupported stance".to_owned()),
    }
}

fn parse_position_status(value: &str) -> Result<PositionStatus, String> {
    match normalize_known_token(value).as_str() {
        "active" => Ok(PositionStatus::Active),
        "superseded" => Ok(PositionStatus::Superseded),
        "retracted" => Ok(PositionStatus::Retracted),
        _ => Err("thought cycle position_change has an unsupported status".to_owned()),
    }
}

fn parse_conflict_status(value: &str) -> Result<ConflictStatus, String> {
    match normalize_known_token(value).as_str() {
        "open" => Ok(ConflictStatus::Open),
        "negotiating" => Ok(ConflictStatus::Negotiating),
        "resolved" => Ok(ConflictStatus::Resolved),
        "accepted_disagreement" => Ok(ConflictStatus::AcceptedDisagreement),
        _ => Err("thought cycle conflict_change has an unsupported status".to_owned()),
    }
}

fn position_from_wire(value: WirePosition) -> Result<Position, String> {
    Ok(Position {
        id: parse_id::<PositionId>(&value.id, "position_change.id")?,
        principal_id: parse_id(&value.principal_id, "position_change.principal_id")?,
        subject: value.subject,
        stance: parse_stance(&value.stance)?,
        version: value.version,
        status: parse_position_status(&value.status)?,
        confidence: value.confidence,
        supersedes: value
            .supersedes
            .as_deref()
            .map(|id| parse_id(id, "position_change.supersedes"))
            .transpose()?,
        reasons: value.reasons,
        evidence_refs: parse_event_ids(&value.evidence_refs, "position_change.evidence_refs")?,
        reconsideration_conditions: value.reconsideration_conditions,
        created_at: value.created_at.unwrap_or_else(crate::core::model::now),
    })
}

fn conflict_from_wire(
    value: WireConflict,
    context: Option<&ThoughtContext>,
) -> Result<Conflict, String> {
    let id = match value.id {
        None => ConflictId::new(),
        Some(id) => {
            let id = parse_id::<ConflictId>(&id, "conflict_change.id")?;
            if let Some(context) = context {
                if !context.conflicts.iter().any(|conflict| conflict.id == id) {
                    return Err(
                        format!(
                            "thought cycle conflict_change.id {id} references an unknown context conflict"
                        ),
                    );
                }
            }
            id
        }
    };
    Ok(Conflict {
        id,
        subject: value.subject,
        participant_positions: value
            .participant_positions
            .iter()
            .map(|id| parse_id(id, "conflict_change.participant_positions"))
            .collect::<Result<_, _>>()?,
        status: parse_conflict_status(&value.status)?,
        revision: value.revision,
        reasons: value.reasons,
        evidence_refs: parse_event_ids(&value.evidence_refs, "conflict_change.evidence_refs")?,
        alternatives: value.alternatives,
        reconsideration_conditions: value.reconsideration_conditions,
        unresolved_questions: value.unresolved_questions,
        resolution: value.resolution,
        resolved_at: value.resolved_at,
        created_at: value.created_at.unwrap_or_else(crate::core::model::now),
    })
}

fn hash(value: &str) -> String {
    Sha256::digest(value.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{Focus, Observation, PrincipalId};
    use uuid::Uuid;

    fn test_context() -> ThoughtContext {
        let event_id = EventId::new();
        let hekate_id = PrincipalId::new();
        let user_id = PrincipalId::new();
        let position = Position {
            id: PositionId::new(),
            principal_id: hekate_id,
            subject: "test position".to_owned(),
            stance: Stance::Oppose,
            version: 1,
            status: PositionStatus::Active,
            confidence: 100,
            supersedes: None,
            reasons: vec!["test reason".to_owned()],
            evidence_refs: vec![event_id],
            reconsideration_conditions: vec!["test condition".to_owned()],
            created_at: crate::core::model::now(),
        };
        let conflict = Conflict {
            id: ConflictId::new(),
            subject: "test conflict".to_owned(),
            participant_positions: vec![position.id],
            status: ConflictStatus::Open,
            revision: 1,
            reasons: vec!["test reason".to_owned()],
            evidence_refs: vec![event_id],
            alternatives: vec!["test alternative".to_owned()],
            reconsideration_conditions: vec!["test condition".to_owned()],
            unresolved_questions: vec!["test question".to_owned()],
            resolution: None,
            resolved_at: None,
            created_at: crate::core::model::now(),
        };
        ThoughtContext {
            event_sequence: 1,
            observation: Observation {
                id: crate::core::ObservationId::new(),
                actor_id: user_id,
                content: "test request".to_owned(),
                source_type: "test".to_owned(),
                source_ref: None,
                thread_id: None,
                message_id: None,
                received_at: crate::core::model::now(),
            },
            focus: Focus::unattached(),
            identity: None,
            relationship: None,
            goal: None,
            task: None,
            run: None,
            working_state: None,
            positions: vec![position],
            user_positions: Vec::new(),
            conflicts: vec![conflict],
            commitments: Vec::new(),
            memories: Vec::new(),
            memory_candidates: Vec::new(),
            pending_approvals: Vec::new(),
            artifacts: Vec::new(),
            recent_event_ids: vec![event_id],
            relevant_events: Vec::new(),
            available_capabilities: Vec::new(),
            output_schema: "test".to_owned(),
            snapshot_hash: "test-hash".to_owned(),
        }
    }

    fn conflict_cycle_json(
        context: &ThoughtContext,
        conflict_id: &str,
        evidence_refs: &str,
    ) -> String {
        format!(
            r#"{{
              "draft_interpretation":"The request conflicts with a position.",
              "draft_initial_judgment":"refuse",
              "draft_reasons":["The request is unsafe."],
              "draft_doubts":[],
              "review_strongest_objection":"The user requested immediate execution.",
              "review_identity_conflicts":[],
              "review_unsupported_claims":[],
              "review_suggested_revision":null,
              "act":"refuse",
              "rationale":"The request is inconsistent with the position.",
              "response":"I cannot do that.",
              "confidence":90,
              "evidence_refs":{evidence_refs},
              "position_change":null,
              "conflict_change":{{
                "id":{conflict_id},
                "subject":"test conflict",
                "participant_positions":["{}"],
                "status":"open",
                "revision":1,
                "reasons":["The positions conflict."],
                "evidence_refs":[],
                "alternatives":["Clarify the request."],
                "reconsideration_conditions":["The request is verified."],
                "unresolved_questions":["What is the exact scope?"],
                "resolution":null,
                "resolved_at":null,
                "created_at":null
              }}
            }}"#,
            context.positions[0].id
        )
    }

    #[test]
    fn parses_flat_cycle_and_only_normalizes_presentation() {
        let cycle = match parse_cycle(
            r#"```json
            {
              "draft_interpretation": "The user asked for a reason.",
              "draft_initial_judgment": "AskWhy",
              "draft_reasons": "The request is ambiguous.",
              "draft_doubts": [],
              "review_strongest_objection": "The user may expect a direct answer.",
              "review_identity_conflicts": [],
              "review_unsupported_claims": [],
              "review_suggested_revision": null,
              "act": "ask why",
              "rationale": "The missing reason blocks a sound answer.",
              "response": "What led you to that conclusion?",
              "confidence": 60,
              "evidence_refs": [],
              "position_change": null,
              "conflict_change": null
            }
            ```"#,
        ) {
            Ok(cycle) => cycle,
            Err(error) => {
                assert!(false, "fixture should parse: {error}");
                return;
            }
        };

        assert!(matches!(cycle.draft.initial_judgment, DecisionKind::AskWhy));
        assert!(matches!(cycle.commitment.final_act, DecisionKind::AskWhy));
        assert_eq!(cycle.draft.reasons, vec!["The request is ambiguous."]);
    }

    #[test]
    fn rejects_natural_language_act_instead_of_guessing() {
        let error = match parse_cycle(
            r#"{
              "draft_interpretation": "The user asked for a reason.",
              "draft_initial_judgment": "agree",
              "draft_reasons": ["The request is ambiguous."],
              "draft_doubts": [],
              "review_strongest_objection": "The user may expect a direct answer.",
              "review_identity_conflicts": [],
              "review_unsupported_claims": [],
              "review_suggested_revision": null,
              "act": "I agree because the request is reasonable.",
              "rationale": "The request is reasonable.",
              "response": "I agree.",
              "confidence": 60,
              "evidence_refs": [],
              "position_change": null,
              "conflict_change": null
            }"#,
        ) {
            Ok(_) => {
                assert!(false, "natural-language acts must fail");
                return;
            }
            Err(error) => error,
        };

        assert!(error.contains("unsupported act"));
    }

    #[test]
    fn null_conflict_id_gets_runtime_id_and_existing_id_is_allowed() {
        let context = test_context();
        let new_cycle =
            parse_cycle_for_context(&conflict_cycle_json(&context, "null", "[]"), &context)
                .expect("new conflict should parse");
        assert_ne!(
            new_cycle.commitment.conflict.as_ref().expect("conflict").id,
            ConflictId::from_uuid(Uuid::nil())
        );

        let existing_id = context.conflicts[0].id.to_string();
        let updated_cycle = parse_cycle_for_context(
            &conflict_cycle_json(&context, &format!("\"{existing_id}\""), "[]"),
            &context,
        )
        .expect("existing conflict should parse");
        assert_eq!(
            updated_cycle.commitment.conflict.expect("conflict").id,
            context.conflicts[0].id
        );
    }

    #[test]
    fn rejects_unknown_conflict_id_and_lists_allowed_ids_in_correction() {
        let context = test_context();
        let error = parse_cycle_for_context(
            &conflict_cycle_json(&context, "\"00000000-0000-0000-0000-000000000099\"", "[]"),
            &context,
        )
        .expect_err("unknown conflict IDs must fail closed");
        assert!(error.message.contains("unknown context conflict"));
        assert!(error.correction.contains(&error.message));
        assert!(error
            .correction
            .contains(&context.conflicts[0].id.to_string()));
        assert!(error
            .correction
            .contains(&context.positions[0].id.to_string()));
        assert!(error
            .correction
            .contains(&context.recent_event_ids[0].to_string()));
        assert!(error.correction.contains("conflict_change.id to null"));

        let invalid = parse_cycle_for_context(
            &conflict_cycle_json(&context, "\"model-created-id\"", "[]"),
            &context,
        )
        .expect_err("arbitrary conflict IDs must fail closed");
        assert!(invalid.message.contains("contains an invalid ID"));
    }

    #[test]
    fn rejects_evidence_outside_context_without_dropping_it() {
        let context = test_context();
        let unknown_event = Uuid::new_v4();
        let error = parse_cycle_for_context(
            &conflict_cycle_json(&context, "null", &format!("[\"{unknown_event}\"]")),
            &context,
        )
        .expect_err("out-of-context evidence must fail closed");
        assert!(error.message.contains("outside the supplied context"));
        assert!(error.correction.contains(&error.message));
        assert!(error.correction.contains("Use evidence_refs only from"));
        assert!(error.correction.contains(&unknown_event.to_string()));
    }
}
