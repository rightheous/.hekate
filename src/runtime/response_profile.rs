use std::collections::BTreeMap;

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::core::{
    ActiveMemory, ActiveMemoryStatus, CurrentState, EventId, EventKind, ExperienceEvent,
    MemoryKind, PrincipalId, ProgressVisibility, RelationshipId, ResponseFormatPreference,
    ResponsePreferenceEvidence, ResponsePreferenceKey, ResponsePreferenceScope, ResponseProfile,
    ResponseProfileReport, ResponseProfileResolution, ResponseStepSize, ResponseVerbosity, TaskId,
    TechnicalDepth,
};

const RESPONSE_PREFERENCE_SCHEMA: &str = "hekate.response_preference.v1";

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum PreferenceValue {
    Language(String),
    Verbosity(ResponseVerbosity),
    StepSize(ResponseStepSize),
    ProgressVisibility(ProgressVisibility),
    NextActionFirst(bool),
    TechnicalDepth(TechnicalDepth),
    PreferredFormat(ResponseFormatPreference),
}

#[derive(Clone, Debug)]
struct PreferenceCandidate {
    value: PreferenceValue,
    evidence: ResponsePreferenceEvidence,
    source_event_sequence: u64,
}

/// Resolves the v1 principal-scoped profile from the supplied projected state.
/// Relationship and task arguments are reserved by the API; v1 has no such Memory scope.
/// Callers must apply current-turn and CLI format requests above this derived result.
pub fn resolve_response_profile(
    state: &CurrentState,
    events: &[ExperienceEvent],
    principal_id: PrincipalId,
    _relationship_id: Option<RelationshipId>,
    _task_id: Option<TaskId>,
    as_of_revision: u64,
) -> ResponseProfile {
    resolve_response_profile_with_report(
        state,
        events,
        principal_id,
        _relationship_id,
        _task_id,
        as_of_revision,
    )
    .profile
}

pub fn resolve_response_profile_with_report(
    state: &CurrentState,
    events: &[ExperienceEvent],
    principal_id: PrincipalId,
    _relationship_id: Option<RelationshipId>,
    _task_id: Option<TaskId>,
    as_of_revision: u64,
) -> ResponseProfileResolution {
    let event_sequences = event_sequences(events, as_of_revision);
    let mut report = ResponseProfileReport::default();
    let mut candidates: BTreeMap<ResponsePreferenceKey, Vec<PreferenceCandidate>> = BTreeMap::new();

    for memory in state.active_memories.values() {
        if !matches!(memory.status, ActiveMemoryStatus::Active) {
            continue;
        }
        report.considered_memories += 1;
        if memory.subject_principal_id != Some(principal_id) {
            report.ignored_scope_mismatch += 1;
            continue;
        }
        if !matches!(memory.kind, MemoryKind::ExplicitPreference) {
            report.ignored_non_explicit += 1;
            continue;
        }
        let Some((key, value)) = parse_preference(&memory.content) else {
            report.ignored_malformed += 1;
            continue;
        };
        let Some((source_event_id, memory_revision, source_event_sequence)) =
            provenance(memory, events, &event_sequences)
        else {
            report.ignored_malformed += 1;
            continue;
        };
        candidates
            .entry(key)
            .or_default()
            .push(PreferenceCandidate {
                value,
                evidence: ResponsePreferenceEvidence {
                    memory_id: memory.id,
                    source_event_id,
                    key,
                    scope: ResponsePreferenceScope::Principal(principal_id),
                    memory_revision_or_version: memory_revision,
                },
                source_event_sequence,
            });
    }

    let mut profile = ResponseProfile {
        as_of_revision,
        ..ResponseProfile::default()
    };
    for (_key, mut options) in candidates {
        if options
            .iter()
            .map(|option| &option.value)
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            > 1
        {
            report.conflicting_keys += 1;
        }
        let winner = options
            .drain(..)
            .max_by(|left, right| compare_candidates(left, right))
            .expect("preference candidate list cannot be empty");
        apply_value(&mut profile, winner.value);
        profile.evidence.push(winner.evidence);
    }
    profile
        .evidence
        .sort_by(|left, right| evidence_sort_key(left).cmp(&evidence_sort_key(right)));
    report.applied_preferences = profile.evidence.len();
    profile.profile_hash = profile_hash(&profile);

    ResponseProfileResolution { profile, report }
}

fn parse_preference(content: &str) -> Option<(ResponsePreferenceKey, PreferenceValue)> {
    let value: serde_json::Value = serde_json::from_str(content).ok()?;
    let object = value.as_object()?;
    if object.len() != 3
        || object.get("schema").and_then(serde_json::Value::as_str)
            != Some(RESPONSE_PREFERENCE_SCHEMA)
    {
        return None;
    }
    let key = object.get("key")?.as_str()?;
    let value = object.get("value")?;
    match key {
        "language" => Some((
            ResponsePreferenceKey::Language,
            PreferenceValue::Language(parse_language(value)?),
        )),
        "verbosity" => Some((
            ResponsePreferenceKey::Verbosity,
            PreferenceValue::Verbosity(parse_verbosity(value)?),
        )),
        "step_size" => Some((
            ResponsePreferenceKey::StepSize,
            PreferenceValue::StepSize(parse_step_size(value)?),
        )),
        "progress_visibility" => Some((
            ResponsePreferenceKey::ProgressVisibility,
            PreferenceValue::ProgressVisibility(parse_progress_visibility(value)?),
        )),
        "next_action_first" => Some((
            ResponsePreferenceKey::NextActionFirst,
            PreferenceValue::NextActionFirst(value.as_bool()?),
        )),
        "technical_depth" => Some((
            ResponsePreferenceKey::TechnicalDepth,
            PreferenceValue::TechnicalDepth(parse_technical_depth(value)?),
        )),
        "preferred_format" => Some((
            ResponsePreferenceKey::PreferredFormat,
            PreferenceValue::PreferredFormat(parse_format(value)?),
        )),
        _ => None,
    }
}

fn parse_language(value: &serde_json::Value) -> Option<String> {
    let language = value.as_str()?.trim();
    if language.is_empty()
        || language.chars().count() > 35
        || language.starts_with('-')
        || language.ends_with('-')
        || language.contains("--")
        || !language
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '-')
    {
        return None;
    }
    Some(language.to_owned())
}

fn parse_verbosity(value: &serde_json::Value) -> Option<ResponseVerbosity> {
    match value.as_str()? {
        "compact" => Some(ResponseVerbosity::Compact),
        "balanced" => Some(ResponseVerbosity::Balanced),
        "detailed" => Some(ResponseVerbosity::Detailed),
        _ => None,
    }
}

fn parse_step_size(value: &serde_json::Value) -> Option<ResponseStepSize> {
    match value.as_str()? {
        "small" => Some(ResponseStepSize::Small),
        "balanced" => Some(ResponseStepSize::Balanced),
        "large" => Some(ResponseStepSize::Large),
        _ => None,
    }
}

fn parse_progress_visibility(value: &serde_json::Value) -> Option<ProgressVisibility> {
    match value.as_str()? {
        "minimal" => Some(ProgressVisibility::Minimal),
        "normal" => Some(ProgressVisibility::Normal),
        "explicit" => Some(ProgressVisibility::Explicit),
        _ => None,
    }
}

fn parse_technical_depth(value: &serde_json::Value) -> Option<TechnicalDepth> {
    match value.as_str()? {
        "low" => Some(TechnicalDepth::Low),
        "balanced" => Some(TechnicalDepth::Balanced),
        "high" => Some(TechnicalDepth::High),
        _ => None,
    }
}

fn parse_format(value: &serde_json::Value) -> Option<ResponseFormatPreference> {
    match value.as_str()? {
        "natural" => Some(ResponseFormatPreference::Natural),
        "markdown" => Some(ResponseFormatPreference::Markdown),
        "structured" => Some(ResponseFormatPreference::Structured),
        _ => None,
    }
}

fn event_sequences(events: &[ExperienceEvent], as_of_revision: u64) -> BTreeMap<EventId, u64> {
    events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| {
            let sequence = u64::try_from(index).ok()?.saturating_add(1);
            (sequence <= as_of_revision).then_some((event.event_id, sequence))
        })
        .collect()
}

fn provenance(
    memory: &ActiveMemory,
    events: &[ExperienceEvent],
    event_sequences: &BTreeMap<EventId, u64>,
) -> Option<(EventId, u64, u64)> {
    let source = memory
        .source_event_ids
        .iter()
        .filter_map(|event_id| {
            let sequence = event_sequences.get(event_id).copied().unwrap_or_default();
            (sequence > 0).then_some((*event_id, sequence))
        })
        .max_by(|left, right| (left.1, left.0).cmp(&(right.1, right.0)));
    let lifecycle = events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| {
            let sequence = u64::try_from(index).ok()?.saturating_add(1);
            if event_sequences.get(&event.event_id).copied() != Some(sequence)
                || !matches!(
                    &event.event_kind,
                    EventKind::MemoryPromoted
                        | EventKind::MemorySuperseded
                        | EventKind::MemoryExpired
                )
                || event.subject.as_ref().map(|subject| subject.id) != Some(memory.id.uuid())
            {
                return None;
            }
            Some((event.event_id, sequence))
        })
        .max_by_key(|(_, sequence)| *sequence);
    let source = source.or(lifecycle)?;
    let memory_revision = lifecycle.map(|(_, sequence)| sequence).unwrap_or(source.1);
    Some((source.0, memory_revision, source.1))
}

fn compare_candidates(
    left: &PreferenceCandidate,
    right: &PreferenceCandidate,
) -> std::cmp::Ordering {
    let authority = (
        left.evidence.memory_revision_or_version,
        left.source_event_sequence,
    )
        .cmp(&(
            right.evidence.memory_revision_or_version,
            right.source_event_sequence,
        ));
    if authority == std::cmp::Ordering::Equal {
        right
            .evidence
            .memory_id
            .to_string()
            .cmp(&left.evidence.memory_id.to_string())
    } else {
        authority
    }
}

fn evidence_sort_key(evidence: &ResponsePreferenceEvidence) -> (String, String, String) {
    (
        evidence.key.as_str().to_owned(),
        evidence.memory_id.to_string(),
        evidence.source_event_id.to_string(),
    )
}

fn apply_value(profile: &mut ResponseProfile, value: PreferenceValue) {
    match value {
        PreferenceValue::Language(value) => profile.language = Some(value),
        PreferenceValue::Verbosity(value) => profile.verbosity = value,
        PreferenceValue::StepSize(value) => profile.step_size = value,
        PreferenceValue::ProgressVisibility(value) => profile.progress_visibility = value,
        PreferenceValue::NextActionFirst(value) => profile.next_action_first = value,
        PreferenceValue::TechnicalDepth(value) => profile.technical_depth = value,
        PreferenceValue::PreferredFormat(value) => profile.preferred_format = value,
    }
}

#[derive(Serialize)]
struct ProfileHashInput {
    language: Option<String>,
    verbosity: ResponseVerbosity,
    step_size: ResponseStepSize,
    progress_visibility: ProgressVisibility,
    next_action_first: bool,
    technical_depth: TechnicalDepth,
    preferred_format: ResponseFormatPreference,
    evidence: Vec<ProfileHashEvidence>,
    as_of_revision: u64,
}

#[derive(Serialize)]
struct ProfileHashEvidence {
    memory_id: String,
    source_event_id: String,
    key: ResponsePreferenceKey,
    scope: String,
    memory_revision_or_version: u64,
}

fn profile_hash(profile: &ResponseProfile) -> String {
    let input = ProfileHashInput {
        language: profile.language.clone(),
        verbosity: profile.verbosity,
        step_size: profile.step_size,
        progress_visibility: profile.progress_visibility,
        next_action_first: profile.next_action_first,
        technical_depth: profile.technical_depth,
        preferred_format: profile.preferred_format,
        evidence: profile
            .evidence
            .iter()
            .map(|evidence| ProfileHashEvidence {
                memory_id: evidence.memory_id.to_string(),
                source_event_id: evidence.source_event_id.to_string(),
                key: evidence.key,
                scope: format!("{}:{}", evidence.scope.kind(), evidence.scope.authority()),
                memory_revision_or_version: evidence.memory_revision_or_version,
            })
            .collect(),
        as_of_revision: profile.as_of_revision,
    };
    let bytes = serde_json::to_vec(&input).unwrap_or_default();
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}
