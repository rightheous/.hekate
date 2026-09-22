use serde::Serialize;

use super::model::{EventId, MemoryId, PrincipalId};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseVerbosity {
    Compact,
    Balanced,
    Detailed,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseStepSize {
    Small,
    Balanced,
    Large,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProgressVisibility {
    Minimal,
    Normal,
    Explicit,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TechnicalDepth {
    Low,
    Balanced,
    High,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseFormatPreference {
    Natural,
    Markdown,
    Structured,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponsePreferenceKey {
    Language,
    Verbosity,
    StepSize,
    ProgressVisibility,
    NextActionFirst,
    TechnicalDepth,
    PreferredFormat,
}

impl ResponsePreferenceKey {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Language => "language",
            Self::Verbosity => "verbosity",
            Self::StepSize => "step_size",
            Self::ProgressVisibility => "progress_visibility",
            Self::NextActionFirst => "next_action_first",
            Self::TechnicalDepth => "technical_depth",
            Self::PreferredFormat => "preferred_format",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ResponsePreferenceScope {
    Principal(PrincipalId),
}

impl ResponsePreferenceScope {
    pub const fn kind(self) -> &'static str {
        match self {
            Self::Principal(_) => "principal",
        }
    }

    pub fn authority(self) -> PrincipalId {
        match self {
            Self::Principal(principal_id) => principal_id,
        }
    }
}

impl serde::Serialize for ResponsePreferenceScope {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.kind())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ResponsePreferenceEvidence {
    pub memory_id: MemoryId,
    pub source_event_id: EventId,
    pub key: ResponsePreferenceKey,
    pub scope: ResponsePreferenceScope,
    pub memory_revision_or_version: u64,
}

/// Derived from active explicit-preference Memory; it is never authoritative or stored.
/// Current-turn format requests, safety, approval, accuracy, and evidence rules remain higher priority.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ResponseProfile {
    pub language: Option<String>,
    pub verbosity: ResponseVerbosity,
    pub step_size: ResponseStepSize,
    pub progress_visibility: ProgressVisibility,
    pub next_action_first: bool,
    pub technical_depth: TechnicalDepth,
    pub preferred_format: ResponseFormatPreference,
    pub evidence: Vec<ResponsePreferenceEvidence>,
    pub as_of_revision: u64,
    pub profile_hash: String,
}

impl Default for ResponseProfile {
    fn default() -> Self {
        Self {
            language: None,
            verbosity: ResponseVerbosity::Balanced,
            step_size: ResponseStepSize::Balanced,
            progress_visibility: ProgressVisibility::Normal,
            next_action_first: false,
            technical_depth: TechnicalDepth::Balanced,
            preferred_format: ResponseFormatPreference::Natural,
            evidence: Vec::new(),
            as_of_revision: 0,
            profile_hash: String::new(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ResponseProfileReport {
    pub considered_memories: usize,
    pub applied_preferences: usize,
    pub ignored_non_explicit: usize,
    pub ignored_malformed: usize,
    pub ignored_scope_mismatch: usize,
    pub conflicting_keys: usize,
}

impl Default for ResponseProfileReport {
    fn default() -> Self {
        Self {
            considered_memories: 0,
            applied_preferences: 0,
            ignored_non_explicit: 0,
            ignored_malformed: 0,
            ignored_scope_mismatch: 0,
            conflicting_keys: 0,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ResponseProfileResolution {
    pub profile: ResponseProfile,
    pub report: ResponseProfileReport,
}
