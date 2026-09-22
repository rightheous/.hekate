use std::sync::Arc;

use serde::Serialize;
use thiserror::Error;

use crate::core::event::{
    EntityKind, EntityRef, EventError, EventKind, EventSource, ExperienceEvent,
};
use crate::core::transition::{transition_operation, transition_run, TransitionError};
use crate::core::{
    ActiveMemory, ActiveMemoryStatus, Approval, ApprovalId, ApprovalStatus, Artifact, ArtifactId,
    Attempt, AttemptStatus, CognitiveTrace, CompletionClaim, CompletionClaimId,
    CompletionCriterion, CompletionCriterionId, ConflictStatus, CurrentState, DecisionKind,
    EvidenceRef, Focus, Goal, GoalId, GoalStatus, IdentityVersion, IdentityVersionId,
    InteractionResult, MemoryCandidate, MemoryCandidateId, MemoryCandidateStatus, MemoryId,
    MemoryKind, Observation, Operation, OperationId, OperationStatus, Position, PositionStatus,
    Principal, PrincipalId, PrincipalKind, RecallBundle, RecallQuery, Receipt, ReceiptId,
    Relationship, RelationshipId, ResponseRecord, Run, RunId, RunStatus, Task, TaskId, TaskStatus,
    Verification, VerificationId, VerificationStatus, WorkingState, WorkingStateId,
};
use crate::ports::{
    CapabilityCatalog, CapabilityError, CognitiveError, CognitiveModel, Policy, PolicyError,
    Storage, StorageError,
};
use crate::runtime::completion::{
    CompletionError, CompletionGate, CompletionGateResult, CompletionStatusReport,
};
use crate::runtime::deliberation::{
    decision_from_cycle, validate_judgment, JudgmentValidationError,
};
use crate::runtime::focus::resolve_focus;
use crate::runtime::projector::{ProjectionError, Projector};
use crate::runtime::recall::{SemanticRecall, DEFAULT_RECALL_LIMIT};
use crate::runtime::recovery::{recover, RecoveryError, RecoveryReport};

#[derive(Debug, Error)]
pub enum EngineError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Projection(#[from] ProjectionError),
    #[error(transparent)]
    Cognitive(#[from] CognitiveError),
    #[error(transparent)]
    Event(#[from] EventError),
    #[error(transparent)]
    Transition(#[from] TransitionError),
    #[error("serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error(transparent)]
    Policy(#[from] PolicyError),
    #[error(transparent)]
    Capability(#[from] CapabilityError),
    #[error("policy denied capability execution: {0}")]
    PolicyDenied(String),
    #[error("invalid observation: {0}")]
    InvalidObservation(String),
    #[error("invalid cognitive trace: {0}")]
    InvalidCognitiveTrace(String),
    #[error(transparent)]
    Judgment(#[from] JudgmentValidationError),
    #[error("stale thought context: expected revision {expected}, actual {actual}")]
    StaleContext { expected: u64, actual: u64 },
    #[error("state item not found: {0}")]
    NotFound(String),
    #[error("operation cannot continue: {0}")]
    InvalidOperation(String),
    #[error(transparent)]
    Completion(#[from] CompletionError),
    #[error(transparent)]
    Recovery(#[from] RecoveryError),
}

pub struct Engine {
    storage: Arc<dyn Storage>,
    projector: Projector,
    model: Arc<dyn CognitiveModel>,
    policy: Arc<dyn Policy>,
    capabilities: Arc<dyn CapabilityCatalog>,
    recall: Option<Arc<SemanticRecall>>,
    hekate_id: PrincipalId,
    user_id: PrincipalId,
}

impl Engine {
    pub fn new(
        storage: Arc<dyn Storage>,
        model: Arc<dyn CognitiveModel>,
        policy: Arc<dyn Policy>,
        capabilities: Arc<dyn CapabilityCatalog>,
        hekate_id: PrincipalId,
        user_id: PrincipalId,
    ) -> Self {
        Self {
            projector: Projector::new(storage.clone()),
            storage,
            model,
            policy,
            capabilities,
            recall: None,
            hekate_id,
            user_id,
        }
    }

    pub fn with_semantic_recall(mut self, recall: Arc<SemanticRecall>) -> Self {
        self.recall = Some(recall);
        self
    }

    pub fn hekate_id(&self) -> PrincipalId {
        self.hekate_id
    }

    pub fn user_id(&self) -> PrincipalId {
        self.user_id
    }

    pub async fn handle(&self, observation: Observation) -> Result<InteractionResult, EngineError> {
        if self.user_id == self.hekate_id {
            return Err(EngineError::InvalidObservation(
                "user and HEKATE must be different principals".to_owned(),
            ));
        }
        if observation.actor_id != self.user_id {
            return Err(EngineError::InvalidObservation(
                "observation actor is not the configured user".to_owned(),
            ));
        }
        if observation.content.trim().is_empty() {
            return Err(EngineError::InvalidObservation(
                "observation content cannot be empty".to_owned(),
            ));
        }
        let mut state = self.storage.load_state().await?;
        if let Some(existing) = state.observations.values().find(|existing| {
            observation.message_id.is_some()
                && existing.source_type == observation.source_type
                && existing.source_ref == observation.source_ref
        }) {
            let events = self.storage.load_events().await?;
            let decision = events
                .iter()
                .rev()
                .find(|event| {
                    event.event_kind == EventKind::DecisionCreated
                        && event.correlation_id.as_deref() == Some(existing.id.to_string().as_str())
                })
                .map(|event| serde_json::from_value(event.payload.clone()))
                .transpose()?;
            if let Some(decision) = decision {
                return Ok(InteractionResult {
                    observation_id: existing.id,
                    focus: resolve_focus(&state, existing),
                    decision,
                    revision: state.revision,
                    operation_id: None,
                    approval_id: None,
                });
            }
            return Err(EngineError::InvalidObservation(
                "duplicate external message has no completed decision".to_owned(),
            ));
        }
        let _ = self.ensure_identity(state).await?;
        let observation_event = self.event(
            self.user_id,
            EventKind::ObservationRecorded,
            Some(EntityRef::new(
                EntityKind::Observation,
                observation.id.uuid(),
            )),
            &observation,
            Some(observation.id.to_string()),
            None,
        )?;
        state = self.projector.record(observation_event.clone()).await?;

        let mut focus = resolve_focus(&state, &observation);
        if focus.run_id.is_none() {
            let (next_state, next_focus) = self.create_focus(&observation).await?;
            state = next_state;
            focus = next_focus;
        }

        let events = self.storage.load_events().await?;
        let recall = self
            .recall_bundle(
                &observation.content,
                observation_event.event_id,
                &state,
                &events,
            )
            .await;
        let context = crate::runtime::context::build_context_with_recall(
            &state,
            &observation,
            &focus,
            &events,
            self.capabilities.names(),
            recall,
        );
        let cycle = match self.model.think(&context).await {
            Ok(cycle) => cycle,
            Err(error) => {
                let trace = error.trace().clone();
                self.save_trace(&trace, "failed", trace.error_kind.clone())
                    .await?;
                return Err(EngineError::Cognitive(error));
            }
        };
        let latest_state = self.storage.load_state().await?;
        if latest_state.revision != context.event_sequence {
            let trace = cycle.trace.clone();
            self.save_trace(&trace, "stale_context", Some("stale_context".to_owned()))
                .await?;
            return Err(EngineError::StaleContext {
                expected: context.event_sequence,
                actual: latest_state.revision,
            });
        }
        if cycle.trace.context_sequence != context.event_sequence
            || cycle.trace.context_hash != context.snapshot_hash
        {
            let trace = cycle.trace.clone();
            self.save_trace(
                &trace,
                "rejected",
                Some("trace_context_mismatch".to_owned()),
            )
            .await?;
            return Err(EngineError::InvalidCognitiveTrace(
                "cognitive trace does not match its thought context".to_owned(),
            ));
        }
        let mut decision = decision_from_cycle(&context, &cycle);
        let trace = trace_for_decision(&cycle.trace, &decision);
        let event_ids = evidence_event_ids(&context);
        if let Err(error) =
            validate_judgment(&context, &state, &event_ids, &decision, self.hekate_id)
        {
            let trace = trace.clone();
            self.save_trace(&trace, "rejected", Some("invalid_judgment".to_owned()))
                .await?;
            return Err(EngineError::Judgment(error));
        }
        let mut action_policy = None;
        if let Some(intent) = &decision.action {
            let policy_decision = self.policy.evaluate(self.hekate_id, intent)?;
            if !policy_decision.allowed {
                let trace = trace.clone();
                self.save_trace(&trace, "rejected", Some("policy_denied".to_owned()))
                    .await?;
                return Err(EngineError::PolicyDenied(policy_decision.reason));
            }
            action_policy = Some(policy_decision);
        }

        let correlation_id = Some(observation.id.to_string());
        let decision_event_id = crate::core::EventId::new();
        if let Some(intent) = decision.action.as_mut() {
            intent.proposed_by_event = Some(decision_event_id);
        }
        let decision_event = self.event_with_id(
            decision_event_id,
            self.hekate_id,
            EventKind::DecisionCreated,
            Some(EntityRef::new(EntityKind::Decision, decision.id.uuid())),
            &decision,
            correlation_id.clone(),
            None,
        )?;
        let mut final_events = vec![decision_event.clone()];
        if let Some(position) = decision.position.clone() {
            final_events.push(self.event(
                self.hekate_id,
                position_event_kind(&state, &position),
                Some(EntityRef::new(EntityKind::Position, position.id.uuid())),
                &position,
                correlation_id.clone(),
                Some(decision_event.event_id),
            )?);
        }
        if let Some(user_position) = decision.user_position.clone() {
            final_events.push(self.event(
                observation.actor_id,
                position_event_kind(&state, &user_position),
                Some(EntityRef::new(
                    EntityKind::Position,
                    user_position.id.uuid(),
                )),
                &user_position,
                correlation_id.clone(),
                Some(decision_event.event_id),
            )?);
        }
        if let Some(conflict) = decision.conflict.clone() {
            final_events.push(self.event(
                self.hekate_id,
                conflict_event_kind(&state, &conflict),
                Some(EntityRef::new(EntityKind::Conflict, conflict.id.uuid())),
                &conflict,
                correlation_id.clone(),
                Some(decision_event.event_id),
            )?);
        }
        let mut planned_operation = None;
        let mut requested_approval = None;
        if let Some(intent) = decision.action.clone() {
            final_events.push(self.event(
                self.hekate_id,
                EventKind::ActionIntentCreated,
                Some(EntityRef::new(EntityKind::ActionIntent, intent.id.uuid())),
                &intent,
                correlation_id.clone(),
                Some(decision_event.event_id),
            )?);
            let operation_id = OperationId::new();
            let approval_id = action_policy
                .as_ref()
                .filter(|policy| policy.requires_approval)
                .map(|_| ApprovalId::new());
            let operation = Operation {
                id: operation_id,
                intent_id: intent.id,
                status: OperationStatus::Planned,
                idempotency_key: format!("intent:{}", intent.id),
                approval_id,
                started_at: None,
                finished_at: None,
            };
            final_events.push(self.event(
                self.hekate_id,
                EventKind::OperationPlanned,
                Some(EntityRef::new(EntityKind::Operation, operation.id.uuid())),
                &operation,
                correlation_id.clone(),
                Some(decision_event.event_id),
            )?);
            if let Some(approval_id) = approval_id {
                let approval = Approval {
                    id: approval_id,
                    intent_id: intent.id,
                    operation_id,
                    requested_by: self.hekate_id,
                    status: ApprovalStatus::Pending,
                    reason: action_policy
                        .as_ref()
                        .map(|policy| policy.reason.clone())
                        .unwrap_or_else(|| "explicit approval requested".to_owned()),
                    resolved_by: None,
                    resolved_at: None,
                    created_at: crate::core::model::now(),
                };
                final_events.push(self.event(
                    self.hekate_id,
                    EventKind::ApprovalRequested,
                    Some(EntityRef::new(EntityKind::Approval, approval.id.uuid())),
                    &approval,
                    correlation_id.clone(),
                    Some(decision_event.event_id),
                )?);
                requested_approval = Some(approval.id);
            }
            planned_operation = Some(operation.id);
        }
        final_events.push(self.event(
            self.hekate_id,
            EventKind::ResponseProduced,
            Some(EntityRef::new(EntityKind::Decision, decision.id.uuid())),
            &ResponseRecord {
                decision_id: decision.id,
                content: decision.message.clone(),
                created_at: crate::core::model::now(),
            },
            correlation_id.clone(),
            Some(decision_event.event_id),
        )?);

        if let Some(run_id) = focus.run_id {
            let mut working_state =
                state
                    .working_states
                    .get(&run_id)
                    .cloned()
                    .unwrap_or(WorkingState {
                        id: WorkingStateId::new(),
                        run_id,
                        revision: 0,
                        completed_observations: Vec::new(),
                        notes: Vec::new(),
                        next_action: None,
                    });
            working_state.revision += 1;
            if !working_state
                .completed_observations
                .contains(&observation.id)
            {
                working_state.completed_observations.push(observation.id);
            }
            working_state.next_action = Some(decision.message.clone());
            final_events.push(self.event(
                self.hekate_id,
                EventKind::WorkingStateUpdated,
                Some(EntityRef::new(
                    EntityKind::WorkingState,
                    working_state.id.uuid(),
                )),
                &working_state,
                correlation_id.clone(),
                Some(decision_event.event_id),
            )?);

            if matches!(
                decision.kind,
                DecisionKind::Suspend | DecisionKind::Complete
            ) {
                if let Some(run) = state.runs.get(&run_id).cloned() {
                    let mut completed = run;
                    let target = if matches!(decision.kind, DecisionKind::Suspend) {
                        RunStatus::Suspended
                    } else {
                        RunStatus::Completed
                    };
                    transition_run(&mut completed, target.clone())?;
                    if matches!(target, RunStatus::Completed) {
                        completed.completed_at = Some(crate::core::model::now());
                    }
                    final_events.push(self.event(
                        self.hekate_id,
                        if matches!(target, RunStatus::Completed) {
                            EventKind::RunCompleted
                        } else {
                            EventKind::RunSuspended
                        },
                        Some(EntityRef::new(EntityKind::Run, completed.id.uuid())),
                        &completed,
                        correlation_id.clone(),
                        Some(decision_event.event_id),
                    )?);
                    if let Some(mut attempt) = state
                        .attempts
                        .values()
                        .find(|attempt| {
                            attempt.run_id == run_id
                                && matches!(attempt.status, AttemptStatus::Started)
                        })
                        .cloned()
                    {
                        attempt.status = AttemptStatus::Succeeded;
                        attempt.finished_at = Some(crate::core::model::now());
                        final_events.push(self.event(
                            self.hekate_id,
                            EventKind::AttemptCompleted,
                            Some(EntityRef::new(EntityKind::Attempt, attempt.id.uuid())),
                            &attempt,
                            correlation_id.clone(),
                            Some(decision_event.event_id),
                        )?);
                    }
                }
            }
        }

        let mut indexed_events = vec![observation_event];
        indexed_events.extend(final_events.iter().cloned());
        let state = match self
            .projector
            .record_batch(&final_events, Some(context.event_sequence), Some(&trace))
            .await
        {
            Ok(state) => state,
            Err(ProjectionError::StaleContext { expected, actual }) => {
                let trace = trace.clone();
                self.save_trace(&trace, "stale_context", Some("stale_context".to_owned()))
                    .await?;
                return Err(EngineError::StaleContext { expected, actual });
            }
            Err(ProjectionError::Storage(StorageError::StaleContext { expected, actual })) => {
                let trace = trace.clone();
                self.save_trace(&trace, "stale_context", Some("stale_context".to_owned()))
                    .await?;
                return Err(EngineError::StaleContext { expected, actual });
            }
            Err(error) => {
                let trace = trace.clone();
                self.save_trace(&trace, "commit_failed", Some("commit_failed".to_owned()))
                    .await?;
                return Err(EngineError::Projection(error));
            }
        };

        self.index_best_effort(&state, &indexed_events).await;

        Ok(InteractionResult {
            observation_id: observation.id,
            focus,
            decision,
            revision: state.revision,
            operation_id: planned_operation,
            approval_id: requested_approval,
        })
    }

    pub fn authorize(
        &self,
        actor_id: PrincipalId,
        intent: &crate::core::ActionIntent,
    ) -> Result<crate::ports::PolicyDecision, EngineError> {
        Ok(self.policy.evaluate(actor_id, intent)?)
    }

    async fn recall_bundle(
        &self,
        text: &str,
        observation_event_id: crate::core::EventId,
        state: &CurrentState,
        events: &[ExperienceEvent],
    ) -> RecallBundle {
        let query = RecallQuery {
            text: text.to_owned(),
            limit: DEFAULT_RECALL_LIMIT,
            exclude_event_ids: vec![observation_event_id],
        };
        let Some(recall) = self.recall.as_ref() else {
            return RecallBundle::empty(query.query_hash(), "");
        };
        match recall.recall(&query, state, events).await {
            Ok(bundle) => bundle,
            Err(error) => {
                tracing::warn!(
                    event_id = %observation_event_id,
                    error = %error,
                    "semantic recall unavailable"
                );
                RecallBundle::empty(query.query_hash(), recall.embedding_space_id())
            }
        }
    }

    async fn index_best_effort(&self, state: &CurrentState, events: &[ExperienceEvent]) {
        let Some(recall) = self.recall.as_ref() else {
            return;
        };
        if let Err(error) = recall.index_events(state, events).await {
            tracing::warn!(
                event_count = events.len(),
                error = %error,
                "incremental semantic indexing skipped"
            );
        }
    }

    pub async fn execute_capability(
        &self,
        actor_id: PrincipalId,
        intent: &crate::core::ActionIntent,
        input: serde_json::Value,
    ) -> Result<crate::ports::CapabilityResult, EngineError> {
        ensure_operation_matches(intent, &input)?;
        let decision = self.policy.evaluate(actor_id, intent)?;
        if !decision.allowed {
            return Err(EngineError::PolicyDenied(decision.reason));
        }
        if decision.requires_approval {
            return Err(EngineError::PolicyDenied(
                "explicit approval is required before capability execution".to_owned(),
            ));
        }
        Ok(self.capabilities.execute(&intent.capability, input).await?)
    }

    pub async fn state(&self) -> Result<CurrentState, EngineError> {
        Ok(self.storage.load_state().await?)
    }

    pub async fn recovery_report(&self) -> Result<RecoveryReport, EngineError> {
        Ok(recover(self.storage.as_ref()).await?)
    }

    pub fn completion_gate(&self) -> CompletionGate {
        CompletionGate::new(self.storage.clone())
    }

    pub async fn define_completion_criterion(
        &self,
        task_id: TaskId,
        description: impl Into<String>,
        required: bool,
    ) -> Result<CompletionCriterion, EngineError> {
        Ok(self
            .completion_gate()
            .define_criterion(self.user_id, task_id, description, required)
            .await?)
    }

    pub async fn create_completion_claim(
        &self,
        task_id: TaskId,
        criterion_id: CompletionCriterionId,
        confidence: u8,
        evidence_refs: Vec<EvidenceRef>,
        blocker: Option<String>,
        supersedes: Option<CompletionClaimId>,
    ) -> Result<CompletionClaim, EngineError> {
        Ok(self
            .completion_gate()
            .create_claim(
                self.user_id,
                task_id,
                criterion_id,
                confidence,
                evidence_refs,
                blocker,
                supersedes,
            )
            .await?)
    }

    pub async fn verify_completion_claim(
        &self,
        claim_id: CompletionClaimId,
        actor_id: PrincipalId,
        reason: impl Into<String>,
    ) -> Result<CompletionClaim, EngineError> {
        if actor_id == self.hekate_id {
            return Err(CompletionError::ModelCannotVerify.into());
        }
        Ok(self
            .completion_gate()
            .verify_claim(actor_id, claim_id, reason)
            .await?)
    }

    pub async fn reject_completion_claim(
        &self,
        claim_id: CompletionClaimId,
        actor_id: PrincipalId,
        reason: impl Into<String>,
    ) -> Result<CompletionClaim, EngineError> {
        if actor_id == self.hekate_id {
            return Err(CompletionError::ModelCannotVerify.into());
        }
        Ok(self
            .completion_gate()
            .reject_claim(actor_id, claim_id, reason)
            .await?)
    }

    pub async fn evaluate_task_completion(
        &self,
        task_id: TaskId,
        as_of_sequence: u64,
    ) -> Result<CompletionGateResult, EngineError> {
        Ok(self
            .completion_gate()
            .evaluate_task_completion(task_id, as_of_sequence)
            .await?)
    }

    pub async fn completion_status(
        &self,
        task_id: TaskId,
        as_of_sequence: u64,
    ) -> Result<CompletionStatusReport, EngineError> {
        Ok(self
            .completion_gate()
            .status(task_id, as_of_sequence)
            .await?)
    }

    pub async fn create_memory_candidate(
        &self,
        kind: MemoryKind,
        content: String,
        confidence: u8,
        promote: bool,
    ) -> Result<MemoryCandidate, EngineError> {
        if content.trim().is_empty() {
            return Err(EngineError::InvalidObservation(
                "memory content cannot be empty".to_owned(),
            ));
        }
        if confidence > 100 {
            return Err(EngineError::InvalidObservation(
                "memory confidence must be between 0 and 100".to_owned(),
            ));
        }
        let candidate = MemoryCandidate {
            id: MemoryCandidateId::new(),
            kind,
            content,
            subject_principal_id: Some(self.user_id),
            status: MemoryCandidateStatus::Candidate,
            confidence,
            source_event_ids: Vec::new(),
            valid_from: Some(crate::core::model::now()),
            valid_until: None,
            supersedes: None,
            created_at: crate::core::model::now(),
        };
        self.append(
            self.user_id,
            EventKind::MemoryCandidateCreated,
            Some(EntityRef::new(
                EntityKind::MemoryCandidate,
                candidate.id.uuid(),
            )),
            &candidate,
            None,
        )
        .await?;
        if promote {
            self.promote_memory(candidate.id).await?;
        }
        Ok(candidate)
    }

    pub async fn promote_memory(
        &self,
        candidate_id: MemoryCandidateId,
    ) -> Result<ActiveMemory, EngineError> {
        let state = self.storage.load_state().await?;
        let candidate = state
            .memory_candidates
            .get(&candidate_id)
            .cloned()
            .ok_or_else(|| EngineError::NotFound(format!("memory candidate {candidate_id}")))?;
        if !matches!(candidate.status, MemoryCandidateStatus::Candidate) {
            return Err(EngineError::InvalidOperation(
                "only a candidate memory can be promoted".to_owned(),
            ));
        }
        let previous_inferred = if matches!(&candidate.kind, MemoryKind::ExplicitPreference) {
            state
                .active_memories
                .values()
                .find(|memory| {
                    matches!(memory.status, ActiveMemoryStatus::Active)
                        && matches!(&memory.kind, MemoryKind::InferredPreference)
                        && memory.subject_principal_id == candidate.subject_principal_id
                })
                .cloned()
        } else {
            None
        };
        let memory = ActiveMemory {
            id: MemoryId::new(),
            candidate_id,
            kind: candidate.kind,
            content: candidate.content,
            subject_principal_id: candidate.subject_principal_id,
            status: ActiveMemoryStatus::Active,
            confidence: candidate.confidence,
            source_event_ids: candidate.source_event_ids,
            valid_from: candidate.valid_from,
            valid_until: candidate.valid_until,
            supersedes: previous_inferred.as_ref().map(|memory| memory.id),
            last_verified_at: Some(crate::core::model::now()),
            created_at: crate::core::model::now(),
        };
        let promoted_event = self.event(
            self.user_id,
            EventKind::MemoryPromoted,
            Some(EntityRef::new(EntityKind::Memory, memory.id.uuid())),
            &memory,
            None,
            None,
        )?;
        let events = if let Some(mut previous) = previous_inferred {
            previous.status = ActiveMemoryStatus::Superseded;
            vec![
                self.event(
                    self.user_id,
                    EventKind::MemorySuperseded,
                    Some(EntityRef::new(EntityKind::Memory, previous.id.uuid())),
                    &previous,
                    None,
                    None,
                )?,
                promoted_event,
            ]
        } else {
            vec![promoted_event]
        };
        let committed_state = self
            .projector
            .record_batch(&events, Some(state.revision), None)
            .await?;
        self.index_best_effort(&committed_state, &events).await;
        Ok(memory)
    }

    pub async fn reject_memory(
        &self,
        candidate_id: MemoryCandidateId,
    ) -> Result<MemoryCandidate, EngineError> {
        let state = self.storage.load_state().await?;
        let mut candidate = state
            .memory_candidates
            .get(&candidate_id)
            .cloned()
            .ok_or_else(|| EngineError::NotFound(format!("memory candidate {candidate_id}")))?;
        if !matches!(candidate.status, MemoryCandidateStatus::Candidate) {
            return Err(EngineError::InvalidOperation(
                "only a candidate memory can be rejected".to_owned(),
            ));
        }
        candidate.status = MemoryCandidateStatus::Rejected;
        self.append(
            self.user_id,
            EventKind::MemoryRejected,
            Some(EntityRef::new(
                EntityKind::MemoryCandidate,
                candidate.id.uuid(),
            )),
            &candidate,
            None,
        )
        .await?;
        Ok(candidate)
    }

    pub async fn supersede_memory(&self, memory_id: MemoryId) -> Result<ActiveMemory, EngineError> {
        self.set_memory_status(
            memory_id,
            ActiveMemoryStatus::Superseded,
            EventKind::MemorySuperseded,
        )
        .await
    }

    pub async fn expire_memory(&self, memory_id: MemoryId) -> Result<ActiveMemory, EngineError> {
        self.set_memory_status(
            memory_id,
            ActiveMemoryStatus::Expired,
            EventKind::MemoryExpired,
        )
        .await
    }

    async fn set_memory_status(
        &self,
        memory_id: MemoryId,
        status: ActiveMemoryStatus,
        event_kind: EventKind,
    ) -> Result<ActiveMemory, EngineError> {
        let state = self.storage.load_state().await?;
        let mut memory = state
            .active_memories
            .get(&memory_id)
            .cloned()
            .ok_or_else(|| EngineError::NotFound(format!("memory {memory_id}")))?;
        if !matches!(memory.status, ActiveMemoryStatus::Active) {
            return Err(EngineError::InvalidOperation(
                "only an active memory can change lifecycle".to_owned(),
            ));
        }
        memory.status = status;
        let event = self.event(
            self.user_id,
            event_kind,
            Some(EntityRef::new(EntityKind::Memory, memory.id.uuid())),
            &memory,
            None,
            None,
        )?;
        let committed_state = self
            .projector
            .record_batch(std::slice::from_ref(&event), Some(state.revision), None)
            .await?;
        self.index_best_effort(&committed_state, std::slice::from_ref(&event))
            .await;
        Ok(memory)
    }

    pub async fn resolve_approval(
        &self,
        approval_id: ApprovalId,
        approved: bool,
    ) -> Result<Approval, EngineError> {
        let state = self.storage.load_state().await?;
        let Some(existing) = state.approvals.get(&approval_id).cloned() else {
            return Err(EngineError::NotFound(format!("approval {approval_id}")));
        };
        if !matches!(existing.status, ApprovalStatus::Pending) {
            return Err(EngineError::InvalidOperation(
                "approval is already resolved".to_owned(),
            ));
        }
        let mut approval = existing;
        approval.status = if approved {
            ApprovalStatus::Approved
        } else {
            ApprovalStatus::Denied
        };
        approval.resolved_by = Some(self.user_id);
        approval.resolved_at = Some(crate::core::model::now());
        let mut operation = state
            .operations
            .get(&approval.operation_id)
            .cloned()
            .ok_or_else(|| EngineError::NotFound(format!("operation {}", approval.operation_id)))?;
        let next_status = if approved {
            OperationStatus::Authorized
        } else {
            OperationStatus::Failed
        };
        transition_operation(&mut operation, next_status.clone())?;
        let approval_event = self.event(
            self.user_id,
            EventKind::ApprovalResolved,
            Some(EntityRef::new(EntityKind::Approval, approval.id.uuid())),
            &approval,
            Some(approval.id.to_string()),
            None,
        )?;
        let operation_event = self.event(
            self.user_id,
            if approved {
                EventKind::OperationAuthorized
            } else {
                EventKind::OperationFailed
            },
            Some(EntityRef::new(EntityKind::Operation, operation.id.uuid())),
            &operation,
            Some(approval.id.to_string()),
            Some(approval_event.event_id),
        )?;
        self.projector
            .record_batch(
                &[approval_event, operation_event],
                Some(state.revision),
                None,
            )
            .await?;
        Ok(approval)
    }

    pub async fn execute_operation(
        &self,
        operation_id: OperationId,
    ) -> Result<Receipt, EngineError> {
        let state = self.storage.load_state().await?;
        let Some(mut operation) = state.operations.get(&operation_id).cloned() else {
            return Err(EngineError::NotFound(format!("operation {operation_id}")));
        };
        if matches!(
            operation.status,
            OperationStatus::Succeeded
                | OperationStatus::Verified
                | OperationStatus::Failed
                | OperationStatus::Disputed
        ) {
            return state
                .receipts
                .values()
                .find(|receipt| receipt.operation_id == operation_id)
                .cloned()
                .ok_or_else(|| {
                    EngineError::InvalidOperation("operation has no receipt".to_owned())
                });
        }
        if matches!(
            operation.status,
            OperationStatus::Started | OperationStatus::Unknown
        ) {
            return Err(EngineError::InvalidOperation(
                "started or unknown operations require reconciliation before execution".to_owned(),
            ));
        }
        let intent = state
            .action_intents
            .get(&operation.intent_id)
            .cloned()
            .ok_or_else(|| EngineError::NotFound(format!("intent {}", operation.intent_id)))?;
        let policy = self.policy.evaluate(self.hekate_id, &intent)?;
        if !policy.allowed {
            return Err(EngineError::PolicyDenied(policy.reason));
        }
        if policy.requires_approval {
            let approval_id = operation.approval_id.ok_or_else(|| {
                EngineError::InvalidOperation("operation has no approval".to_owned())
            })?;
            let approval = state
                .approvals
                .get(&approval_id)
                .ok_or_else(|| EngineError::NotFound(format!("approval {approval_id}")))?;
            if !matches!(approval.status, ApprovalStatus::Approved) {
                return Err(EngineError::InvalidOperation(
                    "operation requires an approved request".to_owned(),
                ));
            }
        }
        if matches!(operation.status, OperationStatus::Planned) {
            transition_operation(&mut operation, OperationStatus::Authorized)?;
            self.append(
                self.hekate_id,
                EventKind::OperationAuthorized,
                Some(EntityRef::new(EntityKind::Operation, operation.id.uuid())),
                &operation,
                Some(operation.id.to_string()),
            )
            .await?;
        }
        transition_operation(&mut operation, OperationStatus::Started)?;
        operation.started_at = Some(crate::core::model::now());
        let started_event = self.event(
            self.hekate_id,
            EventKind::OperationStarted,
            Some(EntityRef::new(EntityKind::Operation, operation.id.uuid())),
            &operation,
            Some(operation.id.to_string()),
            None,
        )?;
        self.projector.record(started_event).await?;
        let result = self
            .capabilities
            .execute(&intent.capability, intent.arguments.clone())
            .await;
        match result {
            Ok(result) => {
                operation.status = if result.verified {
                    OperationStatus::Succeeded
                } else {
                    OperationStatus::Unknown
                };
                operation.finished_at = Some(crate::core::model::now());
                let receipt = Receipt {
                    id: ReceiptId::new(),
                    operation_id,
                    status: operation.status.clone(),
                    external_reference: None,
                    output: result.data,
                    recorded_at: crate::core::model::now(),
                };
                let outcome_event = self.event(
                    self.hekate_id,
                    if result.verified {
                        EventKind::OperationSucceeded
                    } else {
                        EventKind::OperationStateUnknown
                    },
                    Some(EntityRef::new(EntityKind::Operation, operation.id.uuid())),
                    &operation,
                    Some(operation.id.to_string()),
                    None,
                )?;
                let receipt_event = self.event(
                    self.hekate_id,
                    EventKind::ReceiptRecorded,
                    Some(EntityRef::new(EntityKind::Receipt, receipt.id.uuid())),
                    &receipt,
                    Some(operation.id.to_string()),
                    Some(outcome_event.event_id),
                )?;
                let mut events = vec![outcome_event, receipt_event.clone()];
                if let Some(artifact) = artifact_from_receipt(&receipt, receipt_event.event_id) {
                    events.push(self.event(
                        self.hekate_id,
                        EventKind::ArtifactCreated,
                        Some(EntityRef::new(EntityKind::Artifact, artifact.id.uuid())),
                        &artifact,
                        Some(operation.id.to_string()),
                        Some(receipt_event.event_id),
                    )?);
                }
                if result.verified {
                    let verification = Verification {
                        id: VerificationId::new(),
                        operation_id,
                        status: VerificationStatus::Verified,
                        evidence: result.evidence,
                        checked_at: crate::core::model::now(),
                    };
                    events.push(self.event(
                        self.hekate_id,
                        EventKind::VerificationRecorded,
                        Some(EntityRef::new(
                            EntityKind::Verification,
                            verification.id.uuid(),
                        )),
                        &verification,
                        Some(operation.id.to_string()),
                        Some(receipt_event.event_id),
                    )?);
                }
                self.projector.record_batch(&events, None, None).await?;
                Ok(receipt)
            }
            Err(error @ CapabilityError::OutcomeUnknown(_)) => {
                operation.status = OperationStatus::Unknown;
                let receipt = Receipt {
                    id: ReceiptId::new(),
                    operation_id,
                    status: OperationStatus::Unknown,
                    external_reference: None,
                    output: serde_json::json!({"error": error.to_string()}),
                    recorded_at: crate::core::model::now(),
                };
                let event = self.event(
                    self.hekate_id,
                    EventKind::OperationStateUnknown,
                    Some(EntityRef::new(EntityKind::Operation, operation.id.uuid())),
                    &operation,
                    Some(operation.id.to_string()),
                    None,
                )?;
                let receipt_event = self.event(
                    self.hekate_id,
                    EventKind::ReceiptRecorded,
                    Some(EntityRef::new(EntityKind::Receipt, receipt.id.uuid())),
                    &receipt,
                    Some(operation.id.to_string()),
                    Some(event.event_id),
                )?;
                self.projector
                    .record_batch(&[event, receipt_event], None, None)
                    .await?;
                Ok(receipt)
            }
            Err(error) => {
                operation.status = OperationStatus::Failed;
                operation.finished_at = Some(crate::core::model::now());
                let receipt = Receipt {
                    id: ReceiptId::new(),
                    operation_id,
                    status: OperationStatus::Failed,
                    external_reference: None,
                    output: serde_json::json!({"error": error.to_string()}),
                    recorded_at: crate::core::model::now(),
                };
                let failed_event = self.event(
                    self.hekate_id,
                    EventKind::OperationFailed,
                    Some(EntityRef::new(EntityKind::Operation, operation.id.uuid())),
                    &operation,
                    Some(operation.id.to_string()),
                    None,
                )?;
                let receipt_event = self.event(
                    self.hekate_id,
                    EventKind::ReceiptRecorded,
                    Some(EntityRef::new(EntityKind::Receipt, receipt.id.uuid())),
                    &receipt,
                    Some(operation.id.to_string()),
                    Some(failed_event.event_id),
                )?;
                let verification = Verification {
                    id: VerificationId::new(),
                    operation_id,
                    status: VerificationStatus::Failed,
                    evidence: Vec::new(),
                    checked_at: crate::core::model::now(),
                };
                let verification_event = self.event(
                    self.hekate_id,
                    EventKind::VerificationRecorded,
                    Some(EntityRef::new(
                        EntityKind::Verification,
                        verification.id.uuid(),
                    )),
                    &verification,
                    Some(operation.id.to_string()),
                    Some(receipt_event.event_id),
                )?;
                self.projector
                    .record_batch(
                        &[failed_event, receipt_event, verification_event],
                        None,
                        None,
                    )
                    .await?;
                Ok(receipt)
            }
        }
    }

    pub async fn shutdown(&self) -> Result<(), EngineError> {
        self.storage.shutdown().await?;
        Ok(())
    }

    async fn ensure_identity(&self, mut state: CurrentState) -> Result<CurrentState, EngineError> {
        if !state.principals.contains_key(&self.hekate_id) {
            let principal = Principal {
                id: self.hekate_id,
                kind: PrincipalKind::Hekate,
                name: "HEKATE".to_owned(),
                identity_version_id: None,
            };
            state = self
                .append(
                    self.hekate_id,
                    EventKind::PrincipalCreated,
                    Some(EntityRef::new(EntityKind::Principal, principal.id.uuid())),
                    &principal,
                    None,
                )
                .await?;
        }
        if !state.principals.contains_key(&self.user_id) {
            let principal = Principal {
                id: self.user_id,
                kind: PrincipalKind::User,
                name: "User".to_owned(),
                identity_version_id: None,
            };
            state = self
                .append(
                    self.user_id,
                    EventKind::PrincipalCreated,
                    Some(EntityRef::new(EntityKind::Principal, principal.id.uuid())),
                    &principal,
                    None,
                )
                .await?;
        }
        if state.hekate_identity().is_none() {
            let identity = IdentityVersion {
                id: IdentityVersionId::new(),
                principal_id: self.hekate_id,
                version: 1,
                name: "HEKATE".to_owned(),
                values: vec![
                    "preserve evidence before narrative".to_owned(),
                    "maintain independent judgment".to_owned(),
                ],
                boundaries: vec![
                    "do not pretend agreement".to_owned(),
                    "do not execute external mutation without policy".to_owned(),
                ],
                created_at: crate::core::model::now(),
                supersedes: None,
            };
            state = self
                .append(
                    self.hekate_id,
                    EventKind::IdentityVersionCreated,
                    Some(EntityRef::new(
                        EntityKind::IdentityVersion,
                        identity.id.uuid(),
                    )),
                    &identity,
                    None,
                )
                .await?;
        }
        let has_relationship = state.relationships.values().any(|relationship| {
            relationship.participants.contains(&self.hekate_id)
                && relationship.participants.contains(&self.user_id)
        });
        if !has_relationship {
            let relationship = Relationship {
                id: RelationshipId::new(),
                participants: vec![self.user_id, self.hekate_id],
                shared_commitments: Vec::new(),
                unresolved_conflicts: Vec::new(),
                trust_by_domain: Default::default(),
                interaction_norms: vec!["state reasons and evidence when disagreeing".to_owned()],
            };
            state = self
                .append(
                    self.hekate_id,
                    EventKind::RelationshipCreated,
                    Some(EntityRef::new(
                        EntityKind::Relationship,
                        relationship.id.uuid(),
                    )),
                    &relationship,
                    None,
                )
                .await?;
        }
        Ok(state)
    }

    async fn create_focus(
        &self,
        observation: &Observation,
    ) -> Result<(CurrentState, Focus), EngineError> {
        let title = observation.content.chars().take(80).collect::<String>();
        let goal = Goal {
            id: GoalId::new(),
            owner_principal_id: self.user_id,
            participants: vec![self.user_id, self.hekate_id],
            title: if title.is_empty() {
                "Untitled goal".to_owned()
            } else {
                title.clone()
            },
            description: observation.content.clone(),
            status: GoalStatus::Active,
            created_at: crate::core::model::now(),
        };
        self.append(
            self.user_id,
            EventKind::GoalCreated,
            Some(EntityRef::new(EntityKind::Goal, goal.id.uuid())),
            &goal,
            Some(observation.id.to_string()),
        )
        .await?;
        let task = Task {
            id: TaskId::new(),
            goal_id: Some(goal.id),
            title,
            status: TaskStatus::InProgress,
            created_at: crate::core::model::now(),
        };
        self.append(
            self.user_id,
            EventKind::TaskCreated,
            Some(EntityRef::new(EntityKind::Task, task.id.uuid())),
            &task,
            Some(observation.id.to_string()),
        )
        .await?;
        let run = Run {
            id: RunId::new(),
            task_id: Some(task.id),
            status: RunStatus::Running,
            started_at: crate::core::model::now(),
            completed_at: None,
        };
        self.append(
            self.user_id,
            EventKind::RunStarted,
            Some(EntityRef::new(EntityKind::Run, run.id.uuid())),
            &run,
            Some(observation.id.to_string()),
        )
        .await?;
        let attempt = Attempt {
            id: crate::core::AttemptId::new(),
            run_id: run.id,
            status: AttemptStatus::Started,
            started_at: crate::core::model::now(),
            finished_at: None,
        };
        let state = self
            .append(
                self.user_id,
                EventKind::AttemptStarted,
                Some(EntityRef::new(EntityKind::Attempt, attempt.id.uuid())),
                &attempt,
                Some(observation.id.to_string()),
            )
            .await?;
        Ok((
            state,
            Focus {
                goal_id: Some(goal.id),
                task_id: Some(task.id),
                run_id: Some(run.id),
            },
        ))
    }

    async fn append<T: Serialize>(
        &self,
        actor_id: PrincipalId,
        event_kind: EventKind,
        subject: Option<EntityRef>,
        payload: &T,
        correlation_id: Option<String>,
    ) -> Result<CurrentState, EngineError> {
        let event = self.event(actor_id, event_kind, subject, payload, correlation_id, None)?;
        Ok(self.projector.record(event).await?)
    }

    fn event<T: Serialize>(
        &self,
        actor_id: PrincipalId,
        event_kind: EventKind,
        subject: Option<EntityRef>,
        payload: &T,
        correlation_id: Option<String>,
        causation_id: Option<crate::core::EventId>,
    ) -> Result<ExperienceEvent, EngineError> {
        Ok(ExperienceEvent::new(
            actor_id,
            event_kind,
            subject,
            serde_json::to_value(payload)?,
            EventSource::new("engine", None),
            causation_id,
            correlation_id,
            Some(1.0),
        )?)
    }

    fn event_with_id<T: Serialize>(
        &self,
        event_id: crate::core::EventId,
        actor_id: PrincipalId,
        event_kind: EventKind,
        subject: Option<EntityRef>,
        payload: &T,
        correlation_id: Option<String>,
        causation_id: Option<crate::core::EventId>,
    ) -> Result<ExperienceEvent, EngineError> {
        Ok(ExperienceEvent::new_with_id(
            event_id,
            actor_id,
            event_kind,
            subject,
            serde_json::to_value(payload)?,
            EventSource::new("engine", None),
            causation_id,
            correlation_id,
            Some(1.0),
        )?)
    }

    async fn save_trace(
        &self,
        original: &CognitiveTrace,
        outcome: &str,
        error_kind: Option<String>,
    ) -> Result<(), EngineError> {
        let mut trace = original.clone();
        trace.outcome = outcome.to_owned();
        trace.error_kind = error_kind;
        self.storage.record_cognitive_trace(&trace).await?;
        Ok(())
    }
}

fn ensure_operation_matches(
    intent: &crate::core::ActionIntent,
    input: &serde_json::Value,
) -> Result<(), EngineError> {
    if input.get("operation").and_then(serde_json::Value::as_str) != Some(intent.operation.as_str())
    {
        return Err(EngineError::InvalidOperation(
            "capability input operation does not match the authorized intent".to_owned(),
        ));
    }
    Ok(())
}

fn position_event_kind(state: &CurrentState, position: &Position) -> EventKind {
    if matches!(position.status, PositionStatus::Retracted) {
        EventKind::PositionRetracted
    } else if position.supersedes.is_some() {
        EventKind::PositionRevised
    } else if state.positions.contains_key(&position.id) {
        EventKind::PositionMaintained
    } else {
        EventKind::PositionEstablished
    }
}

fn conflict_event_kind(state: &CurrentState, conflict: &crate::core::Conflict) -> EventKind {
    if matches!(
        conflict.status,
        ConflictStatus::Resolved | ConflictStatus::AcceptedDisagreement
    ) {
        EventKind::ConflictResolved
    } else if state.conflicts.contains_key(&conflict.id) {
        EventKind::ConflictUpdated
    } else {
        EventKind::ConflictOpened
    }
}

fn trace_for_decision(
    original: &CognitiveTrace,
    decision: &crate::core::Decision,
) -> CognitiveTrace {
    let mut trace = original.clone();
    let mut references = trace.referenced_event_ids.clone();
    for event_id in &decision.evidence_refs {
        if !references.contains(event_id) {
            references.push(*event_id);
        }
    }
    if let Some(position) = &decision.position {
        for event_id in &position.evidence_refs {
            if !references.contains(event_id) {
                references.push(*event_id);
            }
        }
    }
    if let Some(position) = &decision.user_position {
        for event_id in &position.evidence_refs {
            if !references.contains(event_id) {
                references.push(*event_id);
            }
        }
    }
    if let Some(conflict) = &decision.conflict {
        for event_id in &conflict.evidence_refs {
            if !references.contains(event_id) {
                references.push(*event_id);
            }
        }
    }
    trace.referenced_event_ids = references;
    trace
}

fn evidence_event_ids(context: &crate::core::ThoughtContext) -> Vec<crate::core::EventId> {
    let mut ids = context.recent_event_ids.clone();
    for item in &context.recall.items {
        if !ids.contains(&item.source_event_id) {
            ids.push(item.source_event_id);
        }
    }
    ids
}

fn artifact_from_receipt(
    receipt: &Receipt,
    provenance_event_id: crate::core::EventId,
) -> Option<Artifact> {
    let object = receipt.output.as_object()?;
    let path = object.get("path")?.as_str()?.to_owned();
    let content_hash = object.get("content_hash")?.as_str()?.to_owned();
    let size = object
        .get("size")
        .and_then(serde_json::Value::as_u64)
        .or_else(|| {
            object
                .get("content")
                .and_then(serde_json::Value::as_str)
                .map(|content| content.len() as u64)
        })
        .unwrap_or(0);
    let media_type = object
        .get("media_type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("text/plain")
        .to_owned();
    Some(Artifact {
        id: ArtifactId::new(),
        path,
        content_hash,
        size,
        media_type,
        provenance_event_id,
        created_at: crate::core::model::now(),
    })
}
