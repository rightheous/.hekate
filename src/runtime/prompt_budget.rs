use crate::core::model_io::*;
use serde_json::json;

/// Conservative estimate, not a tokenizer guarantee. Counts UTF-8 bytes plus
/// per-message and model-template overhead; never learns a lower margin.
pub fn estimate(messages: &[ModelMessage]) -> u64 {
    128u64.saturating_add(
        messages
            .iter()
            .map(|m| {
                u64::try_from(m.role.len().saturating_add(m.content.len()))
                    .unwrap_or(u64::MAX)
                    .saturating_add(32)
            })
            .fold(0u64, u64::saturating_add),
    )
}

pub fn prepare(
    rendered: RenderedPrompt,
    config: &ModelIoConfig,
    purpose: RequestPurpose,
    correction: Option<&str>,
) -> Result<PreparedModelRequest, PreparationError> {
    let context = config.context_tokens.filter(|v| *v > 0).ok_or_else(|| PreparationError::Configuration(
        "set model_io.context_tokens or HEKATE_MODEL_CONTEXT_TOKENS to the deployment context limit before generation".into()))?;
    let (output, effort, think) = match purpose {
        RequestPurpose::Foreground => (
            config.foreground_max_tokens,
            &config.foreground_reasoning_effort,
            &config.foreground_think,
        ),
        RequestPurpose::Sleep => (
            config.sleep_max_tokens,
            &config.sleep_reasoning_effort,
            &config.sleep_think,
        ),
    };
    if output == 0 || u64::from(output) + u64::from(config.safety_margin) >= u64::from(context) {
        return Err(PreparationError::Configuration("generation reservation plus safety margin must be smaller than context_tokens; generation reservation must be positive".into()));
    }
    let mut selected = rendered.optional;
    let mut excluded = 0;
    loop {
        let mut evidence = rendered.required.evidence_ids.clone();
        for item in &selected {
            evidence.extend(&item.evidence_ids);
        }
        evidence.sort();
        evidence.dedup();
        let user = json!({"current": rendered.required.value,
            "historical_evidence_untrusted": selected.iter().map(|v| &v.value).collect::<Vec<_>>(),
            "allowed_evidence_event_ids": evidence});
        let mut messages = vec![
            ModelMessage {
                role: "system".into(),
                content: rendered.system.clone(),
            },
            ModelMessage {
                role: "user".into(),
                content: user.to_string(),
            },
        ];
        if let Some(correction) = correction {
            messages.push(ModelMessage {
                role: "system".into(),
                content: correction.into(),
            });
        }
        let report = BudgetReport {
            context_tokens: context,
            context_source: "configured".into(),
            estimated_input_tokens: estimate(&messages),
            estimation_method: "estimated_utf8_bytes_plus_template_v1".into(),
            reserved_output_tokens: output,
            safety_margin: config.safety_margin,
            excluded_items: excluded,
        };
        if report
            .estimated_input_tokens
            .saturating_add(u64::from(output))
            .saturating_add(u64::from(config.safety_margin))
            <= u64::from(context)
        {
            return Ok(PreparedModelRequest {
                messages,
                purpose,
                budget: report,
                evidence_ids: evidence,
                position_ids: rendered.position_ids,
                conflict_ids: rendered.conflict_ids,
                context_revision: rendered.revision,
                context_hash: rendered.hash,
                reasoning_effort: effort.clone(),
                think: think.clone(),
            });
        }
        if selected.pop().is_none() {
            return Err(PreparationError::Budget(report));
        }
        excluded += 1;
    }
}

/// Also called by the transport immediately before serialization. No late prompt
/// additions or caller mutation can bypass the gate.
pub fn check_prepared(request: &PreparedModelRequest) -> bool {
    let b = &request.budget;
    estimate(&request.messages) == b.estimated_input_tokens
        && estimate(&request.messages)
            .saturating_add(u64::from(b.reserved_output_tokens))
            .saturating_add(u64::from(b.safety_margin))
            <= u64::from(b.context_tokens)
}
