use crate::core::model_io::{BudgetReport, PreparationError};
use crate::core::{ContextBudgetReport, SleepContext};

#[derive(Clone, Debug)]
pub(crate) struct SleepBatchPlan {
    pub context: SleepContext,
    pub context_budget: ContextBudgetReport,
}

#[derive(Debug)]
pub(crate) enum SleepBatchError {
    ContextBuild(String),
    ModelConfiguration {
        message: String,
        attempt: SleepBatchPlan,
    },
    AnchorsExceed(SleepBatchPlan),
    ObservationExceeds(SleepBatchPlan),
    NoCandidates,
}

/// Uses already gathered recall data while re-rendering and checking each prefix.
pub(crate) fn plan_new(
    candidate_count: usize,
    deferred_seed_count: usize,
    mut build: impl FnMut(usize, usize) -> Result<SleepBatchPlan, String>,
    mut check: impl FnMut(&SleepContext) -> Result<BudgetReport, PreparationError>,
) -> Result<SleepBatchPlan, SleepBatchError> {
    if candidate_count == 0 {
        return Err(SleepBatchError::NoCandidates);
    }
    let mut anchors =
        build(0, candidate_count + deferred_seed_count).map_err(SleepBatchError::ContextBuild)?;
    match check(&anchors.context) {
        Ok(report) => anchors.context_budget.model_budget = Some(report),
        Err(PreparationError::Budget(report)) => {
            anchors.context_budget.model_budget = Some(report);
            return Err(SleepBatchError::AnchorsExceed(anchors));
        }
        Err(PreparationError::Configuration(message)) => {
            return Err(SleepBatchError::ModelConfiguration {
                message,
                attempt: anchors,
            });
        }
    }

    let mut first_seed_failure = None;
    for prefix_len in (1..=candidate_count).rev() {
        let mut plan = build(
            prefix_len,
            candidate_count + deferred_seed_count - prefix_len,
        )
        .map_err(SleepBatchError::ContextBuild)?;
        match check(&plan.context) {
            Ok(report) => {
                plan.context_budget.model_budget = Some(report);
                return Ok(plan);
            }
            Err(PreparationError::Budget(report)) => {
                plan.context_budget.model_budget = Some(report);
                if prefix_len == 1 {
                    first_seed_failure = Some(plan);
                }
            }
            Err(PreparationError::Configuration(message)) => {
                return Err(SleepBatchError::ModelConfiguration {
                    message,
                    attempt: plan,
                });
            }
        }
    }

    Err(first_seed_failure
        .map(SleepBatchError::ObservationExceeds)
        .unwrap_or(SleepBatchError::NoCandidates))
}
