use std::time::Duration;

use serde::Serialize;
use thiserror::Error;
use tokio::sync::watch;

use super::service::{InitiativeRunResult, InitiativeService};

#[derive(Clone, Debug)]
pub struct InitiativeWorkerConfig {
    pub idle_interval: Duration,
    pub completed_interval: Duration,
    pub deferred_interval: Duration,
    pub initial_error_backoff: Duration,
    pub max_error_backoff: Duration,
}

impl Default for InitiativeWorkerConfig {
    fn default() -> Self {
        Self {
            idle_interval: Duration::from_secs(300),
            completed_interval: Duration::from_secs(30),
            deferred_interval: Duration::from_secs(60),
            initial_error_backoff: Duration::from_secs(30),
            max_error_backoff: Duration::from_secs(900),
        }
    }
}

impl InitiativeWorkerConfig {
    pub fn new(
        idle_interval: Duration,
        completed_interval: Duration,
        deferred_interval: Duration,
        initial_error_backoff: Duration,
        max_error_backoff: Duration,
    ) -> Result<Self, InitiativeWorkerError> {
        let config = Self {
            idle_interval,
            completed_interval,
            deferred_interval,
            initial_error_backoff,
            max_error_backoff,
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), InitiativeWorkerError> {
        if [
            self.idle_interval,
            self.completed_interval,
            self.deferred_interval,
            self.initial_error_backoff,
            self.max_error_backoff,
        ]
        .into_iter()
        .any(|duration| duration.is_zero())
            || self.max_error_backoff < self.initial_error_backoff
        {
            return Err(InitiativeWorkerError::InvalidConfig);
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum InitiativeWorkerError {
    #[error(
        "initiative worker intervals must be positive and max backoff must cover initial backoff"
    )]
    InvalidConfig,
}

pub struct InitiativeWorker {
    service: InitiativeService,
    config: InitiativeWorkerConfig,
}

impl InitiativeWorker {
    pub fn new(
        service: InitiativeService,
        config: InitiativeWorkerConfig,
    ) -> Result<Self, InitiativeWorkerError> {
        config.validate()?;
        Ok(Self { service, config })
    }

    pub async fn run_cycle(&self) -> InitiativeWorkerOutcome {
        match self.service.foreground_active().await {
            Ok(true) => InitiativeWorkerOutcome::DeferredForeground,
            Ok(false) => match self.service.run_once().await {
                Ok(result) => InitiativeWorkerOutcome::Ran(result),
                Err(error) => InitiativeWorkerOutcome::Failed(error.to_string()),
            },
            Err(error) => InitiativeWorkerOutcome::Failed(error.to_string()),
        }
    }

    pub async fn run_until_shutdown(&self, mut shutdown: watch::Receiver<bool>) {
        let mut policy = DelayPolicy::default();
        while !*shutdown.borrow() {
            let outcome = self.run_cycle().await;
            let delay = policy.next_delay(&outcome, &self.config);
            log_outcome(&outcome, delay);
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum InitiativeWorkerOutcome {
    DeferredForeground,
    Ran(InitiativeRunResult),
    Failed(String),
}

#[derive(Default)]
struct DelayPolicy {
    consecutive_failures: u32,
}

impl DelayPolicy {
    fn next_delay(
        &mut self,
        outcome: &InitiativeWorkerOutcome,
        config: &InitiativeWorkerConfig,
    ) -> Duration {
        match outcome {
            InitiativeWorkerOutcome::Failed(_) => {
                let exponent = self.consecutive_failures;
                self.consecutive_failures = self.consecutive_failures.saturating_add(1);
                bounded_backoff(
                    config.initial_error_backoff,
                    config.max_error_backoff,
                    exponent,
                )
            }
            InitiativeWorkerOutcome::DeferredForeground
            | InitiativeWorkerOutcome::Ran(InitiativeRunResult::Deferred) => {
                self.consecutive_failures = 0;
                config.deferred_interval
            }
            InitiativeWorkerOutcome::Ran(InitiativeRunResult::Proposed { .. })
            | InitiativeWorkerOutcome::Ran(InitiativeRunResult::Dismissed { .. }) => {
                self.consecutive_failures = 0;
                config.completed_interval
            }
            InitiativeWorkerOutcome::Ran(
                InitiativeRunResult::NoCandidate | InitiativeRunResult::Duplicate { .. },
            ) => {
                self.consecutive_failures = 0;
                config.idle_interval
            }
        }
    }
}

fn bounded_backoff(initial: Duration, maximum: Duration, exponent: u32) -> Duration {
    let mut delay = initial;
    for _ in 0..exponent {
        delay = delay.checked_add(delay).unwrap_or(maximum).min(maximum);
        if delay == maximum {
            break;
        }
    }
    delay
}

fn log_outcome(outcome: &InitiativeWorkerOutcome, delay: Duration) {
    match outcome {
        InitiativeWorkerOutcome::DeferredForeground => tracing::debug!(
            next_delay_seconds = delay.as_secs(),
            "initiative worker yielded to foreground activity"
        ),
        InitiativeWorkerOutcome::Ran(InitiativeRunResult::Proposed { .. }) => tracing::info!(
            next_delay_seconds = delay.as_secs(),
            "initiative proposal prepared"
        ),
        InitiativeWorkerOutcome::Ran(_) => tracing::debug!(
            next_delay_seconds = delay.as_secs(),
            "initiative worker cycle completed"
        ),
        InitiativeWorkerOutcome::Failed(error) => tracing::warn!(
            %error,
            next_backoff_seconds = delay.as_secs(),
            "initiative worker cycle failed"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_backoff_stops_at_configured_ceiling() {
        assert_eq!(
            bounded_backoff(Duration::from_secs(2), Duration::from_secs(10), 0),
            Duration::from_secs(2)
        );
        assert_eq!(
            bounded_backoff(Duration::from_secs(2), Duration::from_secs(10), 2),
            Duration::from_secs(8)
        );
        assert_eq!(
            bounded_backoff(Duration::from_secs(2), Duration::from_secs(10), 8),
            Duration::from_secs(10)
        );
    }
}
