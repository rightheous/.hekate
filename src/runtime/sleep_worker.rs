use std::time::Duration;

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::config::Config;
use crate::runtime::engine::{Engine, EngineError};
use crate::runtime::sleep::{SleepOnceResult, SleepOnceStatus, SleepRuntimeError};

#[derive(Clone, Debug)]
pub struct SleepWorkerConfig {
    pub idle_interval: Duration,
    pub completed_interval: Duration,
    pub deferred_interval: Duration,
    pub initial_error_backoff: Duration,
    pub max_error_backoff: Duration,
}

impl Default for SleepWorkerConfig {
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

impl SleepWorkerConfig {
    pub fn from_config(config: &Config) -> Result<Self, SleepWorkerError> {
        Self::new(
            Duration::from_secs(config.sleep_worker_idle_seconds),
            Duration::from_secs(config.sleep_worker_completed_seconds),
            Duration::from_secs(config.sleep_worker_deferred_seconds),
            Duration::from_secs(config.sleep_worker_initial_backoff_seconds),
            Duration::from_secs(config.sleep_worker_max_backoff_seconds),
        )
    }

    pub fn new(
        idle_interval: Duration,
        completed_interval: Duration,
        deferred_interval: Duration,
        initial_error_backoff: Duration,
        max_error_backoff: Duration,
    ) -> Result<Self, SleepWorkerError> {
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

    fn validate(&self) -> Result<(), SleepWorkerError> {
        if self.idle_interval.is_zero()
            || self.completed_interval.is_zero()
            || self.deferred_interval.is_zero()
            || self.initial_error_backoff.is_zero()
            || self.max_error_backoff.is_zero()
        {
            return Err(SleepWorkerError::InvalidConfig(
                "sleep worker intervals and backoff must be greater than zero".to_owned(),
            ));
        }
        if self.max_error_backoff < self.initial_error_backoff {
            return Err(SleepWorkerError::InvalidConfig(
                "sleep worker max error backoff must be at least the initial backoff".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum SleepWorkerError {
    #[error("invalid sleep worker config: {0}")]
    InvalidConfig(String),
    #[error("failed to install shutdown listener: {0}")]
    Signal(#[source] std::io::Error),
}

#[async_trait]
pub trait SleepService: Send + Sync {
    async fn foreground_active(&self) -> Result<bool, EngineError>;
    async fn sleep_once(&self) -> Result<SleepOnceResult, EngineError>;
}

#[async_trait]
impl SleepService for Engine {
    async fn foreground_active(&self) -> Result<bool, EngineError> {
        Engine::foreground_active(self).await
    }

    async fn sleep_once(&self) -> Result<SleepOnceResult, EngineError> {
        Engine::sleep_once(self).await
    }
}

pub struct SleepWorker<'a> {
    service: &'a dyn SleepService,
    config: SleepWorkerConfig,
}

impl<'a> SleepWorker<'a> {
    pub fn new(
        service: &'a dyn SleepService,
        config: SleepWorkerConfig,
    ) -> Result<Self, SleepWorkerError> {
        config.validate()?;
        Ok(Self { service, config })
    }

    pub async fn run(&self) -> Result<(), SleepWorkerError> {
        let (mut shutdown, listener) = install_shutdown_listener()?;
        let result = self.run_until_shutdown(&mut shutdown).await;
        listener.abort();
        result
    }

    async fn run_until_shutdown(
        &self,
        shutdown: &mut watch::Receiver<Option<&'static str>>,
    ) -> Result<(), SleepWorkerError> {
        tracing::info!("sleep worker started");
        let mut policy = DelayPolicy::default();

        loop {
            if let Some(reason) = shutdown_reason(shutdown) {
                tracing::info!(signal = reason, "sleep worker shutdown requested");
                break;
            }

            let outcome = self.run_cycle().await;
            let delay = policy.next_delay(&outcome, &self.config);
            log_outcome(&outcome, delay);
            if wait_for_shutdown(shutdown, delay).await {
                let reason = shutdown_reason(shutdown).unwrap_or("listener_closed");
                tracing::info!(signal = reason, "sleep worker shutdown requested");
                break;
            }
        }

        tracing::info!("sleep worker stopped");
        Ok(())
    }

    async fn run_cycle(&self) -> CycleOutcome {
        match self.service.foreground_active().await {
            Ok(true) => CycleOutcome::Status(SleepOnceStatus::Deferred, None, 0),
            Ok(false) => match self.service.sleep_once().await {
                Ok(result) => CycleOutcome::Status(
                    result.status,
                    result.error_kind,
                    result.created_candidates,
                ),
                Err(error) => CycleOutcome::Error(failure_kind(&error)),
            },
            Err(error) => CycleOutcome::Error(failure_kind(&error)),
        }
    }
}

#[derive(Clone, Debug)]
enum CycleOutcome {
    Status(SleepOnceStatus, Option<String>, u32),
    Error(&'static str),
}

#[derive(Default)]
struct DelayPolicy {
    consecutive_failures: u32,
}

impl DelayPolicy {
    fn next_delay(&mut self, outcome: &CycleOutcome, config: &SleepWorkerConfig) -> Duration {
        match outcome {
            CycleOutcome::Error(_) | CycleOutcome::Status(SleepOnceStatus::Failed, _, _) => {
                let exponent = self.consecutive_failures;
                self.consecutive_failures = self.consecutive_failures.saturating_add(1);
                bounded_backoff(
                    config.initial_error_backoff,
                    config.max_error_backoff,
                    exponent,
                )
            }
            CycleOutcome::Status(SleepOnceStatus::Idle, _, _) => {
                self.consecutive_failures = 0;
                config.idle_interval
            }
            CycleOutcome::Status(SleepOnceStatus::Completed, _, _)
            | CycleOutcome::Status(SleepOnceStatus::RunningResumed, _, _) => {
                self.consecutive_failures = 0;
                config.completed_interval
            }
            CycleOutcome::Status(SleepOnceStatus::Deferred, _, _)
            | CycleOutcome::Status(SleepOnceStatus::Interrupted, _, _) => {
                self.consecutive_failures = 0;
                config.deferred_interval
            }
        }
    }
}

fn bounded_backoff(initial: Duration, maximum: Duration, exponent: u32) -> Duration {
    let mut delay = initial;
    let mut remaining = exponent;
    while remaining > 0 && delay < maximum {
        delay = delay.checked_add(delay).unwrap_or(maximum).min(maximum);
        remaining -= 1;
    }
    delay
}

fn log_outcome(outcome: &CycleOutcome, next_delay: Duration) {
    match outcome {
        CycleOutcome::Status(SleepOnceStatus::Completed, error_kind, candidate_count)
        | CycleOutcome::Status(SleepOnceStatus::RunningResumed, error_kind, candidate_count) => {
            tracing::info!(
                next_delay_seconds = next_delay.as_secs(),
                candidate_count,
                error_kind = error_kind.as_deref().unwrap_or("none"),
                "sleep completed"
            );
        }
        CycleOutcome::Status(SleepOnceStatus::Deferred, _, _) => {
            tracing::info!(
                next_delay_seconds = next_delay.as_secs(),
                "sleep worker deferred due to foreground"
            );
        }
        CycleOutcome::Status(SleepOnceStatus::Interrupted, _, _) => {
            tracing::info!(
                next_delay_seconds = next_delay.as_secs(),
                "sleep worker deferred after interruption"
            );
        }
        CycleOutcome::Status(SleepOnceStatus::Idle, _, _) => {
            tracing::debug!(
                next_delay_seconds = next_delay.as_secs(),
                "sleep worker idle"
            );
        }
        CycleOutcome::Status(SleepOnceStatus::Failed, error_kind, _) => {
            tracing::warn!(
                failure_kind = error_kind.as_deref().unwrap_or("sleep_run_failed"),
                next_backoff_seconds = next_delay.as_secs(),
                "sleep worker failure"
            );
        }
        CycleOutcome::Error(error_kind) => {
            tracing::warn!(
                failure_kind = *error_kind,
                next_backoff_seconds = next_delay.as_secs(),
                "sleep worker failure"
            );
        }
    }
}

fn failure_kind(error: &EngineError) -> &'static str {
    match error {
        EngineError::Storage(error) => match error {
            crate::ports::StorageError::Backend(_) => "storage_backend",
            crate::ports::StorageError::Integrity { .. } => "storage_integrity",
            crate::ports::StorageError::RevisionConflict { .. } => "storage_revision_conflict",
            crate::ports::StorageError::StaleContext { .. } => "storage_stale_context",
            crate::ports::StorageError::InvalidState(_) => "storage_invalid_state",
        },
        EngineError::Projection(_) => "projection_error",
        EngineError::Cognitive(error) => match error {
            crate::ports::CognitiveError::Configuration { .. } => "model_configuration",
            crate::ports::CognitiveError::Timeout { .. } => "model_timeout",
            crate::ports::CognitiveError::Provider { .. } => "model_provider",
            crate::ports::CognitiveError::Malformed { .. } => "model_malformed",
        },
        EngineError::Event(_) => "event_error",
        EngineError::Transition(_) => "transition_error",
        EngineError::Serialization(_) => "serialization_error",
        EngineError::Policy(_) => "policy_error",
        EngineError::Capability(_) => "capability_error",
        EngineError::PolicyDenied(_) => "policy_denied",
        EngineError::InvalidObservation(_) => "invalid_observation",
        EngineError::InvalidCognitiveTrace(_) => "invalid_cognitive_trace",
        EngineError::Judgment(_) => "judgment_error",
        EngineError::StaleContext { .. } => "stale_context",
        EngineError::NotFound(_) => "not_found",
        EngineError::InvalidOperation(_) => "invalid_operation",
        EngineError::ForegroundActivityBusy | EngineError::ForegroundActivityLeaseLost => {
            "foreground_activity"
        }
        EngineError::Completion(_) => "completion_error",
        EngineError::Recovery(_) => "recovery_error",
        EngineError::Integration(_) => "integration_error",
        EngineError::ContextBuilder(_) => "context_builder_error",
        EngineError::Sleep(error) => match error {
            SleepRuntimeError::Storage(_) => "storage_error",
            SleepRuntimeError::Projection(_) => "projection_error",
            SleepRuntimeError::Event(_) => "event_error",
            SleepRuntimeError::Serialization(_) => "serialization_error",
            SleepRuntimeError::Invalid(_) => "sleep_invalid_state",
        },
    }
}

async fn wait_for_shutdown(
    shutdown: &mut watch::Receiver<Option<&'static str>>,
    delay: Duration,
) -> bool {
    if shutdown_reason(shutdown).is_some() {
        return true;
    }
    tokio::select! {
        _ = tokio::time::sleep(delay) => false,
        changed = shutdown.changed() => changed.is_err() || shutdown_reason(shutdown).is_some(),
    }
}

fn shutdown_reason(shutdown: &watch::Receiver<Option<&'static str>>) -> Option<&'static str> {
    *shutdown.borrow()
}

fn install_shutdown_listener(
) -> Result<(watch::Receiver<Option<&'static str>>, JoinHandle<()>), SleepWorkerError> {
    let (sender, receiver) = watch::channel(None);
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(SleepWorkerError::Signal)?;

    let listener = tokio::spawn(async move {
        #[cfg(unix)]
        {
            tokio::select! {
                result = tokio::signal::ctrl_c() => notify_signal(&sender, result, "Ctrl-C"),
                result = terminate.recv() => {
                    if result.is_some() {
                        let _ = sender.send(Some("SIGTERM"));
                    } else {
                        let _ = sender.send(Some("signal_listener_error"));
                    }
                }
            }
        }
        #[cfg(not(unix))]
        {
            notify_signal(&sender, tokio::signal::ctrl_c().await, "Ctrl-C");
        }
    });
    Ok((receiver, listener))
}

fn notify_signal(
    sender: &watch::Sender<Option<&'static str>>,
    result: std::io::Result<()>,
    reason: &'static str,
) {
    if result.is_ok() {
        let _ = sender.send(Some(reason));
    } else {
        let _ = sender.send(Some("signal_listener_error"));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use super::*;

    struct FakeService {
        foreground: AtomicBool,
        calls: AtomicUsize,
        active: AtomicUsize,
        max_active: AtomicUsize,
        started: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    }

    impl Default for FakeService {
        fn default() -> Self {
            Self {
                foreground: AtomicBool::new(false),
                calls: AtomicUsize::new(0),
                active: AtomicUsize::new(0),
                max_active: AtomicUsize::new(0),
                started: Mutex::new(None),
            }
        }
    }

    #[async_trait]
    impl SleepService for FakeService {
        async fn foreground_active(&self) -> Result<bool, EngineError> {
            Ok(self.foreground.load(Ordering::SeqCst))
        }

        async fn sleep_once(&self) -> Result<SleepOnceResult, EngineError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            if let Some(sender) = self.started.lock().expect("started lock").take() {
                let _ = sender.send(());
            }
            tokio::task::yield_now().await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(SleepOnceResult {
                status: SleepOnceStatus::Completed,
                run_id: None,
                high_water_revision: None,
                cursor_before: None,
                cursor_after: None,
                processed_observations: 0,
                created_candidates: 1,
                resumed: false,
                error_kind: None,
            })
        }
    }

    fn config() -> SleepWorkerConfig {
        SleepWorkerConfig::new(
            Duration::from_secs(5),
            Duration::from_secs(3),
            Duration::from_secs(4),
            Duration::from_secs(2),
            Duration::from_secs(5),
        )
        .expect("worker config")
    }

    #[tokio::test]
    async fn cycle_policy_and_backoff_are_bounded() {
        let service = FakeService::default();
        let worker = SleepWorker::new(&service, config()).expect("worker");
        let mut policy = DelayPolicy::default();

        service.foreground.store(true, Ordering::SeqCst);
        let outcome = worker.run_cycle().await;
        assert_eq!(service.calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            policy.next_delay(&outcome, &worker.config),
            Duration::from_secs(4)
        );

        service.foreground.store(false, Ordering::SeqCst);
        let outcome = worker.run_cycle().await;
        assert_eq!(service.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            policy.next_delay(&outcome, &worker.config),
            Duration::from_secs(3)
        );

        assert_eq!(
            policy.next_delay(&CycleOutcome::Error("timeout"), &worker.config),
            Duration::from_secs(2)
        );
        assert_eq!(
            policy.next_delay(&CycleOutcome::Error("timeout"), &worker.config),
            Duration::from_secs(4)
        );
        assert_eq!(
            policy.next_delay(&CycleOutcome::Error("timeout"), &worker.config),
            Duration::from_secs(5)
        );
        assert_eq!(
            policy.next_delay(
                &CycleOutcome::Status(SleepOnceStatus::Idle, None, 0),
                &worker.config,
            ),
            Duration::from_secs(5)
        );
        assert_eq!(
            policy.next_delay(&CycleOutcome::Error("timeout"), &worker.config),
            Duration::from_secs(2)
        );
    }

    #[tokio::test]
    async fn shutdown_finishes_cycle_without_overlap() {
        let service = Arc::new(FakeService::default());
        let (started_sender, started_receiver) = tokio::sync::oneshot::channel();
        *service.started.lock().expect("started lock") = Some(started_sender);
        let worker = SleepWorker::new(service.as_ref(), config()).expect("worker");
        let (shutdown_sender, mut shutdown) = watch::channel(None);
        let run = worker.run_until_shutdown(&mut shutdown);
        tokio::pin!(run);
        tokio::select! {
            result = &mut run => panic!("worker stopped before its first cycle: {result:?}"),
            _ = started_receiver => {}
        }
        shutdown_sender.send(Some("test")).expect("shutdown");
        run.await.expect("worker stopped");
        assert_eq!(service.calls.load(Ordering::SeqCst), 1);
        assert_eq!(service.active.load(Ordering::SeqCst), 0);
        assert_eq!(service.max_active.load(Ordering::SeqCst), 1);
    }
}
