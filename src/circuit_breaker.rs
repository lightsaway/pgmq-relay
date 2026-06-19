use crate::error::RelayError;
use backon::{ExponentialBuilder, Retryable};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tower::{service_fn, ServiceExt};
use tower_resilience_circuitbreaker::{
    CircuitBreakerError, CircuitBreakerHandle, CircuitBreakerLayer, CircuitState,
};
use tracing::{debug, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failed operations before opening the circuit.
    #[serde(default = "default_failure_threshold")]
    pub failure_threshold: u32,
    /// Duration to wait before permitting one recovery probe.
    #[serde(with = "humantime_serde", default = "default_recovery_timeout")]
    pub recovery_timeout: Duration,
    /// Initial delay for exponential retry backoff.
    #[serde(with = "humantime_serde", default = "default_initial_delay")]
    pub initial_delay: Duration,
    /// Maximum delay for exponential retry backoff.
    #[serde(with = "humantime_serde", default = "default_max_delay")]
    pub max_delay: Duration,
    /// Maximum number of attempts, including the initial attempt.
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// Exponential retry multiplier.
    #[serde(default = "default_multiplier")]
    pub multiplier: f64,
    /// Enables retry jitter when greater than zero.
    #[serde(default = "default_jitter")]
    pub jitter: f64,
}

fn default_failure_threshold() -> u32 {
    std::env::var("PGMQ_RELAY_CIRCUIT_BREAKER_FAILURE_THRESHOLD")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5)
}

fn default_recovery_timeout() -> Duration {
    std::env::var("PGMQ_RELAY_CIRCUIT_BREAKER_RECOVERY_TIMEOUT")
        .ok()
        .and_then(|v| humantime_serde::re::humantime::parse_duration(&v).ok())
        .unwrap_or_else(|| Duration::from_secs(30))
}

fn default_initial_delay() -> Duration {
    std::env::var("PGMQ_RELAY_CIRCUIT_BREAKER_INITIAL_DELAY")
        .ok()
        .and_then(|v| humantime_serde::re::humantime::parse_duration(&v).ok())
        .unwrap_or_else(|| Duration::from_millis(100))
}

fn default_max_delay() -> Duration {
    std::env::var("PGMQ_RELAY_CIRCUIT_BREAKER_MAX_DELAY")
        .ok()
        .and_then(|v| humantime_serde::re::humantime::parse_duration(&v).ok())
        .unwrap_or_else(|| Duration::from_secs(10))
}

fn default_max_retries() -> u32 {
    std::env::var("PGMQ_RELAY_CIRCUIT_BREAKER_MAX_RETRIES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5)
}

fn default_multiplier() -> f64 {
    std::env::var("PGMQ_RELAY_CIRCUIT_BREAKER_MULTIPLIER")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2.0)
}

fn default_jitter() -> f64 {
    std::env::var("PGMQ_RELAY_CIRCUIT_BREAKER_JITTER")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.1)
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: default_failure_threshold(),
            recovery_timeout: default_recovery_timeout(),
            initial_delay: default_initial_delay(),
            max_delay: default_max_delay(),
            max_retries: default_max_retries(),
            multiplier: default_multiplier(),
            jitter: default_jitter(),
        }
    }
}

/// Trait for preserving application-specific circuit breaker metrics.
pub trait CircuitBreakerMetrics: Send + Sync + 'static {
    fn record_success(&self, name: &str, duration: Duration, attempt: u32);
    fn record_failure(&self, name: &str, duration: Duration, attempt: u32, error_type: &str);
    fn record_rejection(&self, name: &str);
    fn record_state(&self, _name: &str, _state: f64) {}
}

pub const CB_STATE_CLOSED: f64 = 0.0;
pub const CB_STATE_OPEN: f64 = 1.0;
pub const CB_STATE_HALF_OPEN: f64 = 2.0;

/// Retry wrapper backed by BackON and Tower Resilience's circuit state machine.
pub struct GenericCircuitBreaker<M> {
    config: CircuitBreakerConfig,
    name: String,
    metrics: Arc<M>,
    layer: CircuitBreakerLayer,
    handle: CircuitBreakerHandle,
    recovery_probe_in_flight: Arc<AtomicBool>,
    recovery_ready_at: Arc<Mutex<Option<Instant>>>,
}

impl<M> GenericCircuitBreaker<M>
where
    M: CircuitBreakerMetrics,
{
    pub fn new(name: String, config: CircuitBreakerConfig, metrics: M) -> Self {
        let metrics = Arc::new(metrics);
        let transition_metrics = Arc::clone(&metrics);
        let transition_name = name.clone();
        let rejection_metrics = Arc::clone(&metrics);
        let rejection_name = name.clone();
        let recovery_ready_at = Arc::new(Mutex::new(None));
        let transition_recovery_ready_at = Arc::clone(&recovery_ready_at);
        let recovery_timeout = config.recovery_timeout;

        let (layer, handle) = CircuitBreakerLayer::builder()
            .name(name.clone())
            .consecutive_failures(config.failure_threshold.max(1) as usize)
            .wait_duration_in_open(config.recovery_timeout)
            .permitted_calls_in_half_open(1)
            .on_state_transition(move |_, to| {
                transition_metrics.record_state(&transition_name, circuit_state_metric_value(to));
                if let Ok(mut ready_at) = transition_recovery_ready_at.lock() {
                    *ready_at = match to {
                        CircuitState::Open => Some(Instant::now() + recovery_timeout),
                        CircuitState::Closed | CircuitState::HalfOpen => None,
                    };
                }
            })
            .on_call_rejected(move || {
                rejection_metrics.record_rejection(&rejection_name);
            })
            .build_with_handle();

        Self {
            config,
            name,
            metrics,
            layer,
            handle,
            recovery_probe_in_flight: Arc::new(AtomicBool::new(false)),
            recovery_ready_at,
        }
    }

    /// Execute an operation with bounded retries inside one circuit-breaker call.
    ///
    /// The breaker records only the final operation outcome. Individual retry attempts
    /// remain visible through the existing attempt metrics but cannot open the circuit
    /// by themselves.
    pub async fn execute<F, Fut, T>(&self, operation: F) -> Result<T, RelayError>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<T, RelayError>> + Send + 'static,
        T: Send + 'static,
    {
        let _probe_guard = match self.handle.state() {
            CircuitState::Closed => None,
            CircuitState::Open | CircuitState::HalfOpen => {
                if self
                    .recovery_probe_in_flight
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    self.metrics.record_rejection(&self.name);
                    return Err(RelayError::CircuitBreakerOpen(format!(
                        "Circuit breaker '{}' is open or already probing recovery",
                        self.name
                    )));
                }
                Some(RecoveryProbeGuard(Arc::clone(
                    &self.recovery_probe_in_flight,
                )))
            }
        };

        let attempts = Arc::new(AtomicU32::new(0));
        let attempt_counter = Arc::clone(&attempts);
        let metrics = Arc::clone(&self.metrics);
        let name = self.name.clone();

        let mut retry_builder = ExponentialBuilder::default()
            .with_min_delay(self.config.initial_delay)
            .with_max_delay(self.config.max_delay)
            .with_factor(self.config.multiplier as f32)
            .with_max_times(self.config.max_retries.saturating_sub(1) as usize)
            .with_total_delay(Some(
                self.config
                    .max_delay
                    .saturating_mul(self.config.max_retries.max(1)),
            ));
        if self.config.jitter > 0.0 {
            retry_builder = retry_builder.with_jitter();
        }

        let retry_operation = move || {
            let attempt = attempt_counter.fetch_add(1, Ordering::Relaxed) + 1;
            let metrics = Arc::clone(&metrics);
            let name = name.clone();
            let future = operation();
            async move {
                let started = Instant::now();
                let result = future.await;
                let duration = started.elapsed();
                match &result {
                    Ok(_) => metrics.record_success(&name, duration, attempt),
                    Err(error) => {
                        metrics.record_failure(&name, duration, attempt, &error.to_string())
                    }
                }
                result
            }
        };

        let breaker_name = self.name.clone();
        let notify_attempts = Arc::clone(&attempts);
        let retry_future = retry_operation
            .retry(retry_builder)
            .notify(move |error, delay| {
                let attempt = notify_attempts.load(Ordering::Relaxed);
                warn!(
                    circuit_breaker = %breaker_name,
                    attempt,
                    delay_ms = delay.as_millis(),
                    error = %error,
                    "Operation failed, retrying after delay"
                );
            });

        let service = service_fn(|future| future);
        match self.layer.layer_fn(service).oneshot(retry_future).await {
            Ok(value) => {
                let attempt = attempts.load(Ordering::Relaxed);
                if attempt > 1 {
                    debug!(
                        circuit_breaker = %self.name,
                        attempt,
                        "Operation succeeded after retries"
                    );
                }
                Ok(value)
            }
            Err(CircuitBreakerError::Inner(error)) => {
                warn!(
                    circuit_breaker = %self.name,
                    attempt = attempts.load(Ordering::Relaxed),
                    max_retries = self.config.max_retries,
                    error = %error,
                    "Operation failed after all retry attempts"
                );
                Err(error)
            }
            Err(CircuitBreakerError::OpenCircuit) => Err(RelayError::CircuitBreakerOpen(format!(
                "Circuit breaker '{}' is open",
                self.name
            ))),
        }
    }

    pub fn is_call_permitted(&self) -> bool {
        match self.handle.state() {
            CircuitState::Closed => true,
            CircuitState::Open => self
                .recovery_ready_at
                .lock()
                .ok()
                .and_then(|ready_at| *ready_at)
                .is_some_and(|ready_at| Instant::now() >= ready_at),
            CircuitState::HalfOpen => !self.recovery_probe_in_flight.load(Ordering::Acquire),
        }
    }
}

struct RecoveryProbeGuard(Arc<AtomicBool>);

impl Drop for RecoveryProbeGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

fn circuit_state_metric_value(state: CircuitState) -> f64 {
    match state {
        CircuitState::Closed => CB_STATE_CLOSED,
        CircuitState::Open => CB_STATE_OPEN,
        CircuitState::HalfOpen => CB_STATE_HALF_OPEN,
    }
}

pub type PgmqCircuitBreaker<M> = GenericCircuitBreaker<M>;

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::Notify;

    struct MockMetrics;

    impl CircuitBreakerMetrics for MockMetrics {
        fn record_success(&self, _name: &str, _duration: Duration, _attempt: u32) {}
        fn record_failure(
            &self,
            _name: &str,
            _duration: Duration,
            _attempt: u32,
            _error_type: &str,
        ) {
        }
        fn record_rejection(&self, _name: &str) {}
    }

    #[tokio::test]
    async fn succeeds_without_retry() {
        let breaker = PgmqCircuitBreaker::new(
            "test".to_string(),
            CircuitBreakerConfig::default(),
            MockMetrics,
        );

        assert_eq!(
            breaker
                .execute(|| async { Ok::<_, RelayError>(42) })
                .await
                .unwrap(),
            42
        );
    }

    #[tokio::test]
    async fn retries_before_reporting_success() {
        let config = CircuitBreakerConfig {
            failure_threshold: 5,
            max_retries: 3,
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(2),
            jitter: 0.0,
            ..Default::default()
        };
        let breaker = PgmqCircuitBreaker::new("test".to_string(), config, MockMetrics);
        let attempts = Arc::new(AtomicU32::new(0));
        let operation_attempts = Arc::clone(&attempts);

        let result = breaker
            .execute(move || {
                let attempts = Arc::clone(&operation_attempts);
                async move {
                    let attempt = attempts.fetch_add(1, Ordering::Relaxed) + 1;
                    if attempt < 3 {
                        Err(RelayError::PgmqOperation("retry".to_string()))
                    } else {
                        Ok(42)
                    }
                }
            })
            .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts.load(Ordering::Relaxed), 3);
        assert!(breaker.is_call_permitted());
    }

    #[tokio::test]
    async fn one_failed_operation_counts_once_for_breaker() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            max_retries: 3,
            initial_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(2),
            jitter: 0.0,
            recovery_timeout: Duration::from_secs(60),
            ..Default::default()
        };
        let breaker = PgmqCircuitBreaker::new("test".to_string(), config, MockMetrics);

        let fail =
            || async { Err::<(), RelayError>(RelayError::PgmqOperation("failure".to_string())) };
        assert!(breaker.execute(fail).await.is_err());
        assert!(breaker.is_call_permitted());
        assert!(breaker.execute(fail).await.is_err());
        assert!(!breaker.is_call_permitted());
    }

    #[tokio::test]
    async fn half_open_allows_only_one_probe() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            max_retries: 1,
            recovery_timeout: Duration::from_millis(10),
            ..Default::default()
        };
        let breaker = Arc::new(PgmqCircuitBreaker::new(
            "test".to_string(),
            config,
            MockMetrics,
        ));

        assert!(breaker
            .execute(|| async {
                Err::<(), RelayError>(RelayError::PgmqOperation("failure".to_string()))
            })
            .await
            .is_err());
        assert!(!breaker.is_call_permitted());
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(breaker.is_call_permitted());

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let probe_running = Arc::new(AtomicBool::new(false));
        let first = {
            let breaker = Arc::clone(&breaker);
            let task_entered = Arc::clone(&entered);
            let task_release = Arc::clone(&release);
            let task_probe_running = Arc::clone(&probe_running);
            tokio::spawn(async move {
                breaker
                    .execute(move || {
                        let entered = Arc::clone(&task_entered);
                        let release = Arc::clone(&task_release);
                        let probe_running = Arc::clone(&task_probe_running);
                        async move {
                            probe_running.store(true, Ordering::Relaxed);
                            entered.notify_one();
                            release.notified().await;
                            Ok::<_, RelayError>(())
                        }
                    })
                    .await
            })
        };

        entered.notified().await;
        assert!(probe_running.load(Ordering::Relaxed));
        let second = breaker.execute(|| async { Ok::<_, RelayError>(()) }).await;
        assert!(matches!(second, Err(RelayError::CircuitBreakerOpen(_))));

        release.notify_one();
        assert!(first.await.unwrap().is_ok());
        assert!(breaker.is_call_permitted());
    }

    #[tokio::test]
    async fn environment_variables_override_defaults() {
        std::env::set_var("PGMQ_RELAY_CIRCUIT_BREAKER_FAILURE_THRESHOLD", "10");
        std::env::set_var("PGMQ_RELAY_CIRCUIT_BREAKER_MAX_RETRIES", "7");

        let config = CircuitBreakerConfig::default();
        assert_eq!(config.failure_threshold, 10);
        assert_eq!(config.max_retries, 7);

        std::env::remove_var("PGMQ_RELAY_CIRCUIT_BREAKER_FAILURE_THRESHOLD");
        std::env::remove_var("PGMQ_RELAY_CIRCUIT_BREAKER_MAX_RETRIES");
    }
}
