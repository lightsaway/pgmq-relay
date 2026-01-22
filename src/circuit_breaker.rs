use crate::error::RelayError;
use backoff::{backoff::Backoff, ExponentialBackoff};
use serde::{Deserialize, Serialize};

use std::time::Duration;
use tracing::{debug, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures before opening the circuit
    #[serde(default = "default_failure_threshold")]
    pub failure_threshold: u32,
    /// Duration to wait before attempting to close the circuit
    #[serde(with = "humantime_serde", default = "default_recovery_timeout")]
    pub recovery_timeout: Duration,
    /// Initial delay for exponential backoff
    #[serde(with = "humantime_serde", default = "default_initial_delay")]
    pub initial_delay: Duration,
    /// Maximum delay for exponential backoff
    #[serde(with = "humantime_serde", default = "default_max_delay")]
    pub max_delay: Duration,
    /// Maximum number of retry attempts
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// Multiplier for exponential backoff
    #[serde(default = "default_multiplier")]
    pub multiplier: f64,
    /// Random jitter to add to delays (0.0 to 1.0)
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

/// Generic circuit breaker with parameterized metrics
pub struct GenericCircuitBreaker<M> {
    config: CircuitBreakerConfig,
    name: String,
    metrics: M,
    failure_count: std::sync::atomic::AtomicU32,
    last_failure_time: std::sync::Mutex<Option<std::time::Instant>>,
}

/// Trait for circuit breaker metrics recording
pub trait CircuitBreakerMetrics {
    fn record_success(&self, name: &str, duration: Duration, attempt: u32);
    fn record_failure(&self, name: &str, duration: Duration, attempt: u32, error_type: &str);
    fn record_rejection(&self, name: &str);
}

impl<M> GenericCircuitBreaker<M>
where
    M: CircuitBreakerMetrics,
{
    pub fn new(name: String, config: CircuitBreakerConfig, metrics: M) -> Self {
        Self {
            config,
            name,
            metrics,
            failure_count: std::sync::atomic::AtomicU32::new(0),
            last_failure_time: std::sync::Mutex::new(None),
        }
    }

    /// Execute an operation with circuit breaker protection and retry logic
    pub async fn execute<F, Fut, T>(&self, operation: F) -> Result<T, RelayError>
    where
        F: Fn() -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<T, RelayError>> + Send,
        T: Send,
    {
        if !self.is_call_permitted() {
            self.metrics.record_rejection(&self.name);
            return Err(RelayError::CircuitBreakerOpen(format!(
                "Circuit breaker '{}' is open - too many recent failures",
                self.name
            )));
        }

        let mut backoff = self.create_backoff();
        let mut attempt = 1;

        loop {
            let start_time = std::time::Instant::now();

            match operation().await {
                Ok(result) => {
                    let duration = start_time.elapsed();

                    // Record success and reset failure count
                    self.on_success();
                    self.metrics.record_success(&self.name, duration, attempt);

                    if attempt > 1 {
                        debug!(
                            circuit_breaker = %self.name,
                            attempt = attempt,
                            duration_ms = duration.as_millis(),
                            "Operation succeeded after retries"
                        );
                    }

                    return Ok(result);
                }
                Err(e) => {
                    let duration = start_time.elapsed();

                    // Record failure
                    self.on_error();
                    self.metrics
                        .record_failure(&self.name, duration, attempt, &format!("{}", e));

                    // Check if we should retry
                    if attempt >= self.config.max_retries {
                        warn!(
                            circuit_breaker = %self.name,
                            attempt = attempt,
                            max_retries = self.config.max_retries,
                            error = %e,
                            "Operation failed after all retry attempts"
                        );
                        return Err(e);
                    }

                    // Check if the circuit breaker has opened
                    if !self.is_call_permitted() {
                        warn!(
                            circuit_breaker = %self.name,
                            attempt = attempt,
                            error = %e,
                            "Circuit breaker opened during retry sequence"
                        );
                        return Err(RelayError::CircuitBreakerOpen(format!(
                            "Circuit breaker '{}' opened after failure: {}",
                            self.name, e
                        )));
                    }

                    // Calculate delay with exponential backoff
                    if let Some(delay) = backoff.next_backoff() {
                        warn!(
                            circuit_breaker = %self.name,
                            attempt = attempt,
                            delay_ms = delay.as_millis(),
                            error = %e,
                            "Operation failed, retrying after delay"
                        );

                        tokio::time::sleep(delay).await;
                        attempt += 1;
                    } else {
                        // Backoff exhausted
                        return Err(e);
                    }
                }
            }
        }
    }

    /// Check if the circuit breaker allows calls
    pub fn is_call_permitted(&self) -> bool {
        let failure_count = self
            .failure_count
            .load(std::sync::atomic::Ordering::Relaxed);

        if failure_count >= self.config.failure_threshold {
            // Check if enough time has passed for recovery
            if let Ok(last_failure) = self.last_failure_time.lock() {
                if let Some(last_failure_instant) = *last_failure {
                    let elapsed = last_failure_instant.elapsed();
                    if elapsed >= self.config.recovery_timeout {
                        // Recovery timeout has passed, allow a test call
                        return true;
                    }
                }
            }
            false
        } else {
            true
        }
    }

    /// Record a successful operation
    fn on_success(&self) {
        // Reset failure count on success
        self.failure_count
            .store(0, std::sync::atomic::Ordering::Relaxed);
        if let Ok(mut last_failure) = self.last_failure_time.lock() {
            *last_failure = None;
        }
    }

    /// Record a failed operation
    fn on_error(&self) {
        let new_count = self
            .failure_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        if new_count >= self.config.failure_threshold {
            if let Ok(mut last_failure) = self.last_failure_time.lock() {
                *last_failure = Some(std::time::Instant::now());
            }
        }
    }

    /// Get current circuit breaker state for monitoring
    fn create_backoff(&self) -> ExponentialBackoff {
        ExponentialBackoff::default()
    }
}

/// Type alias for PGMQ circuit breaker with metrics
pub type PgmqCircuitBreaker<M> = GenericCircuitBreaker<M>;

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::time::Duration;

    // Mock metrics for testing
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
    async fn test_circuit_breaker_success() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            max_retries: 2,
            ..Default::default()
        };

        let cb = PgmqCircuitBreaker::new("test".to_string(), config, MockMetrics);

        let result = cb.execute(|| async { Ok::<_, RelayError>(42) }).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_circuit_breaker_retry_success() {
        let config = CircuitBreakerConfig {
            failure_threshold: 5,
            max_retries: 3,
            initial_delay: Duration::from_millis(10),
            ..Default::default()
        };

        let cb = PgmqCircuitBreaker::new("test".to_string(), config, MockMetrics);
        let attempt_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));

        let result = cb
            .execute(|| {
                let count = attempt_count.clone();
                async move {
                    let attempt = count.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                    if attempt < 3 {
                        Err(RelayError::PgmqOperation("test_failure".to_string()))
                    } else {
                        Ok(42)
                    }
                }
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempt_count.load(std::sync::atomic::Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn test_circuit_breaker_max_retries() {
        let config = CircuitBreakerConfig {
            failure_threshold: 5,
            max_retries: 2,
            initial_delay: Duration::from_millis(10),
            ..Default::default()
        };

        let cb = PgmqCircuitBreaker::new("test".to_string(), config, MockMetrics);

        let result = cb
            .execute(|| async {
                Err::<i32, RelayError>(RelayError::PgmqOperation("always_fails".to_string()))
            })
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_circuit_breaker_opens_after_failures() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            max_retries: 1,
            recovery_timeout: Duration::from_secs(60), // Long recovery time
            ..Default::default()
        };

        let cb = PgmqCircuitBreaker::new("test".to_string(), config, MockMetrics);

        // Initially, circuit breaker should allow calls
        assert!(cb.is_call_permitted());

        // Execute 3 failing operations to exceed failure threshold
        for i in 1..=3 {
            let result = cb
                .execute(|| async {
                    Err::<i32, RelayError>(RelayError::PgmqOperation(format!("failure_{}", i)))
                })
                .await;
            assert!(result.is_err());
        }

        // Now the circuit breaker should be open (not allowing calls)
        assert!(!cb.is_call_permitted());

        // Subsequent calls should be rejected immediately
        let result = cb
            .execute(|| async {
                Ok::<i32, RelayError>(42) // This would succeed if allowed
            })
            .await;

        assert!(result.is_err());
        if let Err(RelayError::CircuitBreakerOpen(msg)) = result {
            assert!(msg.contains("too many recent failures"));
        } else {
            panic!("Expected CircuitBreakerOpen error");
        }
    }

    #[tokio::test]
    async fn test_circuit_breaker_environment_variables() {
        // Test that environment variables are respected
        std::env::set_var("PGMQ_RELAY_CIRCUIT_BREAKER_FAILURE_THRESHOLD", "10");
        std::env::set_var("PGMQ_RELAY_CIRCUIT_BREAKER_MAX_RETRIES", "7");

        let config = CircuitBreakerConfig::default();

        // Verify environment variables override defaults
        assert_eq!(config.failure_threshold, 10);
        assert_eq!(config.max_retries, 7);

        // Clean up environment variables
        std::env::remove_var("PGMQ_RELAY_CIRCUIT_BREAKER_FAILURE_THRESHOLD");
        std::env::remove_var("PGMQ_RELAY_CIRCUIT_BREAKER_MAX_RETRIES");

        // Test with duration environment variable
        std::env::set_var("PGMQ_RELAY_CIRCUIT_BREAKER_RECOVERY_TIMEOUT", "45s");
        let config2 = CircuitBreakerConfig::default();
        assert_eq!(config2.recovery_timeout, Duration::from_secs(45));

        std::env::remove_var("PGMQ_RELAY_CIRCUIT_BREAKER_RECOVERY_TIMEOUT");
    }
}
