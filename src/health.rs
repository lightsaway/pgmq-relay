use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Shared, cheaply-cloneable health state used to back the `/health` (liveness) and
/// `/ready` (readiness) HTTP probes with the relay's actual runtime status instead of
/// hardcoded `200 OK` responses.
#[derive(Clone)]
pub struct HealthState {
    inner: Arc<HealthInner>,
}

struct HealthInner {
    /// Set once all workers have started successfully.
    started: AtomicBool,
    /// Reflects whether the PGMQ client can currently complete message processing
    /// (i.e. its deletion circuit breaker is not open).
    pgmq_ready: AtomicBool,
}

impl Default for HealthState {
    fn default() -> Self {
        Self::new()
    }
}

impl HealthState {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(HealthInner {
                started: AtomicBool::new(false),
                pgmq_ready: AtomicBool::new(false),
            }),
        }
    }

    pub fn set_started(&self, value: bool) {
        self.inner.started.store(value, Ordering::Relaxed);
    }

    pub fn set_pgmq_ready(&self, value: bool) {
        self.inner.pgmq_ready.store(value, Ordering::Relaxed);
    }

    /// Liveness: the process is running and able to serve. We consider the process live
    /// as long as it is up; readiness captures whether it can actually do work.
    pub fn is_live(&self) -> bool {
        true
    }

    /// Readiness: workers have started and PGMQ is able to complete processing. When this
    /// is false, orchestrators (e.g. Kubernetes) should route traffic away / restart.
    pub fn is_ready(&self) -> bool {
        self.inner.started.load(Ordering::Relaxed) && self.inner.pgmq_ready.load(Ordering::Relaxed)
    }
}
