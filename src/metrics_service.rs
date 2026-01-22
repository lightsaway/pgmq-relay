use crate::circuit_breaker::CircuitBreakerMetrics;
use prometheus::{
    Counter, CounterVec, Gauge, GaugeVec, HistogramOpts, HistogramVec, Opts, Registry,
};
use std::sync::OnceLock;
use std::time::Duration;

static REGISTRY: OnceLock<Registry> = OnceLock::new();
static METRICS: OnceLock<Metrics> = OnceLock::new();

pub struct Metrics {
    // Consolidated worker operation metrics (replaces pgmq_*, relay_worker_*, relay_messages_*, relay_transformation_*)
    pub worker_operation_total: CounterVec,
    pub worker_operation_duration_seconds: HistogramVec,
    pub worker_operation_errors_total: CounterVec,

    // Broker metrics (unchanged)
    pub broker_messages_sent_total: CounterVec,
    pub broker_messages_failed_total: CounterVec,
    pub broker_send_duration_seconds: HistogramVec,
    pub broker_health_check_status: GaugeVec,
    pub broker_batch_size: HistogramVec,

    // System metrics (unchanged)
    pub relay_workers_active: Gauge,

    pub callback_failures_total: CounterVec,
    pub callback_duration_seconds: HistogramVec,
    pub messages_delivered_but_not_completed_total: CounterVec,

    pub relay_uptime_seconds: Counter,
    pub relay_restarts_total: Counter,
    pub relay_memory_usage_bytes: Gauge,

    pub circuit_breaker_state: GaugeVec,
    pub circuit_breaker_operations_total: CounterVec,
    pub circuit_breaker_failures_total: CounterVec,
    pub circuit_breaker_rejections_total: CounterVec,
    pub circuit_breaker_operation_duration_seconds: HistogramVec,
}

impl Metrics {
    pub fn new() -> Result<Self, prometheus::Error> {
        let registry = REGISTRY.get_or_init(|| Registry::new());

        let worker_operation_total = CounterVec::new(
            Opts::new(
                "pgmq_relay_worker_operation_total",
                "Total number of worker operations performed",
            ),
            &["worker_name", "operation", "queue_name", "topic"],
        )?;

        let worker_operation_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "pgmq_relay_worker_operation_duration_seconds",
                "Time spent performing worker operations",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]),
            &["worker_name", "operation", "queue_name", "topic"],
        )?;

        let worker_operation_errors_total = CounterVec::new(
            Opts::new(
                "pgmq_relay_worker_operation_errors_total",
                "Total number of errors during worker operations",
            ),
            &[
                "worker_name",
                "operation",
                "queue_name",
                "topic",
                "error_type",
            ],
        )?;

        let broker_messages_sent_total = CounterVec::new(
            Opts::new(
                "pgmq_relay_broker_messages_sent_total",
                "Total number of messages sent to message brokers",
            ),
            &["broker_name", "broker_type", "topic"],
        )?;

        let broker_messages_failed_total = CounterVec::new(
            Opts::new(
                "pgmq_relay_broker_messages_failed_total",
                "Total number of messages that failed to send to brokers",
            ),
            &["broker_name", "broker_type", "topic", "error_type"],
        )?;

        let broker_send_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "pgmq_relay_broker_send_duration_seconds",
                "Time spent sending messages to brokers",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]),
            &["broker_name", "broker_type", "topic"],
        )?;

        let broker_health_check_status = GaugeVec::new(
            Opts::new(
                "pgmq_relay_broker_health_check_status",
                "Status of broker health checks (1 = healthy, 0 = unhealthy)",
            ),
            &["broker_name", "broker_type"],
        )?;

        let broker_batch_size = HistogramVec::new(
            HistogramOpts::new(
                "pgmq_relay_broker_batch_size",
                "Size of message batches sent to brokers",
            )
            .buckets(vec![
                1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0,
            ]),
            &["broker_name", "broker_type", "topic"],
        )?;

        let relay_workers_active = Gauge::new(
            "pgmq_relay_workers_active",
            "Number of active relay workers",
        )?;

        // Simplified callback metrics (single-queue per worker - removed topic_count dimension)
        let callback_failures_total = CounterVec::new(
            Opts::new(
                "pgmq_relay_callback_failures_total",
                "Total number of success callback failures after Kafka commit",
            ),
            &["worker_name", "queue_name", "topic", "error_type"],
        )?;

        let callback_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "pgmq_relay_callback_duration_seconds",
                "Time spent executing success callbacks after Kafka commit",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]),
            &["worker_name", "queue_name", "topic"],
        )?;

        let messages_delivered_but_not_completed_total = CounterVec::new(
            Opts::new(
                "pgmq_relay_messages_delivered_but_not_completed_total",
                "Messages delivered to Kafka but not completed in PGMQ due to callback failures",
            ),
            &["worker_name", "queue_name", "topic", "error_type"],
        )?;

        let relay_uptime_seconds = Counter::new(
            "pgmq_relay_uptime_seconds_total",
            "Total uptime of the relay service in seconds",
        )?;

        let relay_restarts_total = Counter::new(
            "pgmq_relay_restarts_total",
            "Total number of relay service restarts",
        )?;

        let relay_memory_usage_bytes = Gauge::new(
            "pgmq_relay_memory_usage_bytes",
            "Current memory usage of the relay service in bytes",
        )?;

        let circuit_breaker_state = GaugeVec::new(
            Opts::new(
                "pgmq_relay_circuit_breaker_state",
                "Current state of circuit breakers (0=closed, 1=open, 2=half-open)",
            ),
            &["name", "type"],
        )?;

        let circuit_breaker_operations_total = CounterVec::new(
            Opts::new(
                "pgmq_relay_circuit_breaker_operations_total",
                "Total number of operations through circuit breakers",
            ),
            &["name", "type", "result"],
        )?;

        let circuit_breaker_failures_total = CounterVec::new(
            Opts::new(
                "pgmq_relay_circuit_breaker_failures_total",
                "Total number of failures in circuit breaker operations",
            ),
            &["name", "type", "error_type"],
        )?;

        let circuit_breaker_rejections_total = CounterVec::new(
            Opts::new(
                "pgmq_relay_circuit_breaker_rejections_total",
                "Total number of operations rejected by open circuit breakers",
            ),
            &["name", "type"],
        )?;

        let circuit_breaker_operation_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "pgmq_relay_circuit_breaker_operation_duration_seconds",
                "Duration of operations through circuit breakers",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]),
            &["name", "type"],
        )?;

        registry.register(Box::new(worker_operation_total.clone()))?;
        registry.register(Box::new(worker_operation_duration_seconds.clone()))?;
        registry.register(Box::new(worker_operation_errors_total.clone()))?;
        registry.register(Box::new(broker_messages_sent_total.clone()))?;
        registry.register(Box::new(broker_messages_failed_total.clone()))?;
        registry.register(Box::new(broker_send_duration_seconds.clone()))?;
        registry.register(Box::new(broker_health_check_status.clone()))?;
        registry.register(Box::new(broker_batch_size.clone()))?;
        registry.register(Box::new(relay_workers_active.clone()))?;
        registry.register(Box::new(callback_failures_total.clone()))?;
        registry.register(Box::new(callback_duration_seconds.clone()))?;
        registry.register(Box::new(messages_delivered_but_not_completed_total.clone()))?;
        registry.register(Box::new(relay_uptime_seconds.clone()))?;
        registry.register(Box::new(relay_restarts_total.clone()))?;
        registry.register(Box::new(relay_memory_usage_bytes.clone()))?;
        registry.register(Box::new(circuit_breaker_state.clone()))?;
        registry.register(Box::new(circuit_breaker_operations_total.clone()))?;
        registry.register(Box::new(circuit_breaker_failures_total.clone()))?;
        registry.register(Box::new(circuit_breaker_rejections_total.clone()))?;
        registry.register(Box::new(circuit_breaker_operation_duration_seconds.clone()))?;

        Ok(Metrics {
            worker_operation_total,
            worker_operation_duration_seconds,
            worker_operation_errors_total,
            broker_messages_sent_total,
            broker_messages_failed_total,
            broker_send_duration_seconds,
            broker_health_check_status,
            broker_batch_size,
            relay_workers_active,
            callback_failures_total,
            callback_duration_seconds,
            messages_delivered_but_not_completed_total,
            relay_uptime_seconds,
            relay_restarts_total,
            relay_memory_usage_bytes,
            circuit_breaker_state,
            circuit_breaker_operations_total,
            circuit_breaker_failures_total,
            circuit_breaker_rejections_total,
            circuit_breaker_operation_duration_seconds,
        })
    }

    pub fn registry() -> &'static Registry {
        REGISTRY.get().expect("Metrics registry not initialized")
    }
}

pub fn init_metrics() -> Result<(), prometheus::Error> {
    let metrics = Metrics::new()?;
    METRICS.set(metrics).map_err(|_| {
        prometheus::Error::Msg("Failed to initialize metrics - already initialized".to_string())
    })?;
    Ok(())
}

pub fn get_metrics() -> &'static Metrics {
    METRICS.get().expect("Metrics not initialized")
}

pub trait MetricsService: Send + Sync {
    /// Record worker operation metrics (poll, send, complete)
    fn record_worker_operation(
        &self,
        operation: WorkerOperation,
        worker_name: &str,
        context: OperationContext,
        duration: Duration,
        count: usize,
    );

    /// Record system metrics
    fn record_system_metric(&self, metric: SystemMetric);
}

/// Worker operations that can be tracked
#[derive(Debug, Clone)]
pub enum WorkerOperation {
    Poll,
    Complete,
}

/// Context for worker operations
#[derive(Debug, Clone)]
pub struct OperationContext {
    pub queue_name: String,
    pub destination_topic: String,
}

/// System-level metrics
#[derive(Debug, Clone)]
pub enum SystemMetric {
    WorkerCount(i64),
    UptimeTick,
    Restart,
    MemoryUsage(f64),
}

/// Default implementation using the existing prometheus metrics
pub struct PrometheusMetricsService {
    _metrics: &'static Metrics,
}

impl PrometheusMetricsService {
    /// Create a new metrics service and initialize all metrics
    pub fn new() -> Result<Self, prometheus::Error> {
        // Initialize the global metrics if not already done
        init_metrics()?;
        let metrics_ref = get_metrics();

        Ok(Self {
            _metrics: metrics_ref,
        })
    }
}

impl MetricsService for PrometheusMetricsService {
    fn record_worker_operation(
        &self,
        operation: WorkerOperation,
        worker_name: &str,
        context: OperationContext,
        duration: Duration,
        count: usize,
    ) {
        let operation_str = match operation {
            WorkerOperation::Poll => "poll",
            WorkerOperation::Complete => "complete",
        };

        record_worker_operation(
            worker_name,
            operation_str,
            &context.queue_name,
            &context.destination_topic,
            duration.as_secs_f64(),
            count,
        );
    }

    fn record_system_metric(&self, metric: SystemMetric) {
        match metric {
            SystemMetric::WorkerCount(delta) => {
                if delta > 0 {
                    for _ in 0..delta {
                        inc_active_workers();
                    }
                } else {
                    for _ in 0..(-delta) {
                        dec_active_workers();
                    }
                }
            }
            SystemMetric::UptimeTick => {
                record_uptime_tick();
            }
            SystemMetric::Restart => {
                record_restart();
            }
            SystemMetric::MemoryUsage(bytes) => {
                update_memory_usage(bytes);
            }
        }
    }
}

impl Default for PrometheusMetricsService {
    fn default() -> Self {
        Self::new().expect("Failed to initialize metrics service")
    }
}

/// Consolidated worker operation recording - replaces multiple separate metric functions
pub fn record_worker_operation(
    worker_name: &str,
    operation: &str,
    queue_name: &str,
    topic: &str,
    duration: f64,
    count: usize,
) {
    let metrics = get_metrics();

    metrics
        .worker_operation_total
        .with_label_values(&[worker_name, operation, queue_name, topic])
        .inc_by(count as f64);

    metrics
        .worker_operation_duration_seconds
        .with_label_values(&[worker_name, operation, queue_name, topic])
        .observe(duration);
}

/// Record worker operation errors
pub fn record_worker_operation_error(
    worker_name: &str,
    operation: &str,
    queue_name: &str,
    topic: &str,
    error: &crate::error::RelayError,
) {
    let metrics = get_metrics();
    metrics
        .worker_operation_errors_total
        .with_label_values(&[
            worker_name,
            operation,
            queue_name,
            topic,
            error.error_type(),
        ])
        .inc();
}

// Helper functions for direct metric recording

pub fn record_broker_send(
    broker_name: &str,
    broker_type: &str,
    topic: &str,
    duration: f64,
    message_count: usize,
    success: bool,
) {
    let metrics = get_metrics();

    metrics
        .broker_send_duration_seconds
        .with_label_values(&[broker_name, broker_type, topic])
        .observe(duration);

    metrics
        .broker_batch_size
        .with_label_values(&[broker_name, broker_type, topic])
        .observe(message_count as f64);

    if success {
        metrics
            .broker_messages_sent_total
            .with_label_values(&[broker_name, broker_type, topic])
            .inc_by(message_count as f64);
    } else {
        metrics
            .broker_messages_failed_total
            .with_label_values(&[broker_name, broker_type, topic, "send_error"])
            .inc_by(message_count as f64);
    }
}

pub fn record_broker_health(broker_name: &str, broker_type: &str, healthy: bool) {
    let metrics = get_metrics();
    metrics
        .broker_health_check_status
        .with_label_values(&[broker_name, broker_type])
        .set(if healthy { 1.0 } else { 0.0 });
}

pub fn record_message_transformation(queue_name: &str, transformation_type: &str, success: bool) {
    // Transformations are now tracked as worker operations
    // This is a legacy function kept for backwards compatibility
    if !success {
        let error =
            crate::error::RelayError::MessageTransformation(transformation_type.to_string());
        record_worker_operation_error("unknown", "transformation", queue_name, "unknown", &error);
    }
}

pub fn inc_active_workers() {
    let metrics = get_metrics();
    metrics.relay_workers_active.inc();
}

pub fn dec_active_workers() {
    let metrics = get_metrics();
    metrics.relay_workers_active.dec();
}

// These functions are replaced by record_worker_operation() and record_worker_operation_error()

pub fn record_completion_failure(
    worker_name: &str,
    queue_name: &str,
    topic: &str,
    error_type: &str,
    duration: f64,
    message_count: usize,
) {
    let metrics = get_metrics();

    metrics
        .callback_failures_total
        .with_label_values(&[worker_name, queue_name, topic, error_type])
        .inc();

    metrics
        .callback_duration_seconds
        .with_label_values(&[worker_name, queue_name, topic])
        .observe(duration);

    metrics
        .messages_delivered_but_not_completed_total
        .with_label_values(&[worker_name, queue_name, topic, error_type])
        .inc_by(message_count as f64);
}

pub fn record_uptime_tick() {
    let metrics = get_metrics();
    metrics.relay_uptime_seconds.inc();
}

pub fn record_restart() {
    let metrics = get_metrics();
    metrics.relay_restarts_total.inc();
}

pub fn update_memory_usage(bytes: f64) {
    let metrics = get_metrics();
    metrics.relay_memory_usage_bytes.set(bytes);
}

pub struct GlobalCircuitBreakerMetrics {
    operation_type: String,
}

impl GlobalCircuitBreakerMetrics {
    pub fn new(operation_type: &str) -> Self {
        Self {
            operation_type: operation_type.to_string(),
        }
    }
}

impl CircuitBreakerMetrics for GlobalCircuitBreakerMetrics {
    fn record_success(&self, name: &str, duration: std::time::Duration, attempt: u32) {
        let metrics = get_metrics();

        metrics
            .circuit_breaker_state
            .with_label_values(&[name, &self.operation_type])
            .set(0.0);

        metrics
            .circuit_breaker_operations_total
            .with_label_values(&[name, &self.operation_type, "success"])
            .inc();

        metrics
            .circuit_breaker_operation_duration_seconds
            .with_label_values(&[name, &self.operation_type])
            .observe(duration.as_secs_f64());
        if attempt > 1 {
            metrics
                .circuit_breaker_operations_total
                .with_label_values(&[name, &self.operation_type, "retry_success"])
                .inc();
        }
    }

    fn record_failure(
        &self,
        name: &str,
        duration: std::time::Duration,
        attempt: u32,
        error_type: &str,
    ) {
        let metrics = get_metrics();

        metrics
            .circuit_breaker_operations_total
            .with_label_values(&[name, &self.operation_type, "failure"])
            .inc();

        metrics
            .circuit_breaker_failures_total
            .with_label_values(&[name, &self.operation_type, error_type])
            .inc();

        metrics
            .circuit_breaker_operation_duration_seconds
            .with_label_values(&[name, &self.operation_type])
            .observe(duration.as_secs_f64());
        if attempt > 1 {
            metrics
                .circuit_breaker_operations_total
                .with_label_values(&[name, &self.operation_type, "retry_attempt"])
                .inc();
        }
    }

    fn record_rejection(&self, name: &str) {
        let metrics = get_metrics();

        metrics
            .circuit_breaker_state
            .with_label_values(&[name, &self.operation_type])
            .set(1.0);

        metrics
            .circuit_breaker_rejections_total
            .with_label_values(&[name, &self.operation_type])
            .inc();

        metrics
            .circuit_breaker_operations_total
            .with_label_values(&[name, &self.operation_type, "rejected"])
            .inc();
    }
}
