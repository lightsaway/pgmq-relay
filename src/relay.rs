use futures::FutureExt;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, trace, warn};

use crate::broker::{create_broker, MessageBroker};
use crate::config::{Config, WorkerConfig};
use crate::error::RelayError;
use crate::health::HealthState;
use crate::metrics_service::{
    self as metrics, MetricsService, OperationContext, SystemMetric, WorkerOperation,
};
use crate::pgmq_client::{PgmqClient, PgmqClientImpl};
use crate::transformer::MessageTransformer;

pub struct Relay {
    config: Config,
    pgmq_client: Arc<dyn PgmqClient>,
    shutdown_tx: Option<watch::Sender<bool>>,
    worker_handles: Vec<JoinHandle<()>>,
    worker_exit_tx: mpsc::UnboundedSender<WorkerExit>,
    worker_exit_rx: mpsc::UnboundedReceiver<WorkerExit>,
    background_handles: Vec<JoinHandle<()>>,
    metrics_service: Arc<dyn MetricsService>,
    health: HealthState,
}

struct WorkerExit {
    name: String,
    result: Result<(), RelayError>,
}

impl Relay {
    pub async fn new(
        config: Config,
        metrics_service: Arc<dyn MetricsService>,
        health: HealthState,
    ) -> Result<Self, RelayError> {
        info!("Initializing PGMQ Relay");

        let pgmq_client = Arc::new(PgmqClientImpl::new(&config.pgmq).await?);

        let workers = config.get_workers();
        for worker_config in &workers {
            if !config.brokers.contains_key(&worker_config.broker_name) {
                return Err(RelayError::Configuration(format!(
                    "Broker config '{}' not found for worker '{}'",
                    worker_config.broker_name, worker_config.name
                )));
            }
        }

        info!("All broker configurations validated");

        let (worker_exit_tx, worker_exit_rx) = mpsc::unbounded_channel();

        Ok(Self {
            config,
            pgmq_client,
            shutdown_tx: None,
            worker_handles: Vec::new(),
            worker_exit_tx,
            worker_exit_rx,
            background_handles: Vec::new(),
            metrics_service,
            health,
        })
    }

    pub async fn start(&mut self) -> Result<(), RelayError> {
        let workers = self.config.get_workers();
        let total_queues: usize = workers.len();
        info!(
            "Starting PGMQ Relay with {} workers managing {} total queues",
            workers.len(),
            total_queues
        );

        // Fail fast on dead-letter queues that don't exist. A missing DLQ would
        // otherwise only surface at runtime, failing once per poison message forever.
        let mut checked_dlqs = std::collections::HashSet::new();
        for worker_config in &workers {
            if let Some(dlq) = &worker_config.queue.dead_letter_queue {
                if checked_dlqs.insert(dlq.clone()) && !self.pgmq_client.queue_exists(dlq).await? {
                    return Err(RelayError::Configuration(format!(
                        "dead_letter_queue '{}' configured for queue '{}' does not exist in \
                         PGMQ - create it first with SELECT pgmq.create('{}')",
                        dlq, worker_config.queue.queue_name, dlq
                    )));
                }
            }
        }

        // Broadcast shutdown via a watch channel: every worker owns its own receiver, so
        // workers run fully concurrently. (The previous Arc<RwLock<mpsc::Receiver>> was
        // held across each worker's entire processing iteration, serializing all workers
        // and silently defeating `parallelism`.)
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        self.shutdown_tx = Some(shutdown_tx);

        // Every worker's broker instance, kept for the periodic health monitor. Each
        // worker owns a dedicated connection/producer, so all of them must be checked:
        // a single wedged instance would otherwise hide behind a healthy sibling.
        let mut monitored_brokers: Vec<(String, String, Arc<dyn MessageBroker>)> = Vec::new();

        for worker_config in &workers {
            let broker_config = self
                .config
                .brokers
                .get(&worker_config.broker_name)
                .ok_or_else(|| {
                    RelayError::Configuration(format!(
                        "Broker config '{}' not found for worker '{}'",
                        worker_config.broker_name, worker_config.name
                    ))
                })?;

            // Create a dedicated broker instance for this worker. The worker name is a
            // stable, unique instance id used (e.g. by Kafka) to derive a transactional
            // id that survives restarts and does not collide across parallel workers.
            debug!(
                "Creating dedicated broker instance for worker '{}'",
                worker_config.name
            );
            let worker_broker: Arc<dyn MessageBroker> = Arc::from(
                create_broker(
                    &worker_config.broker_name,
                    broker_config,
                    &worker_config.name,
                )
                .await?,
            );

            let health_check_result = worker_broker.health_check().await;

            let broker_type = broker_config.broker_type();
            match &health_check_result {
                Ok(_) => {
                    metrics::record_broker_health(&worker_config.broker_name, broker_type, true);
                }
                Err(_) => {
                    metrics::record_broker_health(&worker_config.broker_name, broker_type, false);
                }
            }

            health_check_result.map_err(|e| {
                RelayError::BrokerConfiguration(format!(
                    "Health check failed for worker '{}' broker '{}': {}",
                    worker_config.name, worker_config.broker_name, e
                ))
            })?;

            monitored_brokers.push((
                worker_config.broker_name.clone(),
                broker_type.to_string(),
                Arc::clone(&worker_broker),
            ));

            let worker = RelayWorker {
                worker_config: worker_config.clone(),
                pgmq_client: Arc::clone(&self.pgmq_client),
                broker: Arc::clone(&worker_broker),
                broker_type: broker_config.broker_type().to_string(),
                shutdown_rx: shutdown_rx.clone(),
                metrics_service: Arc::clone(&self.metrics_service),
            };

            let worker_exit_tx = self.worker_exit_tx.clone();
            let handle = tokio::spawn(async move {
                let worker_name = worker.worker_config.name.clone();
                worker
                    .metrics_service
                    .record_system_metric(SystemMetric::WorkerCount(1));
                let result = match AssertUnwindSafe(worker.run()).catch_unwind().await {
                    Ok(result) => result,
                    Err(payload) => {
                        let panic_message = if let Some(message) = payload.downcast_ref::<&str>() {
                            (*message).to_string()
                        } else if let Some(message) = payload.downcast_ref::<String>() {
                            message.clone()
                        } else {
                            "unknown panic payload".to_string()
                        };
                        Err(RelayError::Configuration(format!(
                            "worker panicked: {}",
                            panic_message
                        )))
                    }
                };
                if let Err(e) = &result {
                    error!("Worker '{}' failed: {}", worker_name, e);
                }
                worker
                    .metrics_service
                    .record_system_metric(SystemMetric::WorkerCount(-1));
                let _ = worker_exit_tx.send(WorkerExit {
                    name: worker_name,
                    result,
                });
            });

            self.worker_handles.push(handle);

            info!(
                worker = %worker_config.name,
                broker = %worker_config.broker_name,
                queue_name = %worker_config.queue.queue_name,
                destination_topic = %worker_config.queue.effective_destination_topic(),
                fetch_mode = ?worker_config.queue.fetch_mode,
                archive_mode = worker_config.queue.archive_messages,
                batch_size = worker_config.batch_size,
                poll_interval_ms = worker_config.poll_interval.as_millis(),
                "Worker started with queue configuration"
            );
        }

        info!("All workers started successfully");
        for worker_config in &workers {
            info!(
                "Worker '{}': 1 queue → broker '{}' [{}]",
                worker_config.name, worker_config.broker_name, worker_config.queue.queue_name
            );
        }

        // Mark the service started and keep the readiness probe in sync with the PGMQ
        // client's ability to complete processing (its deletion circuit-breaker state).
        self.health.set_started(true);
        self.health
            .set_pgmq_ready(self.pgmq_client.is_ready_to_process());

        let health = self.health.clone();
        let pgmq_client = Arc::clone(&self.pgmq_client);
        let mut readiness_shutdown = shutdown_rx.clone();
        let readiness_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(2));
            loop {
                tokio::select! {
                    _ = readiness_shutdown.changed() => break,
                    _ = interval.tick() => {
                        health.set_pgmq_ready(pgmq_client.is_ready_to_process());
                    }
                }
            }
        });
        self.background_handles.push(readiness_handle);

        // Periodically re-check each broker's health so `broker_health_check_status`
        // tracks live state and alerts on a broker going down actually fire. A broker
        // name is reported healthy only if every worker's instance of it passes.
        if !monitored_brokers.is_empty() {
            let mut broker_shutdown = shutdown_rx.clone();
            let broker_handle = tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(15));
                loop {
                    tokio::select! {
                        _ = broker_shutdown.changed() => break,
                        _ = interval.tick() => {
                            let mut health_by_broker: std::collections::HashMap<(String, String), bool> =
                                std::collections::HashMap::new();
                            for (name, broker_type, broker) in &monitored_brokers {
                                let healthy = broker.health_check().await.is_ok();
                                *health_by_broker
                                    .entry((name.clone(), broker_type.clone()))
                                    .or_insert(true) &= healthy;
                            }
                            for ((name, broker_type), healthy) in health_by_broker {
                                metrics::record_broker_health(&name, &broker_type, healthy);
                            }
                        }
                    }
                }
            });
            self.background_handles.push(broker_handle);
        }

        Ok(())
    }

    pub async fn shutdown(&mut self, timeout: Duration) -> Result<(), RelayError> {
        info!("Shutting down PGMQ Relay");

        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(true);
        }

        let deadline = tokio::time::Instant::now() + timeout;

        join_task_handles(&mut self.worker_handles, deadline, "Worker").await?;
        join_task_handles(&mut self.background_handles, deadline, "Background task").await?;

        info!("All workers shut down successfully");
        Ok(())
    }

    pub async fn wait_for_shutdown(&mut self) -> Result<(), RelayError> {
        match self.worker_exit_rx.recv().await {
            Some(exit) => match exit.result {
                Ok(()) => Err(RelayError::Configuration(format!(
                    "Worker '{}' exited unexpectedly",
                    exit.name
                ))),
                Err(e) => Err(RelayError::Configuration(format!(
                    "Worker '{}' failed: {}",
                    exit.name, e
                ))),
            },
            None => Err(RelayError::Configuration(
                "Worker supervisor channel closed".to_string(),
            )),
        }
    }
}

async fn join_task_handles(
    handles: &mut Vec<JoinHandle<()>>,
    deadline: tokio::time::Instant,
    task_kind: &str,
) -> Result<(), RelayError> {
    while let Some(mut handle) = handles.pop() {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            handle.abort();
            for handle in handles.iter() {
                handle.abort();
            }
            return Err(RelayError::ShutdownTimeout);
        }

        tokio::select! {
            result = &mut handle => {
                if let Err(e) = result {
                    warn!("{} shutdown error: {}", task_kind, e);
                }
            }
            _ = tokio::time::sleep(remaining) => {
                warn!("Shutdown timeout reached, aborting remaining {} tasks", task_kind);
                handle.abort();
                let _ = handle.await;
                for handle in handles.iter() {
                    handle.abort();
                }
                return Err(RelayError::ShutdownTimeout);
            }
        }
    }

    Ok(())
}

struct RelayWorker {
    worker_config: WorkerConfig,
    pgmq_client: Arc<dyn PgmqClient>,
    broker: Arc<dyn MessageBroker>,
    broker_type: String,
    shutdown_rx: watch::Receiver<bool>,
    metrics_service: Arc<dyn MetricsService>,
}

/// Outcome of one processing iteration, used by the worker loop for pacing.
enum ProcessOutcome {
    /// Nothing to do (empty poll or completion gate closed); idle for the remainder of
    /// the poll interval before trying again.
    Idle,
    /// A batch was processed; poll again immediately.
    Processed,
}

/// Backoff after consecutive processing errors: doubles from the poll interval up to a
/// 30s ceiling. Without this a fast-failing dependency (dropped table, connection
/// refused) turns every worker into a busy loop hammering the recovering system.
fn error_backoff(poll_interval: Duration, consecutive_failures: u32) -> Duration {
    const MAX_BACKOFF: Duration = Duration::from_secs(30);
    let exponent = consecutive_failures.saturating_sub(1).min(16);
    poll_interval
        .saturating_mul(2u32.saturating_pow(exponent))
        .clamp(poll_interval, MAX_BACKOFF)
}

impl RelayWorker {
    async fn run(&self) -> Result<(), RelayError> {
        let queue_name = &self.worker_config.queue.queue_name;
        info!(
            "Starting relay worker '{}' for queue: {}",
            self.worker_config.name, queue_name
        );

        // Own a local clone so we can await changes without sharing a lock with other workers.
        let mut shutdown_rx = self.shutdown_rx.clone();
        let mut consecutive_failures: u32 = 0;

        // Shutdown is observed between iterations (and during the idle/backoff sleeps),
        // never by cancelling an in-flight iteration: cancellation could land between
        // broker acknowledgement and PGMQ completion, guaranteeing duplicates on every
        // restart. The shutdown deadline in `Relay::shutdown` still bounds a slow batch.
        while !*shutdown_rx.borrow() {
            let iteration_start = Instant::now();

            match self.process_messages().await {
                Ok(ProcessOutcome::Processed) => {
                    consecutive_failures = 0;
                }
                Ok(ProcessOutcome::Idle) => {
                    consecutive_failures = 0;
                    let remaining = self
                        .worker_config
                        .poll_interval
                        .saturating_sub(iteration_start.elapsed());
                    if !remaining.is_zero() {
                        tokio::select! {
                            _ = shutdown_rx.changed() => break,
                            _ = tokio::time::sleep(remaining) => {}
                        }
                    }
                }
                Err(e) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    let backoff =
                        error_backoff(self.worker_config.poll_interval, consecutive_failures);
                    error!(
                        worker = %self.worker_config.name,
                        error = %e,
                        consecutive_failures,
                        backoff_ms = backoff.as_millis() as u64,
                        "Message processing failed - backing off before retry"
                    );

                    metrics::record_worker_operation_error(
                        &self.worker_config.name,
                        "process",
                        &self.worker_config.queue.queue_name,
                        self.worker_config.queue.effective_destination_topic(),
                        &e,
                    );

                    tokio::select! {
                        _ = shutdown_rx.changed() => break,
                        _ = tokio::time::sleep(backoff) => {}
                    }
                }
            }
        }

        info!("Relay worker '{}' shutting down", self.worker_config.name);
        Ok(())
    }

    async fn process_messages(&self) -> Result<ProcessOutcome, RelayError> {
        let start_time = Instant::now();

        // Each worker handles exactly one queue
        let queue_config = &self.worker_config.queue;
        let is_pop_mode = matches!(queue_config.fetch_mode, crate::config::FetchMode::Pop);

        // Gate on completion readiness BEFORE polling. For read modes, polling while the
        // completion circuit breaker is open would deliver messages we cannot complete
        // (duplicates) or burn visibility timeouts. Pop mode is exempt: pop deletes at
        // fetch time and needs no completion - and gating it after the fetch would
        // permanently discard the already-popped batch.
        if !is_pop_mode && !self.pgmq_client.is_ready_to_process() {
            warn!(
                worker = %self.worker_config.name,
                queue = %queue_config.queue_name,
                "PGMQ client not ready to complete messages - skipping poll until the \
                 circuit breaker closes"
            );

            metrics::record_worker_operation_error(
                &self.worker_config.name,
                "send",
                &queue_config.queue_name,
                queue_config.effective_destination_topic(),
                &RelayError::PgmqClientNotReady,
            );

            return Ok(ProcessOutcome::Idle);
        }

        let poll_start_time = Instant::now();
        let queue_messages = self
            .pgmq_client
            .poll_queue(
                queue_config,
                self.worker_config.batch_size,
                self.worker_config.visibility_timeout_seconds,
            )
            .await?;
        let poll_duration = poll_start_time.elapsed().as_secs_f64();

        let context = OperationContext {
            queue_name: queue_config.queue_name.clone(),
            destination_topic: queue_config.effective_destination_topic().to_string(),
        };

        self.metrics_service.record_worker_operation(
            WorkerOperation::Poll,
            &self.worker_config.name,
            context,
            Duration::from_secs_f64(poll_duration),
            queue_messages.messages.len(),
        );

        if queue_messages.messages.is_empty() {
            // The idle sleep lives in the worker loop, where it stays responsive to
            // shutdown without cancelling an in-flight batch.
            return Ok(ProcessOutcome::Idle);
        }

        let total_messages = queue_messages.messages.len();

        info!(
            worker = %self.worker_config.name,
            queue = %queue_config.queue_name,
            total_messages = total_messages,
            "Processing message batch"
        );

        // Transform messages, isolating poison messages so one bad message cannot fail the
        // whole batch (head-of-line blocking). Failures are collected with their error and
        // handled after the readiness gate (dead-lettered or left to retry).
        let mut relay_messages = Vec::new();
        let mut transform_failed: Vec<(&crate::pgmq_client::PgmqMessageWithHeaders, String)> =
            Vec::new();

        for message in &queue_messages.messages {
            match MessageTransformer::transform_message_with_global_metrics(
                &message.message,
                &message.headers,
                message.msg_id,
                &queue_config.message_transformation,
                &queue_config.queue_name,
                &queue_config.key_field,
            ) {
                Ok(relay_message) => relay_messages.push(relay_message),
                Err(e) => {
                    error!(
                        worker = %self.worker_config.name,
                        msg_id = message.msg_id,
                        queue = %queue_config.queue_name,
                        error = %e,
                        "Failed to transform message - isolating as poison message"
                    );
                    transform_failed.push((message, e.to_string()));
                }
            }
        }

        let destination_topic = queue_config.effective_destination_topic().to_string();

        // msg_ids that have been fully handled and can be removed from the source queue:
        // either delivered to the broker or routed to the dead-letter queue.
        let mut ids_to_complete: std::collections::HashSet<i64> = std::collections::HashSet::new();

        // Route poison messages to the dead-letter queue if configured; otherwise leave
        // them in the source queue (they retry after the visibility timeout without
        // blocking the rest of the batch).
        if !transform_failed.is_empty() {
            match &queue_config.dead_letter_queue {
                Some(dlq) => {
                    for (message, err) in &transform_failed {
                        match self
                            .pgmq_client
                            .send_to_dead_letter(
                                dlq.as_str(),
                                &queue_config.queue_name,
                                message,
                                err.as_str(),
                            )
                            .await
                        {
                            Ok(()) => {
                                ids_to_complete.insert(message.msg_id);
                            }
                            Err(e) => {
                                if is_pop_mode {
                                    error!(
                                        worker = %self.worker_config.name,
                                        msg_id = message.msg_id,
                                        error = %e,
                                        "Failed to route poison message to dead-letter queue; \
                                         pop mode already removed it from the source queue - \
                                         the message is LOST"
                                    );
                                } else {
                                    error!(
                                        worker = %self.worker_config.name,
                                        msg_id = message.msg_id,
                                        error = %e,
                                        "Failed to route poison message to dead-letter queue - will retry"
                                    );
                                }
                            }
                        }
                    }
                }
                // Unreachable when the config was validated (pop mode requires a DLQ);
                // kept as a safety net for hand-built configs.
                None if is_pop_mode => {
                    error!(
                        worker = %self.worker_config.name,
                        failed = transform_failed.len(),
                        "{} message(s) failed transformation in pop mode with no \
                         dead_letter_queue configured; pop already removed them from the \
                         source queue - they are LOST. Configure dead_letter_queue to \
                         capture them.",
                        transform_failed.len()
                    );
                }
                None => {
                    warn!(
                        worker = %self.worker_config.name,
                        failed = transform_failed.len(),
                        "{} message(s) failed transformation and no dead_letter_queue is \
                         configured; leaving them for retry after the visibility timeout",
                        transform_failed.len()
                    );
                }
            }
        }

        // A whole-batch send error must not skip the completion block below: messages
        // already routed to the DLQ have to be completed regardless, or they would be
        // re-read after the visibility timeout and dead-lettered again (duplicates in
        // the DLQ). The error is re-raised after completion.
        let mut send_error: Option<RelayError> = None;

        if !relay_messages.is_empty() {
            let send_start_time = Instant::now();
            match self
                .broker
                .send_batch(&destination_topic, &relay_messages)
                .await
            {
                Ok(send_result) => {
                    let send_duration = send_start_time.elapsed();
                    let send_success = send_result.failed_messages.is_empty();

                    metrics::record_broker_send(
                        &self.worker_config.broker_name,
                        &self.broker_type,
                        &destination_topic,
                        send_duration.as_secs_f64(),
                        relay_messages.len(),
                        send_success,
                    );

                    trace!(
                        worker = %self.worker_config.name,
                        succeeded = send_result.successful_message_ids.len(),
                        failed = send_result.failed_messages.len(),
                        "Broker delivery completed"
                    );

                    if !send_success {
                        warn!(
                            worker = %self.worker_config.name,
                            total = relay_messages.len(),
                            succeeded = send_result.successful_message_ids.len(),
                            failed = send_result.failed_messages.len(),
                            "Partial send failure - only completing successfully sent messages"
                        );

                        // Pop-mode messages no longer exist in the source queue, so a
                        // broker rejection cannot be retried - preserve them in the DLQ.
                        if is_pop_mode {
                            let by_id: std::collections::HashMap<_, _> = queue_messages
                                .messages
                                .iter()
                                .map(|m| (m.msg_id, m))
                                .collect();
                            let failures: Vec<_> = send_result
                                .failed_messages
                                .iter()
                                .filter_map(|(id, err)| {
                                    by_id
                                        .get(id)
                                        .map(|message| ((*message).clone(), err.clone()))
                                })
                                .collect();
                            self.dead_letter_pop_failures(failures).await;
                        }
                    }

                    ids_to_complete.extend(send_result.successful_message_ids.iter().copied());
                }
                Err(e) => {
                    let send_duration = send_start_time.elapsed();
                    error!("Failed to send batch to broker: {}", e);
                    metrics::record_broker_send(
                        &self.worker_config.broker_name,
                        &self.broker_type,
                        &destination_topic,
                        send_duration.as_secs_f64(),
                        relay_messages.len(),
                        false,
                    );

                    // Same as above: pop-mode messages cannot be re-read, so on a
                    // whole-batch failure preserve every attempted message in the DLQ.
                    // (If the failure was a commit-timeout ambiguity this may duplicate
                    // into the DLQ - acceptable under at-least-once.)
                    if is_pop_mode {
                        let relay_ids: std::collections::HashSet<i64> =
                            relay_messages.iter().map(|m| m.id).collect();
                        let failures: Vec<_> = queue_messages
                            .messages
                            .iter()
                            .filter(|m| relay_ids.contains(&m.msg_id))
                            .map(|m| (m.clone(), format!("Broker send failed: {}", e)))
                            .collect();
                        self.dead_letter_pop_failures(failures).await;
                    }

                    send_error = Some(e);
                }
            }
        }

        // Complete (delete or archive) exactly the messages that were delivered to the
        // broker or routed to the dead-letter queue. This is the only place messages are
        // removed from the source queue, so transform failures left for retry are never
        // accidentally deleted.
        let completion_start_time = Instant::now();
        let messages_to_complete = ids_to_complete.len();
        if messages_to_complete > 0 {
            let filtered_messages: Vec<_> = queue_messages
                .messages
                .iter()
                .filter(|m| ids_to_complete.contains(&m.msg_id))
                .cloned()
                .collect();
            let filtered_queue_messages = crate::pgmq_client::QueueMessages {
                queue_name: queue_messages.queue_name.clone(),
                messages: filtered_messages,
            };

            match self
                .pgmq_client
                .complete_queue_messages(&filtered_queue_messages, queue_config)
                .await
            {
                Ok(()) => {
                    let completion_duration = completion_start_time.elapsed();
                    let context = OperationContext {
                        queue_name: queue_config.queue_name.clone(),
                        destination_topic: queue_config.effective_destination_topic().to_string(),
                    };
                    self.metrics_service.record_worker_operation(
                        WorkerOperation::Complete,
                        &self.worker_config.name,
                        context,
                        completion_duration,
                        messages_to_complete,
                    );
                    info!(
                        worker = %self.worker_config.name,
                        completed = messages_to_complete,
                        "PGMQ completion successful"
                    );
                }
                Err(e) => {
                    let completion_duration = completion_start_time.elapsed();
                    error!(
                        worker = %self.worker_config.name,
                        error = %e,
                        "Messages delivered to broker but failed to complete in PGMQ - may cause duplicates on retry"
                    );
                    metrics::record_completion_failure(
                        &self.worker_config.name,
                        &queue_config.queue_name,
                        queue_config.effective_destination_topic(),
                        "pgmq_completion_error",
                        completion_duration.as_secs_f64(),
                        messages_to_complete,
                    );
                }
            }
        }

        // Now that DLQ-routed messages are completed, surface a whole-batch broker
        // failure to the worker loop (which records the error and backs off).
        if let Some(e) = send_error {
            return Err(e);
        }

        let processing_duration = start_time.elapsed().as_secs_f64();

        metrics::record_worker_operation(
            &self.worker_config.name,
            "process",
            &queue_config.queue_name,
            queue_config.effective_destination_topic(),
            processing_duration,
            total_messages,
        );

        // Count archived vs deleted messages for logging (only completed messages are
        // removed from the source queue; transform failures left for retry are excluded).
        let (archived_count, deleted_count) = if queue_config.archive_messages {
            (messages_to_complete, 0)
        } else {
            (0, messages_to_complete)
        };

        info!(
            worker = %self.worker_config.name,
            total_messages = total_messages,
            completed = messages_to_complete,
            archived = archived_count,
            deleted = deleted_count,
            processing_duration_ms = processing_duration * 1000.0,
            "Message batch processing completed"
        );

        Ok(ProcessOutcome::Processed)
    }

    /// Preserve pop-mode messages in the dead-letter queue after a broker failure. Pop
    /// removed them from the source queue at fetch time, so the DLQ is the only place
    /// they survive; a message is lost only if the DLQ write itself fails.
    /// Takes owned messages: this only runs on the failure path, where the clone cost
    /// is irrelevant next to keeping the worker future free of borrowed iterators.
    async fn dead_letter_pop_failures(
        &self,
        failures: Vec<(crate::pgmq_client::PgmqMessageWithHeaders, String)>,
    ) {
        let queue_config = &self.worker_config.queue;
        let Some(dlq) = &queue_config.dead_letter_queue else {
            // Unreachable when the config was validated (pop mode requires a DLQ);
            // kept as a safety net for hand-built configs.
            error!(
                worker = %self.worker_config.name,
                queue = %queue_config.queue_name,
                "No dead_letter_queue configured for pop-mode queue - undeliverable \
                 messages are LOST"
            );
            return;
        };

        for (message, error_text) in failures {
            match self
                .pgmq_client
                .send_to_dead_letter(
                    dlq.as_str(),
                    &queue_config.queue_name,
                    &message,
                    error_text.as_str(),
                )
                .await
            {
                Ok(()) => {
                    warn!(
                        worker = %self.worker_config.name,
                        msg_id = message.msg_id,
                        dead_letter_queue = %dlq,
                        "Pop-mode message preserved in dead-letter queue after broker failure"
                    );
                }
                Err(e) => {
                    error!(
                        worker = %self.worker_config.name,
                        msg_id = message.msg_id,
                        error = %e,
                        "Failed to dead-letter pop-mode message after broker failure - \
                         the message is LOST"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::error_backoff;
    use std::time::Duration;

    #[test]
    fn error_backoff_doubles_from_poll_interval_and_caps_at_thirty_seconds() {
        let base = Duration::from_millis(250);
        assert_eq!(error_backoff(base, 1), Duration::from_millis(250));
        assert_eq!(error_backoff(base, 2), Duration::from_millis(500));
        assert_eq!(error_backoff(base, 3), Duration::from_secs(1));
        assert_eq!(error_backoff(base, 8), Duration::from_secs(30));
        assert_eq!(error_backoff(base, 100), Duration::from_secs(30));
    }

    #[test]
    fn error_backoff_never_drops_below_the_poll_interval() {
        let base = Duration::from_millis(10);
        assert_eq!(error_backoff(base, 0), Duration::from_millis(10));
        assert_eq!(error_backoff(base, 1), Duration::from_millis(10));
    }
}
