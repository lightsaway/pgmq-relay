use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, trace, warn};

use crate::broker::{create_broker, MessageBroker};
use crate::config::{Config, WorkerConfig};
use crate::error::RelayError;
use crate::metrics_service::{
    self as metrics, MetricsService, OperationContext, SystemMetric, WorkerOperation,
};
use crate::pgmq_client::{PgmqClient, PgmqClientImpl};
use crate::transformer::MessageTransformer;

pub struct Relay {
    config: Config,
    pgmq_client: Arc<dyn PgmqClient>,
    shutdown_tx: Option<mpsc::Sender<()>>,
    worker_handles: Vec<JoinHandle<()>>,
    metrics_service: Arc<dyn MetricsService>,
}

impl Relay {
    pub async fn new(
        config: Config,
        metrics_service: Arc<dyn MetricsService>,
    ) -> Result<Self, RelayError> {
        info!("Initializing PGMQ Relay");

        let pgmq_client = Arc::new(PgmqClientImpl::new(&config.pgmq).await?);

        // Validate that all worker broker configs exist
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

        Ok(Self {
            config,
            pgmq_client,
            shutdown_tx: None,
            worker_handles: Vec::new(),
            metrics_service,
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

        let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>(1);
        self.shutdown_tx = Some(shutdown_tx);

        let shutdown_rx = Arc::new(RwLock::new(shutdown_rx));

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

            // Create a dedicated broker instance for this worker
            debug!(
                "Creating dedicated broker instance for worker '{}'",
                worker_config.name
            );
            let worker_broker = create_broker(&worker_config.broker_name, broker_config).await?;

            // Health check the worker's broker
            let health_check_result = worker_broker.health_check().await;

            // Record health check metrics
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

            let worker = RelayWorker {
                worker_config: worker_config.clone(),
                pgmq_client: Arc::clone(&self.pgmq_client),
                broker: Arc::from(worker_broker),
                broker_type: broker_config.broker_type().to_string(),
                shutdown_rx: Arc::clone(&shutdown_rx),
                metrics_service: Arc::clone(&self.metrics_service),
            };

            let handle = tokio::spawn(async move {
                worker
                    .metrics_service
                    .record_system_metric(SystemMetric::WorkerCount(1));
                if let Err(e) = worker.run().await {
                    error!("Worker '{}' failed: {}", worker.worker_config.name, e);
                }
                worker
                    .metrics_service
                    .record_system_metric(SystemMetric::WorkerCount(-1));
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
        Ok(())
    }

    pub async fn shutdown(&mut self, timeout: Duration) -> Result<(), RelayError> {
        info!("Shutting down PGMQ Relay");

        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(()).await;
        }

        let shutdown_future = async {
            for handle in self.worker_handles.drain(..) {
                if let Err(e) = handle.await {
                    warn!("Worker shutdown error: {}", e);
                }
            }
        };

        tokio::select! {
            _ = shutdown_future => {
                info!("All workers shut down successfully");
                Ok(())
            }
            _ = tokio::time::sleep(timeout) => {
                warn!("Shutdown timeout reached, aborting remaining workers");
                for handle in &self.worker_handles {
                    handle.abort();
                }
                Err(RelayError::ShutdownTimeout)
            }
        }
    }

    pub async fn wait_for_shutdown(&mut self) -> Result<(), RelayError> {
        for handle in self.worker_handles.drain(..) {
            handle.await?;
        }
        Ok(())
    }
}

struct RelayWorker {
    worker_config: WorkerConfig,
    pgmq_client: Arc<dyn PgmqClient>,
    broker: Arc<dyn MessageBroker>,
    broker_type: String,
    shutdown_rx: Arc<RwLock<mpsc::Receiver<()>>>,
    metrics_service: Arc<dyn MetricsService>,
}

impl RelayWorker {
    async fn run(&self) -> Result<(), RelayError> {
        let queue_name = &self.worker_config.queue.queue_name;
        info!(
            "Starting relay worker '{}' for queue: {}",
            self.worker_config.name, queue_name
        );

        loop {
            let mut shutdown_rx = self.shutdown_rx.write().await;

            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("Shutdown signal received for worker '{}'", self.worker_config.name);
                    break;
                }
                result = self.process_messages() => {
                    if let Err(e) = result {
                        error!(
                            worker = %self.worker_config.name,
                            error = %e,
                            "Message processing failed"
                        );

                        metrics::record_worker_operation_error(
                            &self.worker_config.name,
                            "process",
                            &self.worker_config.queue.queue_name,
                            self.worker_config.queue.effective_destination_topic(),
                            &e,
                        );
                    }
                }
            }
        }

        info!("Relay worker '{}' shutting down", self.worker_config.name);
        Ok(())
    }

    async fn process_messages(&self) -> Result<(), RelayError> {
        let start_time = Instant::now();

        // Each worker handles exactly one queue
        let queue_config = &self.worker_config.queue;

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

        // If queue is empty, sleep for remaining poll interval time
        if queue_messages.messages.is_empty() {
            let poll_elapsed = poll_start_time.elapsed();
            if poll_elapsed < self.worker_config.poll_interval {
                let remaining_sleep = self.worker_config.poll_interval - poll_elapsed;
                tokio::time::sleep(remaining_sleep).await;
            }
            // If poll took longer than interval, proceed immediately to next poll
            return Ok(());
        }

        let total_messages = queue_messages.messages.len();

        info!(
            worker = %self.worker_config.name,
            queue = %queue_config.queue_name,
            total_messages = total_messages,
            "Processing message batch"
        );

        let mut relay_messages = Vec::new();

        for message in &queue_messages.messages {
            let relay_message = MessageTransformer::transform_message_with_global_metrics(
                &message.message,
                &message.headers,
                message.msg_id,
                &queue_config.message_transformation,
                &queue_config.queue_name,
                &queue_config.key_field,
            )
            .map_err(|e| {
                error!(
                    "Failed to transform message {} from queue '{}': {}",
                    message.msg_id, queue_config.queue_name, e
                );
                e
            })?;

            relay_messages.push(relay_message);
        }

        let destination_topic = queue_config.effective_destination_topic().to_string();

        // Check if PGMQ client is ready to process messages
        // If not, don't send to broker to maintain exactly-once delivery
        if !self.pgmq_client.is_ready_to_process() {
            warn!(
                worker = %self.worker_config.name,
                total_messages = total_messages,
                "PGMQ client not ready to process - skipping broker send to prevent exactly-once violations"
            );

            metrics::record_worker_operation_error(
                &self.worker_config.name,
                "send",
                &queue_config.queue_name,
                queue_config.effective_destination_topic(),
                &RelayError::PgmqClientNotReady,
            );

            return Ok(());
        }

        let send_start_time = Instant::now();
        let send_result = self
            .broker
            .send_batch(&destination_topic, &relay_messages)
            .await
            .map_err(|e| {
                error!("Failed to send batch to broker: {}", e);
                e
            })?;
        let send_duration = send_start_time.elapsed();

        let total_messages_sent = queue_messages.messages.len();
        let send_success = send_result.failed_messages.is_empty();

        // Record broker send metrics
        metrics::record_broker_send(
            &self.worker_config.broker_name,
            &self.broker_type,
            &destination_topic,
            send_duration.as_secs_f64(),
            total_messages_sent,
            send_success,
        );

        trace!(
            worker = %self.worker_config.name,
            succeeded = send_result.successful_message_ids.len(),
            failed = send_result.failed_messages.len(),
            "Broker delivery completed"
        );

        // Only complete messages that were successfully sent to broker
        let completion_start_time = Instant::now();
        let messages_to_complete = if send_result.failed_messages.is_empty() {
            // All messages sent successfully, complete all
            queue_messages.messages.len()
        } else {
            // Partial failure - only complete successfully sent messages
            let successful_ids: std::collections::HashSet<_> =
                send_result.successful_message_ids.iter().collect();

            let filtered_messages: Vec<_> = queue_messages
                .messages
                .iter()
                .filter(|m| successful_ids.contains(&m.msg_id))
                .cloned()
                .collect();

            let count = filtered_messages.len();

            warn!(
                worker = %self.worker_config.name,
                total = total_messages_sent,
                succeeded = count,
                failed = send_result.failed_messages.len(),
                "Partial send failure - only completing successfully sent messages"
            );

            // Create a filtered QueueMessages structure for completion
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
                    info!(
                        worker = %self.worker_config.name,
                        completed = count,
                        "Successfully completed delivered messages"
                    );
                }
                Err(e) => {
                    error!(
                        worker = %self.worker_config.name,
                        error = %e,
                        "Failed to complete successfully sent messages - may cause duplicates"
                    );

                    metrics::record_completion_failure(
                        &self.worker_config.name,
                        &queue_config.queue_name,
                        queue_config.effective_destination_topic(),
                        "pgmq_completion_error",
                        completion_start_time.elapsed().as_secs_f64(),
                        count,
                    );
                }
            }

            count
        };

        // Complete all messages if send was fully successful
        if send_result.failed_messages.is_empty() {
            match self
                .pgmq_client
                .complete_queue_messages(&queue_messages, queue_config)
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

                    info!("PGMQ completion successful");
                }
                Err(e) => {
                    let completion_duration = completion_start_time.elapsed();
                    error!(
                        worker = %self.worker_config.name,
                        error = %e,
                        "Messages delivered to broker but failed to complete in PGMQ - may cause duplicates on retry"
                    );

                    // Record critical failure metrics
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

        let processing_duration = start_time.elapsed().as_secs_f64();

        // Record overall processing metrics
        metrics::record_worker_operation(
            &self.worker_config.name,
            "process",
            &queue_config.queue_name,
            queue_config.effective_destination_topic(),
            processing_duration,
            total_messages,
        );

        // Count archived vs deleted messages for logging
        let (archived_count, deleted_count) = if queue_config.archive_messages {
            (total_messages, 0)
        } else {
            (0, total_messages)
        };

        info!(
            worker = %self.worker_config.name,
            total_messages = total_messages,
            archived = archived_count,
            deleted = deleted_count,
            processing_duration_ms = processing_duration * 1000.0,
            "Message batch processing completed"
        );

        Ok(())
    }
}
