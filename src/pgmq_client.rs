use async_trait::async_trait;
use serde_json::Value;
use sqlx::{Pool, Postgres, Row};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

use crate::circuit_breaker::{CircuitBreakerConfig, PgmqCircuitBreaker};
use crate::config::{FetchMode, PgmqConfig, QueueConfig};
use crate::error::RelayError;
use crate::metrics_service::GlobalCircuitBreakerMetrics;

#[derive(Debug, Clone)]
pub struct PgmqMessageWithHeaders {
    pub msg_id: i64,
    pub message: Value,
    pub headers: Option<Value>,
    #[allow(dead_code)]
    pub vt: chrono::DateTime<chrono::Utc>,
    #[allow(dead_code)]
    pub enqueued_at: chrono::DateTime<chrono::Utc>,
    #[allow(dead_code)]
    pub read_ct: i32,
}

#[derive(Debug, Clone)]
pub struct QueueMessages {
    pub queue_name: String,
    pub messages: Vec<PgmqMessageWithHeaders>,
}

#[async_trait]
pub trait PgmqClient: Send + Sync {
    async fn poll_queue(
        &self,
        queue_config: &QueueConfig,
        batch_size: i32,
        visibility_timeout: i32,
    ) -> Result<QueueMessages, RelayError>;

    async fn delete_messages(&self, queue_messages: &QueueMessages) -> Result<(), RelayError>;

    async fn archive_messages(&self, queue_messages: &QueueMessages) -> Result<(), RelayError>;

    async fn complete_queue_messages(
        &self,
        queue_messages: &QueueMessages,
        queue_config: &QueueConfig,
    ) -> Result<(), RelayError>;

    /// Re-enqueue a single message onto a dead-letter queue, preserving the original
    /// payload and headers and attaching dead-letter metadata (source queue, original
    /// msg_id, failure reason). Does NOT remove the message from the source queue; the
    /// caller is responsible for completing/deleting the original after this succeeds.
    async fn send_to_dead_letter(
        &self,
        dead_letter_queue: &str,
        source_queue: &str,
        message: &PgmqMessageWithHeaders,
        error: &str,
    ) -> Result<(), RelayError>;

    /// Whether a PGMQ queue with this name exists. Used to fail fast at startup on
    /// misconfigured dead-letter queues instead of erroring per message at runtime.
    async fn queue_exists(&self, queue_name: &str) -> Result<bool, RelayError>;

    fn is_ready_to_process(&self) -> bool;
}

pub struct PgmqClientImpl {
    pool: Arc<Pool<Postgres>>,
    /// Protects the completion step (delete or archive). The metric/breaker name keeps
    /// the historical "deletion" label for dashboard compatibility.
    completion_circuit_breaker: PgmqCircuitBreaker<GlobalCircuitBreakerMetrics>,
}

impl PgmqClientImpl {
    pub async fn new(config: &PgmqConfig) -> Result<Self, RelayError> {
        info!(
            "Connecting to PGMQ at: {} with max_connections: {}",
            crate::logging::redact_url(&config.connection_url),
            config.max_connections
        );

        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(config.max_connections)
            .min_connections(1)
            .acquire_timeout(Duration::from_secs(10))
            .idle_timeout(Duration::from_secs(300))
            .max_lifetime(Duration::from_secs(1800))
            .connect(&config.connection_url)
            .await
            .map_err(|e| RelayError::PgmqConnection(format!("Failed to connect: {}", e)))?;

        info!(
            "PGMQ connection pool initialized with {} connections",
            config.max_connections
        );

        let circuit_breaker_config = CircuitBreakerConfig::default();
        let metrics = GlobalCircuitBreakerMetrics::new("pgmq_deletion");
        let completion_circuit_breaker =
            PgmqCircuitBreaker::new("pgmq-deletion".to_string(), circuit_breaker_config, metrics);

        Ok(Self {
            pool: Arc::new(pool),
            completion_circuit_breaker,
        })
    }

    /// Read messages using the configured fetch mode
    async fn read_messages_with_headers(
        &self,
        queue_config: &QueueConfig,
        batch_size: i32,
        visibility_timeout: i32,
    ) -> Result<Vec<PgmqMessageWithHeaders>, RelayError> {
        let queue_name = &queue_config.queue_name;

        let query = match queue_config.fetch_mode {
            FetchMode::Regular => {
                "SELECT msg_id, read_ct, enqueued_at, vt, message, headers \
                 FROM pgmq.read($1::text, $2::integer, $3::integer)"
            }
            FetchMode::ReadWithPoll => {
                "SELECT msg_id, read_ct, enqueued_at, vt, message, headers \
                 FROM pgmq.read_with_poll($1::text, $2::integer, $3::integer, $4::integer, $5::integer)"
            }
            FetchMode::ReadGrouped => {
                "SELECT msg_id, read_ct, enqueued_at, vt, message, headers \
                 FROM pgmq.read_grouped($1::text, $2::integer, $3::integer)"
            }
            FetchMode::ReadGroupedWithPoll => {
                "SELECT msg_id, read_ct, enqueued_at, vt, message, headers \
                 FROM pgmq.read_grouped_with_poll($1::text, $2::integer, $3::integer, $4::integer, $5::integer)"
            }
            FetchMode::ReadGroupedRoundRobin => {
                "SELECT msg_id, read_ct, enqueued_at, vt, message, headers \
                 FROM pgmq.read_grouped_rr($1::text, $2::integer, $3::integer)"
            }
            FetchMode::ReadGroupedRoundRobinWithPoll => {
                "SELECT msg_id, read_ct, enqueued_at, vt, message, headers \
                 FROM pgmq.read_grouped_rr_with_poll($1::text, $2::integer, $3::integer, $4::integer, $5::integer)"
            }
            FetchMode::ReadGroupedHead => {
                "SELECT msg_id, read_ct, enqueued_at, vt, message, headers \
                 FROM pgmq.read_grouped_head($1::text, $2::integer, $3::integer)"
            }
            FetchMode::ReadGroupedHeadWithPoll => {
                "SELECT msg_id, read_ct, enqueued_at, vt, message, headers \
                 FROM pgmq.read_grouped_head_with_poll($1::text, $2::integer, $3::integer, $4::integer, $5::integer)"
            }
            // pop() doesn't use visibility timeout.
            FetchMode::Pop => {
                "SELECT msg_id, read_ct, enqueued_at, vt, message, headers \
                 FROM pgmq.pop($1::text, $2::integer)"
            }
        };

        let mut query_builder = sqlx::query(query).bind(queue_name);

        match queue_config.fetch_mode {
            FetchMode::Regular
            | FetchMode::ReadGrouped
            | FetchMode::ReadGroupedRoundRobin
            | FetchMode::ReadGroupedHead => {
                // These modes use: queue_name, vt, qty
                query_builder = query_builder.bind(visibility_timeout).bind(batch_size);
            }
            FetchMode::ReadWithPoll
            | FetchMode::ReadGroupedWithPoll
            | FetchMode::ReadGroupedRoundRobinWithPoll
            | FetchMode::ReadGroupedHeadWithPoll => {
                // These modes use: queue_name, vt, qty, max_poll_seconds, poll_interval_ms
                query_builder = query_builder
                    .bind(visibility_timeout)
                    .bind(batch_size)
                    .bind(queue_config.max_poll_seconds)
                    .bind(queue_config.poll_interval_ms);
            }
            FetchMode::Pop => {
                // Pop only uses: queue_name, qty (no visibility timeout)
                query_builder = query_builder.bind(batch_size);
            }
        }

        let rows = query_builder.fetch_all(&*self.pool).await.map_err(|e| {
            RelayError::PgmqOperation(format!(
                "Failed to read messages using {:?}: {}",
                queue_config.fetch_mode, e
            ))
        })?;

        let mut messages = Vec::new();
        for row in rows {
            messages.push(PgmqMessageWithHeaders {
                msg_id: row.get("msg_id"),
                message: row.get("message"),
                headers: row.get("headers"),
                vt: row.get("vt"),
                enqueued_at: row.get("enqueued_at"),
                read_ct: row.get("read_ct"),
            });
        }

        Ok(messages)
    }
}

#[async_trait]
impl PgmqClient for PgmqClientImpl {
    async fn poll_queue(
        &self,
        queue_config: &QueueConfig,
        batch_size: i32,
        visibility_timeout: i32,
    ) -> Result<QueueMessages, RelayError> {
        let messages = self
            .read_messages_with_headers(queue_config, batch_size, visibility_timeout)
            .await?;
        Ok(QueueMessages {
            queue_name: queue_config.queue_name.clone(),
            messages,
        })
    }

    async fn delete_messages(&self, queue_messages: &QueueMessages) -> Result<(), RelayError> {
        if queue_messages.messages.is_empty() {
            return Ok(());
        }

        let msg_ids: Vec<i64> = queue_messages.messages.iter().map(|m| m.msg_id).collect();
        let queue_name = &queue_messages.queue_name;

        tracing::trace!(
            "Deleting {} messages from queue '{}' with circuit breaker protection",
            msg_ids.len(),
            queue_name
        );

        let pool = self.pool.clone();
        let queue_name_owned = queue_name.clone();
        let msg_ids_owned = msg_ids.clone();

        let result = self
            .completion_circuit_breaker
            .execute(move || {
                let pool = pool.clone();
                let queue_name_owned = queue_name_owned.clone();
                let msg_ids_owned = msg_ids_owned.clone();
                async move {
                    // Delete the whole batch in a single statement using the array form
                    // pgmq.delete(queue_name text, msg_ids bigint[]). This is one round-trip
                    // and a single atomic statement, so the batch can no longer be left
                    // partially completed by a mid-loop failure.
                    //
                    // pgmq.delete returns the set of ids it actually deleted; a shortfall
                    // (e.g. wrong queue name resolving to zero rows) must not pass as
                    // success or the messages would redeliver forever, invisibly.
                    let deleted: Vec<i64> =
                        sqlx::query_scalar("SELECT pgmq.delete($1::text, $2::bigint[])")
                            .bind(&queue_name_owned)
                            .bind(&msg_ids_owned)
                            .fetch_all(&*pool)
                            .await
                            .map_err(|e| {
                                RelayError::PgmqOperation(format!(
                                    "Failed to delete {} messages: {}",
                                    msg_ids_owned.len(),
                                    e
                                ))
                            })?;
                    if deleted.len() != msg_ids_owned.len() {
                        tracing::warn!(
                            queue = %queue_name_owned,
                            requested = msg_ids_owned.len(),
                            deleted = deleted.len(),
                            "pgmq.delete removed fewer messages than requested (already \
                             deleted by another consumer, or wrong queue?)"
                        );
                    }
                    Ok(())
                }
            })
            .await;

        match result {
            Ok(_) => {
                info!(
                    "Deleted {} messages from queue '{}'",
                    msg_ids.len(),
                    queue_name
                );
            }
            Err(e) => {
                error!(
                    "Failed to delete messages from queue '{}': {}",
                    queue_name, e
                );
                return Err(e);
            }
        }
        Ok(())
    }

    async fn archive_messages(&self, queue_messages: &QueueMessages) -> Result<(), RelayError> {
        if queue_messages.messages.is_empty() {
            return Ok(());
        }

        let msg_ids: Vec<i64> = queue_messages.messages.iter().map(|m| m.msg_id).collect();
        let queue_name = &queue_messages.queue_name;

        tracing::trace!(
            "Archiving {} messages from queue '{}' with circuit breaker protection",
            msg_ids.len(),
            queue_name
        );

        let pool = self.pool.clone();
        let queue_name_owned = queue_name.clone();
        let msg_ids_owned = msg_ids.clone();

        // Same breaker + retry protection as deletion: archive is the completion step
        // for archive-mode queues, and without the breaker a persistently failing
        // archive table would re-publish the same batch to the broker every visibility
        // timeout with `is_ready_to_process()` still reporting true.
        let result = self
            .completion_circuit_breaker
            .execute(move || {
                let pool = pool.clone();
                let queue_name_owned = queue_name_owned.clone();
                let msg_ids_owned = msg_ids_owned.clone();
                async move {
                    // Archive the whole batch in a single statement using the array form
                    // pgmq.archive(queue_name text, msg_ids bigint[]) - one round-trip, atomic.
                    let archived: Vec<i64> =
                        sqlx::query_scalar("SELECT pgmq.archive($1::text, $2::bigint[])")
                            .bind(&queue_name_owned)
                            .bind(&msg_ids_owned)
                            .fetch_all(&*pool)
                            .await
                            .map_err(|e| {
                                RelayError::PgmqOperation(format!(
                                    "Failed to archive {} messages: {}",
                                    msg_ids_owned.len(),
                                    e
                                ))
                            })?;
                    if archived.len() != msg_ids_owned.len() {
                        tracing::warn!(
                            queue = %queue_name_owned,
                            requested = msg_ids_owned.len(),
                            archived = archived.len(),
                            "pgmq.archive moved fewer messages than requested (already \
                             completed by another consumer, or wrong queue?)"
                        );
                    }
                    Ok(())
                }
            })
            .await;

        match result {
            Ok(_) => {
                info!(
                    "Archived {} messages from queue '{}' to archive table",
                    msg_ids.len(),
                    queue_name
                );
            }
            Err(e) => {
                error!(
                    "Failed to archive messages from queue '{}': {}",
                    queue_name, e
                );
                return Err(e);
            }
        }
        Ok(())
    }

    async fn complete_queue_messages(
        &self,
        queue_messages: &QueueMessages,
        queue_config: &QueueConfig,
    ) -> Result<(), RelayError> {
        if queue_messages.messages.is_empty() {
            return Ok(());
        }

        // For Pop mode, messages are already deleted by the pop() function
        if matches!(queue_config.fetch_mode, FetchMode::Pop) {
            tracing::trace!(
                "Queue '{}' using Pop mode - messages already deleted",
                queue_messages.queue_name
            );
            return Ok(());
        }

        if queue_config.archive_messages {
            tracing::trace!(
                "Queue '{}' configured for archiving",
                queue_messages.queue_name
            );
            self.archive_messages(queue_messages).await
        } else {
            tracing::trace!(
                "Queue '{}' configured for deletion",
                queue_messages.queue_name
            );
            self.delete_messages(queue_messages).await
        }
    }

    async fn send_to_dead_letter(
        &self,
        dead_letter_queue: &str,
        source_queue: &str,
        message: &PgmqMessageWithHeaders,
        error: &str,
    ) -> Result<(), RelayError> {
        // Build dead-letter headers: preserve original headers under a nested key and
        // attach metadata describing why the message was dead-lettered.
        let mut dlq_headers = serde_json::Map::new();
        dlq_headers.insert(
            "x-dead-letter-source-queue".to_string(),
            Value::String(source_queue.to_string()),
        );
        dlq_headers.insert(
            "x-dead-letter-msg-id".to_string(),
            Value::String(message.msg_id.to_string()),
        );
        dlq_headers.insert(
            "x-dead-letter-error".to_string(),
            Value::String(error.to_string()),
        );
        if let Some(original) = &message.headers {
            dlq_headers.insert(
                "x-dead-letter-original-headers".to_string(),
                original.clone(),
            );
        }
        let headers_value = Value::Object(dlq_headers);

        sqlx::query("SELECT pgmq.send($1::text, $2::jsonb, $3::jsonb)")
            .bind(dead_letter_queue)
            .bind(&message.message)
            .bind(&headers_value)
            .execute(&*self.pool)
            .await
            .map_err(|e| {
                RelayError::PgmqOperation(format!(
                    "Failed to send message {} to dead-letter queue '{}': {}",
                    message.msg_id, dead_letter_queue, e
                ))
            })?;

        info!(
            "Routed message {} from queue '{}' to dead-letter queue '{}'",
            message.msg_id, source_queue, dead_letter_queue
        );

        Ok(())
    }

    async fn queue_exists(&self, queue_name: &str) -> Result<bool, RelayError> {
        sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pgmq.list_queues() WHERE queue_name = $1::text)",
        )
        .bind(queue_name)
        .fetch_one(&*self.pool)
        .await
        .map_err(|e| {
            RelayError::PgmqOperation(format!(
                "Failed to check whether queue '{}' exists: {}",
                queue_name, e
            ))
        })
    }

    fn is_ready_to_process(&self) -> bool {
        // Check if the completion circuit breaker allows operations
        // If it's open, we can't properly complete message processing
        self.completion_circuit_breaker.is_call_permitted()
    }
}
