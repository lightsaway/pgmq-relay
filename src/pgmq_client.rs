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

    fn is_ready_to_process(&self) -> bool;
}

pub struct PgmqClientImpl {
    pool: Arc<Pool<Postgres>>,
    deletion_circuit_breaker: PgmqCircuitBreaker<GlobalCircuitBreakerMetrics>,
}

impl PgmqClientImpl {
    pub async fn new(config: &PgmqConfig) -> Result<Self, RelayError> {
        info!(
            "Connecting to PGMQ at: {} with max_connections: {}",
            config.connection_url, config.max_connections
        );

        // Create connection pool
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

        // Create circuit breaker for message deletion operations using default config
        let circuit_breaker_config = CircuitBreakerConfig::default();
        let metrics = GlobalCircuitBreakerMetrics::new("pgmq_deletion");
        let deletion_circuit_breaker =
            PgmqCircuitBreaker::new("pgmq-deletion".to_string(), circuit_breaker_config, metrics);

        Ok(Self {
            pool: Arc::new(pool),
            deletion_circuit_breaker,
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
                format!(
                    "SELECT msg_id, read_ct, enqueued_at, vt, message, headers \
                     FROM pgmq.read($1::text, $2::integer, $3::integer)"
                )
            }
            FetchMode::ReadWithPoll => {
                format!(
                    "SELECT msg_id, read_ct, enqueued_at, vt, message, headers \
                     FROM pgmq.read_with_poll($1::text, $2::integer, $3::integer, $4::integer, $5::integer)"
                )
            }
            FetchMode::ReadGrouped => {
                format!(
                    "SELECT msg_id, read_ct, enqueued_at, vt, message, headers \
                     FROM pgmq.read_grouped($1::text, $2::integer, $3::integer)"
                )
            }
            FetchMode::ReadGroupedWithPoll => {
                format!(
                    "SELECT msg_id, read_ct, enqueued_at, vt, message, headers \
                     FROM pgmq.read_grouped_with_poll($1::text, $2::integer, $3::integer, $4::integer, $5::integer)"
                )
            }
            FetchMode::ReadGroupedRoundRobin => {
                format!(
                    "SELECT msg_id, read_ct, enqueued_at, vt, message, headers \
                     FROM pgmq.read_grouped_rr($1::text, $2::integer, $3::integer)"
                )
            }
            FetchMode::ReadGroupedRoundRobinWithPoll => {
                // SELECT * FROM pgmq.read_grouped_rr_with_poll(queue_name, vt, qty, max_poll_seconds, poll_interval_ms)
                format!(
                    "SELECT msg_id, read_ct, enqueued_at, vt, message, headers \
                     FROM pgmq.read_grouped_rr_with_poll($1::text, $2::integer, $3::integer, $4::integer, $5::integer)"
                )
            }
            FetchMode::Pop => {
                // SELECT * FROM pgmq.pop(queue_name, qty)
                // Note: pop() doesn't use visibility timeout
                format!(
                    "SELECT msg_id, read_ct, enqueued_at, vt, message, headers \
                     FROM pgmq.pop($1::text, $2::integer)"
                )
            }
        };

        // Build query based on fetch mode
        let mut query_builder = sqlx::query(&query).bind(queue_name);

        // Bind parameters based on fetch mode
        match queue_config.fetch_mode {
            FetchMode::Regular | FetchMode::ReadGrouped | FetchMode::ReadGroupedRoundRobin => {
                // These modes use: queue_name, vt, qty
                query_builder = query_builder
                    .bind(visibility_timeout)
                    .bind(batch_size);
            }
            FetchMode::ReadWithPoll
            | FetchMode::ReadGroupedWithPoll
            | FetchMode::ReadGroupedRoundRobinWithPoll => {
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

        let rows = query_builder
            .fetch_all(&*self.pool)
            .await
            .map_err(|e| {
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

        // Execute deletion with circuit breaker protection
        let pool = self.pool.clone();
        let queue_name_owned = queue_name.clone();
        let msg_ids_owned = msg_ids.clone();

        let result = self
            .deletion_circuit_breaker
            .execute(|| {
                let pool = pool.clone();
                let queue_name_owned = queue_name_owned.clone();
                let msg_ids_owned = msg_ids_owned.clone();
                async move {
                    // Use SQL: SELECT pgmq.delete(queue_name, msg_id)
                    for msg_id in &msg_ids_owned {
                        sqlx::query("SELECT pgmq.delete($1::text, $2::bigint)")
                            .bind(&queue_name_owned)
                            .bind(msg_id)
                            .execute(&*pool)
                            .await
                            .map_err(|e| {
                                RelayError::PgmqOperation(format!(
                                    "Failed to delete message {}: {}",
                                    msg_id, e
                                ))
                            })?;
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

        tracing::trace!(
            "Archiving {} messages from queue '{}'",
            msg_ids.len(),
            queue_messages.queue_name
        );

        // Use SQL: SELECT pgmq.archive(queue_name, msg_id)
        for msg_id in &msg_ids {
            sqlx::query("SELECT pgmq.archive($1::text, $2::bigint)")
                .bind(&queue_messages.queue_name)
                .bind(msg_id)
                .execute(&*self.pool)
                .await
                .map_err(|e| {
                    RelayError::PgmqOperation(format!(
                        "Failed to archive message {}: {}",
                        msg_id, e
                    ))
                })?;
        }

        info!(
            "Archived {} messages from queue '{}' to archive table",
            msg_ids.len(),
            queue_messages.queue_name
        );

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

    fn is_ready_to_process(&self) -> bool {
        // Check if the deletion circuit breaker allows operations
        // If it's open, we can't properly complete message processing
        self.deletion_circuit_breaker.is_call_permitted()
    }
}
