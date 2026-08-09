use async_trait::async_trait;
use lapin::{
    options::*, types::FieldTable, BasicProperties, Channel, Connection, ConnectionProperties,
};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::time::{timeout_at, Instant as TokioInstant};
use tracing::{debug, error, info, warn};

use crate::broker::{MessageBroker, RelayMessage, SendResult};
use crate::error::RelayError;
use crate::validator::Validator;
use serde::{Deserialize, Serialize};

/// RabbitMQ broker configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RabbitMQConfig {
    /// AMQP connection URL (e.g., "amqp://user:pass@localhost:5672/%2f")
    #[serde(default = "default_amqp_url")]
    pub url: String,

    /// Exchange name (empty string for default exchange)
    #[serde(default)]
    pub exchange: String,

    /// Exchange type (direct, topic, fanout, headers)
    #[serde(default = "default_exchange_type")]
    pub exchange_type: String,

    /// Whether to declare the exchange if it doesn't exist
    #[serde(default = "default_true")]
    pub declare_exchange: bool,

    /// Whether the exchange should survive broker restarts
    #[serde(default = "default_true")]
    pub durable: bool,

    /// Whether to auto-delete the exchange when no longer used
    #[serde(default)]
    pub auto_delete: bool,

    /// Message delivery mode: 1 = non-persistent, 2 = persistent
    #[serde(default = "default_delivery_mode")]
    pub delivery_mode: u8,

    /// Connection pool size
    #[serde(default = "default_pool_size")]
    pub pool_size: usize,

    /// Maximum total time spent publishing and waiting for confirms for one batch.
    #[serde(default = "default_ack_timeout_ms")]
    pub ack_timeout_ms: u64,
}

fn default_amqp_url() -> String {
    std::env::var("PGMQ_RELAY_RABBITMQ_URL")
        .unwrap_or_else(|_| "amqp://guest:guest@localhost:5672/%2f".to_string())
}

fn default_exchange_type() -> String {
    "topic".to_string()
}

fn default_true() -> bool {
    true
}

fn default_delivery_mode() -> u8 {
    2 // Persistent
}

fn default_pool_size() -> usize {
    5
}

const DEFAULT_ACK_TIMEOUT_MS: u64 = 10_000;

fn default_ack_timeout_ms() -> u64 {
    std::env::var("PGMQ_RELAY_RABBITMQ_ACK_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_ACK_TIMEOUT_MS)
}

impl Default for RabbitMQConfig {
    fn default() -> Self {
        Self {
            url: default_amqp_url(),
            exchange: String::new(),
            exchange_type: default_exchange_type(),
            declare_exchange: true,
            durable: true,
            auto_delete: false,
            delivery_mode: 2,
            pool_size: 5,
            ack_timeout_ms: default_ack_timeout_ms(),
        }
    }
}

impl Validator for RabbitMQConfig {
    fn validate(&self) -> Result<(), String> {
        if self.url.trim().is_empty() {
            return Err("RabbitMQ URL cannot be empty".to_string());
        }

        if self.delivery_mode != 1 && self.delivery_mode != 2 {
            return Err("delivery_mode must be 1 (non-persistent) or 2 (persistent)".to_string());
        }

        if self.pool_size == 0 {
            return Err("pool_size must be greater than 0".to_string());
        }

        if !(100..=300_000).contains(&self.ack_timeout_ms) {
            return Err("ack_timeout_ms must be between 100 and 300000".to_string());
        }

        Ok(())
    }
}

/// A live connection plus its pool of publish channels.
struct ConnectionPool {
    connection: Connection,
    channels: Vec<Channel>,
    /// Round-robin cursor for spreading publishes across channels.
    next: usize,
}

/// RabbitMQ broker implementation
pub struct RabbitMQBroker {
    name: String,
    config: RabbitMQConfig,
    /// Interior mutability so `send_batch` (which takes `&self`) can rebuild the
    /// connection on failure and advance the round-robin cursor.
    pool: tokio::sync::Mutex<ConnectionPool>,
}

impl RabbitMQBroker {
    /// Create one publish channel with publisher confirms enabled. Without
    /// `confirm_select`, the confirm future returned by basic_publish resolves to
    /// `NotRequested` immediately, giving a fake ack and risking silent message loss.
    async fn create_channel(connection: &Connection, index: usize) -> Result<Channel, RelayError> {
        let channel = connection.create_channel().await.map_err(|e| {
            RelayError::BrokerConfiguration(format!("Failed to create channel: {}", e))
        })?;

        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .map_err(|e| {
                RelayError::BrokerConfiguration(format!(
                    "Failed to enable publisher confirms on channel {}: {}",
                    index, e
                ))
            })?;

        Ok(channel)
    }

    /// Establish a connection and a pool of `pool_size` channels, each with publisher
    /// confirms enabled, and declare the exchange once.
    async fn connect(config: &RabbitMQConfig) -> Result<ConnectionPool, RelayError> {
        let connection = Connection::connect(&config.url, ConnectionProperties::default())
            .await
            .map_err(|e| {
                RelayError::BrokerConfiguration(format!("Failed to connect to RabbitMQ: {}", e))
            })?;

        let mut channels = Vec::with_capacity(config.pool_size);
        for i in 0..config.pool_size {
            let channel = Self::create_channel(&connection, i).await?;

            // Declare the exchange once (on the first channel) if configured.
            if i == 0 && config.declare_exchange && !config.exchange.is_empty() {
                channel
                    .exchange_declare(
                        config.exchange.as_str().into(),
                        lapin::ExchangeKind::Custom(config.exchange_type.clone()),
                        ExchangeDeclareOptions {
                            durable: config.durable,
                            auto_delete: config.auto_delete,
                            ..Default::default()
                        },
                        FieldTable::default(),
                    )
                    .await
                    .map_err(|e| {
                        RelayError::BrokerConfiguration(format!(
                            "Failed to declare exchange: {}",
                            e
                        ))
                    })?;

                info!(
                    "Declared exchange '{}' of type '{}'",
                    config.exchange, config.exchange_type
                );
            }

            channels.push(channel);
        }

        Ok(ConnectionPool {
            connection,
            channels,
            next: 0,
        })
    }

    /// Create a new RabbitMQ broker
    pub async fn new(name: &str, config: &RabbitMQConfig) -> Result<Self, RelayError> {
        info!(
            "Creating RabbitMQ broker '{}' with URL: {}",
            name,
            crate::logging::redact_url(&config.url)
        );

        let pool = Self::connect(config).await?;

        info!(
            "RabbitMQ broker '{}' initialized successfully with {} channel(s)",
            name, config.pool_size
        );

        Ok(Self {
            name: name.to_string(),
            config: config.clone(),
            pool: tokio::sync::Mutex::new(pool),
        })
    }

    /// Convert relay headers to AMQP headers
    fn convert_headers(headers: &HashMap<String, String>) -> FieldTable {
        let mut field_table = FieldTable::default();
        for (key, value) in headers {
            field_table.insert(
                key.clone().into(),
                lapin::types::AMQPValue::LongString(value.clone().into()),
            );
        }
        field_table
    }
}

#[async_trait]
impl MessageBroker for RabbitMQBroker {
    async fn send_batch(
        &self,
        topic: &str,
        messages: &[RelayMessage],
    ) -> Result<SendResult, RelayError> {
        if messages.is_empty() {
            return Ok(SendResult {
                successful_message_ids: Vec::new(),
                failed_messages: Vec::new(),
            });
        }

        let start_time = Instant::now();
        let deadline = TokioInstant::now() + Duration::from_millis(self.config.ack_timeout_ms);
        let mut successful_ids = Vec::new();
        let mut failed_messages = Vec::new();

        debug!(
            broker = %self.name,
            topic = %topic,
            message_count = messages.len(),
            "Sending batch to RabbitMQ"
        );

        let mut pool = self.pool.lock().await;

        // Reconnect if the connection has dropped. lapin does not auto-recover, so without
        // this every send after a disconnect would fail forever.
        if !pool.connection.status().connected() {
            warn!(
                broker = %self.name,
                "RabbitMQ connection is down - attempting to reconnect"
            );
            match Self::connect(&self.config).await {
                Ok(new_pool) => {
                    *pool = new_pool;
                    info!(broker = %self.name, "RabbitMQ reconnected");
                }
                Err(e) => {
                    error!(broker = %self.name, error = %e, "RabbitMQ reconnect failed");
                    let all_failed = messages
                        .iter()
                        .map(|m| (m.id, format!("Reconnect failed: {}", e)))
                        .collect();
                    return Ok(SendResult {
                        successful_message_ids: Vec::new(),
                        failed_messages: all_failed,
                    });
                }
            }
        }

        let channel_count = pool.channels.len();

        // Phase 1: issue all publishes (spread across the channel pool), collecting the
        // confirm futures. Phase 2 awaits the broker confirms. This pipelines confirms
        // across the pool instead of round-tripping per message.
        let mut pending = Vec::with_capacity(messages.len());
        for (message_index, message) in messages.iter().enumerate() {
            let routing_key = message.key.as_deref().unwrap_or(topic);
            let amqp_headers = Self::convert_headers(&message.headers);
            let properties = BasicProperties::default()
                .with_delivery_mode(self.config.delivery_mode)
                .with_headers(amqp_headers);

            let idx = pool.next % channel_count;
            pool.next = pool.next.wrapping_add(1);

            // A channel-level error (e.g. a precondition failure) closes that channel
            // without dropping the connection. Repair it here, otherwise this pool slot
            // would fail its share of every future batch.
            if !pool.channels[idx].status().connected() {
                warn!(
                    broker = %self.name,
                    channel_index = idx,
                    "RabbitMQ channel is closed - recreating"
                );
                match Self::create_channel(&pool.connection, idx).await {
                    Ok(channel) => pool.channels[idx] = channel,
                    Err(e) => {
                        error!(
                            broker = %self.name,
                            channel_index = idx,
                            error = %e,
                            "Failed to recreate RabbitMQ channel"
                        );
                        failed_messages
                            .push((message.id, format!("Channel recreation failed: {}", e)));
                        continue;
                    }
                }
            }
            let channel = &pool.channels[idx];

            // `mandatory: true` makes the broker return unroutable messages instead of
            // acking and dropping them; the returned message surfaces in phase 2.
            match timeout_at(
                deadline,
                channel.basic_publish(
                    self.config.exchange.as_str().into(),
                    routing_key.into(),
                    BasicPublishOptions {
                        mandatory: true,
                        ..Default::default()
                    },
                    &message.payload,
                    properties,
                ),
            )
            .await
            {
                Ok(Ok(confirm)) => pending.push((message.id, confirm)),
                Ok(Err(e)) => {
                    error!(
                        broker = %self.name,
                        msg_id = message.id,
                        error = %e,
                        "Failed to publish message"
                    );
                    failed_messages.push((message.id, format!("Publish failed: {}", e)));
                }
                Err(_) => {
                    warn!(
                        broker = %self.name,
                        topic = %topic,
                        timeout_ms = self.config.ack_timeout_ms,
                        "RabbitMQ batch publish timed out"
                    );

                    for (pending_id, _) in pending.drain(..) {
                        failed_messages
                            .push((pending_id, "Publisher confirm timed out".to_string()));
                    }
                    for unsent in messages.iter().skip(message_index) {
                        failed_messages.push((
                            unsent.id,
                            "RabbitMQ batch acknowledgement deadline exceeded".to_string(),
                        ));
                    }
                    break;
                }
            }
        }

        // Phase 2: await broker confirmations.
        let mut pending = pending.into_iter();
        while let Some((msg_id, confirm)) = pending.next() {
            match timeout_at(deadline, confirm).await {
                Ok(Ok(confirmation)) if confirmation.is_nack() => {
                    error!(
                        broker = %self.name,
                        msg_id = msg_id,
                        "Message was nack'd by RabbitMQ"
                    );
                    failed_messages.push((msg_id, "Message nack'd by broker".to_string()));
                }
                Ok(Ok(confirmation)) => {
                    // With `mandatory: true`, an unroutable message is acked *and*
                    // returned. Treating that ack as success would delete a message from
                    // PGMQ that the broker dropped (e.g. destination queue not declared).
                    if let Some(returned) = confirmation.take_message() {
                        error!(
                            broker = %self.name,
                            msg_id = msg_id,
                            reply_code = returned.reply_code,
                            reply_text = %returned.reply_text,
                            "Message was returned as unroutable by RabbitMQ"
                        );
                        failed_messages.push((
                            msg_id,
                            format!(
                                "Unroutable message returned by broker: {} (code {})",
                                returned.reply_text, returned.reply_code
                            ),
                        ));
                    } else {
                        successful_ids.push(msg_id);
                    }
                }
                Ok(Err(e)) => {
                    error!(
                        broker = %self.name,
                        msg_id = msg_id,
                        error = %e,
                        "Failed to confirm message"
                    );
                    failed_messages.push((msg_id, format!("Confirmation failed: {}", e)));
                }
                Err(_) => {
                    warn!(
                        broker = %self.name,
                        topic = %topic,
                        timeout_ms = self.config.ack_timeout_ms,
                        "RabbitMQ publisher confirm timed out"
                    );
                    failed_messages.push((msg_id, "Publisher confirm timed out".to_string()));
                    failed_messages.extend(pending.map(|(pending_id, _)| {
                        (pending_id, "Publisher confirm timed out".to_string())
                    }));
                    break;
                }
            }
        }

        let duration = start_time.elapsed();

        if !failed_messages.is_empty() {
            warn!(
                broker = %self.name,
                topic = %topic,
                successful = successful_ids.len(),
                failed = failed_messages.len(),
                "Batch send completed with failures"
            );
        } else {
            info!(
                broker = %self.name,
                topic = %topic,
                message_count = successful_ids.len(),
                duration_ms = duration.as_millis(),
                "Batch sent successfully"
            );
        }

        Ok(SendResult {
            successful_message_ids: successful_ids,
            failed_messages,
        })
    }

    async fn health_check(&self) -> Result<(), RelayError> {
        let pool = self.pool.lock().await;
        if pool.connection.status().connected() {
            Ok(())
        } else {
            Err(RelayError::BrokerHealthCheck(format!(
                "RabbitMQ broker '{}' connection is not connected",
                self.name
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_ack_timeout_is_ten_seconds() {
        assert_eq!(DEFAULT_ACK_TIMEOUT_MS, 10_000);
    }

    #[test]
    fn validates_ack_timeout_range() {
        let below_minimum = RabbitMQConfig {
            ack_timeout_ms: 99,
            ..Default::default()
        };
        assert!(below_minimum.validate().is_err());

        let minimum = RabbitMQConfig {
            ack_timeout_ms: 100,
            ..Default::default()
        };
        assert!(minimum.validate().is_ok());

        let above_maximum = RabbitMQConfig {
            ack_timeout_ms: 300_001,
            ..Default::default()
        };
        assert!(above_maximum.validate().is_err());
    }
}
