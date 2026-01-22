use async_trait::async_trait;
use lapin::{
    options::*, types::FieldTable, BasicProperties, Channel, Connection, ConnectionProperties,
};
use std::collections::HashMap;
use std::time::Instant;
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
        }
    }
}

impl Validator for RabbitMQConfig {
    fn validate(&self) -> Result<(), String> {
        // Validate AMQP URL
        if self.url.trim().is_empty() {
            return Err("RabbitMQ URL cannot be empty".to_string());
        }

        // Validate delivery mode
        if self.delivery_mode != 1 && self.delivery_mode != 2 {
            return Err("delivery_mode must be 1 (non-persistent) or 2 (persistent)".to_string());
        }

        // Validate pool size
        if self.pool_size == 0 {
            return Err("pool_size must be greater than 0".to_string());
        }

        Ok(())
    }
}

/// RabbitMQ broker implementation
pub struct RabbitMQBroker {
    name: String,
    config: RabbitMQConfig,
    connection: Connection,
    channel: Channel,
}

impl RabbitMQBroker {
    /// Create a new RabbitMQ broker
    pub async fn new(name: &str, config: &RabbitMQConfig) -> Result<Self, RelayError> {
        info!(
            "Creating RabbitMQ broker '{}' with URL: {}",
            name, config.url
        );

        // Create connection
        let connection = Connection::connect(&config.url, ConnectionProperties::default())
            .await
            .map_err(|e| {
                RelayError::BrokerConfiguration(format!("Failed to connect to RabbitMQ: {}", e))
            })?;

        // Create channel
        let channel = connection.create_channel().await.map_err(|e| {
            RelayError::BrokerConfiguration(format!("Failed to create channel: {}", e))
        })?;

        // Declare exchange if configured
        if config.declare_exchange && !config.exchange.is_empty() {
            channel
                .exchange_declare(
                    &config.exchange,
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
                    RelayError::BrokerConfiguration(format!("Failed to declare exchange: {}", e))
                })?;

            info!(
                "Declared exchange '{}' of type '{}'",
                config.exchange, config.exchange_type
            );
        }

        info!("RabbitMQ broker '{}' initialized successfully", name);

        Ok(Self {
            name: name.to_string(),
            config: config.clone(),
            connection,
            channel,
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
        let start_time = Instant::now();
        let mut successful_ids = Vec::new();
        let mut failed_messages = Vec::new();

        debug!(
            broker = %self.name,
            topic = %topic,
            message_count = messages.len(),
            "Sending batch to RabbitMQ"
        );

        for message in messages {
            // Use message key as routing key, or topic as fallback
            let routing_key = message.key.as_deref().unwrap_or(topic);

            // Convert headers to AMQP format
            let amqp_headers = Self::convert_headers(&message.headers);

            // Create basic properties
            let properties = BasicProperties::default()
                .with_delivery_mode(self.config.delivery_mode)
                .with_headers(amqp_headers);

            // Publish message
            let result = self
                .channel
                .basic_publish(
                    &self.config.exchange,
                    routing_key,
                    BasicPublishOptions::default(),
                    &message.payload,
                    properties,
                )
                .await;

            match result {
                Ok(confirmation) => {
                    // Wait for confirmation
                    match confirmation.await {
                        Ok(_) => {
                            debug!(
                                broker = %self.name,
                                msg_id = message.id,
                                routing_key = %routing_key,
                                "Message published successfully"
                            );
                            successful_ids.push(message.id);
                        }
                        Err(e) => {
                            error!(
                                broker = %self.name,
                                msg_id = message.id,
                                error = %e,
                                "Failed to confirm message"
                            );
                            failed_messages
                                .push((message.id, format!("Confirmation failed: {}", e)));
                        }
                    }
                }
                Err(e) => {
                    error!(
                        broker = %self.name,
                        msg_id = message.id,
                        error = %e,
                        "Failed to publish message"
                    );
                    failed_messages.push((message.id, format!("Publish failed: {}", e)));
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
        // Check if connection is still open
        if self.connection.status().connected() {
            Ok(())
        } else {
            Err(RelayError::BrokerHealthCheck(
                "RabbitMQ connection is not connected".to_string(),
            ))
        }
    }
}
