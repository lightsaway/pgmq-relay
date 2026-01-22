use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use crate::broker::{MessageBroker, RelayMessage, SendResult};
use crate::error::RelayError;
use crate::validator::Validator;
use serde::{Deserialize, Serialize};

/// Kafka broker configuration with environment variable support
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfig {
    #[serde(default = "default_bootstrap_servers")]
    pub bootstrap_servers: String,

    #[serde(default)]
    pub security_protocol: Option<String>,

    #[serde(default)]
    pub sasl_mechanism: Option<String>,

    #[serde(default)]
    pub sasl_username: Option<String>,

    #[serde(default)]
    pub sasl_password: Option<String>,

    #[serde(default)]
    pub ssl_ca_location: Option<String>,

    #[serde(default)]
    pub ssl_certificate_location: Option<String>,

    #[serde(default)]
    pub ssl_key_location: Option<String>,

    /// Transaction configuration for exactly-once delivery guarantees
    #[serde(default)]
    pub transactions: KafkaTransactionConfig,

    #[serde(flatten)]
    pub additional_config: HashMap<String, String>,
}

// Default functions with environment variable support
fn default_bootstrap_servers() -> String {
    std::env::var("PGMQ_RELAY_KAFKA_BOOTSTRAP_SERVERS")
        .unwrap_or_else(|_| "localhost:9092".to_string())
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: default_bootstrap_servers(),
            security_protocol: std::env::var("PGMQ_RELAY_KAFKA_SECURITY_PROTOCOL").ok(),
            sasl_mechanism: std::env::var("PGMQ_RELAY_KAFKA_SASL_MECHANISM").ok(),
            sasl_username: std::env::var("PGMQ_RELAY_KAFKA_SASL_USERNAME").ok(),
            sasl_password: std::env::var("PGMQ_RELAY_KAFKA_SASL_PASSWORD").ok(),
            ssl_ca_location: std::env::var("PGMQ_RELAY_KAFKA_SSL_CA_LOCATION").ok(),
            ssl_certificate_location: std::env::var("PGMQ_RELAY_KAFKA_SSL_CERT_LOCATION").ok(),
            ssl_key_location: std::env::var("PGMQ_RELAY_KAFKA_SSL_KEY_LOCATION").ok(),
            transactions: KafkaTransactionConfig::default(),
            additional_config: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum KafkaTransactionConfig {
    /// Disable transactions - higher throughput, at-least-once delivery
    Disabled(bool), // false

    /// Enable transactions with configuration
    Enabled {
        /// Transactional ID generation strategy
        #[serde(default)]
        strategy: TransactionalIdStrategy,

        /// Transaction timeout in milliseconds (default: 60000ms)
        #[serde(default = "default_transaction_timeout")]
        timeout_ms: u32,

        /// Maximum number of retries for transactional operations
        #[serde(default = "default_transaction_retries")]
        retries: u32,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TransactionalIdStrategy {
    /// Use hostname prefix: "hostname-uuid" or "custom-hostname-uuid"
    #[serde(rename = "hostname")]
    Hostname {
        /// Optional custom prefix instead of "pgmq-relay"
        prefix: Option<String>,
    },

    /// Use static ID: exactly as provided (must be unique across instances!)
    #[serde(rename = "static")]
    Static {
        /// Static transactional ID (must be unique!)
        id: String,
    },

    /// Use pure random UUID: "prefix-uuid" or "pgmq-relay-uuid"
    #[serde(rename = "random")]
    Random {
        /// Optional custom prefix instead of "pgmq-relay"
        prefix: Option<String>,
    },
}

impl Default for TransactionalIdStrategy {
    fn default() -> Self {
        TransactionalIdStrategy::Hostname { prefix: None }
    }
}

impl Default for KafkaTransactionConfig {
    fn default() -> Self {
        KafkaTransactionConfig::Enabled {
            strategy: TransactionalIdStrategy::default(),
            timeout_ms: default_transaction_timeout(),
            retries: default_transaction_retries(),
        }
    }
}

impl KafkaTransactionConfig {
    pub fn is_enabled(&self) -> bool {
        matches!(self, KafkaTransactionConfig::Enabled { .. })
    }

    pub fn get_strategy(&self) -> Option<&TransactionalIdStrategy> {
        match self {
            KafkaTransactionConfig::Enabled { strategy, .. } => Some(strategy),
            KafkaTransactionConfig::Disabled(_) => None,
        }
    }

    pub fn get_timeout_ms(&self) -> u32 {
        match self {
            KafkaTransactionConfig::Enabled { timeout_ms, .. } => *timeout_ms,
            KafkaTransactionConfig::Disabled(_) => 0,
        }
    }

    pub fn get_retries(&self) -> u32 {
        match self {
            KafkaTransactionConfig::Enabled { retries, .. } => *retries,
            KafkaTransactionConfig::Disabled(_) => 0,
        }
    }
}

impl TransactionalIdStrategy {
    /// Generate a unique transactional ID based on the strategy
    pub fn generate_id(&self) -> String {
        match self {
            TransactionalIdStrategy::Hostname { prefix } => {
                let hostname = get_hostname();
                let base = prefix.as_deref().unwrap_or("pgmq-relay");
                format!("{}-{}-{}", base, hostname, uuid::Uuid::new_v4())
            }
            TransactionalIdStrategy::Static { id } => id.clone(),
            TransactionalIdStrategy::Random { prefix } => {
                let base = prefix.as_deref().unwrap_or("pgmq-relay");
                format!("{}-{}", base, uuid::Uuid::new_v4())
            }
        }
    }
}

/// Get hostname for transaction ID naming
fn get_hostname() -> String {
    // Try various hostname sources in order of preference
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("POD_NAME"))
        .or_else(|_| std::env::var("CONTAINER_NAME"))
        .unwrap_or_else(|_| {
            // Fallback to short UUID if all else fails
            uuid::Uuid::new_v4().to_string()[..8].to_string()
        })
        // Clean hostname for safe usage in transaction IDs
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string()
}

fn default_transaction_timeout() -> u32 {
    60000 // 60 seconds
}

fn default_transaction_retries() -> u32 {
    3
}

impl Validator for KafkaConfig {
    fn validate(&self) -> Result<(), String> {
        // Validate bootstrap servers
        if self.bootstrap_servers.trim().is_empty() {
            return Err("bootstrap_servers cannot be empty".to_string());
        }

        // Validate transaction configuration
        if self.transactions.is_enabled() {
            let timeout = self.transactions.get_timeout_ms();
            if timeout < 1000 || timeout > 300000 {
                // 1s to 5min
                return Err(format!(
                    "transaction timeout {}ms out of range (1000-300000)",
                    timeout
                ));
            }
        }

        Ok(())
    }
}

pub struct KafkaBroker {
    producer: FutureProducer,
    supports_transactions: bool,
}

impl KafkaBroker {
    /// Execute a function within a Kafka transaction
    /// If transactions aren't supported, just executes the function
    async fn with_transaction<F, Fut, T>(&self, f: F) -> Result<T, RelayError>
    where
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future<Output = Result<T, RelayError>> + Send,
    {
        if !self.supports_transactions {
            tracing::trace!("Transactions not supported, executing operation without transaction");
            return f().await;
        }

        // Start transaction
        tracing::trace!("Beginning Kafka transaction");
        self.producer
            .begin_transaction()
            .map_err(|e| RelayError::BrokerSend(format!("Failed to begin transaction: {}", e)))?;

        // Execute the operation
        let result = f().await;

        // Commit or abort based on result
        match result {
            Ok(value) => {
                tracing::trace!("Committing Kafka transaction");
                self.producer
                    .commit_transaction(Timeout::After(Duration::from_secs(30)))
                    .map_err(|e| {
                        error!("Failed to commit transaction: {}", e);
                        RelayError::BrokerSend(format!("Failed to commit transaction: {}", e))
                    })?;
                info!("Kafka transaction committed successfully");
                Ok(value)
            }
            Err(e) => {
                warn!("Operation failed, aborting Kafka transaction: {}", e);
                if let Err(abort_err) = self
                    .producer
                    .abort_transaction(Timeout::After(Duration::from_secs(30)))
                {
                    error!("Failed to abort Kafka transaction: {}", abort_err);
                }
                Err(e)
            }
        }
    }

    pub fn new(_name: &str, config: &KafkaConfig) -> Result<Self, RelayError> {
        let (producer, supports_transactions) = {
            let mut client_config = ClientConfig::new();

            client_config.set("bootstrap.servers", &config.bootstrap_servers);
            client_config.set("message.timeout.ms", "30000");
            client_config.set("queue.buffering.max.messages", "100000");
            client_config.set("queue.buffering.max.kbytes", "1048576");
            client_config.set("batch.num.messages", "1000");

            // Configure transactions if enabled
            let supports_transactions = config.transactions.is_enabled();
            if supports_transactions {
                // Generate transactional ID using the configured strategy
                let unique_tx_id = if let Some(strategy) = config.transactions.get_strategy() {
                    strategy.generate_id()
                } else {
                    // Fallback (should not happen due to default)
                    format!("pgmq-relay-{}", uuid::Uuid::new_v4())
                };

                client_config.set("transactional.id", &unique_tx_id);
                client_config.set(
                    "transaction.timeout.ms",
                    &config.transactions.get_timeout_ms().to_string(),
                );
                client_config.set("retries", &config.transactions.get_retries().to_string());
                client_config.set("enable.idempotence", "true");
                client_config.set("max.in.flight.requests.per.connection", "5");
                client_config.set("acks", "all");

                info!(
                    "Kafka transactions enabled with transactional.id: {}",
                    unique_tx_id
                );
            } else {
                info!("Kafka transactions disabled - using at-least-once delivery");
            }

            if let Some(ref protocol) = config.security_protocol {
                client_config.set("security.protocol", protocol);
            }

            if let Some(ref mechanism) = config.sasl_mechanism {
                client_config.set("sasl.mechanism", mechanism);
            }

            if let Some(ref username) = config.sasl_username {
                client_config.set("sasl.username", username);
            }

            if let Some(ref password) = config.sasl_password {
                client_config.set("sasl.password", password);
            }

            if let Some(ref ca_location) = config.ssl_ca_location {
                client_config.set("ssl.ca.location", ca_location);
            }

            if let Some(ref cert_location) = config.ssl_certificate_location {
                client_config.set("ssl.certificate.location", cert_location);
            }

            if let Some(ref key_location) = config.ssl_key_location {
                client_config.set("ssl.key.location", key_location);
            }

            for (key, value) in &config.additional_config {
                client_config.set(key, value);
            }

            let producer: FutureProducer = client_config
                .create()
                .map_err(|e| RelayError::BrokerConfiguration(e.to_string()))?;

            // Initialize transactions if enabled
            if supports_transactions {
                info!("Initializing Kafka transactions...");
                producer
                    .init_transactions(Timeout::After(Duration::from_secs(30)))
                    .map_err(|e| {
                        RelayError::BrokerConfiguration(format!(
                            "Failed to initialize Kafka transactions: {}",
                            e
                        ))
                    })?;
                info!("Kafka transactions initialized successfully");
            }

            (producer, supports_transactions)
        };

        Ok(Self {
            producer,
            supports_transactions,
        })
    }
}

#[async_trait]
impl MessageBroker for KafkaBroker {
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

        let message_count = messages.len();
        info!("Sending {} messages to topic '{}'", message_count, topic);

        // Use the transaction wrapper for clean transaction management
        let result = self
            .with_transaction(|| {
                let messages_clone = messages.to_vec();
                let topic_clone = topic.to_string();
                async move {
                    let mut successful_message_ids = Vec::new();
                    let mut failed_messages = Vec::new();

                    // Pre-collect all keys to avoid lifetime issues
                    let message_keys: Vec<String> = messages_clone
                        .iter()
                        .map(|message| {
                            message
                                .key
                                .clone()
                                .unwrap_or_else(|| message.id.to_string())
                        })
                        .collect();

                    let mut futures = Vec::new();

                    for (i, message) in messages_clone.iter().enumerate() {
                        // Build headers correctly by accumulating all headers into one OwnedHeaders instance
                        let headers = message.headers.iter().fold(
                            OwnedHeaders::new(),
                            |acc, (key, value)| {
                                acc.insert(Header {
                                    key,
                                    value: Some(value),
                                })
                            },
                        );

                        let record = FutureRecord::to(&topic_clone)
                            .payload(&message.payload)
                            .key(&message_keys[i])
                            .headers(headers);

                        tracing::trace!(
                            "Sending message {} to topic '{}' with {} headers: {:?}",
                            message.id,
                            topic_clone,
                            message.headers.len(),
                            message.headers.keys().collect::<Vec<_>>()
                        );

                        let future = self
                            .producer
                            .send(record, Timeout::After(Duration::from_secs(30)));

                        futures.push((message.id, future));
                    }

                    let mut failed_message_ids = Vec::new();
                    let mut success_count = 0;

                    for (msg_id, future) in futures {
                        match future.await {
                            Ok((_partition, _offset)) => {
                                successful_message_ids.push(msg_id);
                                success_count += 1;
                            }
                            Err((kafka_error, _)) => {
                                error!(
                                    "Failed to send message {} to topic '{}': {}",
                                    msg_id, topic_clone, kafka_error
                                );
                                failed_messages.push((msg_id, kafka_error.to_string()));
                                failed_message_ids.push(msg_id);
                            }
                        }
                    }

                    if !failed_message_ids.is_empty() {
                        // Return error to trigger transaction rollback
                        return Err(RelayError::BrokerSend(format!(
                            "Failed to send {} out of {} messages to topic '{}'",
                            failed_message_ids.len(),
                            messages_clone.len(),
                            topic_clone
                        )));
                    }

                    info!(
                        "Successfully sent {} messages to topic '{}'",
                        success_count, topic_clone
                    );

                    // If we get here, all messages succeeded
                    Ok(SendResult {
                        successful_message_ids,
                        failed_messages: Vec::new(),
                    })
                }
            })
            .await;

        match result {
            Ok(send_result) => {
                info!(
                    "Successfully sent {} messages to topic '{}'",
                    message_count, topic
                );
                Ok(send_result)
            }
            Err(e) => {
                warn!("Failed to send messages to topic '{}': {}", topic, e);

                // When transaction fails, ALL messages are considered failed
                let all_failed: Vec<(i64, String)> = messages
                    .iter()
                    .map(|m| (m.id, "Transaction failed".to_string()))
                    .collect();

                Ok(SendResult {
                    successful_message_ids: Vec::new(),
                    failed_messages: all_failed,
                })
            }
        }
    }

    async fn health_check(&self) -> Result<(), RelayError> {
        let metadata_result = self
            .producer
            .client()
            .fetch_metadata(None, Timeout::After(Duration::from_secs(10)));

        match metadata_result {
            Ok(metadata) => {
                if metadata.brokers().is_empty() {
                    return Err(RelayError::BrokerHealthCheck(
                        "No Kafka brokers available".to_string(),
                    ));
                }

                debug!(
                    "Kafka health check passed. {} brokers available",
                    metadata.brokers().len()
                );
                Ok(())
            }
            Err(e) => Err(RelayError::BrokerHealthCheck(format!(
                "Kafka metadata fetch failed: {}",
                e
            ))),
        }
    }
}
