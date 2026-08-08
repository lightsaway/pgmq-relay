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

#[derive(Debug, Clone)]
pub enum KafkaTransactionConfig {
    /// Disable transactions - higher throughput, at-least-once delivery
    Disabled,

    /// Enable transactions with configuration
    Enabled {
        /// Transactional ID generation strategy
        strategy: TransactionalIdStrategy,

        /// Transaction timeout in milliseconds (default: 60000ms)
        timeout_ms: u32,

        /// Retained for backward compatibility; no longer mapped to librdkafka's
        /// `retries` (idempotent producers manage send retries themselves, bounded
        /// by `message.timeout.ms`).
        retries: u32,
    },
}

/// Accepts either a bare boolean (`transactions = true` / `false`) or a settings table.
/// A hand-written impl because `#[serde(untagged)]` with a `Disabled(bool)` variant
/// listed first captured *every* boolean — including `true` — silently disabling
/// transactions for operators who wrote `transactions = true` to enable them.
impl<'de> Deserialize<'de> for KafkaTransactionConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum RawTransactionConfig {
            Flag(bool),
            Settings {
                #[serde(default)]
                strategy: TransactionalIdStrategy,
                #[serde(default = "default_transaction_timeout")]
                timeout_ms: u32,
                #[serde(default = "default_transaction_retries")]
                retries: u32,
            },
        }

        Ok(match RawTransactionConfig::deserialize(deserializer)? {
            RawTransactionConfig::Flag(false) => KafkaTransactionConfig::Disabled,
            RawTransactionConfig::Flag(true) => KafkaTransactionConfig::default(),
            RawTransactionConfig::Settings {
                strategy,
                timeout_ms,
                retries,
            } => KafkaTransactionConfig::Enabled {
                strategy,
                timeout_ms,
                retries,
            },
        })
    }
}

impl Serialize for KafkaTransactionConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;

        match self {
            KafkaTransactionConfig::Disabled => serializer.serialize_bool(false),
            KafkaTransactionConfig::Enabled {
                strategy,
                timeout_ms,
                retries,
            } => {
                let mut state = serializer.serialize_struct("KafkaTransactionConfig", 3)?;
                state.serialize_field("strategy", strategy)?;
                state.serialize_field("timeout_ms", timeout_ms)?;
                state.serialize_field("retries", retries)?;
                state.end()
            }
        }
    }
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
            KafkaTransactionConfig::Disabled => None,
        }
    }

    pub fn get_timeout_ms(&self) -> u32 {
        match self {
            KafkaTransactionConfig::Enabled { timeout_ms, .. } => *timeout_ms,
            KafkaTransactionConfig::Disabled => 0,
        }
    }
}

impl TransactionalIdStrategy {
    /// Generate a transactional ID based on the strategy.
    ///
    /// `instance_id` is a stable, per-worker identifier (the worker name) used to keep
    /// IDs both stable across restarts (required for Kafka zombie fencing and
    /// transaction recovery) and unique across workers sharing the same broker config.
    pub fn generate_id(&self, instance_id: &str) -> String {
        let safe_instance = sanitize_for_tx_id(instance_id);
        match self {
            // Stable across restarts: "{prefix}-{hostname}-{instance}" with NO random
            // component, so a restarted relay reuses the same transactional.id and Kafka
            // can fence zombies and recover in-flight transactions.
            TransactionalIdStrategy::Hostname { prefix } => {
                let hostname = get_hostname();
                let base = prefix.as_deref().unwrap_or("pgmq-relay");
                format!("{}-{}-{}", base, hostname, safe_instance)
            }
            // Static base, made unique per worker so parallelism > 1 does not cause
            // workers to fence each other while remaining stable across restarts.
            TransactionalIdStrategy::Static { id } => format!("{}-{}", id, safe_instance),
            // Explicitly opt-in unstable: random UUID each startup (no fencing).
            TransactionalIdStrategy::Random { prefix } => {
                let base = prefix.as_deref().unwrap_or("pgmq-relay");
                format!("{}-{}", base, uuid::Uuid::new_v4())
            }
        }
    }
}

/// Sanitize an identifier so it is safe to embed in a Kafka transactional.id.
fn sanitize_for_tx_id(value: &str) -> String {
    let cleaned: String = value
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect();
    cleaned.trim_matches('-').to_string()
}

/// Get hostname for transaction ID naming
fn get_hostname() -> String {
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
        if self.bootstrap_servers.trim().is_empty() {
            return Err("bootstrap_servers cannot be empty".to_string());
        }

        if self.transactions.is_enabled() {
            let timeout = self.transactions.get_timeout_ms();
            if !(1000..=300000).contains(&timeout) {
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
    /// Guarded so the producer can be rebuilt in place after a fatal error (e.g. this
    /// producer was fenced by another instance with the same transactional.id). Reads
    /// clone the producer (cheap: it is an Arc internally) and never hold the lock
    /// across a send.
    producer: tokio::sync::RwLock<FutureProducer>,
    supports_transactions: bool,
    config: KafkaConfig,
    instance_id: String,
}

const KAFKA_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

/// True when the error means the current transaction must be aborted but the producer
/// itself is still usable for the next transaction.
fn requires_transaction_abort(error: &rdkafka::error::KafkaError) -> bool {
    matches!(error, rdkafka::error::KafkaError::Transaction(e) if e.txn_requires_abort())
}

/// Build (and for transactional configs, initialize) a producer. Blocking: call via
/// `spawn_blocking` — `init_transactions` can stall for up to its full timeout.
fn build_producer(config: &KafkaConfig, instance_id: &str) -> Result<FutureProducer, RelayError> {
    let mut client_config = ClientConfig::new();

    client_config.set("bootstrap.servers", &config.bootstrap_servers);
    client_config.set("message.timeout.ms", "30000");
    client_config.set("queue.buffering.max.messages", "100000");
    client_config.set("queue.buffering.max.kbytes", "1048576");
    client_config.set("batch.num.messages", "1000");

    if config.transactions.is_enabled() {
        let unique_tx_id = if let Some(strategy) = config.transactions.get_strategy() {
            strategy.generate_id(instance_id)
        } else {
            // Fallback (should not happen due to default)
            format!("pgmq-relay-{}", sanitize_for_tx_id(instance_id))
        };

        client_config.set("transactional.id", &unique_tx_id);
        client_config.set(
            "transaction.timeout.ms",
            config.transactions.get_timeout_ms().to_string(),
        );
        // Deliberately not setting `retries`: for an idempotent producer librdkafka's
        // default is effectively unlimited, bounded by message.timeout.ms. Pinning it
        // low would fail whole transactional batches on routine leader elections.
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

    if config.transactions.is_enabled() {
        info!("Initializing Kafka transactions...");
        producer
            .init_transactions(Timeout::After(KAFKA_OPERATION_TIMEOUT))
            .map_err(|e| {
                RelayError::BrokerConfiguration(format!(
                    "Failed to initialize Kafka transactions: {}",
                    e
                ))
            })?;
        info!("Kafka transactions initialized successfully");
    }

    Ok(producer)
}

impl KafkaBroker {
    pub async fn new(
        _name: &str,
        config: &KafkaConfig,
        instance_id: &str,
    ) -> Result<Self, RelayError> {
        let supports_transactions = config.transactions.is_enabled();
        let producer =
            Self::build_producer_blocking(config.clone(), instance_id.to_string()).await?;

        Ok(Self {
            producer: tokio::sync::RwLock::new(producer),
            supports_transactions,
            config: config.clone(),
            instance_id: instance_id.to_string(),
        })
    }

    /// Run `build_producer` on the blocking thread pool so `init_transactions` cannot
    /// stall an async executor thread.
    async fn build_producer_blocking(
        config: KafkaConfig,
        instance_id: String,
    ) -> Result<FutureProducer, RelayError> {
        tokio::task::spawn_blocking(move || build_producer(&config, &instance_id))
            .await
            .map_err(|e| {
                RelayError::BrokerConfiguration(format!("Kafka producer build task failed: {}", e))
            })?
    }

    /// Replace a fatally-broken producer with a fresh one. Without this, a fenced
    /// producer would fail every batch until process restart while health checks
    /// (metadata fetch) kept passing.
    async fn rebuild_producer(&self) -> Result<(), RelayError> {
        warn!("Rebuilding Kafka producer after fatal error");
        let new_producer =
            Self::build_producer_blocking(self.config.clone(), self.instance_id.clone()).await?;
        *self.producer.write().await = new_producer;
        info!("Kafka producer rebuilt successfully");
        Ok(())
    }

    async fn current_producer(&self) -> FutureProducer {
        self.producer.read().await.clone()
    }

    /// Commit the in-flight transaction on the blocking pool (librdkafka blocks for up
    /// to the timeout).
    async fn commit_transaction_blocking(
        producer: &FutureProducer,
    ) -> Result<(), rdkafka::error::KafkaError> {
        let producer = producer.clone();
        let handle = tokio::task::spawn_blocking(move || {
            producer.commit_transaction(Timeout::After(KAFKA_OPERATION_TIMEOUT))
        });
        match handle.await {
            Ok(result) => result,
            Err(join_error) => {
                error!("Kafka commit task failed to run: {}", join_error);
                Err(rdkafka::error::KafkaError::Canceled)
            }
        }
    }

    async fn abort_transaction_blocking(
        producer: &FutureProducer,
    ) -> Result<(), rdkafka::error::KafkaError> {
        let producer = producer.clone();
        let handle = tokio::task::spawn_blocking(move || {
            producer.abort_transaction(Timeout::After(KAFKA_OPERATION_TIMEOUT))
        });
        match handle.await {
            Ok(result) => result,
            Err(join_error) => {
                error!("Kafka abort task failed to run: {}", join_error);
                Err(rdkafka::error::KafkaError::Canceled)
            }
        }
    }

    /// Execute a function within a Kafka transaction
    /// If transactions aren't supported, just executes the function
    async fn with_transaction<F, Fut, T>(
        &self,
        producer: &FutureProducer,
        f: F,
    ) -> Result<T, RelayError>
    where
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future<Output = Result<T, RelayError>> + Send,
    {
        if !self.supports_transactions {
            tracing::trace!("Transactions not supported, executing operation without transaction");
            return f().await;
        }

        tracing::trace!("Beginning Kafka transaction");
        producer
            .begin_transaction()
            .map_err(|e| RelayError::BrokerSend(format!("Failed to begin transaction: {}", e)))?;

        let result = f().await;

        match result {
            Ok(value) => {
                tracing::trace!("Committing Kafka transaction");
                match Self::commit_transaction_blocking(producer).await {
                    Ok(()) => {
                        debug!("Kafka transaction committed successfully");
                        Ok(value)
                    }
                    Err(commit_error) if requires_transaction_abort(&commit_error) => {
                        warn!(
                            "Kafka commit failed with abortable error, aborting transaction: {}",
                            commit_error
                        );
                        if let Err(abort_error) = Self::abort_transaction_blocking(producer).await {
                            error!("Failed to abort Kafka transaction: {}", abort_error);
                        }
                        Err(RelayError::BrokerSend(format!(
                            "Failed to commit transaction: {}",
                            commit_error
                        )))
                    }
                    Err(commit_error) => {
                        error!("Failed to commit transaction: {}", commit_error);
                        Err(RelayError::BrokerSend(format!(
                            "Failed to commit transaction: {}",
                            commit_error
                        )))
                    }
                }
            }
            Err(e) => {
                warn!("Operation failed, aborting Kafka transaction: {}", e);
                if let Err(abort_error) = Self::abort_transaction_blocking(producer).await {
                    error!("Failed to abort Kafka transaction: {}", abort_error);
                }
                Err(e)
            }
        }
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
        debug!("Sending {} messages to topic '{}'", message_count, topic);

        let producer = self.current_producer().await;
        let send_producer = producer.clone();
        let result = self
            .with_transaction(&producer, move || {
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

                        let future =
                            send_producer.send(record, Timeout::After(Duration::from_secs(30)));

                        futures.push((message.id, future));
                    }

                    let mut failed_message_ids = Vec::new();
                    let mut success_count = 0;

                    for (msg_id, future) in futures {
                        match future.await {
                            Ok(_delivery) => {
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
                        if !self.supports_transactions {
                            return Ok(SendResult {
                                successful_message_ids,
                                failed_messages,
                            });
                        }

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

                // A fatal producer error (e.g. fenced by another instance with the same
                // transactional.id) breaks every future send on this producer. Rebuild it
                // now; if that fails, propagate the error so the worker surfaces a real
                // failure instead of silently reporting failed batches forever.
                if producer.client().fatal_error().is_some() {
                    error!(
                        "Kafka producer hit a fatal error - rebuilding producer for topic '{}'",
                        topic
                    );
                    self.rebuild_producer().await?;
                }

                let all_failed: Vec<(i64, String)> = messages
                    .iter()
                    .map(|m| (m.id, format!("Transaction failed: {}", e)))
                    .collect();

                Ok(SendResult {
                    successful_message_ids: Vec::new(),
                    failed_messages: all_failed,
                })
            }
        }
    }

    async fn health_check(&self) -> Result<(), RelayError> {
        let producer = self.current_producer().await;

        // A producer with a fatal error can still fetch metadata, so check it explicitly
        // or a fenced producer would report healthy while failing every send.
        if let Some((error_code, reason)) = producer.client().fatal_error() {
            return Err(RelayError::BrokerHealthCheck(format!(
                "Kafka producer has a fatal error ({:?}): {}",
                error_code, reason
            )));
        }

        // fetch_metadata is a blocking librdkafka call (up to its full timeout when the
        // cluster is unreachable), so keep it off the async executor threads.
        let handle = tokio::task::spawn_blocking(move || {
            producer
                .client()
                .fetch_metadata(None, Timeout::After(Duration::from_secs(10)))
                .map(|metadata| metadata.brokers().len())
        });
        let metadata_result = handle.await.map_err(|e| {
            RelayError::BrokerHealthCheck(format!("Kafka health check task failed: {}", e))
        })?;

        match metadata_result {
            Ok(broker_count) => {
                if broker_count == 0 {
                    return Err(RelayError::BrokerHealthCheck(
                        "No Kafka brokers available".to_string(),
                    ));
                }

                debug!(
                    "Kafka health check passed. {} brokers available",
                    broker_count
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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_transactions(toml_snippet: &str) -> KafkaTransactionConfig {
        #[derive(Deserialize)]
        struct Wrapper {
            transactions: KafkaTransactionConfig,
        }
        let wrapper: Wrapper = toml::from_str(toml_snippet).expect("config should parse");
        wrapper.transactions
    }

    #[test]
    fn transactions_true_enables_transactions() {
        // Regression test: the previous #[serde(untagged)] enum parsed `true` into its
        // Disabled(bool) variant, silently turning transactions OFF.
        let config = parse_transactions("transactions = true");
        assert!(config.is_enabled());
        assert_eq!(config.get_timeout_ms(), default_transaction_timeout());
    }

    #[test]
    fn transactions_false_disables_transactions() {
        let config = parse_transactions("transactions = false");
        assert!(!config.is_enabled());
    }

    #[test]
    fn transactions_table_enables_with_settings() {
        let config = parse_transactions("transactions = { timeout_ms = 30000 }");
        assert!(config.is_enabled());
        assert_eq!(config.get_timeout_ms(), 30000);
    }

    #[test]
    fn transactions_default_is_enabled() {
        assert!(KafkaTransactionConfig::default().is_enabled());
    }

    #[test]
    fn disabled_transactions_serialize_back_to_false() {
        let json =
            serde_json::to_value(KafkaTransactionConfig::Disabled).expect("should serialize");
        assert_eq!(json, serde_json::Value::Bool(false));
    }

    #[test]
    fn transactional_ids_are_stable_and_unique_per_worker() {
        let strategy = TransactionalIdStrategy::Static {
            id: "relay".to_string(),
        };
        assert_eq!(
            strategy.generate_id("outbox-worker-1"),
            "relay-outbox-worker-1"
        );
        assert_ne!(
            strategy.generate_id("outbox-worker-1"),
            strategy.generate_id("outbox-worker-2")
        );
    }
}
