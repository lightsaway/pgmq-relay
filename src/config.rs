use humantime_serde::re::humantime;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, time::Duration};
use thiserror::Error;

use crate::validator::Validator;

#[derive(Error, Debug)]
pub enum ConfigValidationError {
    #[error("No queues configured")]
    NoQueues,

    #[error("No brokers configured")]
    NoBrokers,

    #[error("Queue '{queue_name}' has invalid parallelism: {parallelism} (must be >= 1)")]
    InvalidParallelism {
        queue_name: String,
        parallelism: usize,
    },

    #[error("Queue '{queue_name}' references unknown broker '{broker_name}'")]
    UnknownBroker {
        queue_name: String,
        broker_name: String,
    },

    #[error("Queue '{queue_name}' has no broker configured and no default broker available")]
    NoBrokerAvailable { queue_name: String },

    #[error("Queue '{queue_name}' has invalid queue name (must not be empty and contain only valid characters)")]
    InvalidQueueName { queue_name: String },

    #[error(
        "Queue '{queue_name}' has invalid batch_size: {batch_size} (must be between 1 and 1000)"
    )]
    InvalidBatchSize { queue_name: String, batch_size: i32 },

    #[error("Queue '{queue_name}' has invalid visibility_timeout: {timeout}s (must be between 1 and 3600 seconds)")]
    InvalidVisibilityTimeout { queue_name: String, timeout: i32 },

    #[error("Queue '{queue_name}' has invalid poll_interval: {interval:?} (must be between 10ms and 30s)")]
    InvalidPollInterval {
        queue_name: String,
        interval: Duration,
    },

    #[error("PGMQ connection URL is invalid or empty")]
    InvalidConnectionUrl,

    #[error("PGMQ max_connections is invalid: {max_connections} (must be between 1 and 100)")]
    InvalidMaxConnections { max_connections: u32 },

    #[error("Broker '{broker_name}' has invalid configuration: {reason}")]
    InvalidBrokerConfig { broker_name: String, reason: String },

    #[error("Duplicate queue name: '{queue_name}'")]
    DuplicateQueueName { queue_name: String },

    #[error("Metrics configuration is invalid: {reason}")]
    InvalidMetricsConfig { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub pgmq: PgmqConfig,
    #[serde(default)]
    pub relay: RelayConfig,
    pub brokers: HashMap<String, BrokerConfig>,
    /// Default broker name for queues that don't specify their own broker
    #[serde(default)]
    pub default_broker: Option<String>,
    /// Legacy field - if default_broker is not set, falls back to this
    #[serde(default)]
    pub broker_name: Option<String>,
    pub queues: Vec<QueueConfig>,
    #[serde(default)]
    pub metrics: MetricsConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgmqConfig {
    #[serde(deserialize_with = "validate_connection_url")]
    pub connection_url: String,
    #[serde(default = "default_max_connections")]
    #[serde(deserialize_with = "validate_max_connections")]
    pub max_connections: u32,
}

fn validate_connection_url<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let url = String::deserialize(deserializer)?;
    if url.trim().is_empty() {
        return Err(serde::de::Error::custom("connection_url cannot be empty"));
    }
    if !url.starts_with("postgres://") && !url.starts_with("postgresql://") {
        return Err(serde::de::Error::custom(
            "connection_url must start with 'postgres://' or 'postgresql://'",
        ));
    }
    Ok(url)
}

fn validate_max_connections<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let max_connections = u32::deserialize(deserializer)?;
    if max_connections == 0 || max_connections > 100 {
        return Err(serde::de::Error::custom(
            "max_connections must be between 1 and 100",
        ));
    }
    Ok(max_connections)
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RelayConfig {
    #[serde(default = "default_poll_interval")]
    #[serde(with = "humantime_serde")]
    pub poll_interval: Duration,

    #[serde(default = "default_poll_timeout")]
    #[serde(with = "humantime_serde")]
    pub poll_timeout: Duration,

    #[serde(default = "default_batch_size")]
    pub batch_size: i32,

    #[serde(default = "default_visibility_timeout")]
    pub visibility_timeout_seconds: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum BrokerConfig {
    #[serde(rename = "kafka")]
    Kafka(crate::brokers::kafka::KafkaConfig),
    #[serde(rename = "nats")]
    Nats(crate::brokers::nats::NatsConfig),
    #[serde(rename = "rabbitmq")]
    RabbitMQ(crate::brokers::rabbitmq::RabbitMQConfig),
}

impl BrokerConfig {
    /// Get the broker type as a string
    pub fn broker_type(&self) -> &str {
        match self {
            BrokerConfig::Kafka(_) => "kafka",
            BrokerConfig::Nats(_) => "nats",
            BrokerConfig::RabbitMQ(_) => "rabbitmq",
        }
    }
}

impl Config {
    /// Get the list of workers to create based on configuration
    /// Creates workers automatically based on queue parallelism settings
    pub fn get_workers(&self) -> Vec<WorkerConfig> {
        let mut workers = Vec::new();

        for queue in &self.queues {
            let effective_broker = queue
                .effective_broker_name(self)
                .expect("No broker configured for queue and no default broker available")
                .to_string();

            for worker_index in 0..queue.parallelism {
                let worker_name = if queue.parallelism == 1 {
                    format!("{}-worker", queue.queue_name)
                } else {
                    format!("{}-worker-{}", queue.queue_name, worker_index + 1)
                };

                workers.push(WorkerConfig {
                    name: worker_name,
                    broker_name: effective_broker.clone(),
                    queue: queue.clone(),
                    poll_interval: queue.poll_interval,
                    batch_size: queue.batch_size,
                    visibility_timeout_seconds: queue.visibility_timeout_seconds,
                });
            }
        }

        workers
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    pub name: String,
    pub broker_name: String,
    pub queue: QueueConfig,

    #[serde(default = "default_poll_interval")]
    #[serde(with = "humantime_serde")]
    pub poll_interval: Duration,

    #[serde(default = "default_batch_size")]
    pub batch_size: i32,

    #[serde(default = "default_visibility_timeout")]
    pub visibility_timeout_seconds: i32,
}

/// Fetch mode configuration - determines how messages are fetched from the queue
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum FetchMode {
    /// Regular polling using pgmq.read() - simple queue polling
    #[serde(rename = "regular")]
    Regular,

    /// Long-polling using pgmq.read_with_poll() - waits for messages if queue is empty
    /// Reduces polling overhead by waiting max_poll_seconds before returning empty
    #[serde(rename = "read_with_poll")]
    ReadWithPoll,

    /// FIFO grouped reading using pgmq.read_grouped() - messages with same group ID processed in order
    /// Fills batch from earliest available group first
    #[serde(rename = "read_grouped")]
    ReadGrouped,

    /// FIFO grouped reading with long-polling using pgmq.read_grouped_with_poll()
    /// Combines grouped FIFO reading with polling wait
    #[serde(rename = "read_grouped_with_poll")]
    ReadGroupedWithPoll,

    /// Round-robin FIFO grouped reading using pgmq.read_grouped_rr()
    /// Distributes messages fairly across different FIFO groups
    #[serde(rename = "read_grouped_rr")]
    ReadGroupedRoundRobin,

    /// Round-robin FIFO grouped reading with long-polling using pgmq.read_grouped_rr_with_poll()
    /// Combines round-robin grouped reading with polling wait
    #[serde(rename = "read_grouped_rr_with_poll")]
    ReadGroupedRoundRobinWithPoll,

    /// Pop messages using pgmq.pop() - reads and deletes in one operation
    /// WARNING: At-most-once delivery semantics - messages deleted immediately
    #[serde(rename = "pop")]
    Pop,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueConfig {
    #[serde(deserialize_with = "validate_queue_name")]
    pub queue_name: String,
    /// Destination topic name. If not provided, defaults to queue_name
    #[serde(default)]
    pub destination_topic: Option<String>,
    pub message_transformation: Option<MessageTransformation>,

    /// Fetch mode configuration. Defaults to Regular if not specified
    #[serde(default = "default_fetch_mode")]
    pub fetch_mode: FetchMode,

    /// Maximum poll duration for polling-based fetch modes (read_with_poll variants)
    /// How long to wait for messages before returning empty. Defaults to 5 seconds.
    #[serde(default = "default_max_poll_seconds")]
    pub max_poll_seconds: i32,

    /// Poll interval in milliseconds for polling-based fetch modes
    /// How frequently to check for new messages during poll wait. Defaults to 100ms.
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: i32,

    /// Field to use as message key for broker ordering
    #[serde(default = "default_key_field")]
    pub key_field: String,

    /// Whether to archive messages instead of deleting them after successful processing
    /// When true, messages are moved to the archive table for long-term retention
    /// When false (default), messages are permanently deleted
    #[serde(default)]
    pub archive_messages: bool,

    /// Number of parallel workers to spawn for this queue (default: 1)
    /// Can be overridden with PGMQ_RELAY_DEFAULT_PARALLELISM env var
    #[serde(default = "default_parallelism")]
    pub parallelism: usize,

    /// Polling interval for this queue
    /// Can be overridden with PGMQ_RELAY_DEFAULT_POLL_INTERVAL env var
    #[serde(default = "default_poll_interval")]
    #[serde(with = "humantime_serde")]
    pub poll_interval: Duration,

    /// Batch size for processing messages from this queue
    /// Can be overridden with PGMQ_RELAY_DEFAULT_BATCH_SIZE env var
    #[serde(default = "default_batch_size")]
    #[serde(deserialize_with = "validate_batch_size")]
    pub batch_size: i32,

    /// Visibility timeout for messages from this queue
    /// Can be overridden with PGMQ_RELAY_DEFAULT_VISIBILITY_TIMEOUT env var
    #[serde(default = "default_visibility_timeout")]
    #[serde(deserialize_with = "validate_visibility_timeout")]
    pub visibility_timeout_seconds: i32,

    /// Broker to use for this specific queue. If not specified, uses the default broker
    #[serde(default)]
    pub broker_name: Option<String>,
}

impl QueueConfig {
    /// Get the effective destination topic name
    /// If destination_topic is not configured, uses queue_name as fallback
    pub fn effective_destination_topic(&self) -> &str {
        self.destination_topic
            .as_ref()
            .map(|s| s.as_str())
            .unwrap_or(&self.queue_name)
    }

    /// Get the effective broker name for this queue
    /// Precedence: queue.broker_name > config.default_broker > config.broker_name
    pub fn effective_broker_name<'a>(&'a self, config: &'a Config) -> Option<&'a str> {
        self.broker_name
            .as_ref()
            .map(|s| s.as_str())
            .or_else(|| config.default_broker.as_ref().map(|s| s.as_str()))
            .or_else(|| config.broker_name.as_ref().map(|s| s.as_str()))
    }
}

fn default_key_field() -> String {
    "message_id".to_string()
}

fn default_fetch_mode() -> FetchMode {
    FetchMode::Regular
}

fn default_max_poll_seconds() -> i32 {
    std::env::var("PGMQ_RELAY_DEFAULT_MAX_POLL_SECONDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5)
}

fn default_poll_interval_ms() -> i32 {
    std::env::var("PGMQ_RELAY_DEFAULT_POLL_INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100)
}

fn validate_queue_name<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let queue_name = String::deserialize(deserializer)?;
    if queue_name.trim().is_empty() {
        return Err(serde::de::Error::custom("queue_name cannot be empty"));
    }
    if !queue_name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
    {
        return Err(serde::de::Error::custom(
            "queue_name must contain only alphanumeric characters, underscores, and hyphens",
        ));
    }
    Ok(queue_name)
}

fn validate_batch_size<'de, D>(deserializer: D) -> Result<i32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let batch_size = i32::deserialize(deserializer)?;
    if batch_size <= 0 || batch_size > 1000 {
        return Err(serde::de::Error::custom(
            "batch_size must be between 1 and 1000",
        ));
    }
    Ok(batch_size)
}

fn validate_visibility_timeout<'de, D>(deserializer: D) -> Result<i32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let timeout = i32::deserialize(deserializer)?;
    if timeout <= 0 || timeout > 3600 {
        return Err(serde::de::Error::custom(
            "visibility_timeout_seconds must be between 1 and 3600 seconds",
        ));
    }
    Ok(timeout)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum MessageTransformation {
    #[serde(rename = "passthrough")]
    Passthrough,
    #[serde(rename = "json_extract")]
    JsonExtract { field: String },
    #[serde(rename = "custom_template")]
    CustomTemplate { template: String },
}

impl Default for MessageTransformation {
    fn default() -> Self {
        MessageTransformation::Passthrough
    }
}

fn default_parallelism() -> usize {
    std::env::var("PGMQ_RELAY_DEFAULT_PARALLELISM")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1)
}

fn default_poll_interval() -> Duration {
    std::env::var("PGMQ_RELAY_DEFAULT_POLL_INTERVAL")
        .ok()
        .and_then(|v| humantime::parse_duration(&v).ok())
        .unwrap_or_else(|| Duration::from_millis(250))
}

fn default_poll_timeout() -> Duration {
    std::env::var("PGMQ_RELAY_DEFAULT_POLL_TIMEOUT")
        .ok()
        .and_then(|v| humantime::parse_duration(&v).ok())
        .unwrap_or_else(|| Duration::from_secs(5))
}

fn default_batch_size() -> i32 {
    std::env::var("PGMQ_RELAY_DEFAULT_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10)
}

fn default_visibility_timeout() -> i32 {
    std::env::var("PGMQ_RELAY_DEFAULT_VISIBILITY_TIMEOUT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30)
}

fn default_max_connections() -> u32 {
    std::env::var("PGMQ_RELAY_DEFAULT_MAX_CONNECTIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    #[serde(default = "default_metrics_enabled")]
    pub enabled: bool,

    #[serde(default = "default_metrics_bind_address")]
    pub bind_address: String,

    #[serde(default = "default_metrics_port")]
    pub port: u16,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: default_metrics_enabled(),
            bind_address: default_metrics_bind_address(),
            port: default_metrics_port(),
        }
    }
}

fn default_metrics_enabled() -> bool {
    true
}

fn default_metrics_bind_address() -> String {
    "0.0.0.0".to_string()
}

fn default_metrics_port() -> u16 {
    9090
}

impl Config {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        config.validate()?;
        Ok(config)
    }

    /// Comprehensive configuration validation with detailed error reporting
    pub fn validate(&self) -> Result<(), ConfigValidationError> {
        // Validate PGMQ configuration
        self.validate_pgmq_config()?;

        // Validate brokers configuration
        self.validate_brokers_config()?;

        // Validate queues configuration
        self.validate_queues_config()?;

        // Validate metrics configuration
        self.validate_metrics_config()?;

        // Cross-validation checks
        self.validate_cross_references()?;

        Ok(())
    }

    fn validate_pgmq_config(&self) -> Result<(), ConfigValidationError> {
        // Check connection URL
        if self.pgmq.connection_url.trim().is_empty() {
            return Err(ConfigValidationError::InvalidConnectionUrl);
        }

        // Basic URL format validation
        if !self.pgmq.connection_url.starts_with("postgres://")
            && !self.pgmq.connection_url.starts_with("postgresql://")
        {
            return Err(ConfigValidationError::InvalidConnectionUrl);
        }

        // Validate max_connections
        if self.pgmq.max_connections == 0 || self.pgmq.max_connections > 100 {
            return Err(ConfigValidationError::InvalidMaxConnections {
                max_connections: self.pgmq.max_connections,
            });
        }

        Ok(())
    }

    fn validate_brokers_config(&self) -> Result<(), ConfigValidationError> {
        // Check that we have at least one broker
        if self.brokers.is_empty() {
            return Err(ConfigValidationError::NoBrokers);
        }

        // Validate each broker configuration
        for (broker_name, broker_config) in &self.brokers {
            self.validate_broker_config(broker_name, broker_config)?;
        }

        Ok(())
    }

    fn validate_broker_config(
        &self,
        broker_name: &str,
        broker_config: &BrokerConfig,
    ) -> Result<(), ConfigValidationError> {
        // Use the Validator trait implementation from each broker config
        let validation_result = match broker_config {
            BrokerConfig::Kafka(kafka_config) => kafka_config.validate(),
            BrokerConfig::RabbitMQ(rabbitmq_config) => rabbitmq_config.validate(),
            BrokerConfig::Nats(nats_config) => nats_config.validate(),
        };

        validation_result.map_err(|reason| ConfigValidationError::InvalidBrokerConfig {
            broker_name: broker_name.to_string(),
            reason,
        })
    }

    fn validate_queues_config(&self) -> Result<(), ConfigValidationError> {
        // Check that we have at least one queue
        if self.queues.is_empty() {
            return Err(ConfigValidationError::NoQueues);
        }

        // Check for duplicate queue names
        let mut queue_names = std::collections::HashSet::new();
        for queue in &self.queues {
            if !queue_names.insert(&queue.queue_name) {
                return Err(ConfigValidationError::DuplicateQueueName {
                    queue_name: queue.queue_name.clone(),
                });
            }
        }

        // Validate each queue
        for queue in &self.queues {
            self.validate_queue_config(queue)?;
        }

        Ok(())
    }

    fn validate_queue_config(&self, queue: &QueueConfig) -> Result<(), ConfigValidationError> {
        // Validate queue name
        if queue.queue_name.trim().is_empty() {
            return Err(ConfigValidationError::InvalidQueueName {
                queue_name: queue.queue_name.clone(),
            });
        }

        // Queue name should contain only alphanumeric characters, underscores, and hyphens
        if !queue
            .queue_name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
        {
            return Err(ConfigValidationError::InvalidQueueName {
                queue_name: queue.queue_name.clone(),
            });
        }

        // Validate parallelism
        if queue.parallelism == 0 {
            return Err(ConfigValidationError::InvalidParallelism {
                queue_name: queue.queue_name.clone(),
                parallelism: queue.parallelism,
            });
        }

        // Validate batch size
        if queue.batch_size <= 0 || queue.batch_size > 1000 {
            return Err(ConfigValidationError::InvalidBatchSize {
                queue_name: queue.queue_name.clone(),
                batch_size: queue.batch_size,
            });
        }

        // Validate visibility timeout
        if queue.visibility_timeout_seconds <= 0 || queue.visibility_timeout_seconds > 3600 {
            return Err(ConfigValidationError::InvalidVisibilityTimeout {
                queue_name: queue.queue_name.clone(),
                timeout: queue.visibility_timeout_seconds,
            });
        }

        // Validate poll interval
        let min_interval = Duration::from_millis(10);
        let max_interval = Duration::from_secs(30);
        if queue.poll_interval < min_interval || queue.poll_interval > max_interval {
            return Err(ConfigValidationError::InvalidPollInterval {
                queue_name: queue.queue_name.clone(),
                interval: queue.poll_interval,
            });
        }

        // Validate key field
        if queue.key_field.trim().is_empty() {
            return Err(ConfigValidationError::InvalidBrokerConfig {
                broker_name: "queue_config".to_string(),
                reason: format!("Queue '{}' has empty key_field", queue.queue_name),
            });
        }

        // Validate polling parameters for polling-based fetch modes
        match queue.fetch_mode {
            FetchMode::ReadWithPoll
            | FetchMode::ReadGroupedWithPoll
            | FetchMode::ReadGroupedRoundRobinWithPoll => {
                // Validate max_poll_seconds (1-60 seconds recommended)
                if queue.max_poll_seconds <= 0 || queue.max_poll_seconds > 60 {
                    return Err(ConfigValidationError::InvalidBrokerConfig {
                        broker_name: "queue_config".to_string(),
                        reason: format!(
                            "Queue '{}' has invalid max_poll_seconds: {} (must be between 1 and 60)",
                            queue.queue_name, queue.max_poll_seconds
                        ),
                    });
                }

                // Validate poll_interval_ms (10-1000ms recommended)
                if queue.poll_interval_ms <= 0 || queue.poll_interval_ms > 1000 {
                    return Err(ConfigValidationError::InvalidBrokerConfig {
                        broker_name: "queue_config".to_string(),
                        reason: format!(
                            "Queue '{}' has invalid poll_interval_ms: {} (must be between 1 and 1000)",
                            queue.queue_name, queue.poll_interval_ms
                        ),
                    });
                }
            }
            _ => {}
        }

        // Warn if archive_messages is set with Pop mode (messages are already deleted)
        if matches!(queue.fetch_mode, FetchMode::Pop) && queue.archive_messages {
            tracing::warn!(
                "Queue '{}' has archive_messages=true with fetch_mode=pop. \
                 Pop mode deletes messages immediately, archiving will have no effect.",
                queue.queue_name
            );
        }

        Ok(())
    }

    fn validate_metrics_config(&self) -> Result<(), ConfigValidationError> {
        // Validate metrics port
        if self.metrics.port == 0 || self.metrics.port < 1024 {
            return Err(ConfigValidationError::InvalidMetricsConfig {
                reason: format!(
                    "Invalid metrics port: {} (must be >= 1024)",
                    self.metrics.port
                ),
            });
        }

        // Validate bind address
        if self.metrics.bind_address.trim().is_empty() {
            return Err(ConfigValidationError::InvalidMetricsConfig {
                reason: "Metrics bind_address cannot be empty".to_string(),
            });
        }

        Ok(())
    }

    fn validate_cross_references(&self) -> Result<(), ConfigValidationError> {
        // Validate that all queues reference existing brokers
        for queue in &self.queues {
            if let Some(broker_name) = queue.effective_broker_name(self) {
                if !self.brokers.contains_key(broker_name) {
                    return Err(ConfigValidationError::UnknownBroker {
                        queue_name: queue.queue_name.clone(),
                        broker_name: broker_name.to_string(),
                    });
                }
            } else {
                return Err(ConfigValidationError::NoBrokerAvailable {
                    queue_name: queue.queue_name.clone(),
                });
            }
        }

        Ok(())
    }

    /// Validate configuration and provide user-friendly error messages
    pub fn validate_with_suggestions(&self) -> Result<(), String> {
        match self.validate() {
            Ok(()) => Ok(()),
            Err(e) => {
                let suggestion = match &e {
                    ConfigValidationError::NoQueues => {
                        "Add at least one queue configuration under [[queues]]"
                    }
                    ConfigValidationError::NoBrokers => {
                        "Add at least one broker configuration under [brokers]"
                    }
                    ConfigValidationError::InvalidConnectionUrl => {
                        "Use format: postgres://username:password@host:port/database"
                    }
                    ConfigValidationError::InvalidMaxConnections { max_connections } => {
                        &format!("Set max_connections between 1-100, got {}", max_connections)
                    }
                    ConfigValidationError::InvalidBatchSize { .. } => {
                        "Set batch_size between 1-1000 for optimal performance"
                    }
                    ConfigValidationError::InvalidPollInterval { .. } => {
                        "Set poll_interval between 10ms-30s (e.g., '250ms', '1s')"
                    }
                    _ => "Check configuration documentation for valid values",
                };

                Err(format!("{}\nSuggestion: {}", e, suggestion))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_minimal_valid_config() -> Config {
        let mut brokers = HashMap::new();
        brokers.insert(
            "test_kafka".to_string(),
            BrokerConfig::Kafka(crate::brokers::kafka::KafkaConfig {
                bootstrap_servers: "localhost:9092".to_string(),
                security_protocol: None,
                sasl_mechanism: None,
                sasl_username: None,
                sasl_password: None,
                ssl_ca_location: None,
                ssl_certificate_location: None,
                ssl_key_location: None,
                transactions: crate::brokers::kafka::KafkaTransactionConfig::default(),
                additional_config: HashMap::new(),
            }),
        );

        Config {
            pgmq: PgmqConfig {
                connection_url: "postgres://user:pass@localhost:5432/db".to_string(),
                max_connections: 10,
            },
            relay: RelayConfig::default(),
            brokers,
            default_broker: Some("test_kafka".to_string()),
            broker_name: None,
            queues: vec![QueueConfig {
                queue_name: "test_queue".to_string(),
                destination_topic: None,
                message_transformation: None,
                fetch_mode: FetchMode::Regular,
                max_poll_seconds: 5,
                poll_interval_ms: 100,
                key_field: "message_id".to_string(),
                archive_messages: false,
                parallelism: 1,
                poll_interval: Duration::from_millis(250),
                batch_size: 10,
                visibility_timeout_seconds: 30,
                broker_name: None,
            }],
            metrics: MetricsConfig::default(),
        }
    }

    #[test]
    fn test_valid_config_passes() {
        let config = create_minimal_valid_config();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_empty_queues_fails() {
        let mut config = create_minimal_valid_config();
        config.queues.clear();

        match config.validate() {
            Err(ConfigValidationError::NoQueues) => (),
            _ => panic!("Expected NoQueues error"),
        }
    }

    #[test]
    fn test_invalid_connection_url_fails() {
        let mut config = create_minimal_valid_config();
        config.pgmq.connection_url = "invalid://url".to_string();

        match config.validate() {
            Err(ConfigValidationError::InvalidConnectionUrl) => (),
            _ => panic!("Expected InvalidConnectionUrl error"),
        }
    }

    #[test]
    fn test_config_with_suggestions() {
        let mut config = create_minimal_valid_config();
        config.queues.clear();

        let result = config.validate_with_suggestions();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("Add at least one queue configuration"));
    }
}
