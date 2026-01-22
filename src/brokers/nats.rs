use async_nats::{Client, ConnectOptions, HeaderMap};
use async_trait::async_trait;
use std::collections::HashMap;
use std::time::Instant;
use tracing::{debug, error, info, warn};

use crate::broker::{MessageBroker, RelayMessage, SendResult};
use crate::error::RelayError;
use crate::validator::Validator;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NatsConfig {
    #[serde(default = "default_nats_url")]
    pub url: String,

    #[serde(default)]
    pub username: Option<String>,

    #[serde(default)]
    pub password: Option<String>,

    #[serde(default)]
    pub token: Option<String>,

    #[serde(default = "default_client_name")]
    pub client_name: String,

    #[serde(default = "default_max_reconnects")]
    pub max_reconnects: usize,

    #[serde(default = "default_reconnect_delay_ms")]
    pub reconnect_delay_ms: u64,

    #[serde(default)]
    pub jetstream_enabled: bool,

    #[serde(default)]
    pub jetstream_domain: Option<String>,

    /// Whether to append message key as subject suffix (topic.key)
    /// If false, uses topic as-is regardless of message key
    #[serde(default)]
    pub use_key_as_subject_suffix: bool,
}

fn default_nats_url() -> String {
    std::env::var("PGMQ_RELAY_NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string())
}

fn default_client_name() -> String {
    "pgmq-relay".to_string()
}

fn default_max_reconnects() -> usize {
    60 // Retry for ~5 minutes with default delay
}

fn default_reconnect_delay_ms() -> u64 {
    5000 // 5 seconds
}

impl Default for NatsConfig {
    fn default() -> Self {
        Self {
            url: default_nats_url(),
            username: std::env::var("PGMQ_RELAY_NATS_USERNAME").ok(),
            password: std::env::var("PGMQ_RELAY_NATS_PASSWORD").ok(),
            token: std::env::var("PGMQ_RELAY_NATS_TOKEN").ok(),
            client_name: default_client_name(),
            max_reconnects: default_max_reconnects(),
            reconnect_delay_ms: default_reconnect_delay_ms(),
            jetstream_enabled: false,
            jetstream_domain: None,
            use_key_as_subject_suffix: false,
        }
    }
}

impl Validator for NatsConfig {
    fn validate(&self) -> Result<(), String> {
        if self.url.trim().is_empty() {
            return Err("NATS URL cannot be empty".to_string());
        }

        if self.reconnect_delay_ms == 0 {
            return Err("reconnect_delay_ms must be greater than 0".to_string());
        }

        if self.client_name.trim().is_empty() {
            return Err("client_name cannot be empty".to_string());
        }

        Ok(())
    }
}

pub struct NatsBroker {
    name: String,
    config: NatsConfig,
    client: Client,
    jetstream: Option<async_nats::jetstream::Context>,
}

impl NatsBroker {
    /// Create a new NATS broker
    pub async fn new(name: &str, config: &NatsConfig) -> Result<Self, RelayError> {
        info!("Creating NATS broker '{}' with URL: {}", name, config.url);

        let servers: Vec<String> = config
            .url
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        let mut connect_opts = ConnectOptions::new()
            .name(&config.client_name)
            .retry_on_initial_connect();

        let reconnect_delay_ms = config.reconnect_delay_ms;
        let max_reconnects = config.max_reconnects;
        connect_opts = connect_opts.reconnect_delay_callback(move |attempts| {
            // If max_reconnects is set (> 0) and we've exceeded it, use a very long delay
            // to effectively stop reconnection attempts
            if max_reconnects > 0 && attempts >= max_reconnects {
                std::time::Duration::from_secs(86400) // 24 hours - effectively infinite
            } else {
                // Simple exponential backoff with base delay
                let backoff_multiplier = 2u64.saturating_pow((attempts as u32).min(5));
                std::time::Duration::from_millis(reconnect_delay_ms * backoff_multiplier)
            }
        });

        if let Some(ref token) = config.token {
            connect_opts = connect_opts.token(token.clone());
        } else if let (Some(ref username), Some(ref password)) =
            (&config.username, &config.password)
        {
            connect_opts = connect_opts.user_and_password(username.clone(), password.clone());
        }

        // Connect to NATS - use the first URL for simplicity
        // async-nats will handle clustering through the server list from the first connection
        let client = async_nats::connect_with_options(&servers[0], connect_opts)
            .await
            .map_err(|e| {
                RelayError::BrokerConfiguration(format!("Failed to connect to NATS: {}", e))
            })?;

        info!("Connected to NATS server(s): {}", config.url);

        let jetstream = if config.jetstream_enabled {
            let js_context = if let Some(ref domain) = config.jetstream_domain {
                async_nats::jetstream::with_domain(client.clone(), domain)
            } else {
                async_nats::jetstream::new(client.clone())
            };

            info!("JetStream enabled for broker '{}'", name);
            Some(js_context)
        } else {
            None
        };

        info!("NATS broker '{}' initialized successfully", name);

        Ok(Self {
            name: name.to_string(),
            config: config.clone(),
            client,
            jetstream,
        })
    }

    fn convert_headers(headers: &HashMap<String, String>) -> HeaderMap {
        let mut header_map = HeaderMap::new();
        for (key, value) in headers {
            header_map.insert(key.as_str(), value.as_str());
        }
        header_map
    }
}

#[async_trait]
impl MessageBroker for NatsBroker {
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
            subject = %topic,
            message_count = messages.len(),
            jetstream = self.jetstream.is_some(),
            "Sending batch to NATS"
        );

        for message in messages {
            let subject = if self.config.use_key_as_subject_suffix {
                if let Some(ref key) = message.key {
                    format!("{}.{}", topic, key)
                } else {
                    topic.to_string()
                }
            } else {
                topic.to_string()
            };

            let headers = if !message.headers.is_empty() {
                Some(Self::convert_headers(&message.headers))
            } else {
                None
            };

            let result: Result<(), String> = if let Some(ref js) = self.jetstream {
                let payload = message.payload.clone().into();

                let ack_future = if let Some(h) = headers {
                    js.publish_with_headers(subject.clone(), h, payload).await
                } else {
                    js.publish(subject.clone(), payload).await
                };

                match ack_future {
                    Ok(ack_fut) => match ack_fut.await {
                        Ok(_) => {
                            debug!(
                                broker = %self.name,
                                msg_id = message.id,
                                subject = %subject,
                                "JetStream message acknowledged"
                            );
                            Ok(())
                        }
                        Err(e) => Err(format!("JetStream ack failed: {}", e)),
                    },
                    Err(e) => Err(format!("JetStream publish failed: {}", e)),
                }
            } else {
                let publish_result = if let Some(h) = headers {
                    self.client
                        .publish_with_headers(subject.clone(), h, message.payload.clone().into())
                        .await
                } else {
                    self.client
                        .publish(subject.clone(), message.payload.clone().into())
                        .await
                };

                publish_result
                    .map(|_| {
                        debug!(
                            broker = %self.name,
                            msg_id = message.id,
                            subject = %subject,
                            "Core NATS message published"
                        );
                    })
                    .map_err(|e| format!("Core NATS publish failed: {}", e))
            };

            match result {
                Ok(_) => {
                    successful_ids.push(message.id);
                }
                Err(e) => {
                    error!(
                        broker = %self.name,
                        msg_id = message.id,
                        subject = %subject,
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
                subject = %topic,
                successful = successful_ids.len(),
                failed = failed_messages.len(),
                "Batch send completed with failures"
            );
        } else {
            info!(
                broker = %self.name,
                subject = %topic,
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
        // figure out proper way to healthcheck
        Ok(())
    }
}
