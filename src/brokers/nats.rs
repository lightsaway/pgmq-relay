use async_nats::{Client, ConnectOptions, HeaderMap};
use async_trait::async_trait;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

use crate::broker::{MessageBroker, RelayMessage, SendResult};
use crate::error::RelayError;
use crate::validator::Validator;
use serde::{Deserialize, Serialize};

const NATS_READY_TIMEOUT: Duration = Duration::from_secs(10);

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

    #[serde(default = "default_jetstream_enabled")]
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

// Unlimited: once max_reconnects is exhausted the async-nats client closes permanently
// and every publish fails until process restart, so a bounded default turns any outage
// longer than the reconnect budget into a wedged worker. Health checks report the
// disconnected state in the meantime.
fn default_max_reconnects() -> usize {
    0 // 0 = reconnect forever
}

fn default_reconnect_delay_ms() -> u64 {
    5000 // 5 seconds
}

// JetStream publishes get a durable server acknowledgement before a message is deleted
// from PGMQ. Core NATS is fire-and-forget (flush only proves the server received the
// bytes), so making it the default would silently violate at-least-once delivery.
fn default_jetstream_enabled() -> bool {
    true
}

fn reconnect_delay(base_delay_ms: u64, attempts: usize) -> Duration {
    let exponent = attempts.saturating_sub(1).min(5) as u32;
    let multiplier = 2u64.saturating_pow(exponent);
    Duration::from_millis(base_delay_ms.saturating_mul(multiplier))
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
            jetstream_enabled: default_jetstream_enabled(),
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

        if !self.jetstream_enabled {
            warn!(
                "NATS jetstream_enabled=false: core NATS publishes carry no server \
                 acknowledgement, so messages can be silently lost (no subscriber, server \
                 restart). Only use this if downstream loss is acceptable."
            );
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
        info!(
            "Creating NATS broker '{}' with URL: {}",
            name,
            crate::logging::redact_url(&config.url)
        );

        let servers: Vec<String> = config
            .url
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        let max_reconnects = if config.max_reconnects == 0 {
            None
        } else {
            Some(config.max_reconnects)
        };
        let mut connect_opts = ConnectOptions::new()
            .name(&config.client_name)
            .max_reconnects(max_reconnects);

        let reconnect_delay_ms = config.reconnect_delay_ms;
        connect_opts = connect_opts.reconnect_delay_callback(move |attempts| {
            reconnect_delay(reconnect_delay_ms, attempts)
        });

        if let Some(ref token) = config.token {
            connect_opts = connect_opts.token(token.clone());
        } else if let (Some(ref username), Some(ref password)) =
            (&config.username, &config.password)
        {
            connect_opts = connect_opts.user_and_password(username.clone(), password.clone());
        }

        let client = tokio::time::timeout(
            NATS_READY_TIMEOUT,
            async_nats::connect_with_options(servers, connect_opts),
        )
        .await
        .map_err(|_| {
            RelayError::BrokerConfiguration(format!(
                "Timed out after {}s connecting to NATS broker '{}'",
                NATS_READY_TIMEOUT.as_secs(),
                name
            ))
        })?
        .map_err(|e| {
            RelayError::BrokerConfiguration(format!("Failed to connect to NATS: {}", e))
        })?;

        tokio::time::timeout(NATS_READY_TIMEOUT, client.flush())
            .await
            .map_err(|_| {
                RelayError::BrokerConfiguration(format!(
                    "Timed out after {}s waiting for NATS broker '{}' to become ready",
                    NATS_READY_TIMEOUT.as_secs(),
                    name
                ))
            })?
            .map_err(|e| {
                RelayError::BrokerConfiguration(format!(
                    "NATS broker '{}' readiness check failed: {}",
                    name, e
                ))
            })?;

        info!(
            "Connected to NATS server(s): {}",
            crate::logging::redact_url(&config.url)
        );

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

        // For core NATS, publish() only buffers to the local connection; it returns no
        // server acknowledgement. We must flush to push the buffer to the server (a
        // PING/PONG round-trip) before we can treat the messages as delivered - otherwise
        // a connection drop would lose messages we already deleted from PGMQ. JetStream
        // already waited for per-message acks above, so it does not need this.
        if self.jetstream.is_none() && !successful_ids.is_empty() {
            if let Err(e) = self.client.flush().await {
                error!(
                    broker = %self.name,
                    subject = %topic,
                    error = %e,
                    "Core NATS flush failed - treating all buffered messages as failed"
                );
                // We cannot prove any buffered message reached the server: fail them all
                // so they are retried rather than silently dropped.
                for id in successful_ids.drain(..) {
                    failed_messages.push((id, format!("NATS flush failed: {}", e)));
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
        use async_nats::connection::State;

        // Check the current connection state, then actively flush as a liveness probe
        // (round-trips to the server). async-nats reconnects in the background, so a
        // transient Disconnected is not necessarily fatal, but a failed flush is.
        match self.client.connection_state() {
            State::Connected => {}
            State::Pending => {
                return Err(RelayError::BrokerHealthCheck(format!(
                    "NATS broker '{}' connection is pending (not yet connected)",
                    self.name
                )));
            }
            State::Disconnected => {
                return Err(RelayError::BrokerHealthCheck(format!(
                    "NATS broker '{}' is disconnected",
                    self.name
                )));
            }
        }

        self.client.flush().await.map_err(|e| {
            RelayError::BrokerHealthCheck(format!(
                "NATS broker '{}' flush/ping failed: {}",
                self.name, e
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_favor_durable_delivery() {
        let config = NatsConfig::default();
        // JetStream by default: core NATS has no server acknowledgement, so it must be
        // an explicit opt-in rather than the silent default.
        assert!(config.jetstream_enabled);
        // Reconnect forever: a bounded budget permanently kills the client after a long
        // outage, requiring a process restart.
        assert_eq!(config.max_reconnects, 0);
    }

    #[test]
    fn jetstream_can_be_explicitly_disabled() {
        let config: NatsConfig =
            toml::from_str("url = \"nats://localhost:4222\"\njetstream_enabled = false")
                .expect("config should parse");
        assert!(!config.jetstream_enabled);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn reconnect_backoff_starts_at_base_delay_and_is_capped() {
        assert_eq!(reconnect_delay(5_000, 0), Duration::from_secs(5));
        assert_eq!(reconnect_delay(5_000, 1), Duration::from_secs(5));
        assert_eq!(reconnect_delay(5_000, 2), Duration::from_secs(10));
        assert_eq!(reconnect_delay(5_000, 6), Duration::from_secs(160));
        assert_eq!(reconnect_delay(5_000, 100), Duration::from_secs(160));
    }
}
