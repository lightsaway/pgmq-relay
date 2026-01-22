use async_trait::async_trait;
use std::collections::HashMap;
use tracing::info;

use crate::config::BrokerConfig;
use crate::error::RelayError;

#[derive(Debug, Clone)]
pub struct RelayMessage {
    pub id: i64,
    pub payload: Vec<u8>,
    pub headers: HashMap<String, String>,
    pub key: Option<String>,
}

#[derive(Debug)]
pub struct SendResult {
    pub successful_message_ids: Vec<i64>,
    pub failed_messages: Vec<(i64, String)>, // message_id, error
}

#[async_trait]
pub trait MessageBroker: Send + Sync {
    async fn send_batch(
        &self,
        topic: &str,
        messages: &[RelayMessage],
    ) -> Result<SendResult, RelayError>;

    async fn health_check(&self) -> Result<(), RelayError>;
}

pub async fn create_broker(
    name: &str,
    config: &BrokerConfig,
) -> Result<Box<dyn MessageBroker>, RelayError> {
    match config {
        BrokerConfig::Kafka(kafka_config) => {
            info!("Creating Kafka broker connection: {}", name);
            let broker = crate::brokers::kafka::KafkaBroker::new(name, kafka_config)?;
            Ok(Box::new(broker))
        }
        BrokerConfig::Nats(nats_config) => {
            info!("Creating NATS broker connection: {}", name);
            let broker = crate::brokers::nats::NatsBroker::new(name, nats_config).await?;
            Ok(Box::new(broker))
        }
        BrokerConfig::RabbitMQ(rabbitmq_config) => {
            info!("Creating RabbitMQ broker connection: {}", name);
            let broker =
                crate::brokers::rabbitmq::RabbitMQBroker::new(name, rabbitmq_config).await?;
            Ok(Box::new(broker))
        }
    }
}
