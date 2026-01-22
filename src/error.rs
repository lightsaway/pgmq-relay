use thiserror::Error;

#[derive(Error, Debug)]
pub enum RelayError {
    #[error("PGMQ connection error: {0}")]
    PgmqConnection(String),

    #[error("PGMQ operation error: {0}")]
    PgmqOperation(String),

    #[error("Broker configuration error: {0}")]
    BrokerConfiguration(String),

    #[error("Broker send error: {0}")]
    BrokerSend(String),

    #[error("Broker health check failed: {0}")]
    BrokerHealthCheck(String),

    #[error("Message transformation error: {0}")]
    MessageTransformation(String),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("TOML parsing error: {0}")]
    TomlParsing(#[from] toml::de::Error),

    #[error("Task join error: {0}")]
    TaskJoin(#[from] tokio::task::JoinError),

    #[error("Shutdown timeout")]
    ShutdownTimeout,

    #[error("Database error: {0}")]
    DatabaseError(#[from] sqlx::Error),

    #[error("Circuit breaker open: {0}")]
    CircuitBreakerOpen(String),

    #[error("PGMQ client not ready")]
    PgmqClientNotReady,
}

impl RelayError {
    /// Get the error type string for metrics
    pub fn error_type(&self) -> &str {
        match self {
            RelayError::PgmqConnection(_) => "pgmq_connection",
            RelayError::PgmqOperation(_) => "pgmq_operation",
            RelayError::BrokerConfiguration(_) => "broker_configuration",
            RelayError::BrokerSend(_) => "broker_send",
            RelayError::BrokerHealthCheck(_) => "broker_health_check",
            RelayError::MessageTransformation(_) => "message_transformation",
            RelayError::Configuration(_) => "configuration",
            RelayError::Io(_) => "io",
            RelayError::Serialization(_) => "serialization",
            RelayError::TomlParsing(_) => "toml_parsing",
            RelayError::TaskJoin(_) => "task_join",
            RelayError::ShutdownTimeout => "shutdown_timeout",
            RelayError::DatabaseError(_) => "database",
            RelayError::CircuitBreakerOpen(_) => "circuit_breaker_open",
            RelayError::PgmqClientNotReady => "client_not_ready",
        }
    }
}
