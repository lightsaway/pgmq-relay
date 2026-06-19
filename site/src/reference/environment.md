# Environment Variables

Environment variables provide defaults only when the corresponding TOML field is omitted.

## Process

| Variable | Default | Purpose |
|---|---|---|
| `RUST_LOG` | `pgmq_relay=info` | Tracing filter |
| `LOG_FORMAT` | `text` | `text` or `json` |

## PGMQ and queues

| Variable | Default | TOML field |
|---|---:|---|
| `PGMQ_RELAY_DEFAULT_MAX_CONNECTIONS` | `10` | `pgmq.max_connections` |
| `PGMQ_RELAY_DEFAULT_PARALLELISM` | `1` | `queues.parallelism` |
| `PGMQ_RELAY_DEFAULT_BATCH_SIZE` | `10` | `queues.batch_size` |
| `PGMQ_RELAY_DEFAULT_POLL_INTERVAL` | `250ms` | `queues.poll_interval` |
| `PGMQ_RELAY_DEFAULT_VISIBILITY_TIMEOUT` | `30` | `queues.visibility_timeout_seconds` |
| `PGMQ_RELAY_DEFAULT_MAX_POLL_SECONDS` | `5` | `queues.max_poll_seconds` |
| `PGMQ_RELAY_DEFAULT_POLL_INTERVAL_MS` | `100` | `queues.poll_interval_ms` |

## Kafka

| Variable | TOML field |
|---|---|
| `PGMQ_RELAY_KAFKA_BOOTSTRAP_SERVERS` | `bootstrap_servers` |
| `PGMQ_RELAY_KAFKA_SECURITY_PROTOCOL` | `security_protocol` |
| `PGMQ_RELAY_KAFKA_SASL_MECHANISM` | `sasl_mechanism` |
| `PGMQ_RELAY_KAFKA_SASL_USERNAME` | `sasl_username` |
| `PGMQ_RELAY_KAFKA_SASL_PASSWORD` | `sasl_password` |
| `PGMQ_RELAY_KAFKA_SSL_CA_LOCATION` | `ssl_ca_location` |
| `PGMQ_RELAY_KAFKA_SSL_CERT_LOCATION` | `ssl_certificate_location` |
| `PGMQ_RELAY_KAFKA_SSL_KEY_LOCATION` | `ssl_key_location` |

## RabbitMQ and NATS

| Variable | TOML field |
|---|---|
| `PGMQ_RELAY_RABBITMQ_URL` | RabbitMQ `url` |
| `PGMQ_RELAY_RABBITMQ_ACK_TIMEOUT_MS` | RabbitMQ `ack_timeout_ms` |
| `PGMQ_RELAY_NATS_URL` | NATS `url` |
| `PGMQ_RELAY_NATS_USERNAME` | NATS `username` |
| `PGMQ_RELAY_NATS_PASSWORD` | NATS `password` |
| `PGMQ_RELAY_NATS_TOKEN` | NATS `token` |

## PGMQ completion resilience

| Variable | Default |
|---|---:|
| `PGMQ_RELAY_CIRCUIT_BREAKER_FAILURE_THRESHOLD` | `5` |
| `PGMQ_RELAY_CIRCUIT_BREAKER_RECOVERY_TIMEOUT` | `30s` |
| `PGMQ_RELAY_CIRCUIT_BREAKER_INITIAL_DELAY` | `100ms` |
| `PGMQ_RELAY_CIRCUIT_BREAKER_MAX_DELAY` | `10s` |
| `PGMQ_RELAY_CIRCUIT_BREAKER_MAX_RETRIES` | `5` |
| `PGMQ_RELAY_CIRCUIT_BREAKER_MULTIPLIER` | `2.0` |
| `PGMQ_RELAY_CIRCUIT_BREAKER_JITTER` | `0.1` |
