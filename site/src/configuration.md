# Configuration Overview

The relay reads TOML from `config.toml` by default:

```bash
pgmq-relay --config /etc/pgmq-relay/config.toml
```

Validate syntax and cross-references without connecting to PostgreSQL or brokers:

```bash
pgmq-relay --config config.toml --validate-config
```

## Minimal configuration

```toml
default_broker = "kafka"

[pgmq]
connection_url = "postgres://relay:secret@postgres:5432/app"
max_connections = 10

[metrics]
enabled = true
bind_address = "0.0.0.0"
port = 9090

[brokers.kafka]
type = "kafka"
bootstrap_servers = "kafka:9092"

[[queues]]
queue_name = "outbox"
destination_topic = "events"
key_field = "event_id"
batch_size = 100
visibility_timeout_seconds = 30
```

Top-level keys such as `default_broker` must appear before TOML table headers.

## Queue routing

Every queue selects `broker_name`, then falls back to `default_broker`.

```toml
default_broker = "kafka"

[[queues]]
queue_name = "events"

[[queues]]
queue_name = "notifications"
broker_name = "rabbit"
```

`destination_topic` means:

- Kafka topic
- NATS subject
- RabbitMQ fallback routing key when no message key exists

RabbitMQ normally uses the extracted message key as its routing key.

When the configured field is absent, Kafka falls back to the PGMQ message ID, RabbitMQ falls back to `destination_topic`, and NATS keeps the base subject.

## Visibility timeout

The visibility timeout must cover the longest expected transformation, publish, acknowledgement, and PGMQ completion time. With parallel workers, an expired timeout can expose the same row to another worker.

For RabbitMQ, keep `visibility_timeout_seconds * 1000` greater than `ack_timeout_ms`. The relay warns when this is not true.

## Configuration sources

Most settings are TOML-only. Environment variables provide defaults for selected fields when the TOML value is omitted. See [Environment Variables](./reference/environment.md) and the [complete reference](./reference/configuration.md).
