# Configuration Reference

## Top level

| Field | Required | Default | Notes |
|---|---:|---|---|
| `default_broker` | Conditional | none | Required unless every queue sets `broker_name` |
| `broker_name` | No | none | Deprecated legacy fallback |

## `[pgmq]`

| Field | Required | Default | Validation |
|---|---:|---|---|
| `connection_url` | Yes* | none | `postgres://` or `postgresql://`; *may be omitted when `PGMQ_RELAY_CONNECTION_URL` is set (the environment always wins) |
| `max_connections` | No | `10` | 1-100 |

## `[metrics]`

| Field | Default | Validation |
|---|---|---|
| `enabled` | `true` | Boolean |
| `bind_address` | `0.0.0.0` | Non-empty |
| `port` | `9090` | 1024-65535 |

## `[[queues]]`

| Field | Default | Validation / behavior |
|---|---|---|
| `queue_name` | required | Alphanumeric, `_`, and `-`; unique |
| `destination_topic` | queue name | Topic, subject, or fallback routing key |
| `broker_name` | `default_broker` | Must reference a configured broker |
| `fetch_mode` | `regular` | See [Fetch Modes](../fetch-modes.md) |
| `max_poll_seconds` | `5` | 1-60 for polling modes |
| `poll_interval_ms` | `100` | 1-1000 for polling modes |
| `key_field` | `message_id` | Header first, then body; dot paths supported; broker-specific fallback when absent |
| `archive_messages` | `false` | Archive instead of delete |
| `dead_letter_queue` | none | Existing PGMQ queue (verified at startup); required when `fetch_mode = "pop"` |
| `parallelism` | `1` | At least 1 |
| `poll_interval` | `250ms` | 10ms-30s |
| `batch_size` | `10` | 1-1000 |
| `visibility_timeout_seconds` | `30` | 1-3600 |

## Broker fields

See the broker-specific references:

- [Kafka](../brokers/kafka.md)
- [RabbitMQ](../brokers/rabbitmq.md)
- [NATS](../brokers/nats.md)

## CLI

```text
pgmq-relay [OPTIONS]

--config <PATH>              Configuration file; default config.toml
--shutdown-timeout <SECONDS> Graceful shutdown deadline; default 30
--validate-config            Validate and exit
```
