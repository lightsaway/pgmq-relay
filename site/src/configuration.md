# Configuration

The relay is configured with TOML. By default it reads `config.toml`; pass `--config` to use another path.

```toml
default_broker = "my_kafka"

[pgmq]
connection_url = "postgres://postgres:postgres@localhost:5432/postgres"
max_connections = 10

[metrics]
enabled = true
bind_address = "0.0.0.0"
port = 9090

[brokers.my_kafka]
type = "kafka"
bootstrap_servers = "localhost:9092"

[[queues]]
queue_name = "my_queue"
destination_topic = "my_topic"
fetch_mode = "regular"
key_field = "x-pgmq-group"
parallelism = 1
batch_size = 100
poll_interval = "250ms"
visibility_timeout_seconds = 30
archive_messages = false
dead_letter_queue = "my_queue_dlq"
```

## Top-Level Keys

| Key | Description |
|---|---|
| `default_broker` | Broker used by queues that do not set `broker_name`. |
| `broker_name` | Deprecated legacy fallback. Prefer `default_broker`. |

## PGMQ

| Key | Description |
|---|---|
| `connection_url` | PostgreSQL connection string. Must start with `postgres://` or `postgresql://`. |
| `max_connections` | Postgres connection pool size. Range: 1-100. |

## Queues

| Key | Description |
|---|---|
| `queue_name` | PGMQ queue to consume. |
| `destination_topic` | Broker topic, subject, or routing key. Defaults to `queue_name`. |
| `fetch_mode` | PGMQ read mode. Defaults to `regular`. |
| `parallelism` | Number of workers for this queue. |
| `batch_size` | Number of messages per poll. Range: 1-1000. |
| `visibility_timeout_seconds` | PGMQ visibility timeout. |
| `archive_messages` | Archive instead of delete successfully processed messages. |
| `dead_letter_queue` | Optional PGMQ queue for transformation failures. |
| `broker_name` | Optional per-queue broker override. |

## Fetch Modes

Supported values:

- `regular`
- `read_with_poll`
- `read_grouped`
- `read_grouped_with_poll`
- `read_grouped_rr`
- `read_grouped_rr_with_poll`
- `read_grouped_head`
- `read_grouped_head_with_poll`
- `pop`

`pop` removes messages from PGMQ before broker delivery. Use it only when message loss is acceptable.
