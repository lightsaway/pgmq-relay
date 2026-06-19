# PGMQ Relay

A Rust-based message relay service that reads from PGMQ (Postgres Message Queue) topics via polling and forwards messages to configurable message brokers like Kafka.

## Features

- **Multi-Queue Workers**: Each worker polls a single queue; configurable parallelism runs multiple workers per queue concurrently
- **At-Least-Once Delivery**: Messages are only deleted/archived from PGMQ *after* the broker confirms receipt (publisher confirms on RabbitMQ, server flush on core NATS, JetStream/Kafka acks). On a crash between broker-ack and PGMQ-delete, messages are redelivered — **downstream consumers must be idempotent / deduplicate.** See [Delivery Semantics](#delivery-semantics).
- **Poison-Message Handling**: A message that fails transformation no longer blocks its queue; with a `dead_letter_queue` configured it is routed there, otherwise it is retried after the visibility timeout while the rest of the batch proceeds
- **Multiple Fetch Modes**: Support for PGMQ 1.11.1 read methods used by the relay (regular, long-polling, FIFO grouped, grouped-head, round-robin, pop)
- **FIFO Support**: Proper FIFO queue polling with configurable batch sizes
- **Multiple Brokers**: Abstracted broker interface supporting Kafka, RabbitMQ, and NATS
- **Message Transformation**: Support for passthrough, JSON field extraction, and custom templates
- **Header Forwarding**: Preserves PGMQ headers and adds relay metadata to Kafka messages
- **Prometheus Metrics**: Built-in metrics endpoint for monitoring and alerting

## Architecture

The relay derives a set of **workers** from your configured **queues** (one queue can fan out to `parallelism` workers). Each worker owns a dedicated broker connection, polls its queue, transforms each message, sends the batch to its broker, and only then removes the delivered messages from PGMQ.

```mermaid
flowchart LR
    subgraph PG["PostgreSQL + PGMQ"]
        QA[("queue: orders")]
        QB[("queue: events")]
        DLQ[("orders_dlq")]
    end

    subgraph RELAY["pgmq-relay process"]
        direction TB
        WA1["worker orders-1"]
        WA2["worker orders-2"]
        WB1["worker events-1"]
        OBS["HTTP: /metrics, /health, /ready"]
    end

    subgraph BR["Message Brokers"]
        K{{"Kafka"}}
        R{{"RabbitMQ"}}
        N{{"NATS"}}
    end

    QA -- "read batch (vt)" --> WA1
    QA -- "read batch (vt)" --> WA2
    QB -- "read batch (vt)" --> WB1

    WA1 -- "transform + send" --> K
    WA2 -- "transform + send" --> K
    WB1 -- "transform + send" --> R
    WB1 -. "or" .-> N

    WA1 -- "delete/archive after ack" --> QA
    WA1 -. "poison message" .-> DLQ
```

### Per-batch message flow (at-least-once)

Messages are removed from PGMQ **only after** the broker acknowledges them, which is what makes delivery at-least-once. See [Delivery Semantics](#delivery-semantics) for the full guarantees.

```mermaid
sequenceDiagram
    autonumber
    participant Q as PGMQ
    participant W as Worker
    participant B as Broker

    loop every poll_interval
        W->>Q: read(batch_size, visibility_timeout)
        Q-->>W: messages (now invisible to others)
        Note over W: transform each message
        alt transformation fails
            W->>Q: send to dead_letter_queue (if configured)
        end
        W->>B: send_batch(messages) and await ack/confirm
        B-->>W: ack / publisher-confirm / JetStream ack
        W->>Q: delete or archive (batch) delivered + dead-lettered msgs
        Note over Q,W: crash before this step → redelivery after vt
    end
```

## Quick Start with Docker Compose

The fastest way to try out pgmq-relay is using the included Docker Compose setup, which provides a complete working example with PGMQ, multiple message brokers, and the relay service pre-configured.

### What's Included

The `docker-compose.yml` provides a full-stack example environment:

- **PostgreSQL with PGMQ 1.11.1** - Message queue database
- **Redpanda** - Kafka-compatible broker with web console
- **RabbitMQ** - AMQP broker with management UI
- **NATS** - Lightweight pub/sub broker with JetStream
- **PGMQ Relay** - The relay service configured with all three brokers
- **Test Message Sender** (optional) - Generates realistic test messages

### Start the Stack

```bash
# Start all services
docker-compose up -d

# Start with test message generator (recommended for trying it out)
docker-compose --profile testing up -d

# Watch the relay process messages
docker-compose logs -f pgmq-relay

# View test messages being sent
docker-compose logs -f test-message-sender
```

### Access the UIs and Metrics

- **Redpanda Console**: http://localhost:8080 - View Kafka topics and messages
- **RabbitMQ Management**: http://localhost:15672 - Login: admin/admin
- **NATS UI**: http://localhost:31311 - Modern NATS management interface
  - **First time setup**: Click "+" to add connection with URL: `nats://nats:4222`
  - Connection will be saved in a persistent volume
- **NATS Monitoring**: http://localhost:8222 - NATS server metrics (JSON)
- **Relay Metrics**: http://localhost:9090/metrics - Prometheus metrics

### Example Configuration

The docker setup demonstrates:
- **3 queues** routing to different brokers (Kafka, RabbitMQ, NATS)
- **Different fetch modes** (regular, long-polling, FIFO grouped)
- **Parallel workers** for high-throughput queues
- **Message archiving** vs deletion strategies
- **Realistic test data** flowing through the system

See `docker/config-docker.toml` for the complete configuration example.

## Configuration

The relay is configured via a single TOML file (default path: `config.toml`; override with `--config`). A complete, commented example lives at [`config.toml`](config.toml), and the Docker example at [`docker/config-docker.toml`](docker/config-docker.toml).

### Configuration Validation

The relay validates the entire configuration on startup and fails fast with a descriptive error. You can also validate without starting the service (no database or broker connection is made):

```bash
pgmq-relay --config config.toml --validate-config
# or, from source:
cargo run -- --config config.toml --validate-config
```

### Basic Structure

A configuration has four parts: `[pgmq]`, `[metrics]`, one or more `[brokers.<name>]`, and one or more `[[queues]]`. **There is no separate `[relay]` or `[[workers]]` section** — workers are derived automatically from each queue's `parallelism`.

```toml
# Top-level keys must come BEFORE any [table] header.
# Default broker used by queues that don't set their own `broker_name`.
default_broker = "my_kafka"

# --- Database ----------------------------------------------------------------
[pgmq]
connection_url = "postgres://postgres:postgres@localhost:5432/postgres"
max_connections = 10

# --- Observability -----------------------------------------------------------
[metrics]
enabled = true
bind_address = "0.0.0.0"
port = 9090

# --- Brokers (define one or more, each under [brokers.<name>]) ----------------
[brokers.my_kafka]
type = "kafka"
bootstrap_servers = "localhost:9092"

# --- Queues (each [[queues]] becomes one or more workers) --------------------
[[queues]]
queue_name = "my_queue"           # PGMQ queue to read from (required)
destination_topic = "my_topic"    # broker topic/subject (default: queue_name)
fetch_mode = "regular"
key_field = "x-pgmq-group"        # field used as the broker partition/routing key
parallelism = 1                   # number of concurrent workers for this queue
batch_size = 100
poll_interval = "250ms"
visibility_timeout_seconds = 30
archive_messages = false          # archive instead of delete on success
dead_letter_queue = "my_queue_dlq" # optional; route poison messages here
# broker_name = "my_kafka"        # optional per-queue override of default_broker
```

### Configuration Reference

#### Top level

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `default_broker` | string | — | Broker name used by any queue that does not set its own `broker_name`. Required unless every queue sets `broker_name`. |
| `broker_name` | string | — | **Deprecated** legacy fallback for `default_broker`. Prefer `default_broker`. |

#### `[pgmq]`

| Key | Type | Default | Range / Notes |
|-----|------|---------|---------------|
| `connection_url` | string | *(required)* | Must start with `postgres://` or `postgresql://`. |
| `max_connections` | int | `10` | 1–100. Postgres connection-pool size. Env: `PGMQ_RELAY_DEFAULT_MAX_CONNECTIONS`. |

#### `[metrics]`

| Key | Type | Default | Notes |
|-----|------|---------|-------|
| `enabled` | bool | `true` | When true, serves `/metrics`, `/health`, `/ready`. |
| `bind_address` | string | `"0.0.0.0"` | Listen address. |
| `port` | int | `9090` | Must be ≥ 1024. |

#### `[[queues]]`

Each queue spawns `parallelism` workers, each with its own broker connection.

| Key | Type | Default | Range / Notes |
|-----|------|---------|---------------|
| `queue_name` | string | *(required)* | Alphanumeric, `_`, `-` only. The PGMQ queue to consume. |
| `destination_topic` | string | `queue_name` | Kafka topic / NATS subject / RabbitMQ routing key. |
| `fetch_mode` | enum | `"regular"` | See [Fetch Modes](#pgmq-fetch-modes-pgmq-1111). |
| `key_field` | string | `"message_id"` | Field (header first, then body, dot-path for nesting) used as the broker key. Falls back to the PGMQ message id. |
| `parallelism` | int | `1` | ≥ 1. Concurrent workers for this queue. Use `1` for strict ordering. Env: `PGMQ_RELAY_DEFAULT_PARALLELISM`. |
| `batch_size` | int | `10` | 1–1000. Messages read per poll. Env: `PGMQ_RELAY_DEFAULT_BATCH_SIZE`. |
| `poll_interval` | duration | `"250ms"` | 10ms–30s. Idle wait between polls (e.g. `"1s"`, `"500ms"`). Env: `PGMQ_RELAY_DEFAULT_POLL_INTERVAL`. |
| `visibility_timeout_seconds` | int | `30` | 1–3600. How long a read message stays invisible. Env: `PGMQ_RELAY_DEFAULT_VISIBILITY_TIMEOUT`. |
| `max_poll_seconds` | int | `5` | 1–60. Long-poll wait (polling fetch modes only). Env: `PGMQ_RELAY_DEFAULT_MAX_POLL_SECONDS`. |
| `poll_interval_ms` | int | `100` | 1–1000. Server-side check frequency during a long poll. Env: `PGMQ_RELAY_DEFAULT_POLL_INTERVAL_MS`. |
| `archive_messages` | bool | `false` | Archive to the PGMQ archive table instead of deleting on success. |
| `dead_letter_queue` | string | — | PGMQ queue to route poison (un-transformable) messages to. See [Dead-Letter Queue](#dead-letter-queue-poison-messages). |
| `broker_name` | string | `default_broker` | Per-queue broker override. |
| `[queues.message_transformation]` | table | passthrough | See [Message Transformations](#message-transformations). |

#### Environment-variable overrides

Any value sourced via these variables is used only when the corresponding TOML field is omitted:

| Variable | Overrides |
|----------|-----------|
| `PGMQ_RELAY_DEFAULT_PARALLELISM` | queue `parallelism` |
| `PGMQ_RELAY_DEFAULT_BATCH_SIZE` | queue `batch_size` |
| `PGMQ_RELAY_DEFAULT_POLL_INTERVAL` | queue `poll_interval` |
| `PGMQ_RELAY_DEFAULT_VISIBILITY_TIMEOUT` | queue `visibility_timeout_seconds` |
| `PGMQ_RELAY_RABBITMQ_ACK_TIMEOUT_MS` | RabbitMQ whole-batch publish/confirm deadline |
| `PGMQ_RELAY_DEFAULT_MAX_POLL_SECONDS` | queue `max_poll_seconds` |
| `PGMQ_RELAY_DEFAULT_POLL_INTERVAL_MS` | queue `poll_interval_ms` |
| `PGMQ_RELAY_DEFAULT_MAX_CONNECTIONS` | `pgmq.max_connections` |
| `PGMQ_RELAY_KAFKA_*` | Kafka broker fields (see below) |
| `PGMQ_RELAY_NATS_*` | NATS broker fields (see below) |
| `PGMQ_RELAY_RABBITMQ_URL` | RabbitMQ `url` |
| `PGMQ_RELAY_CIRCUIT_BREAKER_*` | Circuit-breaker tuning (see below) |
| `LOG_FORMAT` | `text` (default) or `json` log output |
| `RUST_LOG` | log filter, e.g. `pgmq_relay=debug` |

### Broker Configuration

Each broker is declared under `[brokers.<name>]` and selected by its `type`.

#### Kafka (`type = "kafka"`)

```toml
[brokers.my_kafka]
type = "kafka"
bootstrap_servers = "localhost:9092"     # env: PGMQ_RELAY_KAFKA_BOOTSTRAP_SERVERS
# security_protocol = "SASL_SSL"          # env: PGMQ_RELAY_KAFKA_SECURITY_PROTOCOL
# sasl_mechanism = "PLAIN"                # env: PGMQ_RELAY_KAFKA_SASL_MECHANISM
# sasl_username = "user"                  # env: PGMQ_RELAY_KAFKA_SASL_USERNAME
# sasl_password = "pass"                  # env: PGMQ_RELAY_KAFKA_SASL_PASSWORD
# ssl_ca_location = "/certs/ca.pem"       # env: PGMQ_RELAY_KAFKA_SSL_CA_LOCATION
# Any other key is passed straight to librdkafka:
# "compression.type" = "zstd"

# Transactions (enabled by default). Set `transactions = false` for plain
# at-least-once producing at higher throughput.
[brokers.my_kafka.transactions]
strategy = { type = "hostname" }   # hostname | static | random
timeout_ms = 60000                 # 1000–300000
retries = 3
```

`transactions.strategy` controls the `transactional.id`, which is made stable per worker as `{prefix}-{hostname}-{worker-name}`:
- `{ type = "hostname", prefix = "pgmq-relay" }` *(default)* — stable across restarts; recommended.
- `{ type = "static", id = "..." }` — your base id, suffixed with the worker name so parallel workers don't fence each other.
- `{ type = "random", prefix = "..." }` — new UUID each start; **no** zombie fencing.

#### RabbitMQ (`type = "rabbitmq"`)

```toml
[brokers.my_rabbit]
type = "rabbitmq"
url = "amqp://guest:guest@localhost:5672/%2f"  # env: PGMQ_RELAY_RABBITMQ_URL
exchange = "pgmq.relay"        # "" = default exchange
exchange_type = "topic"        # direct | topic | fanout | headers
declare_exchange = true
durable = true
auto_delete = false
delivery_mode = 2              # 1 = transient, 2 = persistent
pool_size = 5                  # publish channels per worker (publisher confirms are pipelined across them)
ack_timeout_ms = 10000         # whole-batch publish/confirm deadline (100–300000)
```

Publisher confirms are always enabled; a message counts as delivered only after RabbitMQ confirms it. The acknowledgement deadline applies to the whole batch, not each message. Messages still unresolved when it expires remain in PGMQ for retry; because RabbitMQ may have accepted them before the timeout, downstream consumers must tolerate duplicates.

#### NATS (`type = "nats"`)

```toml
[brokers.my_nats]
type = "nats"
url = "nats://localhost:4222"  # env: PGMQ_RELAY_NATS_URL (comma-separated for a cluster)
# username / password / token  # env: PGMQ_RELAY_NATS_USERNAME / _PASSWORD / _TOKEN
client_name = "pgmq-relay"
max_reconnects = 60
reconnect_delay_ms = 5000
jetstream_enabled = false      # true = wait for per-message stream acks
# jetstream_domain = "hub"
use_key_as_subject_suffix = false  # true publishes to "<subject>.<key>"
```

With JetStream disabled (core NATS), the relay flushes the connection before treating a batch as delivered. For durable delivery, enable JetStream with a configured stream.

### Circuit Breaker

PGMQ completion (delete/archive) is wrapped in a circuit breaker with exponential backoff. All knobs have env-var overrides:

| Variable | Default | Description |
|----------|---------|-------------|
| `PGMQ_RELAY_CIRCUIT_BREAKER_FAILURE_THRESHOLD` | `5` | Consecutive failed completion operations, after retries, before the breaker opens. |
| `PGMQ_RELAY_CIRCUIT_BREAKER_RECOVERY_TIMEOUT` | `30s` | Wait before a half-open trial after opening. |
| `PGMQ_RELAY_CIRCUIT_BREAKER_INITIAL_DELAY` | `100ms` | First retry backoff. |
| `PGMQ_RELAY_CIRCUIT_BREAKER_MAX_DELAY` | `10s` | Maximum retry backoff. |
| `PGMQ_RELAY_CIRCUIT_BREAKER_MAX_RETRIES` | `5` | Maximum attempts per operation, including the initial attempt. |
| `PGMQ_RELAY_CIRCUIT_BREAKER_MULTIPLIER` | `2.0` | Backoff growth factor. |
| `PGMQ_RELAY_CIRCUIT_BREAKER_JITTER` | `0.1` | Values greater than zero enable full retry jitter. |

When the breaker is open the relay stops sending to the broker (and `/ready` reports not-ready) to avoid deleting messages it cannot confirm.

### PGMQ Fetch Modes (PGMQ 1.11.1)

The relay supports all PGMQ read methods for different use cases:

#### Available Fetch Modes

- **`regular`** (default): Standard `pgmq.read()` polling
  - Simple queue polling with visibility timeout
  - Returns immediately if no messages available

- **`read_with_poll`**: Long-polling with `pgmq.read_with_poll()`
  - Waits for messages if queue is empty (up to `max_poll_seconds`)
  - Reduces polling overhead and database load
  - Ideal for low-volume queues

- **`read_grouped`**: FIFO grouped reading with `pgmq.read_grouped()`
  - Messages with same group ID (from `x-pgmq-group` header) processed in order
  - Fills batch from earliest available group first
  - Maintains strict ordering within message groups

- **`read_grouped_with_poll`**: FIFO grouped with long-polling
  - Combines grouped FIFO reading with polling wait
  - Best for ordered message processing with variable message rates

- **`read_grouped_rr`**: Round-robin FIFO grouped reading
  - Distributes messages fairly across different FIFO groups
  - Messages from different groups can be processed in parallel
  - Better group fairness than `read_grouped`

- **`read_grouped_rr_with_poll`**: Round-robin FIFO with long-polling
  - Combines round-robin grouped reading with polling wait
  - Optimal for multi-tenant scenarios with ordering requirements

- **`read_grouped_head`**: FIFO grouped-head reading
  - Reads at most one visible head message per FIFO group
  - Useful when you want broad group concurrency without draining one group into a batch

- **`read_grouped_head_with_poll`**: FIFO grouped-head reading with long-polling
  - Combines grouped-head behavior with polling wait

- **`pop`**: At-most-once delivery with `pgmq.pop()`
  - ⚠️ **WARNING**: Messages deleted immediately upon read
  - At-most-once semantics (no redelivery on failure)
  - Only use for non-critical, fire-and-forget messages
  - `archive_messages` setting has no effect with this mode

#### Fetch Mode Configuration

```toml
[[queues]]
queue_name = "my_queue"
destination_topic = "my_topic"

# Simple regular polling (default)
fetch_mode = "regular"

# Long-polling configuration
fetch_mode = "read_with_poll"
max_poll_seconds = 5        # Wait up to 5 seconds for messages
poll_interval_ms = 100      # Check every 100ms during wait

# FIFO grouped processing
fetch_mode = "read_grouped"

# FIFO grouped with polling
fetch_mode = "read_grouped_with_poll"
max_poll_seconds = 3
poll_interval_ms = 50

# Round-robin FIFO
fetch_mode = "read_grouped_rr"

# Round-robin FIFO with polling
fetch_mode = "read_grouped_rr_with_poll"
max_poll_seconds = 5
poll_interval_ms = 100

# At-most-once delivery (use with caution!)
fetch_mode = "pop"
```

#### Choosing the Right Fetch Mode

| Use Case | Recommended Mode |
|----------|-----------------|
| High-throughput, unordered messages | `regular` |
| Low-volume queues, reduce DB load | `read_with_poll` |
| Strict message ordering per group | `read_grouped` or `read_grouped_with_poll` |
| Multi-tenant with fair group processing | `read_grouped_rr` or `read_grouped_rr_with_poll` |
| Non-critical, fire-and-forget events | `pop` (use sparingly) |

## Delivery Semantics

The relay provides **at-least-once delivery** for all fetch modes except `pop` (which is at-most-once).

The per-batch flow is:

1. **Read** a batch from PGMQ with a visibility timeout (the messages become invisible to other readers).
2. **Send** the batch to the broker and wait for the broker's acknowledgement:
   - **Kafka**: producer acks (`acks=all`); optionally wrapped in a Kafka transaction.
   - **RabbitMQ**: publisher confirms (`confirm.select`) — the broker must confirm before a message counts as delivered.
   - **NATS core**: the connection is flushed (server round-trip) before messages count as delivered.
   - **NATS JetStream**: per-message stream ack.
3. **Complete** the delivered messages in PGMQ (batched `pgmq.delete` / `pgmq.archive` in a single statement).

Because step 3 happens *after* step 2 and the two are not a single distributed transaction, a crash between broker-ack and PGMQ-completion will cause the affected messages to be **redelivered** after the visibility timeout. **Consumers must therefore be idempotent (deduplicate on a message key).** Kafka transactions make the *produce* atomic but do not extend the transaction to the PGMQ offset, so they do not provide end-to-end exactly-once on their own.

> Note: a stable Kafka `transactional.id` is derived per worker as `{prefix}-{hostname}-{worker-name}` so it survives restarts (required for transaction recovery / zombie fencing) and stays unique across parallel workers.

### Dead-Letter Queue (poison messages)

A message that fails transformation cannot succeed on retry and would otherwise be re-read every visibility timeout. Configure a `dead_letter_queue` (another PGMQ queue) to route such messages out of the source queue:

```toml
[[queues]]
queue_name = "my_queue"
dead_letter_queue = "my_queue_dlq"   # optional; must be an existing PGMQ queue
```

When set, a failing message is re-enqueued onto the dead-letter queue (preserving its payload and original headers, plus `x-dead-letter-*` metadata) and then removed from the source queue. When **unset**, a failing message is left in the source queue and retried after the visibility timeout — it no longer blocks the rest of the batch, but it will be retried indefinitely, so configuring a dead-letter queue is recommended.

### Ordering and parallelism

`parallelism > 1` runs multiple workers against the same queue concurrently. For the grouped/FIFO fetch modes, per-group order is preserved only while a group is held by a single worker (via the visibility timeout); combined with at-least-once redelivery, strict global ordering is not guaranteed. **Use `parallelism = 1` for queues that require strict ordering** (a startup warning is emitted otherwise).

### Message Transformations

- **Passthrough**: Forward messages as-is
- **JSON Extract**: Extract a specific field from JSON messages
- **Custom Template**: Use template strings with field substitution

### Kafka Message Keys

The relay automatically extracts message keys for Kafka partitioning:

- **key_field**: Specifies which field to use as the broker key (Kafka partition key, NATS subject suffix when enabled, RabbitMQ routing key)
- **Default**: `"message_id"` — with no such field present this falls back to the PGMQ message id, i.e. keying by message id
- **Headers First**: Searches the PGMQ headers column first, then the message body
- **Fallback**: Uses the PGMQ message id if the field is not found
- **Nested fields**: Dot-paths are supported (e.g. `metadata.tenant_id`)

Examples:
```toml
# Default: key by PGMQ message id
key_field = "message_id"

# Use the PGMQ FIFO group header (recommended for ordered partitioning)
key_field = "x-pgmq-group"

# Use a custom header
key_field = "correlation_id"

# Use field from message body
key_field = "user_id"

# Use nested field
key_field = "metadata.tenant_id"
```

This ensures proper message ordering and partitioning in Kafka based on your PGMQ message headers.

### Header Forwarding

The relay automatically forwards PGMQ headers to Kafka messages and adds relay-specific metadata:

#### Original PGMQ Headers (Preserved)
- Original headers are prefixed with `pgmq_header_` to prevent conflicts
- All header values are converted to strings for Kafka compatibility

#### Relay-Added Headers
- `pgmq_msg_id` - Original PGMQ message ID
- `pgmq_relay_timestamp` - ISO 8601 timestamp when message was processed
- `pgmq_queue_name` - Source queue name
- `pgmq_message_key` - Extracted message key (if found)

Example Kafka message headers:
```
pgmq_header_correlation_id: abc-123
pgmq_header_x-pgmq-group: user-456
pgmq_msg_id: 12345
pgmq_relay_timestamp: 2023-12-01T10:30:00Z
pgmq_queue_name: user_events
pgmq_message_key: user-456
```

## Usage

### Running Locally

```bash
# Run with default config
cargo run

# Run with custom config
cargo run -- --config /path/to/config.toml

# Validate configuration without starting the relay
cargo run -- --config /path/to/config.toml --validate-config

# Set log level
RUST_LOG=debug cargo run
```

### Running with Docker Compose

```bash
# Start all services (PGMQ, brokers, relay)
docker-compose up -d

# Start with test message sender (continuously sends test messages)
docker-compose --profile testing up -d

# View relay logs
docker-compose logs -f pgmq-relay

# View test message sender logs
docker-compose logs -f test-message-sender

# Stop all services
docker-compose down

# Stop and remove volumes
docker-compose down -v
```

#### Test Message Sender

The test message sender service continuously sends test messages to PGMQ queues for testing and development. It's part of the `testing` profile and must be explicitly enabled.

**Configuration:**
- Default interval: 2 seconds between messages
- Customize interval: Set `MESSAGE_INTERVAL` environment variable in docker-compose.yml
- Queues targeted: `user_events`, `order_processing`, `notifications`, `alerts`, `financial_transactions`, `customer_changes`

**Message Types:**
Each queue receives different message types with realistic test data:
- **user_events**: Login events with user IDs, timestamps, IP addresses
- **order_processing**: Orders with FIFO grouping, customer IDs, amounts
- **notifications**: Email notifications with priority levels
- **alerts**: System alerts with severity levels
- **financial_transactions**: Transactions with FIFO grouping for account ordering
- **customer_changes**: Customer data changes with FIFO grouping

**Manual Usage:**
```bash
# Run from host (requires psql)
./scripts/send_test_messages.sh

# Custom interval (0.5 seconds)
./scripts/send_test_messages.sh --interval 0.5

# Run for limited duration (60 seconds)
./scripts/send_test_messages.sh --interval 1 --duration 60
```

## Monitoring and Metrics

The relay exposes Prometheus metrics at `GET /metrics` (default port 9090), plus `GET /health` (liveness) and `GET /ready` (readiness). **All metric names are prefixed `pgmq_relay_`.**

> Note on consolidation: per-message activity is tracked by a single `pgmq_relay_worker_operation_total` counter labelled by `operation` (`poll`, `complete`), incremented by the *message count*. The companion histogram's `_count` gives the number of poll/complete *cycles*. There are no separate `pgmq_messages_polled/deleted/archived` series.

### Worker metrics
| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `pgmq_relay_worker_operation_total` | counter | `worker_name`, `operation`, `queue_name`, `topic` | Messages processed, by operation (`poll` = read from PGMQ, `complete` = deleted/archived). |
| `pgmq_relay_worker_operation_duration_seconds` | histogram | `worker_name`, `operation`, `queue_name`, `topic` | Duration of each poll/complete cycle (`_count` = number of cycles). |
| `pgmq_relay_worker_operation_errors_total` | counter | `worker_name`, `operation`, `queue_name`, `topic`, `error_type` | Errors, by operation (`poll`, `send`, `process`, `transformation`). |
| `pgmq_relay_workers_active` | gauge | — | Number of running workers. |

### Broker metrics
| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `pgmq_relay_broker_messages_sent_total` | counter | `broker_name`, `broker_type`, `topic` | Messages acknowledged by the broker. |
| `pgmq_relay_broker_messages_failed_total` | counter | `broker_name`, `broker_type`, `topic`, `error_type` | Messages that failed to send. |
| `pgmq_relay_broker_send_duration_seconds` | histogram | `broker_name`, `broker_type`, `topic` | Batch send duration. |
| `pgmq_relay_broker_batch_size` | histogram | `broker_name`, `broker_type`, `topic` | Messages per batch. |
| `pgmq_relay_broker_health_check_status` | gauge | `broker_name`, `broker_type` | `1` healthy / `0` unhealthy. Refreshed every 15s. |

### Delivery-safety metrics
These track the at-least-once completion step (delete/archive after broker ack):
| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `pgmq_relay_messages_delivered_but_not_completed_total` | counter | `worker_name`, `queue_name`, `topic`, `error_type` | **Critical:** messages sent to the broker but not removed from PGMQ → will be redelivered. |
| `pgmq_relay_callback_failures_total` | counter | `worker_name`, `queue_name`, `topic`, `error_type` | PGMQ completion failures after a successful broker send. |
| `pgmq_relay_callback_duration_seconds` | histogram | `worker_name`, `queue_name`, `topic` | Time spent on the completion step. |

### Circuit-breaker metrics
| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `pgmq_relay_circuit_breaker_state` | gauge | `name`, `type` | `0` closed, `1` open, `2` half-open. |
| `pgmq_relay_circuit_breaker_operations_total` | counter | `name`, `type`, `result` | `result` ∈ `success`, `failure`, `retry_success`, `retry_attempt`. |
| `pgmq_relay_circuit_breaker_failures_total` | counter | `name`, `type`, `error_type` | Failed operations. |
| `pgmq_relay_circuit_breaker_rejections_total` | counter | `name`, `type` | Calls rejected while the breaker was open. |
| `pgmq_relay_circuit_breaker_operation_duration_seconds` | histogram | `name`, `type` | Operation duration through the breaker. |

### System metrics
| Metric | Type | Description |
|--------|------|-------------|
| `pgmq_relay_uptime_seconds_total` | counter | Process uptime in seconds. |
| `pgmq_relay_restarts_total` | counter | Incremented once per process start. |
| `pgmq_relay_memory_usage_bytes` | gauge | Resident memory (Linux only). |

### Example Prometheus Queries
```promql
# Messages read from PGMQ per second
sum(rate(pgmq_relay_worker_operation_total{operation="poll"}[5m])) by (queue_name)

# Messages delivered to brokers per second
sum(rate(pgmq_relay_broker_messages_sent_total[5m])) by (broker_name, topic)

# Broker send failure ratio
sum(rate(pgmq_relay_broker_messages_failed_total[5m])) by (broker_name)
  / sum(rate(pgmq_relay_broker_messages_sent_total[5m])) by (broker_name)

# Average broker send duration
rate(pgmq_relay_broker_send_duration_seconds_sum[5m])
  / rate(pgmq_relay_broker_send_duration_seconds_count[5m])

# Live broker health
pgmq_relay_broker_health_check_status
```

## Alerting Examples

### Prometheus Alerting Rules
```yaml
groups:
- name: pgmq-relay
  rules:
  - alert: PGMQRelayHighBrokerFailureRate
    expr: |
      sum(rate(pgmq_relay_broker_messages_failed_total[5m])) by (broker_name)
        / sum(rate(pgmq_relay_broker_messages_sent_total[5m])) by (broker_name) > 0.1
    for: 2m
    labels:
      severity: warning
    annotations:
      summary: "High broker send failure rate"
      description: "{{ $value | humanizePercentage }} of sends are failing for broker {{ $labels.broker_name }}"

  - alert: PGMQRelayDeliveredButNotCompleted
    expr: increase(pgmq_relay_messages_delivered_but_not_completed_total[10m]) > 0
    for: 0m
    labels:
      severity: critical
    annotations:
      summary: "Messages delivered to broker but not completed in PGMQ"
      description: "{{ $value }} message(s) on queue {{ $labels.queue_name }} will be redelivered (duplicate risk)."

  - alert: PGMQRelayBrokerDown
    expr: pgmq_relay_broker_health_check_status == 0
    for: 1m
    labels:
      severity: critical
    annotations:
      summary: "Message broker is unhealthy"
      description: "Broker {{ $labels.broker_name }} ({{ $labels.broker_type }}) is unhealthy"

  - alert: PGMQRelayCircuitBreakerOpen
    expr: pgmq_relay_circuit_breaker_state > 0
    for: 1m
    labels:
      severity: warning
    annotations:
      summary: "Circuit breaker not closed"
      description: "Circuit breaker {{ $labels.name }} is in state {{ $value }} (1=open, 2=half-open)"

  - alert: PGMQRelayWorkerDown
    expr: pgmq_relay_workers_active == 0
    for: 5m
    labels:
      severity: critical
    annotations:
      summary: "No active relay workers"
      description: "All relay workers have stopped"
```

## Building

```bash
cd pgmq-relay
cargo build --release
```

## Releases

Pushing a version tag triggers the [release workflow](.github/workflows/release.yml), which:

- builds standalone binaries for `linux/amd64`, `linux/arm64`, `macOS x86_64`, and `macOS arm64` and attaches them (with `.sha256` checksums) to a GitHub Release; and
- builds and pushes a multi-arch (`linux/amd64`, `linux/arm64`) Docker image to GHCR.

```bash
# Cut a release
git tag v0.1.0
git push origin v0.1.0

# Pull the published image
docker pull ghcr.io/<owner>/<repo>:v0.1.0
docker run --rm -v "$PWD/config.toml:/etc/pgmq-relay/config.toml" ghcr.io/<owner>/<repo>:v0.1.0
```

> The `linux/arm64` Linux binaries build on native ARM runners (`ubuntu-24.04-arm`), which are free for public repositories. The Docker image's arm64 layer is built under QEMU emulation and is correspondingly slower.

## Dependencies

- PGMQ instance running in PostgreSQL
- Message broker (e.g., Kafka) for forwarding messages
- Rust 1.70+ for building

## Implementation Details

The relay consists of:

- **PGMQ Client**: Polls messages and completes them with batched delete/archive and connection pooling
- **Broker Abstraction**: Pluggable interface for different message brokers
- **Relay Workers**: Each worker handles exactly one queue; a queue scales out via `parallelism`
- **Message Transformation**: Configurable message processing pipeline

### Benefits of This Architecture

- **Reliability**: At-least-once delivery — messages are completed in PGMQ only after broker acknowledgement (see [Delivery Semantics](#delivery-semantics))
- **Efficiency**: Multi-queue polling reduces database connections
- **Performance**: Connection pooling with configurable pool size handles high load
- **Scalability**: Workers can be distributed across different processes/machines
- **Flexibility**: Each worker can have different polling configurations
- **Monitoring**: Comprehensive metrics for alerting and capacity planning

### Performance Configuration

The relay uses connection pooling to optimize database performance:

```toml
[pgmq]
connection_url = "postgres://postgres:postgres@localhost:5432/postgres"
max_connections = 20  # Adjust based on your database capacity and load
```

- **max_connections**: Maximum number of connections in the pool (default: 10)
- **Connection lifecycle**: Automatic connection management with idle timeout (5 min) and max lifetime (30 min)
- **Acquire timeout**: 10-second timeout for acquiring connections from the pool
