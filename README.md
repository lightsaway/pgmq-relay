# PGMQ Relay

A Rust-based message relay service that reads from PGMQ (Postgres Message Queue) topics via polling and forwards messages to configurable message brokers like Kafka.

## Features

- **Multi-Queue Workers**: Each worker can poll multiple queues efficiently
- **Transactional Reliability**: Single PostgreSQL transaction for poll → relay → delete
- **Multiple Fetch Modes**: Support for all PGMQ 1.9.0 read methods (regular, long-polling, FIFO grouped, round-robin)
- **FIFO Support**: Proper FIFO queue polling with configurable batch sizes
- **Multiple Brokers**: Abstracted broker interface supporting Kafka, RabbitMQ, and NATS
- **Message Transformation**: Support for passthrough, JSON field extraction, and custom templates
- **Header Forwarding**: Preserves PGMQ headers and adds relay metadata to Kafka messages
- **Prometheus Metrics**: Built-in metrics endpoint for monitoring and alerting

## Quick Start with Docker Compose

The fastest way to try out pgmq-relay is using the included Docker Compose setup, which provides a complete working example with PGMQ, multiple message brokers, and the relay service pre-configured.

### What's Included

The `docker-compose.yml` provides a full-stack example environment:

- **PostgreSQL with PGMQ 1.9.0** - Message queue database
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

The relay is configured via a TOML file. See `config.toml` for a complete example.

### Configuration Validation

The relay provides comprehensive configuration validation with detailed error messages:

```bash
# Validate your configuration file
cargo run -- --config config.toml --validate-config
```
### Basic Structure

```toml
[pgmq]
connection_url = "postgres://postgres:postgres@localhost:5432/postgres"
max_connections = 10  # Connection pool size (default: 10)

[relay]
poll_interval = "250ms"
poll_timeout = "5s"
batch_size = 10

[metrics]
enabled = true
bind_address = "0.0.0.0"
port = 9090

[brokers.my_kafka]
type = "kafka"
bootstrap_servers = "localhost:9092"

[[workers]]
name = "my_worker"
broker_name = "my_kafka"
poll_interval = "250ms"
batch_size = 10
visibility_timeout_seconds = 30

[[workers.queues]]
queue_name = "my_queue"
destination_topic = "my_topic"
fifo = true
key_field = "x-pgmq-group"  # Extract from PGMQ headers for Kafka partitioning
```

### PGMQ Fetch Modes (PGMQ 1.9.0+)

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

### Message Transformations

- **Passthrough**: Forward messages as-is
- **JSON Extract**: Extract a specific field from JSON messages
- **Custom Template**: Use template strings with field substitution

### Kafka Message Keys

The relay automatically extracts message keys for Kafka partitioning:

- **key_field**: Specifies which field to use as the Kafka message key
- **Default**: `"x-pgmq-group"` (extracted from PGMQ headers)
- **Headers First**: Searches PGMQ headers column first, then message body
- **Fallback**: Uses PGMQ message ID if key field not found

Examples:
```toml
# Use PGMQ FIFO header (default)
key_field = "x-pgmq-group"

# Use custom header
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

The relay exposes Prometheus metrics on `/metrics` endpoint (default port 9090). Key metrics include:

### PGMQ Metrics
- `pgmq_messages_polled_total` - Total messages polled from queues (by queue, fifo mode)
- `pgmq_messages_archived_total` - Total messages archived (by queue)
- `pgmq_messages_deleted_total` - Total messages deleted (by queue)
- `pgmq_poll_duration_seconds` - Time spent polling queues (by queue, fifo mode)
- `pgmq_poll_errors_total` - Total polling errors (by queue, error type)
- `pgmq_queue_size_estimate` - Estimated queue size based on poll results

### Broker Metrics
- `broker_messages_sent_total` - Total messages sent to brokers (by broker, topic)
- `broker_messages_failed_total` - Total failed broker sends (by broker, topic, error type)
- `broker_send_duration_seconds` - Time spent sending to brokers (by broker, topic)
- `broker_health_check_status` - Broker health status (1=healthy, 0=unhealthy)
- `broker_batch_size` - Size of message batches sent to brokers

### Worker Metrics
- `relay_workers_active` - Number of active workers
- `relay_worker_processing_duration_seconds` - Message processing time (by worker, topic/operation)
- `relay_worker_errors_total` - Total worker errors (by queue, topic, error type)
- `relay_messages_transformed_total` - Total message transformations (by queue, transformation type)
- `relay_transformation_errors_total` - Total transformation errors

### Enhanced Topic-Level Metrics
All broker metrics now include individual topic names rather than generic "multi_topic" labels:
- `broker_messages_sent_total{topic="actual_topic_name"}` - Per-topic message counts
- `broker_batch_size{topic="actual_topic_name"}` - Per-topic batch sizes
- `broker_send_duration_seconds{topic="actual_topic_name"}` - Per-topic send durations

### System Metrics
- `relay_uptime_seconds_total` - Total relay uptime in seconds
- `relay_restarts_total` - Total number of relay restarts
- `relay_memory_usage_bytes` - Current memory usage in bytes (Linux only)

### Callback Metrics (Critical for Exactly-Once Semantics)
- `callback_executions_total` - Successful PGMQ completions after Kafka commit
- `callback_failures_total` - Failed PGMQ completions after Kafka commit (CRITICAL ALERT)
- `callback_duration_seconds` - Time spent on PGMQ completion operations
- `messages_delivered_but_not_completed_total` - Messages delivered to Kafka but not completed in PGMQ

### Example Prometheus Queries
```promql
# Message processing rate
rate(pgmq_messages_polled_total[5m])

# Error rate by queue
rate(pgmq_poll_errors_total[5m]) / rate(pgmq_messages_polled_total[5m])

# Broker health
broker_health_check_status

# Average processing time
rate(relay_worker_processing_duration_seconds_sum[5m]) / rate(relay_worker_processing_duration_seconds_count[5m])
```

## Alerting Examples

### Prometheus Alerting Rules
```yaml
groups:
- name: pgmq-relay
  rules:
  - alert: PGMQRelayHighErrorRate
    expr: rate(pgmq_poll_errors_total[5m]) / rate(pgmq_messages_polled_total[5m]) > 0.1
    for: 2m
    labels:
      severity: warning
    annotations:
      summary: "High error rate in PGMQ relay"
      description: "Error rate is {{ $value | humanizePercentage }} for queue {{ $labels.queue_name }}"

  - alert: PGMQRelayBrokerDown
    expr: broker_health_check_status == 0
    for: 1m
    labels:
      severity: critical
    annotations:
      summary: "Message broker is unhealthy"
      description: "Broker {{ $labels.broker_name }} ({{ $labels.broker_type }}) is unhealthy"

  - alert: PGMQRelayWorkerDown
    expr: relay_workers_active == 0
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

## Dependencies

- PGMQ instance running in PostgreSQL
- Message broker (e.g., Kafka) for forwarding messages
- Rust 1.70+ for building

## Implementation Details

The relay consists of:

- **PGMQ Client**: Handles transactional polling and message deletion with connection pooling for improved performance
- **Broker Abstraction**: Pluggable interface for different message brokers
- **Relay Workers**: Each worker handles multiple queue-to-topic mappings
- **Message Transformation**: Configurable message processing pipeline

### Benefits of This Architecture

- **Reliability**: Transactional processing ensures no message loss
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
