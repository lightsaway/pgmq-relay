# PGMQ Relay

PGMQ Relay moves messages from [PGMQ](https://github.com/pgmq/pgmq) queues in PostgreSQL to Kafka, RabbitMQ, or NATS.

It is intended for systems that use Postgres as a transactional outbox or durable queue but need to publish those messages to an external broker.

## What it guarantees

For every fetch mode except `pop`, the relay completes a source message only after the destination broker reports success. This provides **at-least-once delivery** across the PGMQ-to-broker boundary.

At-least-once means duplicates are possible. Consumers must be idempotent or deduplicate using `pgmq_msg_id`, a domain event ID, or another stable key.

See [Delivery Guarantees](./delivery-semantics.md) before using the relay for critical data.

## Supported destinations

| Broker | Success boundary |
|---|---|
| Kafka | Producer acknowledgement; optional transaction commit |
| RabbitMQ | Publisher confirmation |
| Core NATS | Successful connection flush |
| NATS JetStream | Per-message stream acknowledgement |

## Capabilities

- Multiple PGMQ queues in one process
- Per-queue broker selection and worker parallelism
- Regular, long-polling, grouped FIFO, round-robin grouped, and `pop` fetch modes
- Passthrough, JSON extraction, and template transformations
- PGMQ dead-letter queues for transformation failures
- Prometheus metrics and liveness/readiness endpoints
- Supervised workers and bounded graceful shutdown

## Start here

1. [Install the relay](./install.md).
2. Run the [Docker Compose quick start](./quick-start.md).
3. [Send and verify a message](./first-message.md).
4. Select a broker: [Kafka](./brokers/kafka.md), [RabbitMQ](./brokers/rabbitmq.md), or [NATS](./brokers/nats.md).
