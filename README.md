# PGMQ Relay

PGMQ Relay moves messages from PostgreSQL queues managed by [PGMQ](https://github.com/pgmq/pgmq) to Kafka, RabbitMQ, or NATS.

It is designed for transactional-outbox and queue-bridging workloads that need:

- at-least-once delivery for normal fetch modes
- per-queue broker routing and worker parallelism
- Kafka transactions, RabbitMQ publisher confirms, and NATS/JetStream acknowledgements
- grouped FIFO and long-poll PGMQ reads
- transformation dead-letter queues
- Prometheus metrics, health, and readiness endpoints

Full documentation: <https://lightsaway.github.io/pgmq-relay/>

## Delivery model

The relay publishes a message before deleting or archiving it in PGMQ. A crash between broker acknowledgement and PGMQ completion causes redelivery, so downstream consumers must be idempotent.

`fetch_mode = "pop"` is different: PGMQ removes the message before delivery, so failures can lose messages.

Read [Delivery Guarantees](https://lightsaway.github.io/pgmq-relay/delivery-semantics.html) before using the relay for critical data.

## Quick start

```bash
git clone https://github.com/lightsaway/pgmq-relay.git
cd pgmq-relay
docker compose up -d
docker compose ps
curl --fail http://127.0.0.1:9090/ready
```

The Compose stack includes PostgreSQL with PGMQ, Redpanda, RabbitMQ, NATS, and routes demonstrating all three broker implementations.

Follow the [first-message walkthrough](https://lightsaway.github.io/pgmq-relay/first-message.html) to publish a message, consume it from Kafka, and verify its PGMQ source row was completed.

## Minimal configuration

```toml
default_broker = "kafka"

[pgmq]
connection_url = "postgres://relay:secret@postgres:5432/app"

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

Validate without connecting to PostgreSQL or a broker:

```bash
pgmq-relay --config config.toml --validate-config
```

See the [configuration guide](https://lightsaway.github.io/pgmq-relay/configuration.html) and broker pages for production settings.

## Build

```bash
docker build -t pgmq-relay:local .
```

Source:

```bash
cargo build --release --locked
./target/release/pgmq-relay --config config.toml
```

The release workflow will publish images to `ghcr.io/lightsaway/pgmq-relay`, plus release archives, checksums, and attestations after the first version tag is created.

Stable releases publish `latest`, but production deployments should pin an exact version:

```bash
docker pull ghcr.io/lightsaway/pgmq-relay:latest
docker pull ghcr.io/lightsaway/pgmq-relay:0.1.0
```

## Development

```bash
cargo fmt --all --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
```

Documentation source lives in `site/src` and is built with mdBook:

```bash
mdbook build
```

See [Build and Contribute](https://lightsaway.github.io/pgmq-relay/contributing.html).
