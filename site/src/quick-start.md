# Quick Start

## Run With Docker Compose

```bash
docker-compose up -d
docker-compose --profile testing up -d
docker-compose logs -f pgmq-relay
```

The compose stack includes PostgreSQL with PGMQ 1.11.1, Redpanda, RabbitMQ, NATS, and the relay.

Useful local URLs:

| Service | URL |
|---|---|
| Relay metrics | <http://localhost:9090/metrics> |
| Redpanda Console | <http://localhost:8080> |
| RabbitMQ Management | <http://localhost:15672> |
| NATS monitoring | <http://localhost:8222> |

## Run From Source

```bash
cargo run -- --config config.toml
```

Validate configuration without starting the relay:

```bash
cargo run -- --config config.toml --validate-config
```

## Build The Static Site

This documentation site is built with `mdBook`.

```bash
mdbook build
```

The generated static site is written to `book/`.
