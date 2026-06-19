# Docker Compose Quick Start

The repository Compose stack starts PostgreSQL with PGMQ, Redpanda, RabbitMQ, NATS, and the relay.

## Start the stack

```bash
git clone https://github.com/lightsaway/pgmq-relay.git
cd pgmq-relay
docker compose up -d
docker compose ps
```

Wait until `postgres`, `redpanda`, `rabbitmq`, `nats`, and `pgmq-relay` are healthy.

Check the relay:

```bash
curl --fail http://127.0.0.1:9090/health
curl --fail http://127.0.0.1:9090/ready
```

Expected responses:

```text
OK
Ready
```

## Included routes

| PGMQ queue | Destination | Broker | Completion |
|---|---|---|---|
| `user_events` | `events.users` | Redpanda/Kafka | Delete |
| `notifications` | `pgmq.relay` exchange | RabbitMQ | Delete |
| `logs` | `logs.system` | NATS | Archive |

The exact configuration is in `docker/config-docker.toml`.

## Local interfaces

| Service | URL |
|---|---|
| Relay metrics | <http://localhost:9090/metrics> |
| Redpanda Console | <http://localhost:8080> |
| RabbitMQ Management | <http://localhost:15672> |
| NATS monitoring | <http://localhost:8222> |
| NATS UI | <http://localhost:31311> |

RabbitMQ credentials are `admin` / `admin`. The NATS UI requires a first-time connection to `nats://nats:4222`.

## Watch relay activity

```bash
docker compose logs -f pgmq-relay
```

Normal long-polling queries can be reported by SQLx as slow queries because they intentionally wait for messages.

Continue with [Send Your First Message](./first-message.md) to verify an actual delivery.
