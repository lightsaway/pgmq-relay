# RabbitMQ

## Configuration

```toml
[brokers.rabbit]
type = "rabbitmq"
url = "amqps://relay:secret@rabbit.example.com:5671/%2f"
exchange = "pgmq.relay"
exchange_type = "topic"
declare_exchange = true
durable = true
auto_delete = false
delivery_mode = 2
pool_size = 5
ack_timeout_ms = 10000
```

The relay declares the exchange when `declare_exchange = true`. It does not declare destination queues or bindings.

## Routing

For each message:

1. The extracted message key is used as the routing key.
2. If there is no key, `destination_topic` is used.

With:

```toml
destination_topic = "notifications.email"
key_field = "customer_id"
```

a message containing `customer_id = "customer-42"` is published with routing key `customer-42`.

Design exchange bindings for this behavior. If the configured key field is absent, RabbitMQ falls back to `destination_topic`.

## Publisher confirms

Every publish channel enables RabbitMQ publisher confirms. A source message is completed only after a positive confirmation.

`ack_timeout_ms` is one deadline for publishing and confirming the whole batch, not a per-message timeout.

When the deadline expires, unresolved messages remain in PGMQ. RabbitMQ may already have accepted some of them, so a later retry can create duplicates.

Set:

```text
visibility_timeout_seconds * 1000 > ack_timeout_ms
```

with enough margin for transformation and PGMQ completion.

Publisher confirms acknowledge broker acceptance. They do not wait for downstream consumers and do not depend on consumer processing speed.
