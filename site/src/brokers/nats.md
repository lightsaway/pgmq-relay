# NATS

## Core NATS

```toml
[brokers.nats]
type = "nats"
url = "nats://nats-1:4222,nats://nats-2:4222"
client_name = "pgmq-relay"
max_reconnects = 60
reconnect_delay_ms = 5000
jetstream_enabled = false
```

Core NATS is live pub/sub. It does not retain a message for subscribers that are offline.

After publishing a core NATS batch, the relay flushes the client connection. A successful flush proves the server processed the connection buffer, but it is not a durable storage acknowledgement.

Use core NATS only when this delivery model is acceptable.

## JetStream

```toml
[brokers.nats]
type = "nats"
url = "nats://nats:4222"
jetstream_enabled = true
# jetstream_domain = "hub"
```

JetStream publishing waits for a per-message stream acknowledgement. The relay does not create streams; configure a stream whose subjects include the relay destination subject.

## Authentication

Use either a token or username/password:

```toml
token = "secret"
```

or:

```toml
username = "relay"
password = "secret"
```

## Subjects

By default, every message uses `destination_topic` as its subject.

```toml
destination_topic = "events.orders"
```

To append the extracted key:

```toml
use_key_as_subject_suffix = true
```

A key of `customer-42` then publishes to `events.orders.customer-42`.

The initial connection is bounded and must complete before the worker starts. Reconnects use exponential delays based on `reconnect_delay_ms`, capped at 32 times the base delay.
