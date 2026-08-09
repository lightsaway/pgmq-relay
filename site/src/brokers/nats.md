# NATS

## JetStream (default)

```toml
[brokers.nats]
type = "nats"
url = "nats://nats:4222"
# jetstream_enabled = true is the default
# jetstream_domain = "hub"
```

JetStream publishing waits for a per-message stream acknowledgement before the source message is completed in PGMQ, which is what makes at-least-once delivery hold. It is therefore the default. The relay does not create streams; configure a stream whose subjects include the relay destination subject.

## Core NATS (opt-in)

```toml
[brokers.nats]
type = "nats"
url = "nats://nats-1:4222,nats://nats-2:4222"
client_name = "pgmq-relay"
reconnect_delay_ms = 5000
jetstream_enabled = false
```

Core NATS is live pub/sub. It does not retain a message for subscribers that are offline, and it returns no delivery acknowledgement — messages can be silently lost.

After publishing a core NATS batch, the relay flushes the client connection. A successful flush proves the server processed the connection buffer, but it is not a durable storage acknowledgement.

Disabling JetStream logs a startup warning. Use core NATS only when this delivery model is acceptable.

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

The initial connection is bounded and must complete before the worker starts. Reconnects use exponential delays based on `reconnect_delay_ms`, capped at 32 times the base delay. By default the client reconnects forever (`max_reconnects = 0`); setting a positive `max_reconnects` closes the client permanently once the budget is exhausted, requiring a process restart.
