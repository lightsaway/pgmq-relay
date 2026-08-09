# Kafka

## Basic configuration

```toml
[brokers.kafka]
type = "kafka"
bootstrap_servers = "kafka-1:9092,kafka-2:9092"
```

Unknown keys are forwarded to `librdkafka`, allowing settings such as:

```toml
"compression.type" = "zstd"
"linger.ms" = "5"
```

## Authentication and TLS

```toml
[brokers.kafka]
type = "kafka"
bootstrap_servers = "kafka.example.com:9093"
security_protocol = "SASL_SSL"
sasl_mechanism = "PLAIN"
sasl_username = "relay"
sasl_password = "secret"
ssl_ca_location = "/certs/ca.pem"
# ssl_certificate_location = "/certs/client.pem"
# ssl_key_location = "/certs/client.key"
```

Mount certificate files into the relay container and keep credentials outside committed configuration.

## Transactions

Transactions are enabled by default:

```toml
[brokers.kafka.transactions]
strategy = { type = "hostname", prefix = "pgmq-relay" }
timeout_ms = 60000
retries = 3
```

The hostname strategy creates a stable transactional ID per worker. Stable IDs allow Kafka to fence stale producers after restart.

`retries` is accepted for backward compatibility but no longer mapped to librdkafka's `retries` setting: idempotent producers manage send retries themselves, bounded by `message.timeout.ms`.

Other strategies:

```toml
strategy = { type = "static", id = "relay-production" }
strategy = { type = "random", prefix = "relay" }
```

A static base is suffixed by worker name. Random IDs do not provide stable producer fencing across restarts.

Both boolean forms are accepted: `transactions = true` enables transactions with default settings (equivalent to omitting the field), and disabling them for higher throughput is:

```toml
transactions = false
```

Without transactions, each message delivery future is tracked independently. Successfully acknowledged messages are completed in PGMQ while failed messages remain for retry.

## Keys and ordering

The extracted message key becomes the Kafka record key:

```toml
key_field = "tenant_id"
```

The relay checks PGMQ headers first, then the message body, including nested dot paths. If no value exists, the PGMQ message ID is used as the broker key.

Transactions make a Kafka batch atomic, but they do not include PGMQ completion. Consumers must still handle duplicates.
