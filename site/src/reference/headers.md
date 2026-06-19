# Message Headers

The relay forwards PGMQ headers and adds delivery metadata.

## Relay metadata

| Header | Meaning |
|---|---|
| `pgmq_msg_id` | Original PGMQ message ID |
| `pgmq_relay_timestamp` | UTC relay transformation time |
| `pgmq_queue_name` | Source PGMQ queue |
| `pgmq_message_key` | Extracted broker key, when available |

Original PGMQ headers are prefixed with `pgmq_header_`.

For example:

```text
pgmq_header_correlation_id: request-123
pgmq_header_x-pgmq-group: customer-42
pgmq_msg_id: 817
pgmq_queue_name: orders
pgmq_message_key: customer-42
```

Header values are converted to strings. Objects and arrays are serialized as JSON strings.

## Broker mapping

- Kafka: relay headers become Kafka record headers.
- RabbitMQ: relay headers become AMQP headers.
- NATS: relay headers become NATS headers.

The extracted key is also used as Kafka record key and RabbitMQ routing key. NATS only appends it to the subject when `use_key_as_subject_suffix = true`.

## Dead-letter metadata

PGMQ dead-letter messages receive:

| Header | Meaning |
|---|---|
| `x-dead-letter-source-queue` | Original queue |
| `x-dead-letter-msg-id` | Original PGMQ ID |
| `x-dead-letter-error` | Transformation error |
| `x-dead-letter-original-headers` | Serialized original headers, when present |
