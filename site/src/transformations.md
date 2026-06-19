# Message Transformations

Transformation happens after fetch and before broker publish.

## Passthrough

Passthrough is the default and serializes the complete PGMQ JSON message:

```toml
[queues.message_transformation]
type = "passthrough"
```

## JSON extraction

Extract one top-level field:

```toml
[[queues]]
queue_name = "wrapped_events"

[queues.message_transformation]
type = "json_extract"
field = "payload"
```

Input:

```json
{"payload":{"event":"created"},"internal":{"attempt":1}}
```

Published payload:

```json
{"event":"created"}
```

`json_extract` currently accepts a top-level field name, not a dot path.

## Custom template

Templates substitute `{field}` and nested `{object.field}` placeholders:

```toml
[queues.message_transformation]
type = "custom_template"
template = "Order {order.id} belongs to {customer.id}"
```

Missing placeholders remain unchanged in the output. Templates produce UTF-8 text, not a JSON object unless the template itself is valid JSON.

## Failure handling

A transformation failure affects only that message, not every message in the batch.

Configure a PGMQ dead-letter queue:

```toml
dead_letter_queue = "events_dlq"
```

The dead-letter queue must already exist. Without it, failed messages retry after the visibility timeout and can retry indefinitely.
