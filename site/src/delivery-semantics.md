# Delivery Guarantees

## Normal modes: at least once

All fetch modes except `pop` use this sequence:

<div class="sequence">
  <div><b>1</b><span>Read from PGMQ</span><small>The row becomes invisible for the visibility timeout.</small></div>
  <div><b>2</b><span>Transform</span><small>Failures are isolated and optionally dead-lettered.</small></div>
  <div><b>3</b><span>Publish</span><small>The relay waits for the broker-specific success boundary.</small></div>
  <div><b>4</b><span>Complete in PGMQ</span><small>Delivered rows are deleted or archived.</small></div>
</div>

If the relay crashes after publish succeeds but before PGMQ completion succeeds, the message becomes visible again and is published again.

This is intentional at-least-once behavior. Consumers must support duplicates.

## Broker success boundaries

| Broker mode | Considered successful when | Important limitation |
|---|---|---|
| Kafka transactions | Transaction commit succeeds | PGMQ completion is outside the Kafka transaction |
| Kafka without transactions | Individual delivery future succeeds | Partial batch success is possible |
| RabbitMQ | Publisher confirm succeeds | A timeout can leave acceptance uncertain |
| Core NATS | Connection flush succeeds | Flush is not durable storage |
| JetStream | Stream acknowledgement succeeds | The target stream must already exist and match the subject |

## Duplicate keys

Prefer a stable domain event identifier. The relay also adds `pgmq_msg_id` to message headers, but PGMQ IDs are only meaningful with their source queue.

Kafka transactions prevent partially committed Kafka transactions and fence stale producers. They do not provide end-to-end exactly-once delivery between PostgreSQL and Kafka.

## Poison messages

When transformation fails:

- With `dead_letter_queue`, the original message is inserted into that PGMQ queue and the source row is completed.
- Without `dead_letter_queue`, the source row remains and becomes visible after its timeout.

The dead-letter operation preserves the original headers and adds diagnostic metadata. See [Message Headers](./reference/headers.md).

## Pop mode: at most once

`fetch_mode = "pop"` removes rows while fetching them. A later transformation error, broker error, process crash, or network failure loses those messages.

The relay logs a startup warning for every `pop` queue. Use it only when loss is explicitly acceptable.
