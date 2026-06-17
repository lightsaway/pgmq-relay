# Delivery Semantics

PGMQ Relay normally provides at-least-once delivery.

For non-`pop` fetch modes, a worker:

1. Reads messages from PGMQ with a visibility timeout.
2. Transforms each message.
3. Sends transformed messages to the broker.
4. Waits for broker acknowledgement, confirm, flush, or transaction commit.
5. Deletes or archives only messages that were delivered or dead-lettered.

If the process crashes after broker delivery but before PGMQ completion, PGMQ will redeliver the message after the visibility timeout. Downstream consumers should be idempotent or deduplicate by message key or relay headers.

## Poison Messages

If transformation fails, the message is isolated from the rest of the batch.

- With `dead_letter_queue` configured, the relay sends the original payload to that queue and then completes the source message.
- Without `dead_letter_queue`, the source message is left in PGMQ and retried after the visibility timeout.

## Pop Mode

`fetch_mode = "pop"` uses `pgmq.pop()`, which removes messages during fetch. If transformation, broker delivery, or the relay process fails afterward, those messages cannot be retried by PGMQ.

The relay logs a startup warning when a queue uses `pop`.
