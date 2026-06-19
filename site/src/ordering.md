# Ordering and Parallelism

Ordering depends on PGMQ fetch mode, worker parallelism, broker partitioning, and duplicate handling.

## Worker parallelism

`parallelism` creates independent workers against the same PGMQ queue.

```toml
[[queues]]
queue_name = "orders"
parallelism = 3
```

This improves throughput but permits concurrent batches and therefore does not provide global ordering.

Use `parallelism = 1` when strict processing order is more important than throughput.

## Group ordering

Grouped fetch modes use the PGMQ header `x-pgmq-group`. Messages in the same group are selected according to the grouped function’s FIFO rules.

```sql
SELECT pgmq.send(
  'orders',
  '{"order_id":"order-42","state":"paid"}'::jsonb,
  '{"x-pgmq-group":"customer-7"}'::jsonb
);
```

Set the broker key to the same group:

```toml
key_field = "x-pgmq-group"
```

This keeps a group on one Kafka partition or RabbitMQ routing key. For NATS, enable `use_key_as_subject_suffix` only when subscribers are designed for subjects such as `orders.customer-7`.

## Redelivery and order

At-least-once redelivery can repeat an older event after a newer event was already observed. Consumers that require ordered state transitions should track sequence numbers or versions, not rely only on broker arrival order.
