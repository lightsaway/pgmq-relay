# Architecture

Configuration defines queues. Each queue creates one or more workers according to `parallelism`. Every worker owns a broker client and repeatedly fetches, transforms, publishes, and completes messages.

<div class="flow" role="img" aria-label="PGMQ source queue flows through relay workers to Kafka, RabbitMQ, or NATS, then successful messages are deleted or archived in PGMQ">
  <div class="flow-node"><strong>PostgreSQL + PGMQ</strong><span>source queue</span></div>
  <div class="flow-arrow">fetch batch</div>
  <div class="flow-node"><strong>Relay worker</strong><span>transform and publish</span></div>
  <div class="flow-arrow">broker success</div>
  <div class="flow-node"><strong>Destination broker</strong><span>Kafka, RabbitMQ, or NATS</span></div>
  <div class="flow-arrow flow-return">complete source</div>
  <div class="flow-node"><strong>PGMQ completion</strong><span>delete, archive, or dead-letter</span></div>
</div>

## Runtime ownership

- The process supervises every worker and exits if any worker exits unexpectedly.
- Metrics, uptime collection, readiness polling, and broker health polling are owned by the process lifecycle.
- Shutdown signals stop workers and background tasks within `--shutdown-timeout`, which defaults to 30 seconds.
- Each worker receives one queue configuration. `parallelism = 3` creates three independent workers and broker clients.

## Processing boundary

The relay does not run a distributed transaction across PostgreSQL and the broker:

1. PGMQ marks fetched rows invisible for the visibility timeout.
2. The relay publishes the transformed batch.
3. The broker reports success.
4. The relay deletes or archives successful source rows.

A crash between steps 3 and 4 creates a duplicate after the visibility timeout. See [Delivery Guarantees](./delivery-semantics.md).

## Failure isolation

Transformation failures are isolated per message. Successfully transformed messages in the same batch can still be delivered. A configured `dead_letter_queue` receives the original message and diagnostic headers.
