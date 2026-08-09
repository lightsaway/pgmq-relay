# Add a Destination Broker

The relay's destinations are implementations of one trait. Adding a new destination (for example Pulsar, SQS, or Redis Streams) means implementing that trait, registering a config variant, and wiring the factory. This page walks through each step using the existing brokers as reference.

## The contract

Every destination implements `MessageBroker` (`src/broker.rs`):

```rust
#[async_trait]
pub trait MessageBroker: Send + Sync {
    async fn send_batch(
        &self,
        topic: &str,
        messages: &[RelayMessage],
    ) -> Result<SendResult, RelayError>;

    async fn health_check(&self) -> Result<(), RelayError>;
}
```

`send_batch` receives the destination topic and a batch of transformed messages, and returns a `SendResult`:

```rust
pub struct SendResult {
    pub successful_message_ids: Vec<i64>,   // completed (deleted/archived) in PGMQ
    pub failed_messages: Vec<(i64, String)>, // left in PGMQ, redelivered after the VT
}
```

The semantics of that return value are the heart of the relay's delivery guarantee:

- **Only report an ID successful after the broker has durably acknowledged it.** The relay deletes those messages from PGMQ immediately afterwards. A "successful" ID that the broker never persisted is silent data loss (this is why RabbitMQ uses publisher confirms with `mandatory: true`, NATS defaults to JetStream acknowledgements, and Kafka awaits delivery futures inside a transaction).
- **Report per-message failures in `failed_messages`, not as an `Err`.** Failed messages stay in the source queue and are retried after the visibility timeout. Partial success is normal.
- **Return `Err(...)` only for whole-batch failures** (connection lost, transaction aborted). The worker records the error, backs off exponentially, and retries the batch later.
- **When in doubt, fail.** An ambiguous outcome (timeout while awaiting an ack) must be reported as failed; a duplicate delivery is recoverable under at-least-once, a lost message is not.

`health_check` must actually round-trip to the broker (metadata fetch, flush/ping, connection status). It runs at worker startup — a failing check prevents the worker from starting — and every 15 seconds afterwards to keep the `broker_health_check_status` metric live. Make sure it fails when the client can no longer send (see the Kafka fatal-error check for an example of a broker that can fetch metadata while being unable to publish).

## Step by step

1. **Create `src/brokers/<name>.rs`** with:
   - A `<Name>Config` struct: `Deserialize`/`Serialize` + `Debug` + `Clone`, `#[serde(default = ...)]` for every field so minimal configs work, and environment-variable fallbacks for secrets (see `KafkaConfig::default`). Never log credential-bearing fields — pass URLs through `crate::logging::redact_url`.
   - An implementation of the `Validator` trait (`src/validator.rs`) with range/sanity checks and actionable error messages.
   - The broker struct and its `MessageBroker` implementation. Handle reconnection: a dropped connection must be repaired on the next `send_batch` (see the RabbitMQ pool) or by the client library itself (see the NATS reconnect settings), never left dead until process restart.

2. **Register the module** in `src/brokers/mod.rs`.

3. **Add a config variant** to `BrokerConfig` in `src/config.rs`:

   ```rust
   #[serde(rename = "<name>")]
   <Name>(crate::brokers::<name>::<Name>Config),
   ```

   and extend `broker_type()` plus the `validate_broker_config` match.

4. **Wire the factory** in `create_broker` (`src/broker.rs`). The `instance_id` parameter is the stable per-worker name — use it if your broker needs a stable, unique client identity across restarts (Kafka derives its `transactional.id` from it).

5. **Blocking clients:** if the client library makes blocking calls, wrap them in `tokio::task::spawn_blocking` so they cannot stall the async runtime (see the Kafka commit/metadata calls).

6. **Tests:** unit-test config parsing (including the minimal form), validation errors, and any pure helpers. Add an end-to-end scenario to `brokers_tests/` and a service to `docker-compose.yml` so the delivery path can be exercised against a real broker.

7. **Docs:** add a `site/src/brokers/<name>.md` page and list it in `site/src/SUMMARY.md`.

## What you get for free

The worker loop handles everything around your `send_batch`: polling and fetch modes, transformation and poison-message isolation, the dead-letter queue, completion of exactly the acknowledged IDs, retry pacing with backoff, shutdown draining, and per-batch metrics. Your implementation only has to move bytes and tell the truth about what was acknowledged.
