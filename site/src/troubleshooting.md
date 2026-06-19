# Troubleshooting

## Relay does not start

Validate configuration first:

```bash
pgmq-relay --config config.toml --validate-config
```

Common causes:

- Missing or unknown `default_broker`
- Duplicate queue names
- Invalid PostgreSQL URL
- Queue values outside validation ranges
- Broker authentication or TLS failure
- Kafka transaction initialization failure
- NATS initial connection timeout

## `/ready` returns 503

Workers may still be starting, or the PGMQ completion circuit breaker is open.

Check:

```bash
curl -i http://127.0.0.1:9090/ready
curl -s http://127.0.0.1:9090/metrics | grep pgmq_relay_circuit_breaker
```

## Broker is down but `/ready` is 200

This is currently expected. Broker health is exposed through:

```promql
pgmq_relay_broker_health_check_status
```

Use that metric for broker alerts.

## Messages are duplicated

Duplicates are expected after:

- process failure after broker acknowledgement
- PGMQ delete/archive failure
- RabbitMQ confirmation timeout with uncertain broker acceptance
- visibility timeout expiring before a slow worker completes

Check `pgmq_relay_messages_delivered_but_not_completed_total`. Increase visibility timeout where appropriate and make consumers idempotent.

## Messages remain in PGMQ

Inspect:

- broker send errors
- transformation errors
- dead-letter queue existence
- PGMQ completion errors
- circuit-breaker state
- whether the message is still invisible because its visibility timeout has not expired

## RabbitMQ receives no routed messages

The relay declares an exchange, not queues or bindings. Verify the destination queue is bound to the exchange and that its binding matches the extracted message key used as routing key.

## NATS subscribers miss messages

Core NATS does not retain messages for offline subscribers. Use JetStream with a matching stream for durable delivery.

## SQLx reports slow queries

Long-poll fetch modes intentionally wait up to `max_poll_seconds`. A slow-query warning with zero returned rows can be normal.

## Container reports unhealthy

Probe `127.0.0.1`, not `localhost`, when the listener binds IPv4:

```bash
wget -qO- http://127.0.0.1:9090/health
```
