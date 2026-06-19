# Metrics and Alerts

All metric names begin with `pgmq_relay_`.

## Primary metrics

| Metric | Type | Important labels | Meaning |
|---|---|---|---|
| `pgmq_relay_worker_operation_total` | Counter | worker, operation, queue, topic | Messages polled or completed |
| `pgmq_relay_worker_operation_errors_total` | Counter | operation, queue, error type | Worker failures |
| `pgmq_relay_broker_messages_sent_total` | Counter | broker, type, topic | Broker-acknowledged messages |
| `pgmq_relay_broker_messages_failed_total` | Counter | broker, type, topic, error type | Failed publishes |
| `pgmq_relay_broker_send_duration_seconds` | Histogram | broker, type, topic | Batch send latency |
| `pgmq_relay_broker_health_check_status` | Gauge | broker, type | `1` healthy, `0` unhealthy |
| `pgmq_relay_messages_delivered_but_not_completed_total` | Counter | worker, queue, topic, error type | Duplicate-risk events |
| `pgmq_relay_circuit_breaker_state` | Gauge | name, type | `0` closed, `1` open, `2` half-open |
| `pgmq_relay_workers_active` | Gauge | none | Running workers |

`worker_operation_total` counts messages. The matching histogram `_count` counts operation cycles.

## Useful PromQL

Delivery throughput:

```promql
sum(rate(pgmq_relay_broker_messages_sent_total[5m]))
  by (broker_name, topic)
```

Broker failures:

```promql
sum(rate(pgmq_relay_broker_messages_failed_total[5m]))
  by (broker_name, error_type)
```

PGMQ completion failures after broker success:

```promql
increase(pgmq_relay_messages_delivered_but_not_completed_total[10m]) > 0
```

Broker health:

```promql
pgmq_relay_broker_health_check_status == 0
```

Open or half-open completion circuit:

```promql
pgmq_relay_circuit_breaker_state > 0
```

## Minimum alerts

Create alerts for:

1. Any increase in delivered-but-not-completed messages.
2. Broker health equal to zero for more than one health interval.
3. Circuit breaker state above zero.
4. No active workers.
5. Sustained broker failure ratio.

The delivered-but-not-completed counter is the highest-signal duplicate-risk alert.
