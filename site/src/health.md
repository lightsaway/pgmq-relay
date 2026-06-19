# Health and Readiness

Endpoints are available when metrics are enabled.

| Endpoint | Success | Failure | Meaning |
|---|---|---|---|
| `/health` | `200 OK` / `OK` | Currently never fails while the server responds | Process liveness |
| `/ready` | `200 OK` / `Ready` | `503` / `Not Ready` | Workers started and PGMQ completion circuit permits calls |
| `/metrics` | `200` Prometheus text | `500` on encoding failure | Metrics scrape |

Default bind address:

```text
0.0.0.0:9090
```

## What readiness includes

Readiness becomes true after all configured workers start and the PGMQ completion circuit breaker permits delete/archive operations.

## What readiness does not include

Broker health polling updates broker metrics every 15 seconds, but broker health is not currently part of `/ready`.

This distinction matters operationally:

- A broker outage can leave `/ready` returning `200`.
- Alert on `pgmq_relay_broker_health_check_status` in addition to probing `/ready`.
- Worker failure is fatal and causes process shutdown.

## Recommended probes

Use `/health` for liveness and `/ready` for readiness. Use explicit IPv4 loopback in Alpine-based containers:

```bash
wget -q --spider http://127.0.0.1:9090/health
```
