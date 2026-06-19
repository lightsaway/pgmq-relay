# Deploy

## Container example

```bash
docker run -d \
  --name pgmq-relay \
  --restart unless-stopped \
  -p 9090:9090 \
  -e RUST_LOG=pgmq_relay=info \
  -e LOG_FORMAT=json \
  -v /etc/pgmq-relay/config.toml:/etc/pgmq-relay/config.toml:ro \
  -v /etc/pgmq-relay/certs:/certs:ro \
  pgmq-relay:local
```

Replace `pgmq-relay:local` with a fixed GHCR release tag after the project publishes its first versioned release:

```text
ghcr.io/lightsaway/pgmq-relay:0.1.0
```

Do not deploy `latest` when reproducible rollback matters.

## Kubernetes probe example

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 9090
readinessProbe:
  httpGet:
    path: /ready
    port: 9090
```

Do not use `/health` as the readiness probe. It intentionally reports process liveness, while `/ready` reports whether workers started and PGMQ completion is currently permitted.

## Shutdown

The relay handles Ctrl+C/SIGINT and attempts coordinated shutdown:

```bash
pgmq-relay --config config.toml --shutdown-timeout 30
```

Container orchestrators should provide at least the same termination grace period.

## Capacity considerations

- Each queue creates `parallelism` workers.
- Each worker owns a broker connection.
- RabbitMQ creates `pool_size` channels per worker.
- PostgreSQL connections are shared through a pool capped by `pgmq.max_connections`.
- Long-polling calls can hold PostgreSQL connections while waiting.

Increase parallelism only after measuring broker latency, PostgreSQL connection use, and duplicate behavior.

## Secrets

Configuration contains database and broker credentials. Mount it as a secret or generate it at deployment time. Restrict file permissions and avoid logging full connection URLs.
