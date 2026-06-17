# Operations

## Health And Metrics

When metrics are enabled, the relay exposes:

| Endpoint | Description |
|---|---|
| `/metrics` | Prometheus metrics. |
| `/health` | Liveness probe. |
| `/ready` | Readiness probe. |

Default address:

```text
0.0.0.0:9090
```

## Useful Commands

```bash
make test
make clippy
make build-release
make dev-up
make dev-logs
make dev-down
```

## GitHub Pages

The static documentation site is built with `mdBook` and published by the `pages.yml` workflow.

Repository settings must use GitHub Pages source `GitHub Actions`.
