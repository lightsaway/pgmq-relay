# Fetch Modes

Fetch mode controls which PGMQ function a worker calls.

| Mode | Waits when empty | Group aware | Selection behavior | Delivery |
|---|---:|---:|---|---|
| `regular` | No | No | Standard queue read | At least once |
| `read_with_poll` | Yes | No | Standard read with server-side polling | At least once |
| `read_grouped` | No | Yes | Fills from the earliest available group | At least once |
| `read_grouped_with_poll` | Yes | Yes | Grouped read with polling | At least once |
| `read_grouped_rr` | No | Yes | Fairer round-robin selection across groups | At least once |
| `read_grouped_rr_with_poll` | Yes | Yes | Round-robin grouped read with polling | At least once |
| `read_grouped_head` | No | Yes | At most one visible head per group | At least once |
| `read_grouped_head_with_poll` | Yes | Yes | Group-head read with polling | At least once |
| `pop` | No | No | Reads and deletes immediately | At most once |

## Selection guide

- Use `regular` for active, unordered queues.
- Use `read_with_poll` for lower-volume queues where reducing empty queries matters.
- Use `read_grouped` when draining one available group efficiently is desirable.
- Use `read_grouped_rr` for multi-tenant fairness across groups.
- Use `read_grouped_head` to expose concurrency across many groups while taking one head message from each.
- Use `pop` only for explicitly disposable events. Pop queues must configure a `dead_letter_queue` (validation enforces it) so messages that fail transformation or broker delivery are preserved.

## Long-poll settings

Polling variants use:

```toml
fetch_mode = "read_with_poll"
max_poll_seconds = 5
poll_interval_ms = 100
```

`max_poll_seconds` must be from 1 to 60. `poll_interval_ms` must be from 1 to 1000.

SQLx can log these intentional waits as slow statements. A five-second `read_with_poll` query returning no rows is expected behavior.
