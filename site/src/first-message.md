# Send Your First Message

This walkthrough uses the Compose stack and verifies the Kafka route.

## Publish to PGMQ

Insert a uniquely identifiable message:

```bash
docker compose exec -T postgres \
  psql -U postgres -d pgmq_relay -c \
  "SELECT pgmq.send(
     'user_events',
     '{\"event_id\":\"docs-001\",\"user_id\":\"user-42\",\"event\":\"signed_in\"}'::jsonb
   );"
```

PGMQ returns the source message ID.

## Verify Kafka delivery

Consume the topic from Redpanda:

```bash
docker compose exec -T redpanda \
  rpk topic consume events.users --offset start --num 10
```

Find the payload containing `"event_id":"docs-001"`. Its Kafka key should be `user-42`, because the Compose queue uses `key_field = "user_id"`.

## Verify PGMQ completion

The source row should be gone:

```bash
docker compose exec -T postgres \
  psql -U postgres -d pgmq_relay -c \
  "SELECT count(*) FROM pgmq.q_user_events
   WHERE message->>'event_id' = 'docs-001';"
```

Expected result:

```text
 count
-------
     0
```

## Inspect relay evidence

```bash
docker compose logs --since=5m pgmq-relay
```

Look for:

- a batch read from `user_events`
- successful Kafka send or transaction commit
- successful PGMQ deletion

Broker receipt alone is not the full success condition. The source message must also be deleted or archived.
