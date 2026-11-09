# Runbook

My notes for whoever is on call for this system. All services log JSON to stdout, so
`docker compose logs <service>` shows what's going on.

## Health checks and metrics

| Service | Health | Other |
|---|---|---|
| command-service | `GET :8081/actuator/health` | `/actuator/metrics` |
| ledger-processor | `GET :8083/health` returns status, database and consumer. 503 means DOWN | `GET :8083/metrics` shows events applied, duplicates skipped, dead letters, fraud alerts, last event time |
| query-service | `GET :8082/actuator/health` | `/actuator/metrics` |

Commands I use:

```bash
# How far behind the processor is (the LAG column should go down to 0)
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:19092 --describe --group ledger-processor

# Events saved but not yet sent to Kafka
docker compose exec postgres psql -U ledger -d eventstore -c \
  "SELECT COUNT(*), MIN(occurred_at) FROM events WHERE published_at IS NULL;"

# Latest dead letters
docker compose exec postgres psql -U ledger -d readmodel -c \
  "SELECT id, partition_no, offset_no, reason, created_at FROM dead_letters ORDER BY id DESC LIMIT 20;"
```

## Common problems

### The dashboard is behind (balances don't show new payments)

1. Check how many events are waiting to be sent (second command above).
   - If that number keeps growing, the command service can't reach Kafka. Look for
     "Outbox publish failed" in `docker compose logs command-service` and check Kafka.
     The events are safe in Postgres and will be sent once Kafka is back.
2. Check the consumer lag (first command above).
   - Lag growing and processor health shows `database:false`: the read database is down.
     The processor keeps retrying (1 second up to 30 seconds) and continues from where it
     stopped. Nothing to do except bring the database back.
   - Lag growing but the processor is UP: it's slow. Check its logs for slow queries.
3. If someone needs the exact balance right now, use
   `GET :8081/api/v1/accounts/{id}/state`, which reads the events directly.

### Dead letters showing up

- `invalid event: ...` means something sent a bad message. The `reason` column lists
  everything wrong with it. Fix the sender. The bad message is skipped so other messages
  keep flowing.
- `version gap for account ...` means an event arrived before an earlier one for the same
  account. This shouldn't happen because messages are keyed by account. Check if the topic
  was recreated or events were sent out of order. After fixing it, run a replay (below).

### Lots of fraud alerts

The dashboard and the `fraud_alerts` table show the rule, the account and the payment that
caused it. Alerts don't block payments. The limits are in
`ledger-processor/src/main/resources/application.conf` under `ledger.fraud`.

### Clients getting 409 CONCURRENT_MODIFICATION

The command service retries a clash on the same account up to 6 times, with a short random
wait, before returning 409. A few under heavy load on one account are normal, and the client
should retry with the same `Idempotency-Key`. If it happens a lot, one account is very busy.

## How to do things

### Rebuild the read data (replay)

```bash
./scripts/replay-projections.sh
```

It stops the processor, empties the read tables, moves the Kafka consumer back to the
start, starts the processor again, waits until it has caught up, and then checks that the
account count and total balance match what was there before. The event store is never
touched.

### Restart one service

```bash
docker compose restart ledger-processor
```

The processor continues from its last committed position and reloads the last 10 minutes of
payments for the fraud windows.

### Start fresh (local only, deletes all data)

```bash
./scripts/run-local.sh --clean
```
