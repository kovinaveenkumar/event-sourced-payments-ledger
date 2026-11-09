#!/usr/bin/env bash
# Rebuilds all the read data from the events in Kafka.
#
# I use it after fixing a bug in the processor, adding a new read table, or losing the
# read database. It never touches the event store, so no money data can be lost.
#
#   1. stop the processor
#   2. empty the read tables
#   3. move the consumer back to the first message
#   4. start the processor so it reads every event again
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/lib.sh

psql_read() { docker compose exec -T postgres psql -U ledger -d readmodel -tAc "$1"; }

before=$(psql_read "SELECT COUNT(*) || ' accounts, ' || COALESCE(SUM(balance_cents),0) || ' cents' FROM account_view")
info "Read model before replay: $before"

info "Stopping ledger-processor"
docker compose stop ledger-processor

info "Truncating projection tables"
psql_read "TRUNCATE account_view, account_transactions, fraud_alerts, dead_letters" > /dev/null

info "Resetting consumer group 'ledger-processor' to the earliest offset"
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 \
  --group ledger-processor --topic ledger-events --reset-offsets --to-earliest --execute

info "Starting ledger-processor"
docker compose start ledger-processor
wait_for_url "${PROCESSOR}/health" 120

info "Waiting for consumer lag to reach zero"
lag() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 \
    --describe --group ledger-processor 2>/dev/null | awk 'NR>1 && $6 ~ /^[0-9]+$/ {s+=$6} END {print s+0}'
}
lag_is_zero() { [[ "$(lag)" == "0" ]]; }
retry_until 120 lag_is_zero
sleep 2

after=$(psql_read "SELECT COUNT(*) || ' accounts, ' || COALESCE(SUM(balance_cents),0) || ' cents' FROM account_view")
info "Read model after replay:  $after"
if [[ "$before" == "$after" ]]; then
  green "Replay reproduced the same totals"
else
  red "Totals differ: investigate before trusting the read model"
  exit 1
fi
