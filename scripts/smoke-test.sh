#!/usr/bin/env bash
# End-to-end test through all three services: command API, Kafka, processor, query API.
# Runs every check and exits with an error if any failed, so CI can use it.
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/lib.sh

failures=0
check() {
  local name=$1 expected=$2 actual=$3
  if [[ "$expected" == "$actual" ]]; then
    green "  PASS  $name"
  else
    red "  FAIL  $name (expected '$expected', got '$actual')"
    failures=$((failures + 1))
  fi
}

post() {  # post PATH BODY [IDEMPOTENCY_KEY] -> prints "<status> <body>"
  local headers=(-H 'Content-Type: application/json')
  [[ -n "${3:-}" ]] && headers+=(-H "Idempotency-Key: $3")
  curl -s -o /tmp/ledger_body.$$ -w '%{http_code}' "${headers[@]}" -X POST -d "$2" "${COMMAND_API}$1"
  printf ' '
  cat /tmp/ledger_body.$$
  rm -f /tmp/ledger_body.$$
}

balance() { json_field "$(curl -fsS "${QUERY_API}/accounts/$1")" "d['balance']"; }

run_id=$(date +%s)
info "Opening accounts"
alice=$(json_field "$(post /accounts '{"owner":"Alice '"$run_id"'","currency":"USD","initialDeposit":1000.00}' | cut -d' ' -f2-)" "d['id']")
bob=$(json_field "$(post /accounts '{"owner":"Bob '"$run_id"'","currency":"USD","initialDeposit":0}' | cut -d' ' -f2-)" "d['id']")
echo "  alice=$alice bob=$bob"

info "Transfer 250.00 Alice -> Bob, sent twice with the same Idempotency-Key"
r1=$(post /transfers '{"fromAccountId":"'"$alice"'","toAccountId":"'"$bob"'","amount":250.00}' "smoke-$run_id")
r2=$(post /transfers '{"fromAccountId":"'"$alice"'","toAccountId":"'"$bob"'","amount":250.00}' "smoke-$run_id")
check "first transfer accepted" "202" "${r1%% *}"
check "retry returns the same transfer id" "$(json_field "${r1#* }" "d['id']")" "$(json_field "${r2#* }" "d['id']")"

info "Overdraft is rejected"
r3=$(post /transfers '{"fromAccountId":"'"$bob"'","toAccountId":"'"$alice"'","amount":999.00}')
check "overdraft status" "422" "${r3%% *}"
check "overdraft code" "INSUFFICIENT_FUNDS" "$(json_field "${r3#* }" "d['code']")"

info "Validation errors are 400"
r4=$(post /accounts '{"owner":"","currency":"XX"}')
check "invalid account request" "400" "${r4%% *}"

info "Read side catches up (eventual consistency)"
balance_is() { [[ "$(balance "$1" 2>/dev/null)" == "$2" ]]; }
retry_until 30 balance_is "$bob" "250.0" || true
check "Alice balance on read side" "750.0" "$(balance "$alice")"
check "Bob balance on read side" "250.0" "$(balance "$bob")"

info "Write side replay agrees with the read side"
check "Alice replayed state" "750.0" "$(json_field "$(curl -fsS "${COMMAND_API}/accounts/$alice/state")" "d['balance']")"

info "Six quick payments trigger the VELOCITY fraud rule"
for _ in 1 2 3 4 5 6; do
  post /transfers '{"fromAccountId":"'"$alice"'","toAccountId":"'"$bob"'","amount":1.00}' > /dev/null
done
has_alert() {
  json_field "$(curl -fsS "${QUERY_API}/alerts?limit=200")" \
    "any(a['accountId']=='$alice' and a['rule']=='VELOCITY' for a in d)"
}
alert_raised() { [[ "$(has_alert)" == "True" ]]; }
retry_until 30 alert_raised || true
check "velocity alert raised for Alice" "True" "$(has_alert)"

info "The alert is also published to the fraud-alerts Kafka topic"
alert_on_topic() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 \
    --topic fraud-alerts --from-beginning --timeout-ms 5000 2>/dev/null | grep "$alice" | grep -q VELOCITY
}
if retry_until 30 alert_on_topic; then on_topic=yes; else on_topic=no; fi
check "velocity alert on fraud-alerts topic" "yes" "$on_topic"

info "Processor metrics"
curl -fsS "${PROCESSOR}/metrics"; echo

if (( failures > 0 )); then
  red "$failures check(s) failed"
  exit 1
fi
green "All smoke checks passed"
