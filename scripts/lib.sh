#!/usr/bin/env bash
# Shared helpers for the scripts in this folder.

COMMAND_API="${COMMAND_API:-http://localhost:8081/api/v1}"
QUERY_API="${QUERY_API:-http://localhost:8082/api/v1}"
PROCESSOR="${PROCESSOR:-http://localhost:8083}"

red()   { printf '\033[31m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
info()  { printf '\033[36m==>\033[0m %s\n' "$*"; }

# json_field '<json>' 'python expression on d' -> prints the value
json_field() {
  python3 -c "import json,sys; d=json.loads(sys.argv[1]); print($2)" "$1"
}

# wait_for_url URL [timeout_seconds]
wait_for_url() {
  local url=$1 timeout=${2:-120} waited=0
  until curl -fsS "$url" > /dev/null 2>&1; do
    if (( waited >= timeout )); then
      red "Timed out after ${timeout}s waiting for $url"
      return 1
    fi
    sleep 2
    waited=$((waited + 2))
  done
}

# retry_until TIMEOUT_SECONDS COMMAND [ARGS...]: polls once a second until COMMAND succeeds
retry_until() {
  local timeout=$1 waited=0
  shift
  until "$@"; do
    if (( waited >= timeout )); then
      return 1
    fi
    sleep 1
    waited=$((waited + 1))
  done
}
