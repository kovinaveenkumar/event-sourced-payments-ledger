#!/usr/bin/env bash
# Builds and starts everything, then waits until all services are healthy.
#   ./scripts/run-local.sh          start (builds images if needed)
#   ./scripts/run-local.sh --clean  wipe volumes first for a fresh ledger
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/lib.sh

command -v docker > /dev/null || { red "Docker is required"; exit 1; }
docker info > /dev/null 2>&1 || { red "Docker daemon is not running"; exit 1; }

if [[ "${1:-}" == "--clean" ]]; then
  info "Removing containers and volumes"
  docker compose down -v --remove-orphans
fi

info "Building and starting services"
docker compose up -d --build

info "Waiting for health checks"
wait_for_url "http://localhost:8081/actuator/health" 180
wait_for_url "${PROCESSOR}/health" 180
wait_for_url "http://localhost:8082/actuator/health" 180

green "All services are up"
cat <<EOF

  Dashboard          http://localhost:8082
  Command API docs   http://localhost:8081/swagger-ui.html
  Query API docs     http://localhost:8082/swagger-ui.html
  Processor health   ${PROCESSOR}/health   metrics: ${PROCESSOR}/metrics

  Next: ./scripts/smoke-test.sh   or   python3 scripts/load_gen.py
EOF
