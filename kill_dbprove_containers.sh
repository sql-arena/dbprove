#!/bin/bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
DOCKER_DIR="$PROJECT_ROOT/docker"

echo "Stopping dbprove compose services..."
(
  cd "$DOCKER_DIR"
  docker compose down --remove-orphans || true
  docker compose -p dbprove-scale down --remove-orphans || true
)

MANUAL_CONTAINERS=(
  "dbprove-clickhouse-manual"
)

for container in "${MANUAL_CONTAINERS[@]}"; do
  if docker ps -a --format '{{.Names}}' | grep -Fxq "$container"; then
    echo "Removing container: $container"
    docker rm -f "$container" >/dev/null
  fi
done

echo "dbprove container cleanup complete."
