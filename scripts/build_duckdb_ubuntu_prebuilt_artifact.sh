#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ARTIFACT_DIR="$ROOT/artifacts/duckdb-ubuntu-prebuilt-arm64"
RUNTIME_IMAGE_TAG="dbprove-duckdb-bench:latest"
LOG_FILE="$ARTIFACT_DIR/build.log"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_LOG_FILE="$ARTIFACT_DIR/build-$TIMESTAMP.log"

if [[ "${1:-}" != "--force" ]] && docker image inspect "$RUNTIME_IMAGE_TAG" >/dev/null 2>&1; then
  echo "DuckDB runtime image already available: $RUNTIME_IMAGE_TAG"
  exit 0
fi

mkdir -p "$ARTIFACT_DIR"
rm -f "$LOG_FILE"
touch "$LOG_FILE"
ln -sf "$(basename "$RUN_LOG_FILE")" "$ARTIFACT_DIR/build-latest.log"

exec > >(tee "$LOG_FILE" "$RUN_LOG_FILE") 2>&1

echo "DuckDB Ubuntu image build log: $LOG_FILE"
echo "DuckDB Ubuntu timestamped log: $RUN_LOG_FILE"

docker build --progress=plain -f "$ROOT/docker/duckdb/Dockerfile" -t "$RUNTIME_IMAGE_TAG" "$ROOT"
