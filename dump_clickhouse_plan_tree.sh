#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${BUILD_DIR:-$ROOT_DIR/build}"
ARTIFACTS_DIR_DEFAULT="$ROOT_DIR/run/artifacts/clickhouse"

usage() {
  cat <<'EOF'
Dump resolved ClickHouse PlanNode/ExpressionNode tree from an artifact.

Usage:
  ./dump_clickhouse_plan_tree.sh <QNN|TPCH-QNN|artifact.json> [--artifacts-dir DIR]

Examples:
  ./dump_clickhouse_plan_tree.sh Q01
  ./dump_clickhouse_plan_tree.sh TPCH-Q06
  ./dump_clickhouse_plan_tree.sh run/artifacts/clickhouse/TPCH-Q12.json
  ./dump_clickhouse_plan_tree.sh Q08 --artifacts-dir ./run/artifacts/clickhouse

Notes:
  - This builds target 'clickhouse_plan_tree_dump' if needed.
  - By default it looks under ./run/artifacts/clickhouse for TPCH artifacts.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || $# -lt 1 ]]; then
  usage
  exit 0
fi

if [[ ! -d "$BUILD_DIR" ]]; then
  echo "Build directory not found: $BUILD_DIR"
  echo "Run CMake configure first, e.g.: cmake -S . -B build"
  exit 1
fi

QUERY_OR_ARTIFACT="$1"
shift || true

ARTIFACTS_DIR="$ARTIFACTS_DIR_DEFAULT"
PASS_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --artifacts-dir)
      shift
      if [[ $# -eq 0 ]]; then
        echo "Missing value for --artifacts-dir"
        exit 1
      fi
      ARTIFACTS_DIR="$1"
      ;;
    *)
      PASS_ARGS+=("$1")
      ;;
  esac
  shift || true
done

echo "[1/2] Building clickhouse_plan_tree_dump..."
cmake --build "$BUILD_DIR" --target clickhouse_plan_tree_dump -j4

echo "[2/2] Running tree dump..."
CMD=(
  "$BUILD_DIR/src/sql/clickhouse/clickhouse_plan_tree_dump"
  "$QUERY_OR_ARTIFACT"
  --artifacts-dir "$ARTIFACTS_DIR"
)
if [[ ${#PASS_ARGS[@]} -gt 0 ]]; then
  CMD+=("${PASS_ARGS[@]}")
fi
"${CMD[@]}"
