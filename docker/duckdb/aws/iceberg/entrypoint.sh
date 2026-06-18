#!/usr/bin/env bash
set -euo pipefail

DUCKDB_NVME_ROOT="${DUCKDB_NVME_ROOT:-/mnt/nvme}"
DUCKDB_HOME="${DUCKDB_HOME:-${DUCKDB_NVME_ROOT}/duckdb}"

mkdir -p "${DUCKDB_NVME_ROOT}" "${DUCKDB_HOME}"
cd "${DUCKDB_HOME}"

exec "$@"
