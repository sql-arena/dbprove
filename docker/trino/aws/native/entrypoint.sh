#!/usr/bin/env bash
set -euo pipefail

readonly NVME_ROOT="${TRINO_NVME_ROOT:-/mnt/nvme}"
readonly METASTORE_DB_DIR="${NVME_ROOT}/metastore_db"
readonly METASTORE_LOG="/tmp/hive-metastore.log"
readonly METASTORE_PORT="${TRINO_METASTORE_PORT:-9083}"
readonly WAREHOUSE_DIR="${NVME_ROOT}/warehouse"
readonly SPILL_DIR="${NVME_ROOT}/spill"
readonly READY_MARKER="/tmp/trino-bootstrap-ready"
readonly DBPROVE_ROOT="/opt/dbprove/bin"
readonly HIVE_JAVA_HOME="/opt/hive-java"
readonly PATH="${DBPROVE_ROOT}:${PATH}"

prepare_nvme() {
  rm -f "${READY_MARKER}" "${METASTORE_LOG}"
  mkdir -p "${METASTORE_DB_DIR}" "${WAREHOUSE_DIR}" "${SPILL_DIR}"
}

initialize_metastore_schema() {
  if [[ ! -d "${METASTORE_DB_DIR}/seg0" ]]; then
    JAVA_HOME="${HIVE_JAVA_HOME}" PATH="${HIVE_JAVA_HOME}/bin:${PATH}" \
      /opt/hive/bin/schematool -dbType derby -initSchema
  fi
}

start_metastore() {
  : >"${METASTORE_LOG}"
  export HIVE_HOME=/opt/hive
  export HIVE_CONF_DIR=/opt/hive/conf
  export METASTORE_PORT
  JAVA_HOME="${HIVE_JAVA_HOME}" PATH="${HIVE_JAVA_HOME}/bin:${PATH}" \
    /opt/hive/bin/hive --service metastore >>"${METASTORE_LOG}" 2>&1 &
  METASTORE_PID=$!
}

start_readiness_probe() {
  (
    for _ in $(seq 1 180); do
      if bash -lc "exec 3<>/dev/tcp/127.0.0.1/${METASTORE_PORT}" >/dev/null 2>&1 \
        && curl -sf http://127.0.0.1:8080/v1/info >/dev/null 2>&1; then
        touch "${READY_MARKER}"
        exit 0
      fi
      sleep 1
    done
    cat "${METASTORE_LOG}" >&2 || true
    exit 1
  ) &
}

stop_children() {
  if [[ -n "${METASTORE_PID:-}" ]]; then
    kill "${METASTORE_PID}" >/dev/null 2>&1 || true
  fi
}

trap stop_children EXIT

prepare_nvme
initialize_metastore_schema
start_metastore
start_readiness_probe

exec /usr/lib/trino/bin/run-trino
