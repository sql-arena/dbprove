#!/usr/bin/env bash
set -euo pipefail

MSSQL_NVME_ROOT="${MSSQL_NVME_ROOT:-/mnt/nvme}"
MSSQL_ROOT="${MSSQL_ROOT:-${MSSQL_NVME_ROOT}/mssql}"
MSSQL_DATA_DIR="${MSSQL_DATA_DIR:-${MSSQL_ROOT}/data}"
MSSQL_LOG_DIR="${MSSQL_LOG_DIR:-${MSSQL_ROOT}/log}"
MSSQL_BACKUP_DIR="${MSSQL_BACKUP_DIR:-${MSSQL_ROOT}/backup}"
MSSQL_DUMP_DIR="${MSSQL_DUMP_DIR:-${MSSQL_ROOT}/dump}"
MSSQL_TCP_PORT="${MSSQL_TCP_PORT:-1433}"
MSSQL_PID="${MSSQL_PID:-Developer}"
MSSQL_USER="${MSSQL_USER:-sa}"
MSSQL_SA_PASSWORD="${MSSQL_SA_PASSWORD:-YourStrong!Passw0rd}"
ACCEPT_EULA="${ACCEPT_EULA:-Y}"
CONFIG_MARKER="/var/opt/mssql/.dbprove_configured"

prepare_dirs() {
  mkdir -p \
    "${MSSQL_NVME_ROOT}" \
    "${MSSQL_ROOT}" \
    "${MSSQL_DATA_DIR}" \
    "${MSSQL_LOG_DIR}" \
    "${MSSQL_BACKUP_DIR}" \
    "${MSSQL_DUMP_DIR}" \
    /var/opt/mssql
  chown -R mssql:mssql /var/opt/mssql "${MSSQL_ROOT}"
}

initial_setup() {
  if [[ -f "${CONFIG_MARKER}" ]]; then
    return
  fi

  export ACCEPT_EULA MSSQL_PID MSSQL_SA_PASSWORD MSSQL_TCP_PORT
  export MSSQL_DATA_DIR MSSQL_LOG_DIR MSSQL_BACKUP_DIR MSSQL_DUMP_DIR

  /opt/mssql/bin/mssql-conf -n setup
  touch "${CONFIG_MARKER}"
  chown mssql:mssql "${CONFIG_MARKER}"
}

prepare_dirs
initial_setup

if [[ "${MSSQL_USER}" != "sa" ]]; then
  echo "mssql/aws/native currently supports only the default SQL Server login 'sa'" >&2
  exit 1
fi

exec su -s /bin/bash mssql -c "/opt/mssql/bin/sqlservr"
