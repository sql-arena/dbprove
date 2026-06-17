#!/usr/bin/env bash
set -euo pipefail

PGDATA="${PGDATA:-/var/lib/postgresql/data}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_DB="${POSTGRES_DB:-postgres}"
PG_BIN_DIR="$(pg_config --bindir)"

ensure_permissions() {
  mkdir -p "${PGDATA}"
  chown -R postgres:postgres /var/lib/postgresql
  chmod 700 "${PGDATA}"
}

initialize_cluster() {
  if [[ -s "${PGDATA}/PG_VERSION" ]]; then
    return
  fi

  runuser -u postgres -- "${PG_BIN_DIR}/initdb" -D "${PGDATA}" --auth-local=trust --auth-host=trust
}

configure_cluster() {
  local default_conf="${PGDATA}/postgresql.conf"
  local hba_file="${PGDATA}/pg_hba.conf"

  if ! grep -q "/etc/postgresql/benchmark.conf" "${default_conf}" 2>/dev/null; then
    echo "include '/etc/postgresql/benchmark.conf'" >> "${default_conf}"
  fi

  if ! grep -q "host all all 0.0.0.0/0 trust" "${hba_file}" 2>/dev/null; then
    echo "host all all 0.0.0.0/0 trust" >> "${hba_file}"
  fi

  if ! grep -q "host all all ::0/0 trust" "${hba_file}" 2>/dev/null; then
    echo "host all all ::0/0 trust" >> "${hba_file}"
  fi
}

bootstrap_database() {
  if [[ -f "${PGDATA}/.dbprove_bootstrapped" ]]; then
    return
  fi

  runuser -u postgres -- "${PG_BIN_DIR}/pg_ctl" -D "${PGDATA}" -o "-c listen_addresses=localhost" -w start

  if [[ "${POSTGRES_DB}" != "postgres" ]]; then
    runuser -u postgres -- psql -v ON_ERROR_STOP=1 --dbname postgres <<SQL
SELECT 'CREATE DATABASE "${POSTGRES_DB}"'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${POSTGRES_DB}')
\gexec
SQL
  fi

  touch "${PGDATA}/.dbprove_bootstrapped"
  runuser -u postgres -- "${PG_BIN_DIR}/pg_ctl" -D "${PGDATA}" -m fast -w stop
}

ensure_permissions
initialize_cluster
configure_cluster
bootstrap_database

exec runuser -u postgres -- "${PG_BIN_DIR}/postgres" -D "${PGDATA}"
