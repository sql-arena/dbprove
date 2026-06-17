#!/usr/bin/env bash
set -euo pipefail

# This script runs during image initialization (docker-entrypoint-initdb.d)
# It appends an include directive to the default postgresql.conf
# so that our benchmark settings are loaded from /etc/postgresql/benchmark.conf

CONF_DIR="$PGDATA"
DEFAULT_CONF="$CONF_DIR/postgresql.conf"

# Ensure PGDATA exists (created by entrypoint) and is writable
mkdir -p "$CONF_DIR"
chmod 700 "$CONF_DIR"

# Append include only once
if ! grep -q "/etc/postgresql/benchmark.conf" "$DEFAULT_CONF" 2>/dev/null; then
  echo "include '/etc/postgresql/benchmark.conf'" >> "$DEFAULT_CONF"
fi

# Ensure host-based auth allows connections (adjust as needed for your environment)
HBA_FILE="$CONF_DIR/pg_hba.conf"
if ! grep -q "host all all 0.0.0.0/0 trust" "$HBA_FILE" 2>/dev/null; then
  echo "host all all 0.0.0.0/0 trust" >> "$HBA_FILE"
fi
if ! grep -q "host all all ::0/0 trust" "$HBA_FILE" 2>/dev/null; then
  echo "host all all ::0/0 trust" >> "$HBA_FILE"
fi
