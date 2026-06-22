#!/usr/bin/env bash
set -euo pipefail

mkdir -p /mnt/tpch-tmpfs
mkdir -p /mnt/tpch-tmpfs/scale
if [[ -d /opt/dbprove/table_data/scale ]]; then
  cp -R /opt/dbprove/table_data/scale/. /mnt/tpch-tmpfs/scale/
fi

exec "$@"
