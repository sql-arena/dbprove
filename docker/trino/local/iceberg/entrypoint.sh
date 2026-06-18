#!/usr/bin/env bash
set -euo pipefail

stage_tpch_tables() {
  if [[ -d /opt/table-data-source/tpch_sf1 ]]; then
    cp -R /opt/table-data-source/tpch_sf1/. /mnt/tpch-tmpfs/tpch_sf1/
    return
  fi

  local tables=(region nation supplier customer part partsupp orders lineitem)
  for table in "${tables[@]}"; do
    cp "/opt/table-data-source/${table}.parquet" "/mnt/tpch-tmpfs/tpch_sf1/${table}.parquet"
  done
}

prepare_tpch_tmpfs() {
  rm -f /tmp/trino-bootstrap-ready
  rm -rf /mnt/tpch-tmpfs/join_scale /mnt/tpch-tmpfs/warehouse /mnt/tpch-tmpfs/tpch_sf1
  mkdir -p /mnt/tpch-tmpfs/join_scale /mnt/tpch-tmpfs/warehouse /mnt/tpch-tmpfs/tpch_sf1 /data/trino/spill
  cp -R /opt/join-scale-source/. /mnt/tpch-tmpfs/join_scale/
  stage_tpch_tables
}

bootstrap_trino() {
  : > /tmp/trino-bootstrap.log

  for _ in $(seq 1 180); do
    if curl -sf http://127.0.0.1:8080/v1/info >/dev/null 2>&1; then
      if /usr/bin/trino \
        --server http://127.0.0.1:8080 \
        --catalog tpch \
        --schema default \
        --execute "SELECT 1" >>/tmp/trino-bootstrap.log 2>&1; then
        return 0
      fi
    fi
    sleep 1
  done

  cat /tmp/trino-bootstrap.log >&2 || true
  return 1
}

prepare_tpch_tmpfs
(bootstrap_trino && touch /tmp/trino-bootstrap-ready) &

exec /usr/lib/trino/bin/run-trino
