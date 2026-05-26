#!/usr/bin/env bash
set -euo pipefail

prepare_tpch_tmpfs() {
  rm -f /tmp/trino-bootstrap-ready
  rm -rf /mnt/tpch-tmpfs/join_scale /mnt/tpch-tmpfs/warehouse
  mkdir -p /mnt/tpch-tmpfs/join_scale /mnt/tpch-tmpfs/warehouse /data/trino/spill
  cp -R /opt/join-scale-source/. /mnt/tpch-tmpfs/join_scale/
}

bootstrap_trino() {
  : > /tmp/trino-bootstrap.log

  for _ in $(seq 1 180); do
    if curl -sf http://127.0.0.1:8080/v1/info >/dev/null 2>&1; then
      if /usr/bin/trino \
        --server http://127.0.0.1:8080 \
        --catalog tpch \
        --schema default \
        --file /opt/trino/bootstrap.sql >>/tmp/trino-bootstrap.log 2>&1 \
        && /usr/bin/trino \
          --server http://127.0.0.1:8080 \
          --catalog tpch \
          --schema sf1 \
          --execute "SELECT * FROM tpch.sf1.lineitem_25x LIMIT 1" >>/tmp/trino-bootstrap.log 2>&1 \
        && /usr/bin/trino \
          --server http://127.0.0.1:8080 \
          --catalog tpch \
          --schema sf1 \
          --execute "SELECT * FROM tpch.sf1.orders_scale_20 LIMIT 1" >>/tmp/trino-bootstrap.log 2>&1; then
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
