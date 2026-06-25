#!/usr/bin/env bash
set -euo pipefail

bootstrap_sql="/workspace/datafusion-bootstrap.sql"
ready_marker="/tmp/datafusion-bootstrap-ready"
host_ready_marker="/workspace/datafusion-ready"

stage_tpch_tables() {
  if [[ -d /opt/dbprove/table_data/tpch_sf1 ]]; then
    cp -R /opt/dbprove/table_data/tpch_sf1/. /mnt/tpch-tmpfs/tpch_sf1/
    return
  fi

  local tables=(region nation supplier customer part partsupp orders lineitem)
  for table in "${tables[@]}"; do
    cp "/opt/dbprove/table_data/${table}.parquet" "/mnt/tpch-tmpfs/tpch_sf1/${table}.parquet"
  done
}

stage_scale_tables() {
  if [[ -d /opt/dbprove/table_data/scale ]]; then
    cp -R /opt/dbprove/table_data/scale/. /mnt/tpch-tmpfs/scale/
  fi
}

prepare_tmpfs() {
  rm -f "${ready_marker}"
  rm -f "${host_ready_marker}"
  rm -rf /mnt/tpch-tmpfs/scale /mnt/tpch-tmpfs/tpch_sf1
  mkdir -p /mnt/tpch-tmpfs/scale /mnt/tpch-tmpfs/tpch_sf1 /workspace/datafusion-spill
  stage_scale_tables
  stage_tpch_tables
}

write_bootstrap_sql() {
  : > "${bootstrap_sql}"

  cat >> "${bootstrap_sql}" <<'SQL'
CREATE SCHEMA IF NOT EXISTS tpch_sf1;
CREATE SCHEMA IF NOT EXISTS scale;

CREATE EXTERNAL TABLE IF NOT EXISTS scale.lineitem_25x (
  l_orderkey BIGINT,
  l_suppkey BIGINT,
  l_linenumber BIGINT,
  lineitem_replica_id BIGINT
)
STORED AS PARQUET
LOCATION '/mnt/tpch-tmpfs/scale/lineitem_25x.parquet';

SQL

  for parquet_file in /mnt/tpch-tmpfs/scale/orders_scale_*.parquet; do
    [[ -f "${parquet_file}" ]] || continue
    table_name="$(basename "${parquet_file}" .parquet)"
    cat >> "${bootstrap_sql}" <<SQL
CREATE EXTERNAL TABLE IF NOT EXISTS scale.${table_name} (
  join_key BIGINT,
  o_orderkey BIGINT,
  orders_replica_id BIGINT,
  o_custkey BIGINT,
  o_orderstatus VARCHAR,
  o_totalprice DECIMAL(15,2),
  o_orderdate DATE,
  o_orderpriority VARCHAR,
  o_clerk VARCHAR,
  o_shippriority BIGINT,
  o_comment VARCHAR
)
STORED AS PARQUET
LOCATION '/mnt/tpch-tmpfs/scale/${table_name}.parquet';

SQL
  done

  cat >> "${bootstrap_sql}" <<'SQL'
SQL
}

prepare_tmpfs
write_bootstrap_sql
touch "${ready_marker}"
touch "${host_ready_marker}"
mkdir -p /workspace/datafusion-spill
export TMPDIR=/workspace/datafusion-spill

exec tail -f /dev/null
