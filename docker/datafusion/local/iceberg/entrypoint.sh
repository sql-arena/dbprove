#!/usr/bin/env bash
set -euo pipefail

bootstrap_sql="/workspace/datafusion-bootstrap.sql"
ready_marker="/tmp/datafusion-bootstrap-ready"
host_ready_marker="/workspace/datafusion-ready"
tpch_root="${DATAFUSION_TPCH_ROOT:-/opt/tpch-source/sf1}"

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

prepare_tmpfs() {
  rm -f "${ready_marker}"
  rm -f "${host_ready_marker}"
  rm -rf /mnt/tpch-tmpfs/join_scale /mnt/tpch-tmpfs/tpch_sf1
  mkdir -p /mnt/tpch-tmpfs/join_scale /mnt/tpch-tmpfs/tpch_sf1 /workspace/datafusion-spill
  cp -R /opt/join-scale-source/. /mnt/tpch-tmpfs/join_scale/
  stage_tpch_tables
}

write_bootstrap_sql() {
  : > "${bootstrap_sql}"

  cat >> "${bootstrap_sql}" <<'SQL'
CREATE SCHEMA IF NOT EXISTS tpch_sf1;

CREATE EXTERNAL TABLE IF NOT EXISTS tpch_sf1.lineitem_25x (
  l_orderkey BIGINT,
  l_suppkey BIGINT,
  l_linenumber BIGINT,
  lineitem_replica_id BIGINT
)
STORED AS PARQUET
LOCATION '/mnt/tpch-tmpfs/join_scale/lineitem_25x/lineitem_25x.parquet';

SQL

  for parquet_dir in /mnt/tpch-tmpfs/join_scale/orders_scale_*; do
    [[ -d "${parquet_dir}" ]] || continue
    table_name="$(basename "${parquet_dir}")"
    cat >> "${bootstrap_sql}" <<SQL
CREATE EXTERNAL TABLE IF NOT EXISTS tpch_sf1.${table_name} (
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
LOCATION '/mnt/tpch-tmpfs/join_scale/${table_name}/${table_name}.parquet';

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
