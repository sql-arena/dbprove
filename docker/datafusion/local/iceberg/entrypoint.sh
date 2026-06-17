#!/usr/bin/env bash
set -euo pipefail

bootstrap_sql="/workspace/datafusion-bootstrap.sql"
ready_marker="/tmp/datafusion-bootstrap-ready"
host_ready_marker="/workspace/datafusion-ready"
tpch_root="${DATAFUSION_TPCH_ROOT:-/opt/tpch-source/sf1}"

prepare_tmpfs() {
  rm -f "${ready_marker}"
  rm -f "${host_ready_marker}"
  rm -rf /mnt/tpch-tmpfs/join_scale
  mkdir -p /mnt/tpch-tmpfs/join_scale /workspace/datafusion-spill
  cp -R /opt/join-scale-source/. /mnt/tpch-tmpfs/join_scale/
}

write_bootstrap_sql() {
  : > "${bootstrap_sql}"

  cat >> "${bootstrap_sql}" <<'SQL'
CREATE EXTERNAL TABLE IF NOT EXISTS lineitem_25x (
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
CREATE EXTERNAL TABLE IF NOT EXISTS ${table_name} (
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
CREATE EXTERNAL TABLE IF NOT EXISTS region (
  r_regionkey INT,
  r_name VARCHAR,
  r_comment VARCHAR
)
STORED AS PARQUET
LOCATION '${tpch_root}/region.parquet';

CREATE EXTERNAL TABLE IF NOT EXISTS nation (
  n_nationkey INT,
  n_name VARCHAR,
  n_regionkey INT,
  n_comment VARCHAR
)
STORED AS PARQUET
LOCATION '${tpch_root}/nation.parquet';

CREATE EXTERNAL TABLE IF NOT EXISTS supplier (
  s_suppkey INT,
  s_name VARCHAR,
  s_address VARCHAR,
  s_nationkey INT,
  s_phone VARCHAR,
  s_acctbal DECIMAL(15,2),
  s_comment VARCHAR
)
STORED AS PARQUET
LOCATION '${tpch_root}/supplier.parquet';

CREATE EXTERNAL TABLE IF NOT EXISTS customer (
  c_custkey INT,
  c_name VARCHAR,
  c_address VARCHAR,
  c_nationkey INT,
  c_phone VARCHAR,
  c_acctbal DECIMAL(15,2),
  c_mktsegment VARCHAR,
  c_comment VARCHAR
)
STORED AS PARQUET
LOCATION '${tpch_root}/customer.parquet';

CREATE EXTERNAL TABLE IF NOT EXISTS part (
  p_partkey INT,
  p_name VARCHAR,
  p_mfgr VARCHAR,
  p_brand VARCHAR,
  p_type VARCHAR,
  p_size INT,
  p_container VARCHAR,
  p_retailprice DECIMAL(15,2),
  p_comment VARCHAR
)
STORED AS PARQUET
LOCATION '${tpch_root}/part.parquet';

CREATE EXTERNAL TABLE IF NOT EXISTS partsupp (
  ps_partkey INT,
  ps_suppkey INT,
  ps_availqty INT,
  ps_supplycost DECIMAL(15,2),
  ps_comment VARCHAR
)
STORED AS PARQUET
LOCATION '${tpch_root}/partsupp.parquet';

CREATE EXTERNAL TABLE IF NOT EXISTS orders (
  o_orderkey INT,
  o_custkey INT,
  o_orderstatus VARCHAR,
  o_totalprice DECIMAL(15,2),
  o_orderdate DATE,
  o_orderpriority VARCHAR,
  o_clerk VARCHAR,
  o_shippriority INT,
  o_comment VARCHAR
)
STORED AS PARQUET
LOCATION '${tpch_root}/orders.parquet';

CREATE EXTERNAL TABLE IF NOT EXISTS lineitem (
  l_orderkey INT,
  l_partkey INT,
  l_suppkey INT,
  l_linenumber INT,
  l_quantity DECIMAL(15,2),
  l_extendedprice DECIMAL(15,2),
  l_discount DECIMAL(15,2),
  l_tax DECIMAL(15,2),
  l_returnflag VARCHAR,
  l_linestatus VARCHAR,
  l_shipdate DATE,
  l_commitdate DATE,
  l_receiptdate DATE,
  l_shipinstruct VARCHAR,
  l_shipmode VARCHAR,
  l_comment VARCHAR
)
STORED AS PARQUET
LOCATION '${tpch_root}/lineitem.parquet';

SQL
}

prepare_tmpfs
write_bootstrap_sql
touch "${ready_marker}"
touch "${host_ready_marker}"
mkdir -p /workspace/datafusion-spill
export TMPDIR=/workspace/datafusion-spill

exec tail -f /dev/null
