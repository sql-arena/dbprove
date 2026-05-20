CREATE SCHEMA IF NOT EXISTS tpch;

CREATE EXTERNAL TABLE IF NOT EXISTS tpch.lineitem_25x (
  l_orderkey BIGINT,
  l_suppkey BIGINT,
  l_linenumber BIGINT,
  lineitem_replica_id BIGINT
)
STORED AS PARQUET
LOCATION '/mnt/tpch-tmpfs/join_scale/lineitem_25x/lineitem_25x.parquet';

CREATE EXTERNAL TABLE IF NOT EXISTS tpch.orders_scale_01 (
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
LOCATION '/mnt/tpch-tmpfs/join_scale/orders_scale_01/orders_scale_01.parquet';

CREATE EXTERNAL TABLE IF NOT EXISTS tpch.orders_scale_02 (
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
LOCATION '/mnt/tpch-tmpfs/join_scale/orders_scale_02/orders_scale_02.parquet';

CREATE EXTERNAL TABLE IF NOT EXISTS tpch.orders_scale_03 (
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
LOCATION '/mnt/tpch-tmpfs/join_scale/orders_scale_03/orders_scale_03.parquet';

CREATE EXTERNAL TABLE IF NOT EXISTS tpch.orders_scale_04 (
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
LOCATION '/mnt/tpch-tmpfs/join_scale/orders_scale_04/orders_scale_04.parquet';

CREATE EXTERNAL TABLE IF NOT EXISTS tpch.orders_scale_05 (
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
LOCATION '/mnt/tpch-tmpfs/join_scale/orders_scale_05/orders_scale_05.parquet';

CREATE EXTERNAL TABLE IF NOT EXISTS tpch.orders_scale_06 (
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
LOCATION '/mnt/tpch-tmpfs/join_scale/orders_scale_06/orders_scale_06.parquet';

CREATE EXTERNAL TABLE IF NOT EXISTS tpch.orders_scale_08 (
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
LOCATION '/mnt/tpch-tmpfs/join_scale/orders_scale_08/orders_scale_08.parquet';

CREATE EXTERNAL TABLE IF NOT EXISTS tpch.orders_scale_10 (
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
LOCATION '/mnt/tpch-tmpfs/join_scale/orders_scale_10/orders_scale_10.parquet';

CREATE EXTERNAL TABLE IF NOT EXISTS tpch.orders_scale_12 (
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
LOCATION '/mnt/tpch-tmpfs/join_scale/orders_scale_12/orders_scale_12.parquet';

CREATE EXTERNAL TABLE IF NOT EXISTS tpch.orders_scale_14 (
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
LOCATION '/mnt/tpch-tmpfs/join_scale/orders_scale_14/orders_scale_14.parquet';

CREATE EXTERNAL TABLE IF NOT EXISTS tpch.orders_scale_16 (
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
LOCATION '/mnt/tpch-tmpfs/join_scale/orders_scale_16/orders_scale_16.parquet';

CREATE EXTERNAL TABLE IF NOT EXISTS tpch.orders_scale_18 (
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
LOCATION '/mnt/tpch-tmpfs/join_scale/orders_scale_18/orders_scale_18.parquet';

CREATE EXTERNAL TABLE IF NOT EXISTS tpch.orders_scale_20 (
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
LOCATION '/mnt/tpch-tmpfs/join_scale/orders_scale_20/orders_scale_20.parquet';
