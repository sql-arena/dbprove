CREATE SCHEMA IF NOT EXISTS tpch.sf1
WITH (
  location = 'local:///warehouse/sf1'
);

DROP TABLE IF EXISTS tpch.sf1.lineitem_25x;
CREATE TABLE tpch.sf1.lineitem_25x (
  l_orderkey BIGINT,
  l_suppkey BIGINT,
  l_linenumber BIGINT,
  lineitem_replica_id BIGINT
)
WITH (
  format = 'PARQUET'
);
ALTER TABLE tpch.sf1.lineitem_25x EXECUTE add_files(
  location => 'local:///join_scale/lineitem_25x',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.sf1.orders_scale_01;
CREATE TABLE tpch.sf1.orders_scale_01 (
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
WITH (location = 'local:///warehouse/sf1/orders_scale_01');

ALTER TABLE tpch.sf1.orders_scale_01 EXECUTE add_files(
  location => 'local:///join_scale/orders_scale_01',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.sf1.orders_scale_02;
CREATE TABLE tpch.sf1.orders_scale_02 (
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
WITH (
  format = 'PARQUET'
);
ALTER TABLE tpch.sf1.orders_scale_02 EXECUTE add_files(
  location => 'local:///join_scale/orders_scale_02',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.sf1.orders_scale_03;
CREATE TABLE tpch.sf1.orders_scale_03 (
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
WITH (location = 'local:///warehouse/sf1/orders_scale_03');

ALTER TABLE tpch.sf1.orders_scale_03 EXECUTE add_files(
  location => 'local:///join_scale/orders_scale_03',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.sf1.orders_scale_04;
CREATE TABLE tpch.sf1.orders_scale_04 (
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
WITH (
  format = 'PARQUET'
);
ALTER TABLE tpch.sf1.orders_scale_04 EXECUTE add_files(
  location => 'local:///join_scale/orders_scale_04',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.sf1.orders_scale_05;
CREATE TABLE tpch.sf1.orders_scale_05 (
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
WITH (location = 'local:///warehouse/sf1/orders_scale_05');

ALTER TABLE tpch.sf1.orders_scale_05 EXECUTE add_files(
  location => 'local:///join_scale/orders_scale_05',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.sf1.orders_scale_06;
CREATE TABLE tpch.sf1.orders_scale_06 (
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
WITH (
  format = 'PARQUET'
);
ALTER TABLE tpch.sf1.orders_scale_06 EXECUTE add_files(
  location => 'local:///join_scale/orders_scale_06',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.sf1.orders_scale_08;
CREATE TABLE tpch.sf1.orders_scale_08 (
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
WITH (
  format = 'PARQUET'
);
ALTER TABLE tpch.sf1.orders_scale_08 EXECUTE add_files(
  location => 'local:///join_scale/orders_scale_08',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.sf1.orders_scale_10;
CREATE TABLE tpch.sf1.orders_scale_10 (
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
WITH (
  format = 'PARQUET'
);
ALTER TABLE tpch.sf1.orders_scale_10 EXECUTE add_files(
  location => 'local:///join_scale/orders_scale_10',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.sf1.orders_scale_12;
CREATE TABLE tpch.sf1.orders_scale_12 (
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
WITH (
  format = 'PARQUET'
);
ALTER TABLE tpch.sf1.orders_scale_12 EXECUTE add_files(
  location => 'local:///join_scale/orders_scale_12',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.sf1.orders_scale_14;
CREATE TABLE tpch.sf1.orders_scale_14 (
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
WITH (
  format = 'PARQUET'
);
ALTER TABLE tpch.sf1.orders_scale_14 EXECUTE add_files(
  location => 'local:///join_scale/orders_scale_14',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.sf1.orders_scale_16;
CREATE TABLE tpch.sf1.orders_scale_16 (
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
WITH (
  format = 'PARQUET'
);
ALTER TABLE tpch.sf1.orders_scale_16 EXECUTE add_files(
  location => 'local:///join_scale/orders_scale_16',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.sf1.orders_scale_18;
CREATE TABLE tpch.sf1.orders_scale_18 (
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
WITH (
  format = 'PARQUET'
);
ALTER TABLE tpch.sf1.orders_scale_18 EXECUTE add_files(
  location => 'local:///join_scale/orders_scale_18',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.sf1.orders_scale_20;
CREATE TABLE tpch.sf1.orders_scale_20 (
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
WITH (
  format = 'PARQUET'
);
ALTER TABLE tpch.sf1.orders_scale_20 EXECUTE add_files(
  location => 'local:///join_scale/orders_scale_20',
  format => 'PARQUET'
);
