CREATE SCHEMA IF NOT EXISTS tpch.tpch_sf1
WITH (
  location = 'local:///warehouse/tpch_sf1'
);

DROP TABLE IF EXISTS tpch.tpch_sf1.region;
CREATE TABLE tpch.tpch_sf1.region (
  r_regionkey INT,
  r_name VARCHAR,
  r_comment VARCHAR
)
WITH (
  format = 'PARQUET'
);
ALTER TABLE tpch.tpch_sf1.region EXECUTE add_files(
  location => 'local:///tpch_sf1/region.parquet',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.tpch_sf1.nation;
CREATE TABLE tpch.tpch_sf1.nation (
  n_nationkey INT,
  n_name VARCHAR,
  n_regionkey INT,
  n_comment VARCHAR
)
WITH (
  format = 'PARQUET'
);
ALTER TABLE tpch.tpch_sf1.nation EXECUTE add_files(
  location => 'local:///tpch_sf1/nation.parquet',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.tpch_sf1.supplier;
CREATE TABLE tpch.tpch_sf1.supplier (
  s_suppkey INT,
  s_name VARCHAR,
  s_address VARCHAR,
  s_nationkey INT,
  s_phone VARCHAR,
  s_acctbal DECIMAL(15,2),
  s_comment VARCHAR
)
WITH (
  format = 'PARQUET'
);
ALTER TABLE tpch.tpch_sf1.supplier EXECUTE add_files(
  location => 'local:///tpch_sf1/supplier.parquet',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.tpch_sf1.customer;
CREATE TABLE tpch.tpch_sf1.customer (
  c_custkey INT,
  c_name VARCHAR,
  c_address VARCHAR,
  c_nationkey INT,
  c_phone VARCHAR,
  c_acctbal DECIMAL(15,2),
  c_mktsegment VARCHAR,
  c_comment VARCHAR
)
WITH (
  format = 'PARQUET'
);
ALTER TABLE tpch.tpch_sf1.customer EXECUTE add_files(
  location => 'local:///tpch_sf1/customer.parquet',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.tpch_sf1.part;
CREATE TABLE tpch.tpch_sf1.part (
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
WITH (
  format = 'PARQUET'
);
ALTER TABLE tpch.tpch_sf1.part EXECUTE add_files(
  location => 'local:///tpch_sf1/part.parquet',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.tpch_sf1.partsupp;
CREATE TABLE tpch.tpch_sf1.partsupp (
  ps_partkey INT,
  ps_suppkey INT,
  ps_availqty INT,
  ps_supplycost DECIMAL(15,2),
  ps_comment VARCHAR
)
WITH (
  format = 'PARQUET'
);
ALTER TABLE tpch.tpch_sf1.partsupp EXECUTE add_files(
  location => 'local:///tpch_sf1/partsupp.parquet',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.tpch_sf1.orders;
CREATE TABLE tpch.tpch_sf1.orders (
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
WITH (
  format = 'PARQUET'
);
ALTER TABLE tpch.tpch_sf1.orders EXECUTE add_files(
  location => 'local:///tpch_sf1/orders.parquet',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.tpch_sf1.lineitem;
CREATE TABLE tpch.tpch_sf1.lineitem (
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
WITH (
  format = 'PARQUET'
);
ALTER TABLE tpch.tpch_sf1.lineitem EXECUTE add_files(
  location => 'local:///tpch_sf1/lineitem.parquet',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.tpch_sf1.lineitem_25x;
CREATE TABLE tpch.tpch_sf1.lineitem_25x (
  l_orderkey BIGINT,
  l_suppkey BIGINT,
  l_linenumber BIGINT,
  lineitem_replica_id BIGINT
)
WITH (
  format = 'PARQUET'
);
ALTER TABLE tpch.tpch_sf1.lineitem_25x EXECUTE add_files(
  location => 'local:///join_scale/lineitem_25x',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.tpch_sf1.orders_scale_01;
CREATE TABLE tpch.tpch_sf1.orders_scale_01 (
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
WITH (location = 'local:///warehouse/tpch_sf1/orders_scale_01');

ALTER TABLE tpch.tpch_sf1.orders_scale_01 EXECUTE add_files(
  location => 'local:///join_scale/orders_scale_01',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.tpch_sf1.orders_scale_02;
CREATE TABLE tpch.tpch_sf1.orders_scale_02 (
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
ALTER TABLE tpch.tpch_sf1.orders_scale_02 EXECUTE add_files(
  location => 'local:///join_scale/orders_scale_02',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.tpch_sf1.orders_scale_03;
CREATE TABLE tpch.tpch_sf1.orders_scale_03 (
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
WITH (location = 'local:///warehouse/tpch_sf1/orders_scale_03');

ALTER TABLE tpch.tpch_sf1.orders_scale_03 EXECUTE add_files(
  location => 'local:///join_scale/orders_scale_03',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.tpch_sf1.orders_scale_04;
CREATE TABLE tpch.tpch_sf1.orders_scale_04 (
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
ALTER TABLE tpch.tpch_sf1.orders_scale_04 EXECUTE add_files(
  location => 'local:///join_scale/orders_scale_04',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.tpch_sf1.orders_scale_05;
CREATE TABLE tpch.tpch_sf1.orders_scale_05 (
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
WITH (location = 'local:///warehouse/tpch_sf1/orders_scale_05');

ALTER TABLE tpch.tpch_sf1.orders_scale_05 EXECUTE add_files(
  location => 'local:///join_scale/orders_scale_05',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.tpch_sf1.orders_scale_06;
CREATE TABLE tpch.tpch_sf1.orders_scale_06 (
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
ALTER TABLE tpch.tpch_sf1.orders_scale_06 EXECUTE add_files(
  location => 'local:///join_scale/orders_scale_06',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.tpch_sf1.orders_scale_08;
CREATE TABLE tpch.tpch_sf1.orders_scale_08 (
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
ALTER TABLE tpch.tpch_sf1.orders_scale_08 EXECUTE add_files(
  location => 'local:///join_scale/orders_scale_08',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.tpch_sf1.orders_scale_10;
CREATE TABLE tpch.tpch_sf1.orders_scale_10 (
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
ALTER TABLE tpch.tpch_sf1.orders_scale_10 EXECUTE add_files(
  location => 'local:///join_scale/orders_scale_10',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.tpch_sf1.orders_scale_12;
CREATE TABLE tpch.tpch_sf1.orders_scale_12 (
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
ALTER TABLE tpch.tpch_sf1.orders_scale_12 EXECUTE add_files(
  location => 'local:///join_scale/orders_scale_12',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.tpch_sf1.orders_scale_14;
CREATE TABLE tpch.tpch_sf1.orders_scale_14 (
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
ALTER TABLE tpch.tpch_sf1.orders_scale_14 EXECUTE add_files(
  location => 'local:///join_scale/orders_scale_14',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.tpch_sf1.orders_scale_16;
CREATE TABLE tpch.tpch_sf1.orders_scale_16 (
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
ALTER TABLE tpch.tpch_sf1.orders_scale_16 EXECUTE add_files(
  location => 'local:///join_scale/orders_scale_16',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.tpch_sf1.orders_scale_18;
CREATE TABLE tpch.tpch_sf1.orders_scale_18 (
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
ALTER TABLE tpch.tpch_sf1.orders_scale_18 EXECUTE add_files(
  location => 'local:///join_scale/orders_scale_18',
  format => 'PARQUET'
);

DROP TABLE IF EXISTS tpch.tpch_sf1.orders_scale_20;
CREATE TABLE tpch.tpch_sf1.orders_scale_20 (
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
ALTER TABLE tpch.tpch_sf1.orders_scale_20 EXECUTE add_files(
  location => 'local:///join_scale/orders_scale_20',
  format => 'PARQUET'
);
