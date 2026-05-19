SET datafusion.execution.collect_statistics TO true;
CREATE SCHEMA IF NOT EXISTS tpch;

CREATE EXTERNAL TABLE IF NOT EXISTS tpch.region
STORED AS PARQUET
LOCATION '/opt/tpch/sf1/region.parquet';

CREATE EXTERNAL TABLE IF NOT EXISTS tpch.nation
STORED AS PARQUET
LOCATION '/opt/tpch/sf1/nation.parquet';

CREATE EXTERNAL TABLE IF NOT EXISTS tpch.supplier
STORED AS PARQUET
LOCATION '/opt/tpch/sf1/supplier.parquet';

CREATE EXTERNAL TABLE IF NOT EXISTS tpch.customer
STORED AS PARQUET
LOCATION '/opt/tpch/sf1/customer.parquet';

CREATE EXTERNAL TABLE IF NOT EXISTS tpch.part
STORED AS PARQUET
LOCATION '/opt/tpch/sf1/part.parquet';

CREATE EXTERNAL TABLE IF NOT EXISTS tpch.partsupp
STORED AS PARQUET
LOCATION '/opt/tpch/sf1/partsupp.parquet';

CREATE EXTERNAL TABLE IF NOT EXISTS tpch.orders
STORED AS PARQUET
LOCATION '/mnt/tpch-tmpfs/orders.parquet';

CREATE EXTERNAL TABLE IF NOT EXISTS tpch.lineitem
STORED AS PARQUET
LOCATION '/mnt/tpch-tmpfs/lineitem.parquet';
