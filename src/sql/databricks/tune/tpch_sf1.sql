-- Databricks tuning script for TPCH key metadata.
-- Databricks does not use traditional indexes; constraints are informational.
-- This script is idempotent by dropping constraints (if present) before adding them.

-- Drop foreign keys first to avoid PK dependency errors.
ALTER TABLE tpch_sf1.orders DROP CONSTRAINT IF EXISTS fk_orders_customer;
ALTER TABLE tpch_sf1.partsupp DROP CONSTRAINT IF EXISTS fk_partsupp_part;
ALTER TABLE tpch_sf1.partsupp DROP CONSTRAINT IF EXISTS fk_partsupp_supplier;
ALTER TABLE tpch_sf1.lineitem DROP CONSTRAINT IF EXISTS fk_lineitem_orders;
ALTER TABLE tpch_sf1.lineitem DROP CONSTRAINT IF EXISTS fk_lineitem_part;
ALTER TABLE tpch_sf1.lineitem DROP CONSTRAINT IF EXISTS fk_lineitem_supplier;
ALTER TABLE tpch_sf1.lineitem DROP CONSTRAINT IF EXISTS fk_lineitem_partsupp;
ALTER TABLE tpch_sf1.supplier DROP CONSTRAINT IF EXISTS fk_supplier_nation;
ALTER TABLE tpch_sf1.customer DROP CONSTRAINT IF EXISTS fk_customer_nation;
ALTER TABLE tpch_sf1.nation DROP CONSTRAINT IF EXISTS fk_nation_region;

-- Drop primary keys after foreign keys are gone.
ALTER TABLE tpch_sf1.lineitem DROP CONSTRAINT IF EXISTS pk_lineitem;
ALTER TABLE tpch_sf1.partsupp DROP CONSTRAINT IF EXISTS pk_partsupp;
ALTER TABLE tpch_sf1.orders DROP CONSTRAINT IF EXISTS pk_orders;
ALTER TABLE tpch_sf1.customer DROP CONSTRAINT IF EXISTS pk_customer;
ALTER TABLE tpch_sf1.supplier DROP CONSTRAINT IF EXISTS pk_supplier;
ALTER TABLE tpch_sf1.part DROP CONSTRAINT IF EXISTS pk_part;
ALTER TABLE tpch_sf1.nation DROP CONSTRAINT IF EXISTS pk_nation;
ALTER TABLE tpch_sf1.region DROP CONSTRAINT IF EXISTS pk_region;

-- Add primary keys first.
ALTER TABLE tpch_sf1.part
  ADD CONSTRAINT pk_part PRIMARY KEY (p_partkey) NOT ENFORCED;

ALTER TABLE tpch_sf1.supplier
  ADD CONSTRAINT pk_supplier PRIMARY KEY (s_suppkey) NOT ENFORCED;

ALTER TABLE tpch_sf1.partsupp
  ADD CONSTRAINT pk_partsupp PRIMARY KEY (ps_partkey, ps_suppkey) NOT ENFORCED;

ALTER TABLE tpch_sf1.customer
  ADD CONSTRAINT pk_customer PRIMARY KEY (c_custkey) NOT ENFORCED;

ALTER TABLE tpch_sf1.orders
  ADD CONSTRAINT pk_orders PRIMARY KEY (o_orderkey) NOT ENFORCED;

ALTER TABLE tpch_sf1.lineitem
  ADD CONSTRAINT pk_lineitem PRIMARY KEY (l_orderkey, l_linenumber) NOT ENFORCED;

ALTER TABLE tpch_sf1.nation
  ADD CONSTRAINT pk_nation PRIMARY KEY (n_nationkey) NOT ENFORCED;

ALTER TABLE tpch_sf1.region
  ADD CONSTRAINT pk_region PRIMARY KEY (r_regionkey) NOT ENFORCED;

-- Add foreign keys after parent primary keys exist.
ALTER TABLE tpch_sf1.orders
  ADD CONSTRAINT fk_orders_customer FOREIGN KEY (o_custkey)
  REFERENCES tpch_sf1.customer (c_custkey) NOT ENFORCED;

ALTER TABLE tpch_sf1.partsupp
  ADD CONSTRAINT fk_partsupp_part FOREIGN KEY (ps_partkey)
  REFERENCES tpch_sf1.part (p_partkey) NOT ENFORCED;

ALTER TABLE tpch_sf1.partsupp
  ADD CONSTRAINT fk_partsupp_supplier FOREIGN KEY (ps_suppkey)
  REFERENCES tpch_sf1.supplier (s_suppkey) NOT ENFORCED;

ALTER TABLE tpch_sf1.lineitem
  ADD CONSTRAINT fk_lineitem_orders FOREIGN KEY (l_orderkey)
  REFERENCES tpch_sf1.orders (o_orderkey) NOT ENFORCED;

ALTER TABLE tpch_sf1.lineitem
  ADD CONSTRAINT fk_lineitem_part FOREIGN KEY (l_partkey)
  REFERENCES tpch_sf1.part (p_partkey) NOT ENFORCED;

ALTER TABLE tpch_sf1.lineitem
  ADD CONSTRAINT fk_lineitem_supplier FOREIGN KEY (l_suppkey)
  REFERENCES tpch_sf1.supplier (s_suppkey) NOT ENFORCED;

ALTER TABLE tpch_sf1.lineitem
  ADD CONSTRAINT fk_lineitem_partsupp FOREIGN KEY (l_partkey, l_suppkey)
  REFERENCES tpch_sf1.partsupp (ps_partkey, ps_suppkey) NOT ENFORCED;

ALTER TABLE tpch_sf1.supplier
  ADD CONSTRAINT fk_supplier_nation FOREIGN KEY (s_nationkey)
  REFERENCES tpch_sf1.nation (n_nationkey) NOT ENFORCED;

ALTER TABLE tpch_sf1.customer
  ADD CONSTRAINT fk_customer_nation FOREIGN KEY (c_nationkey)
  REFERENCES tpch_sf1.nation (n_nationkey) NOT ENFORCED;

ALTER TABLE tpch_sf1.nation
  ADD CONSTRAINT fk_nation_region FOREIGN KEY (n_regionkey)
  REFERENCES tpch_sf1.region (r_regionkey) NOT ENFORCED;

-- Refresh table statistics for the optimizer.
ANALYZE TABLE tpch_sf1.part COMPUTE STATISTICS;
ANALYZE TABLE tpch_sf1.supplier COMPUTE STATISTICS;
ANALYZE TABLE tpch_sf1.partsupp COMPUTE STATISTICS;
ANALYZE TABLE tpch_sf1.customer COMPUTE STATISTICS;
ANALYZE TABLE tpch_sf1.orders COMPUTE STATISTICS;
ANALYZE TABLE tpch_sf1.lineitem COMPUTE STATISTICS;
ANALYZE TABLE tpch_sf1.nation COMPUTE STATISTICS;
ANALYZE TABLE tpch_sf1.region COMPUTE STATISTICS;
