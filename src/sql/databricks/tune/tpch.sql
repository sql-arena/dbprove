-- Databricks tuning script for TPCH key metadata.
-- Databricks does not use traditional indexes; constraints are informational.
-- This script is idempotent by dropping constraints (if present) before adding them.

-- Drop foreign keys first to avoid PK dependency errors.
ALTER TABLE tpch.orders DROP CONSTRAINT IF EXISTS fk_orders_customer;
ALTER TABLE tpch.partsupp DROP CONSTRAINT IF EXISTS fk_partsupp_part;
ALTER TABLE tpch.partsupp DROP CONSTRAINT IF EXISTS fk_partsupp_supplier;
ALTER TABLE tpch.lineitem DROP CONSTRAINT IF EXISTS fk_lineitem_orders;
ALTER TABLE tpch.lineitem DROP CONSTRAINT IF EXISTS fk_lineitem_part;
ALTER TABLE tpch.lineitem DROP CONSTRAINT IF EXISTS fk_lineitem_supplier;
ALTER TABLE tpch.lineitem DROP CONSTRAINT IF EXISTS fk_lineitem_partsupp;
ALTER TABLE tpch.supplier DROP CONSTRAINT IF EXISTS fk_supplier_nation;
ALTER TABLE tpch.customer DROP CONSTRAINT IF EXISTS fk_customer_nation;
ALTER TABLE tpch.nation DROP CONSTRAINT IF EXISTS fk_nation_region;

-- Drop primary keys after foreign keys are gone.
ALTER TABLE tpch.lineitem DROP CONSTRAINT IF EXISTS pk_lineitem;
ALTER TABLE tpch.partsupp DROP CONSTRAINT IF EXISTS pk_partsupp;
ALTER TABLE tpch.orders DROP CONSTRAINT IF EXISTS pk_orders;
ALTER TABLE tpch.customer DROP CONSTRAINT IF EXISTS pk_customer;
ALTER TABLE tpch.supplier DROP CONSTRAINT IF EXISTS pk_supplier;
ALTER TABLE tpch.part DROP CONSTRAINT IF EXISTS pk_part;
ALTER TABLE tpch.nation DROP CONSTRAINT IF EXISTS pk_nation;
ALTER TABLE tpch.region DROP CONSTRAINT IF EXISTS pk_region;

-- Add primary keys first.
ALTER TABLE tpch.part
  ADD CONSTRAINT pk_part PRIMARY KEY (p_partkey) NOT ENFORCED;

ALTER TABLE tpch.supplier
  ADD CONSTRAINT pk_supplier PRIMARY KEY (s_suppkey) NOT ENFORCED;

ALTER TABLE tpch.partsupp
  ADD CONSTRAINT pk_partsupp PRIMARY KEY (ps_partkey, ps_suppkey) NOT ENFORCED;

ALTER TABLE tpch.customer
  ADD CONSTRAINT pk_customer PRIMARY KEY (c_custkey) NOT ENFORCED;

ALTER TABLE tpch.orders
  ADD CONSTRAINT pk_orders PRIMARY KEY (o_orderkey) NOT ENFORCED;

ALTER TABLE tpch.lineitem
  ADD CONSTRAINT pk_lineitem PRIMARY KEY (l_orderkey, l_linenumber) NOT ENFORCED;

ALTER TABLE tpch.nation
  ADD CONSTRAINT pk_nation PRIMARY KEY (n_nationkey) NOT ENFORCED;

ALTER TABLE tpch.region
  ADD CONSTRAINT pk_region PRIMARY KEY (r_regionkey) NOT ENFORCED;

-- Add foreign keys after parent primary keys exist.
ALTER TABLE tpch.orders
  ADD CONSTRAINT fk_orders_customer FOREIGN KEY (o_custkey)
  REFERENCES tpch.customer (c_custkey) NOT ENFORCED;

ALTER TABLE tpch.partsupp
  ADD CONSTRAINT fk_partsupp_part FOREIGN KEY (ps_partkey)
  REFERENCES tpch.part (p_partkey) NOT ENFORCED;

ALTER TABLE tpch.partsupp
  ADD CONSTRAINT fk_partsupp_supplier FOREIGN KEY (ps_suppkey)
  REFERENCES tpch.supplier (s_suppkey) NOT ENFORCED;

ALTER TABLE tpch.lineitem
  ADD CONSTRAINT fk_lineitem_orders FOREIGN KEY (l_orderkey)
  REFERENCES tpch.orders (o_orderkey) NOT ENFORCED;

ALTER TABLE tpch.lineitem
  ADD CONSTRAINT fk_lineitem_part FOREIGN KEY (l_partkey)
  REFERENCES tpch.part (p_partkey) NOT ENFORCED;

ALTER TABLE tpch.lineitem
  ADD CONSTRAINT fk_lineitem_supplier FOREIGN KEY (l_suppkey)
  REFERENCES tpch.supplier (s_suppkey) NOT ENFORCED;

ALTER TABLE tpch.lineitem
  ADD CONSTRAINT fk_lineitem_partsupp FOREIGN KEY (l_partkey, l_suppkey)
  REFERENCES tpch.partsupp (ps_partkey, ps_suppkey) NOT ENFORCED;

ALTER TABLE tpch.supplier
  ADD CONSTRAINT fk_supplier_nation FOREIGN KEY (s_nationkey)
  REFERENCES tpch.nation (n_nationkey) NOT ENFORCED;

ALTER TABLE tpch.customer
  ADD CONSTRAINT fk_customer_nation FOREIGN KEY (c_nationkey)
  REFERENCES tpch.nation (n_nationkey) NOT ENFORCED;

ALTER TABLE tpch.nation
  ADD CONSTRAINT fk_nation_region FOREIGN KEY (n_regionkey)
  REFERENCES tpch.region (r_regionkey) NOT ENFORCED;

-- Refresh table statistics for the optimizer.
ANALYZE TABLE tpch.part COMPUTE STATISTICS;
ANALYZE TABLE tpch.supplier COMPUTE STATISTICS;
ANALYZE TABLE tpch.partsupp COMPUTE STATISTICS;
ANALYZE TABLE tpch.customer COMPUTE STATISTICS;
ANALYZE TABLE tpch.orders COMPUTE STATISTICS;
ANALYZE TABLE tpch.lineitem COMPUTE STATISTICS;
ANALYZE TABLE tpch.nation COMPUTE STATISTICS;
ANALYZE TABLE tpch.region COMPUTE STATISTICS;
