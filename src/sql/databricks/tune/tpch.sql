-- Databricks tuning script for TPCH key metadata.
-- Databricks does not use traditional indexes; constraints are informational.
-- This script is idempotent by dropping constraints (if present) before adding them.

-- Primary keys
ALTER TABLE IF EXISTS tpch.part DROP CONSTRAINT IF EXISTS pk_part;
ALTER TABLE IF EXISTS tpch.part
  ADD CONSTRAINT pk_part PRIMARY KEY (p_partkey) NOT ENFORCED;

ALTER TABLE IF EXISTS tpch.supplier DROP CONSTRAINT IF EXISTS pk_supplier;
ALTER TABLE IF EXISTS tpch.supplier
  ADD CONSTRAINT pk_supplier PRIMARY KEY (s_suppkey) NOT ENFORCED;

ALTER TABLE IF EXISTS tpch.partsupp DROP CONSTRAINT IF EXISTS pk_partsupp;
ALTER TABLE IF EXISTS tpch.partsupp
  ADD CONSTRAINT pk_partsupp PRIMARY KEY (ps_partkey, ps_suppkey) NOT ENFORCED;

ALTER TABLE IF EXISTS tpch.customer DROP CONSTRAINT IF EXISTS pk_customer;
ALTER TABLE IF EXISTS tpch.customer
  ADD CONSTRAINT pk_customer PRIMARY KEY (c_custkey) NOT ENFORCED;

ALTER TABLE IF EXISTS tpch.orders DROP CONSTRAINT IF EXISTS pk_orders;
ALTER TABLE IF EXISTS tpch.orders
  ADD CONSTRAINT pk_orders PRIMARY KEY (o_orderkey) NOT ENFORCED;

ALTER TABLE IF EXISTS tpch.lineitem DROP CONSTRAINT IF EXISTS pk_lineitem;
ALTER TABLE IF EXISTS tpch.lineitem
  ADD CONSTRAINT pk_lineitem PRIMARY KEY (l_orderkey, l_linenumber) NOT ENFORCED;

ALTER TABLE IF EXISTS tpch.nation DROP CONSTRAINT IF EXISTS pk_nation;
ALTER TABLE IF EXISTS tpch.nation
  ADD CONSTRAINT pk_nation PRIMARY KEY (n_nationkey) NOT ENFORCED;

ALTER TABLE IF EXISTS tpch.region DROP CONSTRAINT IF EXISTS pk_region;
ALTER TABLE IF EXISTS tpch.region
  ADD CONSTRAINT pk_region PRIMARY KEY (r_regionkey) NOT ENFORCED;

-- Foreign keys
ALTER TABLE IF EXISTS tpch.orders DROP CONSTRAINT IF EXISTS fk_orders_customer;
ALTER TABLE IF EXISTS tpch.orders
  ADD CONSTRAINT fk_orders_customer FOREIGN KEY (o_custkey)
  REFERENCES tpch.customer (c_custkey) NOT ENFORCED;

ALTER TABLE IF EXISTS tpch.partsupp DROP CONSTRAINT IF EXISTS fk_partsupp_part;
ALTER TABLE IF EXISTS tpch.partsupp
  ADD CONSTRAINT fk_partsupp_part FOREIGN KEY (ps_partkey)
  REFERENCES tpch.part (p_partkey) NOT ENFORCED;

ALTER TABLE IF EXISTS tpch.partsupp DROP CONSTRAINT IF EXISTS fk_partsupp_supplier;
ALTER TABLE IF EXISTS tpch.partsupp
  ADD CONSTRAINT fk_partsupp_supplier FOREIGN KEY (ps_suppkey)
  REFERENCES tpch.supplier (s_suppkey) NOT ENFORCED;

ALTER TABLE IF EXISTS tpch.lineitem DROP CONSTRAINT IF EXISTS fk_lineitem_orders;
ALTER TABLE IF EXISTS tpch.lineitem
  ADD CONSTRAINT fk_lineitem_orders FOREIGN KEY (l_orderkey)
  REFERENCES tpch.orders (o_orderkey) NOT ENFORCED;

ALTER TABLE IF EXISTS tpch.lineitem DROP CONSTRAINT IF EXISTS fk_lineitem_part;
ALTER TABLE IF EXISTS tpch.lineitem
  ADD CONSTRAINT fk_lineitem_part FOREIGN KEY (l_partkey)
  REFERENCES tpch.part (p_partkey) NOT ENFORCED;

ALTER TABLE IF EXISTS tpch.lineitem DROP CONSTRAINT IF EXISTS fk_lineitem_supplier;
ALTER TABLE IF EXISTS tpch.lineitem
  ADD CONSTRAINT fk_lineitem_supplier FOREIGN KEY (l_suppkey)
  REFERENCES tpch.supplier (s_suppkey) NOT ENFORCED;

ALTER TABLE IF EXISTS tpch.lineitem DROP CONSTRAINT IF EXISTS fk_lineitem_partsupp;
ALTER TABLE IF EXISTS tpch.lineitem
  ADD CONSTRAINT fk_lineitem_partsupp FOREIGN KEY (l_partkey, l_suppkey)
  REFERENCES tpch.partsupp (ps_partkey, ps_suppkey) NOT ENFORCED;

ALTER TABLE IF EXISTS tpch.supplier DROP CONSTRAINT IF EXISTS fk_supplier_nation;
ALTER TABLE IF EXISTS tpch.supplier
  ADD CONSTRAINT fk_supplier_nation FOREIGN KEY (s_nationkey)
  REFERENCES tpch.nation (n_nationkey) NOT ENFORCED;

ALTER TABLE IF EXISTS tpch.customer DROP CONSTRAINT IF EXISTS fk_customer_nation;
ALTER TABLE IF EXISTS tpch.customer
  ADD CONSTRAINT fk_customer_nation FOREIGN KEY (c_nationkey)
  REFERENCES tpch.nation (n_nationkey) NOT ENFORCED;

ALTER TABLE IF EXISTS tpch.nation DROP CONSTRAINT IF EXISTS fk_nation_region;
ALTER TABLE IF EXISTS tpch.nation
  ADD CONSTRAINT fk_nation_region FOREIGN KEY (n_regionkey)
  REFERENCES tpch.region (r_regionkey) NOT ENFORCED;

-- Refresh table statistics for the optimizer.
-- Databricks exposes table existence via information_schema; use that to keep this re-runnable.
BEGIN
  IF EXISTS (
    SELECT 1
    FROM system.information_schema.tables
    WHERE table_catalog = current_catalog()
      AND table_schema = 'tpch'
      AND table_name = 'part'
  ) THEN
    ANALYZE TABLE tpch.part COMPUTE STATISTICS;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM system.information_schema.tables
    WHERE table_catalog = current_catalog()
      AND table_schema = 'tpch'
      AND table_name = 'supplier'
  ) THEN
    ANALYZE TABLE tpch.supplier COMPUTE STATISTICS;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM system.information_schema.tables
    WHERE table_catalog = current_catalog()
      AND table_schema = 'tpch'
      AND table_name = 'partsupp'
  ) THEN
    ANALYZE TABLE tpch.partsupp COMPUTE STATISTICS;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM system.information_schema.tables
    WHERE table_catalog = current_catalog()
      AND table_schema = 'tpch'
      AND table_name = 'customer'
  ) THEN
    ANALYZE TABLE tpch.customer COMPUTE STATISTICS;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM system.information_schema.tables
    WHERE table_catalog = current_catalog()
      AND table_schema = 'tpch'
      AND table_name = 'orders'
  ) THEN
    ANALYZE TABLE tpch.orders COMPUTE STATISTICS;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM system.information_schema.tables
    WHERE table_catalog = current_catalog()
      AND table_schema = 'tpch'
      AND table_name = 'lineitem'
  ) THENz``
    ANALYZE TABLE tpch.lineitem COMPUTE STATISTICS;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM system.information_schema.tables
    WHERE table_catalog = current_catalog()
      AND table_schema = 'tpch'
      AND table_name = 'nation'
  ) THEN
    ANALYZE TABLE tpch.nation COMPUTE STATISTICS;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM system.information_schema.tables
    WHERE table_catalog = current_catalog()
      AND table_schema = 'tpch'
      AND table_name = 'region'
  ) THEN
    ANALYZE TABLE tpch.region COMPUTE STATISTICS;
  END IF;
END;
