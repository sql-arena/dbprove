/* PostgreSQL Tuning Script */
SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

DO $$
BEGIN
    -- Primary keys
    IF to_regclass('tpch.part') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pk_part') THEN
        ALTER TABLE tpch.part ADD CONSTRAINT pk_part PRIMARY KEY (p_partkey);
    END IF;

    IF to_regclass('tpch.supplier') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pk_supplier') THEN
        ALTER TABLE tpch.supplier ADD CONSTRAINT pk_supplier PRIMARY KEY (s_suppkey);
    END IF;

    IF to_regclass('tpch.partsupp') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pk_partsupp') THEN
        ALTER TABLE tpch.partsupp ADD CONSTRAINT pk_partsupp PRIMARY KEY (ps_partkey, ps_suppkey);
    END IF;

    IF to_regclass('tpch.customer') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pk_customer') THEN
        ALTER TABLE tpch.customer ADD CONSTRAINT pk_customer PRIMARY KEY (c_custkey);
    END IF;

    IF to_regclass('tpch.orders') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pk_orders') THEN
        ALTER TABLE tpch.orders ADD CONSTRAINT pk_orders PRIMARY KEY (o_orderkey);
    END IF;

    IF to_regclass('tpch.lineitem') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pk_lineitem') THEN
        ALTER TABLE tpch.lineitem ADD CONSTRAINT pk_lineitem PRIMARY KEY (l_orderkey, l_linenumber);
    END IF;

    IF to_regclass('tpch.nation') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pk_nation') THEN
        ALTER TABLE tpch.nation ADD CONSTRAINT pk_nation PRIMARY KEY (n_nationkey);
    END IF;

    IF to_regclass('tpch.region') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'pk_region') THEN
        ALTER TABLE tpch.region ADD CONSTRAINT pk_region PRIMARY KEY (r_regionkey);
    END IF;

    -- Foreign keys
    IF to_regclass('tpch.orders') IS NOT NULL
       AND to_regclass('tpch.customer') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_orders_customer') THEN
        ALTER TABLE tpch.orders
            ADD CONSTRAINT fk_orders_customer FOREIGN KEY (o_custkey) REFERENCES tpch.customer (c_custkey);
    END IF;

    IF to_regclass('tpch.partsupp') IS NOT NULL
       AND to_regclass('tpch.part') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_partsupp_part') THEN
        ALTER TABLE tpch.partsupp
            ADD CONSTRAINT fk_partsupp_part FOREIGN KEY (ps_partkey) REFERENCES tpch.part (p_partkey);
    END IF;

    IF to_regclass('tpch.partsupp') IS NOT NULL
       AND to_regclass('tpch.supplier') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_partsupp_supplier') THEN
        ALTER TABLE tpch.partsupp
            ADD CONSTRAINT fk_partsupp_supplier FOREIGN KEY (ps_suppkey) REFERENCES tpch.supplier (s_suppkey);
    END IF;

    IF to_regclass('tpch.lineitem') IS NOT NULL
       AND to_regclass('tpch.orders') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_lineitem_orders') THEN
        ALTER TABLE tpch.lineitem
            ADD CONSTRAINT fk_lineitem_orders FOREIGN KEY (l_orderkey) REFERENCES tpch.orders (o_orderkey);
    END IF;

    IF to_regclass('tpch.lineitem') IS NOT NULL
       AND to_regclass('tpch.part') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_lineitem_part') THEN
        ALTER TABLE tpch.lineitem
            ADD CONSTRAINT fk_lineitem_part FOREIGN KEY (l_partkey) REFERENCES tpch.part (p_partkey);
    END IF;

    IF to_regclass('tpch.lineitem') IS NOT NULL
       AND to_regclass('tpch.supplier') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_lineitem_supplier') THEN
        ALTER TABLE tpch.lineitem
            ADD CONSTRAINT fk_lineitem_supplier FOREIGN KEY (l_suppkey) REFERENCES tpch.supplier (s_suppkey);
    END IF;

    IF to_regclass('tpch.lineitem') IS NOT NULL
       AND to_regclass('tpch.partsupp') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_lineitem_partsupp') THEN
        ALTER TABLE tpch.lineitem
            ADD CONSTRAINT fk_lineitem_partsupp FOREIGN KEY (l_partkey, l_suppkey)
                REFERENCES tpch.partsupp (ps_partkey, ps_suppkey);
    END IF;

    IF to_regclass('tpch.supplier') IS NOT NULL
       AND to_regclass('tpch.nation') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_supplier_nation') THEN
        ALTER TABLE tpch.supplier
            ADD CONSTRAINT fk_supplier_nation FOREIGN KEY (s_nationkey) REFERENCES tpch.nation (n_nationkey);
    END IF;

    IF to_regclass('tpch.customer') IS NOT NULL
       AND to_regclass('tpch.nation') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_customer_nation') THEN
        ALTER TABLE tpch.customer
            ADD CONSTRAINT fk_customer_nation FOREIGN KEY (c_nationkey) REFERENCES tpch.nation (n_nationkey);
    END IF;

    IF to_regclass('tpch.nation') IS NOT NULL
       AND to_regclass('tpch.region') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_nation_region') THEN
        ALTER TABLE tpch.nation
            ADD CONSTRAINT fk_nation_region FOREIGN KEY (n_regionkey) REFERENCES tpch.region (r_regionkey);
    END IF;

    -- Supporting indexes
    IF to_regclass('tpch.lineitem') IS NOT NULL THEN
        CREATE INDEX IF NOT EXISTS lineitem_orderkey_idx ON tpch.lineitem (l_orderkey);
        CREATE INDEX IF NOT EXISTS ix_q17 ON tpch.lineitem (l_partkey);
        CREATE INDEX IF NOT EXISTS lineitem_suppkey_idx ON tpch.lineitem (l_suppkey);
        CREATE INDEX IF NOT EXISTS lineitem_part_supp_idx ON tpch.lineitem (l_partkey, l_suppkey);
    END IF;

    IF to_regclass('tpch.orders') IS NOT NULL THEN
        CREATE INDEX IF NOT EXISTS orders_custkey_idx ON tpch.orders (o_custkey);
    END IF;

    IF to_regclass('tpch.partsupp') IS NOT NULL THEN
        CREATE INDEX IF NOT EXISTS partsupp_partkey_idx ON tpch.partsupp (ps_partkey);
        CREATE INDEX IF NOT EXISTS partsupp_suppkey_idx ON tpch.partsupp (ps_suppkey);
    END IF;

    IF to_regclass('tpch.supplier') IS NOT NULL THEN
        CREATE INDEX IF NOT EXISTS supplier_nationkey_idx ON tpch.supplier (s_nationkey);
    END IF;

    IF to_regclass('tpch.customer') IS NOT NULL THEN
        CREATE INDEX IF NOT EXISTS customer_nationkey_idx ON tpch.customer (c_nationkey);
    END IF;

    IF to_regclass('tpch.nation') IS NOT NULL THEN
        CREATE INDEX IF NOT EXISTS nation_regionkey_idx ON tpch.nation (n_regionkey);
    END IF;
END $$;

DO $$
DECLARE
    rec RECORD;
BEGIN
    -- Refresh stats only when missing or stale based on table modifications since last analyze.
    FOR rec IN
        SELECT
            format('%I.%I', st.schemaname, st.relname) AS qualified_name
        FROM pg_stat_all_tables st
        WHERE st.schemaname = 'tpch'
          AND (
              COALESCE(st.last_analyze, st.last_autoanalyze) IS NULL
              OR st.n_mod_since_analyze > 0
          )
    LOOP
        EXECUTE 'ANALYZE ' || rec.qualified_name;
    END LOOP;
END $$;
