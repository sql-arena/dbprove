/* CedarDB Tuning Script — TPC-H SF1
 *
 * CedarDB is HTAP and manages columnar storage internally; no columnar DDL exists.
 * DO $$ blocks are not yet supported; all statements are plain SQL.
 * The tune script runs once on freshly loaded tables, so no idempotency guards are needed.
 */

-- Primary keys
ALTER TABLE tpch_sf1.part     ADD CONSTRAINT pk_part     PRIMARY KEY (p_partkey);
ALTER TABLE tpch_sf1.supplier ADD CONSTRAINT pk_supplier PRIMARY KEY (s_suppkey);
ALTER TABLE tpch_sf1.partsupp ADD CONSTRAINT pk_partsupp PRIMARY KEY (ps_partkey, ps_suppkey);
ALTER TABLE tpch_sf1.customer ADD CONSTRAINT pk_customer PRIMARY KEY (c_custkey);
ALTER TABLE tpch_sf1.orders   ADD CONSTRAINT pk_orders   PRIMARY KEY (o_orderkey);
ALTER TABLE tpch_sf1.lineitem ADD CONSTRAINT pk_lineitem PRIMARY KEY (l_orderkey, l_linenumber);
ALTER TABLE tpch_sf1.nation   ADD CONSTRAINT pk_nation   PRIMARY KEY (n_nationkey);
ALTER TABLE tpch_sf1.region   ADD CONSTRAINT pk_region   PRIMARY KEY (r_regionkey);

-- Foreign keys
ALTER TABLE tpch_sf1.orders
    ADD CONSTRAINT fk_orders_customer FOREIGN KEY (o_custkey) REFERENCES tpch_sf1.customer (c_custkey);
ALTER TABLE tpch_sf1.partsupp
    ADD CONSTRAINT fk_partsupp_part FOREIGN KEY (ps_partkey) REFERENCES tpch_sf1.part (p_partkey);
ALTER TABLE tpch_sf1.partsupp
    ADD CONSTRAINT fk_partsupp_supplier FOREIGN KEY (ps_suppkey) REFERENCES tpch_sf1.supplier (s_suppkey);
ALTER TABLE tpch_sf1.lineitem
    ADD CONSTRAINT fk_lineitem_orders FOREIGN KEY (l_orderkey) REFERENCES tpch_sf1.orders (o_orderkey);
ALTER TABLE tpch_sf1.lineitem
    ADD CONSTRAINT fk_lineitem_part FOREIGN KEY (l_partkey) REFERENCES tpch_sf1.part (p_partkey);
ALTER TABLE tpch_sf1.lineitem
    ADD CONSTRAINT fk_lineitem_supplier FOREIGN KEY (l_suppkey) REFERENCES tpch_sf1.supplier (s_suppkey);
ALTER TABLE tpch_sf1.lineitem
    ADD CONSTRAINT fk_lineitem_partsupp FOREIGN KEY (l_partkey, l_suppkey)
        REFERENCES tpch_sf1.partsupp (ps_partkey, ps_suppkey);
ALTER TABLE tpch_sf1.supplier
    ADD CONSTRAINT fk_supplier_nation FOREIGN KEY (s_nationkey) REFERENCES tpch_sf1.nation (n_nationkey);
ALTER TABLE tpch_sf1.customer
    ADD CONSTRAINT fk_customer_nation FOREIGN KEY (c_nationkey) REFERENCES tpch_sf1.nation (n_nationkey);
ALTER TABLE tpch_sf1.nation
    ADD CONSTRAINT fk_nation_region FOREIGN KEY (n_regionkey) REFERENCES tpch_sf1.region (r_regionkey);

-- Supporting B-tree indexes on high-frequency join columns
CREATE INDEX lineitem_orderkey_idx   ON tpch_sf1.lineitem (l_orderkey);
CREATE INDEX lineitem_partkey_idx    ON tpch_sf1.lineitem (l_partkey);
CREATE INDEX lineitem_suppkey_idx    ON tpch_sf1.lineitem (l_suppkey);
CREATE INDEX lineitem_part_supp_idx  ON tpch_sf1.lineitem (l_partkey, l_suppkey);
CREATE INDEX orders_custkey_idx      ON tpch_sf1.orders   (o_custkey);
CREATE INDEX partsupp_partkey_idx    ON tpch_sf1.partsupp (ps_partkey);
CREATE INDEX partsupp_suppkey_idx    ON tpch_sf1.partsupp (ps_suppkey);
CREATE INDEX supplier_nationkey_idx  ON tpch_sf1.supplier (s_nationkey);
CREATE INDEX customer_nationkey_idx  ON tpch_sf1.customer (c_nationkey);
CREATE INDEX nation_regionkey_idx    ON tpch_sf1.nation   (n_regionkey);

ANALYZE tpch_sf1.part;
ANALYZE tpch_sf1.supplier;
ANALYZE tpch_sf1.partsupp;
ANALYZE tpch_sf1.customer;
ANALYZE tpch_sf1.orders;
ANALYZE tpch_sf1.lineitem;
ANALYZE tpch_sf1.nation;
ANALYZE tpch_sf1.region;
