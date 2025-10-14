CREATE TABLE tpch.lineitem
(
    l_orderkey      INT,
    l_partkey       INT,
    l_suppkey       INT,
    l_linenumber    INT,
    l_quantity      DECIMAL(15, 2),
    l_extendedprice DECIMAL(15, 2),
    l_discount      DECIMAL(15, 2),
    l_tax           DECIMAL(15, 2),
    l_returnflag    VARCHAR(1),
    l_linestatus    VARCHAR(1),
    l_shipdate      DATE,
    l_commitdate    DATE,
    l_receiptdate   DATE,
    l_shipinstruct  VARCHAR(25),
    l_shipmode      VARCHAR(10),
    l_comment       VARCHAR(44)
);
