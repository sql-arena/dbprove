CREATE TABLE tpch.orders
(
    o_orderkey      INT PRIMARY KEY,
    o_custkey       INT,
    o_orderstatus   VARCHAR(1),
    o_totalprice    DECIMAL(15, 2),
    o_orderdate     DATE,
    o_orderpriority VARCHAR(15),
    o_clerk         VARCHAR(15),
    o_shippriority  INT,
    o_comment       VARCHAR(79)
);
