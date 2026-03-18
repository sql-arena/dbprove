CREATE TABLE tpch.orders
(
    o_orderkey      INT NOT NULL,
    o_custkey       INT NOT NULL,
    o_orderstatus   VARCHAR(1) NOT NULL,
    o_totalprice    DECIMAL(15, 2) NOT NULL,
    o_orderdate     DATE NOT NULL,
    o_orderpriority VARCHAR(15) NOT NULL,
    o_clerk         VARCHAR(15) NOT NULL,
    o_shippriority  INT NOT NULL,
    o_comment       VARCHAR(79) NOT NULL
);
