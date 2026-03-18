CREATE TABLE tpch.supplier
(
    s_suppkey   INT NOT NULL,
    s_name      VARCHAR(25) NOT NULL,
    s_address   VARCHAR(40) NOT NULL,
    s_nationkey INT NOT NULL,
    s_phone     VARCHAR(15) NOT NULL,
    s_acctbal   DECIMAL(15, 2) NOT NULL,
    s_comment   VARCHAR(101) NOT NULL
);
