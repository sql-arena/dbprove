CREATE TABLE tpch.supplier
(
    s_suppkey   INT PRIMARY KEY,
    s_name      VARCHAR(25),
    s_address   VARCHAR(40),
    s_nationkey INT,
    s_phone     VARCHAR(15),
    s_acctbal   DECIMAL,
    s_comment   VARCHAR(101),
);
