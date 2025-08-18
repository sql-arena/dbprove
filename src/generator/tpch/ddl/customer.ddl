CREATE TABLE tpch.customer
(
    c_custkey    INT PRIMARY KEY,
    c_name       VARCHAR(25),
    c_address    VARCHAR(40),
    c_nationkey  INT,
    c_phone      VARCHAR(15),
    c_acctbal    DECIMAL,
    c_mktsegment VARCHAR(10),
    c_comment    VARCHAR(117)
);
