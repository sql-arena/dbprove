CREATE TABLE tpch.customer
(
    c_custkey    INT NOT NULL,
    c_name       VARCHAR(25) NOT NULL,
    c_address    VARCHAR(40) NOT NULL,
    c_nationkey  INT NOT NULL,
    c_phone      VARCHAR(15) NOT NULL,
    c_acctbal    DECIMAL NOT NULL,
    c_mktsegment VARCHAR(10) NOT NULL,
    c_comment    VARCHAR(117) NOT NULL
);
