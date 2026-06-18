CREATE TABLE tpch_sf1.supplier
(
    s_suppkey   INT NOT NULL,
    s_name      TEXT NOT NULL,
    s_address   TEXT NOT NULL,
    s_nationkey INT NOT NULL,
    s_phone     TEXT NOT NULL,
    s_acctbal   DECIMAL(15, 2) NOT NULL,
    s_comment   TEXT NOT NULL
);
