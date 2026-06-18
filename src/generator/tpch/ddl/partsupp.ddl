CREATE TABLE tpch_sf1.partsupp
(
    ps_partkey    INT NOT NULL,
    ps_suppkey    INT NOT NULL,
    ps_availqty   INT NOT NULL,
    ps_supplycost DECIMAL(15, 2) NOT NULL,
    ps_comment    TEXT NOT NULL
);
