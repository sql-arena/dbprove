CREATE TABLE tpch.partsupp
(
    ps_partkey    INT PRIMARY KEY,
    ps_suppkey    INT,
    ps_availqty   INT,
    ps_supplycost DECIMAL,
    ps_comment    VARCHAR(199)
);
