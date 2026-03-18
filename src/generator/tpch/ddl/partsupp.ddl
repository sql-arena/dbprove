CREATE TABLE tpch.partsupp
(
    ps_partkey    INT NOT NULL,
    ps_suppkey    INT NOT NULL,
    ps_availqty   INT NOT NULL,
    ps_supplycost DECIMAL(15, 2) NOT NULL,
    ps_comment    VARCHAR(199) NOT NULL
);
