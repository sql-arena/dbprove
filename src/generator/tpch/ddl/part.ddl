CREATE TABLE tpch.part
(
    p_partkey     INT PRIMARY KEY,
    p_name        VARCHAR(55),
    p_mfgr        VARCHAR(25),
    p_brand       VARCHAR(10),
    p_type        VARCHAR(25),
    p_size        INT,
    p_container   VARCHAR(10),
    p_retailprice DECIMAL(15, 2),
    p_comment     VARCHAR(23)
);
