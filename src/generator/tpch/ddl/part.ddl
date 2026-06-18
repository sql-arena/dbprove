CREATE TABLE tpch_sf1.part
(
    p_partkey     INT NOT NULL,
    p_name        TEXT NOT NULL,
    p_mfgr        TEXT NOT NULL,
    p_brand       TEXT NOT NULL,
    p_type        TEXT NOT NULL,
    p_size        INT NOT NULL,
    p_container   TEXT NOT NULL,
    p_retailprice DECIMAL(15, 2) NOT NULL,
    p_comment     TEXT NOT NULL
);
