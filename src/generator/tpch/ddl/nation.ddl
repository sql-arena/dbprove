CREATE TABLE tpch.nation
(
    n_nationkey INT PRIMARY KEY,
    n_name      VARCHAR(25),
    n_regionkey INT,
    n_comment   VARCHAR(152)
);
