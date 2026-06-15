SELECT COUNT(o.o_orderkey), COUNT(c.c_custkey)
FROM tpch.orders AS o
LEFT JOIN (
  SELECT *
  FROM tpch.customer
  WHERE c_custkey = 1
) AS c
  ON o.o_custkey = c.c_custkey;
