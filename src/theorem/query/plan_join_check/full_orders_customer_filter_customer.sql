SELECT COUNT(o.o_orderkey), COUNT(c.c_custkey)
FROM tpch_sf1.orders AS o
FULL JOIN (
  SELECT *
  FROM tpch_sf1.customer
  WHERE c_custkey = 1
) AS c
  ON o.o_custkey = c.c_custkey;
