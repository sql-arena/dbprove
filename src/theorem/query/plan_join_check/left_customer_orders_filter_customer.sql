SELECT COUNT(c.c_custkey), COUNT(o.o_orderkey)
FROM (
  SELECT *
  FROM tpch_sf1.customer
  WHERE c_custkey = 1
) AS c
LEFT JOIN tpch_sf1.orders AS o
  ON c.c_custkey = o.o_custkey;
