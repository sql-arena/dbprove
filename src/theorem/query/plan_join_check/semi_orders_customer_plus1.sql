SELECT COUNT(o.o_orderkey)
FROM tpch_sf1.orders AS o
WHERE EXISTS (
  SELECT 1
  FROM tpch_sf1.customer AS c
  WHERE c.c_custkey + 1 = o.o_custkey + 1
);
