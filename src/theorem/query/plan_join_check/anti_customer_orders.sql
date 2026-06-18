SELECT COUNT(c.c_custkey)
FROM tpch_sf1.customer AS c
WHERE NOT EXISTS (
  SELECT 1
  FROM tpch_sf1.orders AS o
  WHERE o.o_custkey = c.c_custkey
);
