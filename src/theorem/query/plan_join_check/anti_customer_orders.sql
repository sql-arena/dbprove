SELECT COUNT(c.c_custkey)
FROM tpch.customer AS c
WHERE NOT EXISTS (
  SELECT 1
  FROM tpch.orders AS o
  WHERE o.o_custkey = c.c_custkey
);
