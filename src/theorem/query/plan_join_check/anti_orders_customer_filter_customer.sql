SELECT COUNT(o.o_orderkey)
FROM tpch.orders AS o
WHERE NOT EXISTS (
  SELECT 1
  FROM tpch.customer AS c
  WHERE c.c_custkey = o.o_custkey
    AND c.c_custkey = 1
);
