SELECT COUNT(o.o_orderkey)
FROM tpch.orders AS o
WHERE EXISTS (
  SELECT 1
  FROM tpch.customer AS c
  WHERE c.c_custkey + 1 = o.o_custkey + 1
    AND c.c_custkey = 1
);
