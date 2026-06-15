SELECT COUNT(c.c_custkey)
FROM (
  SELECT *
  FROM tpch.customer
  WHERE c_custkey = 1
) AS c
WHERE EXISTS (
  SELECT 1
  FROM tpch.orders AS o
  WHERE o.o_custkey = c.c_custkey
);
