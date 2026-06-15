SELECT COUNT(o.o_orderkey), COUNT(c.c_custkey)
FROM tpch.orders AS o
LEFT JOIN tpch.customer AS c
  ON o.o_custkey = c.c_custkey;
