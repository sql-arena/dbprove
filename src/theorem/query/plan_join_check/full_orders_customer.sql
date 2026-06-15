SELECT COUNT(o.o_orderkey), COUNT(c.c_custkey)
FROM tpch.orders AS o
FULL JOIN tpch.customer AS c
  ON o.o_custkey = c.c_custkey;
