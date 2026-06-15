SELECT COUNT(c.c_custkey), COUNT(o.o_orderkey)
FROM tpch.customer AS c
LEFT JOIN tpch.orders AS o
  ON c.c_custkey = o.o_custkey;
