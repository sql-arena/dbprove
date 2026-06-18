SELECT COUNT(o.o_orderkey), COUNT(c.c_custkey)
FROM tpch_sf1.orders AS o
LEFT JOIN tpch_sf1.customer AS c
  ON o.o_custkey = c.c_custkey;
