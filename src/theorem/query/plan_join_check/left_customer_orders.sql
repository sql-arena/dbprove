SELECT COUNT(c.c_custkey), COUNT(o.o_orderkey)
FROM tpch_sf1.customer AS c
LEFT JOIN tpch_sf1.orders AS o
  ON c.c_custkey = o.o_custkey;
