/* TPC-H Q05 Handroll */
WITH supplier_push AS (SELECT s.*
                       FROM tpch.supplier AS s
                       INNER JOIN tpch.nation
                           ON s_nationkey = n_nationkey
                       INNER JOIN tpch.region
                           ON n_regionkey = r_regionkey
                       WHERE r_name = 'EUROPE')
SELECT n_name,
       SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM tpch.lineitem
INNER JOIN tpch.orders
    ON l_orderkey = o_orderkey
INNER JOIN tpch.customer
    ON o_custkey = c_custkey
INNER JOIN tpch.nation
    ON c_nationkey = n_nationkey
INNER JOIN tpch.region
    ON n_regionkey = r_regionkey
WHERE r_name = 'EUROPE'
  AND o_orderdate >= '1995-01-01'
  AND o_orderdate < '1996-01-01'
  AND l_suppkey IN (SELECT s_suppkey FROM supplier_push)
GROUP BY n_name
ORDER BY revenue DESC
