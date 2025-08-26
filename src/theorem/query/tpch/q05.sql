/* TPC-H Q05 */
SELECT n_name,
       SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM tpch.lineitem
INNER JOIN tpch.orders
    ON l_orderkey = o_orderkey
INNER JOIN tpch.customer
    ON o_custkey = c_custkey
INNER JOIN tpch.supplier
    ON l_suppkey = s_suppkey
INNER JOIN tpch.nation
    ON s_nationkey = n_nationkey
INNER JOIN tpch.region
    ON n_regionkey = r_regionkey
WHERE r_name = 'EUROPE'
  AND o_orderdate >= DATE '1995-01-01'
  AND o_orderdate < DATE '1996-01-01'
  AND s_nationkey = c_nationkey
GROUP BY n_name
ORDER BY revenue DESC;
