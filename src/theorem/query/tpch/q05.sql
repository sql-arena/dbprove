/* TPC-H Q05 */
SELECT n_name,
       SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM tpch_sf1.lineitem
INNER JOIN tpch_sf1.orders
    ON l_orderkey = o_orderkey
INNER JOIN tpch_sf1.customer
    ON o_custkey = c_custkey
INNER JOIN tpch_sf1.supplier
    ON l_suppkey = s_suppkey
INNER JOIN tpch_sf1.nation
    ON s_nationkey = n_nationkey
INNER JOIN tpch_sf1.region
    ON n_regionkey = r_regionkey
WHERE r_name = 'EUROPE'
  AND o_orderdate >= '1995-01-01'
  AND o_orderdate < '1996-01-01'
  AND s_nationkey = c_nationkey
GROUP BY n_name
ORDER BY revenue DESC
