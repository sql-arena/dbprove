/* TPC-H Q03 */
SELECT l_orderkey,
       SUM(l_extendedprice * (1 - l_discount)) AS revenue,
       o_orderdate,
       o_shippriority
FROM tpch.lineitem
INNER JOIN tpch.orders
    ON l_orderkey = o_orderkey
INNER JOIN tpch.customer
    ON o_custkey = c_custkey
WHERE c_mktsegment = 'MACHINERY'
  AND o_orderdate < '1995-03-15'
  AND l_shipdate > '1995-03-15'
GROUP BY l_orderkey,
         o_orderdate,
         o_shippriority
ORDER BY revenue DESC,
         o_orderdate
LIMIT 10
