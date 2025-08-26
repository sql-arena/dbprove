/* TPC-H Q18 */
SELECT c_name,
       c_custkey,
       o_orderkey,
       o_orderdate,
       o_totalprice,
       SUM(l_quantity)
FROM tpch.lineitem
INNER JOIN tpch.orders
    ON l_orderkey = o_orderkey
INNER JOIN tpch.customer
    ON o_custkey = c_custkey
WHERE o_orderkey IN (SELECT l_orderkey
                     FROM tpch.lineitem
                     GROUP BY l_orderkey
                     HAVING SUM(l_quantity) > 314)
GROUP BY c_name,
         c_custkey,
         o_orderkey,
         o_orderdate,
         o_totalprice
ORDER BY o_totalprice DESC,
         o_orderdate;