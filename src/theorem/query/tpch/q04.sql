/* TPC-H Q04 */
SELECT o_orderpriority,
       COUNT(*) AS order_count
FROM tpch.orders
WHERE o_orderdate >= DATE '1995-02-01'
  AND o_orderdate < DATE '1995-02-01' + INTERVAL '3' MONTH
  AND EXISTS (SELECT *
              FROM tpch.lineitem
              WHERE l_orderkey = o_orderkey
                AND l_commitdate < l_receiptdate)
GROUP BY o_orderpriority
ORDER BY o_orderpriority
