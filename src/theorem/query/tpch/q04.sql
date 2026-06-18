/* TPC-H Q04 */
SELECT o_orderpriority,
       COUNT(*) AS order_count
FROM tpch_sf1.orders
WHERE o_orderdate >= '1995-02-01'
  AND o_orderdate < '1995-05-01'
  AND EXISTS (SELECT *
              FROM tpch_sf1.lineitem
              WHERE l_orderkey = o_orderkey
                AND l_commitdate < l_receiptdate)
GROUP BY o_orderpriority
ORDER BY o_orderpriority
