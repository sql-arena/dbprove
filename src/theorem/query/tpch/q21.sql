/* TPC-H Q21 */
SELECT s_name, COUNT(*) AS numwait
FROM tpch.lineitem l1
INNER JOIN tpch.orders
    ON l1.l_orderkey = o_orderkey
INNER JOIN tpch.supplier
    ON l1.l_suppkey = s_suppkey
INNER JOIN tpch.nation
    ON s_nationkey = n_nationkey
WHERE o_orderstatus = 'F'
  AND l1.l_receiptdate > l1.l_commitdate
  AND EXISTS (SELECT 1
              FROM tpch.lineitem l2
              WHERE l2.l_orderkey = l1.l_orderkey
                AND l2.l_suppkey <> l1.l_suppkey)
  AND NOT EXISTS (SELECT 1
                  FROM tpch.lineitem l3
                  WHERE l3.l_orderkey = l1.l_orderkey
                    AND l3.l_suppkey <> l1.l_suppkey
                    AND l3.l_receiptdate
                      > l3.l_commitdate)
  AND n_name = 'GERMANY'
GROUP BY s_name
ORDER BY numwait DESC,
         s_name
