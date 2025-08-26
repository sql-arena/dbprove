/* TPC-H Q15 */
WITH revenue AS (SELECT l_suppkey                               AS supplier_no,
                        SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
                 FROM tpch.lineitem
                 WHERE l_shipdate >= DATE '1997-09-01'
                   AND l_shipdate < DATE '1997-12-01'
                 GROUP BY l_suppkey)
SELECT s_suppkey,
       s_name,
       s_address,
       s_phone,
       total_revenue
FROM tpch.supplier
INNER JOIN revenue
    ON s_suppkey = supplier_no
WHERE total_revenue = (SELECT MAX(total_revenue)
                       FROM revenue)
ORDER BY s_suppkey;
