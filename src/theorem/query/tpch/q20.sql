/* TPC-H Q20 */
SELECT s_name,
       s_address
FROM tpch.supplier
INNER JOIN tpch.nation
    ON s_nationkey = n_nationkey
WHERE s_suppkey IN (SELECT ps_suppkey
                    FROM tpch.partsupp
                    WHERE ps_partkey IN (SELECT p_partkey
                                         FROM tpch.part AS p_inner
                                         WHERE p_name LIKE 'almond%')
                      AND ps_availqty > (SELECT 0.5 * SUM(l_quantity) AS qty
                                         FROM tpch.lineitem as li_inner
                                         WHERE l_partkey = ps_partkey
                                           AND l_suppkey = ps_suppkey
                                           AND l_shipdate >= '1993-01-01'
                                           AND l_shipdate < '1994-01-01'))
  AND n_name = 'KENYA'
ORDER BY s_name

