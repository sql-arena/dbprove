/* TPC-H Q20 */
SELECT s_name,
       s_address
FROM tpch_sf1.supplier
INNER JOIN tpch_sf1.nation
    ON s_nationkey = n_nationkey
WHERE s_suppkey IN (SELECT ps_suppkey
                    FROM tpch_sf1.partsupp
                    WHERE ps_partkey IN (SELECT p_partkey
                                         FROM tpch_sf1.part AS p_inner
                                         WHERE p_name LIKE 'almond%')
                      AND ps_availqty > (SELECT 0.5 * SUM(l_quantity) AS qty
                                         FROM tpch_sf1.lineitem as li_inner
                                         WHERE l_partkey = ps_partkey
                                           AND l_suppkey = ps_suppkey
                                           AND l_shipdate >= '1993-01-01'
                                           AND l_shipdate < '1994-01-01'))
  AND n_name = 'KENYA'
ORDER BY s_name

