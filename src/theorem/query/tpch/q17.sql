/* TPC-H Q17 */
SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly
FROM tpch_sf1.lineitem
INNER JOIN tpch_sf1.part
    ON l_partkey = p_partkey
WHERE p_brand = 'Brand#13'
  AND p_container = 'MED CAN'
  AND l_quantity < (SELECT 0.2 * AVG(l_quantity) AS l_avg
                    FROM tpch_sf1.lineitem li
                    WHERE l_partkey = p_partkey)