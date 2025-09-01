/* TPC-H Q17 */
SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly
FROM tpch.lineitem
INNER JOIN tpch.part
    ON l_partkey = p_partkey
WHERE p_brand = 'Brand#13'
  AND p_container = 'MED CAN'
  AND l_quantity < (SELECT 0.2 * AVG(l_quantity)
                    FROM tpch.lineitem li
                    WHERE l_partkey = p_partkey);