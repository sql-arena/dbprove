/* TPC-H Q14 */
SELECT 100.00 * SUM(CASE
                        WHEN p_type LIKE 'PROMO%'
                            THEN l_extendedprice * (1 - l_discount)
                        ELSE 0
    END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM tpch.lineitem
INNER JOIN tpch.part
    ON l_partkey = p_partkey
WHERE l_shipdate >= DATE '1996-02-01'
  AND l_shipdate < DATE '1996-03-01'