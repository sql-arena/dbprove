/* TPC-H Q11 */
SELECT ps_partkey,
       SUM(ps_supplycost * ps_availqty) AS v
FROM tpch_sf1.partsupp
INNER JOIN tpch_sf1.supplier
    ON ps_suppkey = s_suppkey
INNER JOIN tpch_sf1.nation
    ON s_nationkey = n_nationkey
WHERE n_name = 'JAPAN'
GROUP BY ps_partkey
HAVING SUM(ps_supplycost * ps_availqty)
           > (SELECT SUM(ps_supplycost * ps_availqty) * 0.0001
              FROM tpch_sf1.partsupp
              INNER JOIN tpch_sf1.supplier
                  ON ps_suppkey = s_suppkey
              INNER JOIN tpch_sf1.nation
                  ON s_nationkey = n_nationkey
              WHERE n_name = 'JAPAN')
ORDER BY v DESC
