/* TPC-H Q02 */
SELECT s_acctbal,
       s_name,
       n_name,
       p_partkey,
       p_mfgr,
       s_address,
       s_phone,
       s_comment
FROM tpch_sf1.partsupp
INNER JOIN tpch_sf1.part
    ON ps_partkey = p_partkey
INNER JOIN tpch_sf1.supplier
    ON ps_suppkey = s_suppkey
INNER JOIN tpch_sf1.nation
    ON s_nationkey = n_nationkey
INNER JOIN tpch_sf1.region
    ON n_regionkey = r_regionkey
WHERE p_size = 25
  AND p_type LIKE '%BRASS'
  AND r_name = 'EUROPE'
  AND ps_supplycost = (SELECT MIN(ps_supplycost) AS min_cost
                       FROM tpch_sf1.partsupp AS ps_min
                       INNER JOIN tpch_sf1.supplier AS s_min
                           ON ps_suppkey = s_suppkey
                       INNER JOIN tpch_sf1.nation AS n_min
                           ON s_nationkey = n_nationkey
                       INNER JOIN tpch_sf1.region AS r_min
                           ON n_regionkey = r_regionkey
                       WHERE ps_partkey = p_partkey
                         AND r_name = 'EUROPE')
ORDER BY s_acctbal DESC,
         n_name,
         s_name,
         p_partkey


