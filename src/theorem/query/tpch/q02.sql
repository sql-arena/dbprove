/* TPC-H Q02 */
SELECT s_acctbal,
       s_name,
       n_name,
       p_partkey,
       p_mfgr,
       s_address,
       s_phone,
       s_comment
FROM tpch.partsupp
INNER JOIN tpch.part
    ON ps_partkey = p_partkey
INNER JOIN tpch.supplier
    ON ps_suppkey = s_suppkey
INNER JOIN tpch.nation
    ON s_nationkey = n_nationkey
INNER JOIN tpch.region
    ON n_regionkey = r_regionkey
WHERE p_size = 25
  AND p_type LIKE '%BRASS'
  AND r_name = 'EUROPE'
  AND ps_supplycost = (SELECT MIN(ps_supplycost)
                       FROM tpch.partsupp AS ps_min
                       INNER JOIN tpch.supplier AS s_min
                           ON ps_suppkey = s_suppkey
                       INNER JOIN tpch.nation AS n_min
                           ON s_nationkey = n_nationkey
                       INNER JOIN region AS r_min
                           ON n_regionkey = r_regionkey
                       WHERE ps_partkey = p_partkey
                         AND r_name = 'EUROPE')
ORDER BY s_acctbal DESC,
         n_name,
         s_name,
         p_partkey;
