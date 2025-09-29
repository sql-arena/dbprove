/* TPC-H Q08 */
SELECT o_year,
       SUM(CASE
               WHEN nation = 'FRANCE'
                   THEN volume
               ELSE 0
           END) / SUM(volume) AS mkt_share
FROM (SELECT EXTRACT(YEAR FROM o_orderdate)     AS o_year,
             l_extendedprice * (1 - l_discount) AS volume,
             n2.n_name                          AS nation
      FROM tpch.lineitem
      INNER JOIN tpch.orders
          ON o_orderkey = l_orderkey
      INNER JOIN tpch.part
          ON l_partkey = p_partkey
      INNER JOIN tpch.supplier
          ON l_suppkey = s_suppkey
      INNER JOIN tpch.customer
          ON o_custkey = c_custkey
      INNER JOIN tpch.nation n1
          ON c_nationkey = n1.n_nationkey
      INNER JOIN tpch.region
          ON n1.n_regionkey = r_regionkey
      INNER JOIN tpch.nation n2
          ON s_nationkey = n2.n_nationkey
      WHERE r_name = 'EUROPE'
        AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
        AND p_type = 'SMALL POLISHED NICKEL') AS all_nations
GROUP BY o_year
ORDER BY o_year