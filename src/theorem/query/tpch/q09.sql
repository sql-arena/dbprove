/* TPC-H Q09 */
SELECT nation,
       o_year,
       SUM(amount) AS sum_profit
FROM (SELECT n_name                                                          AS nation,
             EXTRACT(YEAR FROM o_orderdate)                                  AS o_year,
             l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
      FROM tpch.lineitem
      INNER JOIN tpch.orders
          ON l_orderkey = o_orderkey
      INNER JOIN tpch.partsupp
          ON l_suppkey = ps_suppkey
          AND l_partkey = ps_partkey
      INNER JOIN tpch.part
          ON l_partkey = p_partkey
      INNER JOIN tpch.supplier
          ON l_suppkey = s_suppkey
      INNER JOIN tpch.nation
          ON s_nationkey = n_nationkey
      WHERE p_name LIKE '%lace%') AS profit
GROUP BY nation,
         o_year
ORDER BY nation,
         o_year DESC