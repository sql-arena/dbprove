/* TPC-H Q09 */
SELECT nation,
       o_year,
       SUM(amount) AS sum_profit
FROM (SELECT n_name                                                          AS nation,
             EXTRACT(YEAR FROM o_orderdate)                                  AS o_year,
             l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
      FROM tpch_sf1.lineitem
      INNER JOIN tpch_sf1.orders
          ON l_orderkey = o_orderkey
      INNER JOIN tpch_sf1.partsupp
          ON l_suppkey = ps_suppkey
          AND l_partkey = ps_partkey
      INNER JOIN tpch_sf1.part
          ON l_partkey = p_partkey
      INNER JOIN tpch_sf1.supplier
          ON l_suppkey = s_suppkey
      INNER JOIN tpch_sf1.nation
          ON s_nationkey = n_nationkey
      WHERE p_name LIKE '%lace%') AS profit
GROUP BY nation,
         o_year
ORDER BY nation,
         o_year DESC