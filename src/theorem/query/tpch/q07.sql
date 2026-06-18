/* TPC-H Q07 */
SELECT supp_nation,
       cust_nation,
       l_year,
       SUM(volume) AS revenue
FROM (SELECT n1.n_name                          AS supp_nation,
             n2.n_name                          AS cust_nation,
             EXTRACT(YEAR FROM l_shipdate)      AS l_year,
             l_extendedprice * (1 - l_discount) AS volume
      FROM tpch_sf1.lineitem
      INNER JOIN tpch_sf1.orders
          ON l_orderkey = o_orderkey
      INNER JOIN tpch_sf1.supplier
          ON l_suppkey = s_suppkey
      INNER JOIN tpch_sf1.customer
          ON o_custkey = c_custkey
      INNER JOIN tpch_sf1.nation n1
          ON s_nationkey = n1.n_nationkey
      INNER JOIN tpch_sf1.nation n2
          ON c_nationkey = n2.n_nationkey
      WHERE ((n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
          OR (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY'))
        AND l_shipdate BETWEEN '1995-01-01' AND '1996-12-31') AS shipping
GROUP BY supp_nation,
         cust_nation,
         l_year
ORDER BY supp_nation,
         cust_nation,
         l_year


