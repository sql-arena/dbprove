/* TPC-H Q22 */
SELECT cntrycode,
       COUNT(*)       AS numcust,
       SUM(c_acctbal) AS totacctbal
FROM (SELECT LEFT(c_phone,2) AS cntrycode,
             c_acctbal
      FROM tpch.customer
      WHERE LEFT(c_phone,2) IN
            ('10', '17', '19', '23', '22', '31', '27')
        AND c_acctbal > (SELECT AVG(c_acctbal)
                         FROM tpch.customer
                         WHERE c_acctbal > 0.00
                           AND LEFT(c_phone,2) IN
                               ('10', '17', '19', '23', '22', '31', '27'))
        AND NOT EXISTS (SELECT *
                        FROM tpch.orders
                        WHERE o_custkey = c_custkey)) AS custsale
GROUP BY cntrycode
ORDER BY cntrycode


