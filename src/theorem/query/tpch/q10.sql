/* TPC-H Q10 */
SELECT c_custkey,
       c_name,
       SUM(l_extendedprice * (1 - l_discount)) AS revenue,
       c_acctbal,
       n_name,
       c_address,
       c_phone,
       c_comment
FROM tpch.lineitem
INNER JOIN tpch.orders
    ON l_orderkey = o_orderkey
INNER JOIN tpch.customer
    ON o_custkey = c_custkey
INNER JOIN tpch.nation
    ON c_nationkey = n_nationkey
WHERE o_orderdate >= DATE '1994-06-01'
  AND o_orderdate < DATE '1994-09-01'
  AND l_returnflag = 'R'
GROUP BY c_custkey,
         c_name,
         c_acctbal,
         c_phone,
         n_name,
         c_address,
         c_comment
ORDER BY revenue DESC