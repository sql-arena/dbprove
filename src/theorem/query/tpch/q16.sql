/* TPC-H Q16 */
SELECT p_brand,
       p_type,
       p_size,
       COUNT(DISTINCT ps_suppkey) AS supplier_cnt
FROM tpch.partsupp
INNER JOIN tpch.part
    ON ps_partkey = p_partkey

WHERE p_brand <> 'Brand#42'
  AND p_type NOT LIKE 'STANDARD ANODIZED%'
  AND p_size IN (3, 7, 11, 29, 31, 37, 41, 49)
  AND ps_suppkey NOT IN (SELECT s_suppkey
                         FROM tpch.supplier
                         WHERE s_comment LIKE '%Customer%Complaints%')
GROUP BY p_brand,
         p_type,
         p_size
ORDER BY supplier_cnt DESC,
         p_brand,
         p_type,
         p_size;