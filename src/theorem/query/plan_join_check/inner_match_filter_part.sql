SELECT COUNT(l.l_partkey), COUNT(p.p_partkey)
FROM tpch_sf1.lineitem AS l
JOIN (
  SELECT *
  FROM tpch_sf1.part
  WHERE p_partkey = 1
) AS p
  ON l.l_partkey = p.p_partkey;
