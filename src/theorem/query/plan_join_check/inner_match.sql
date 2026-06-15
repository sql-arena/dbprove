SELECT COUNT(l.l_partkey), COUNT(p.p_partkey)
FROM tpch.lineitem AS l
JOIN tpch.part AS p
  ON l.l_partkey = p.p_partkey;
