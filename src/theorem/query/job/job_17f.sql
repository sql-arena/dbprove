SELECT MIN(n.name) AS member_in_charnamed_movie
FROM job.cast_info AS ci,
     job.company_name AS cn,
     job.keyword AS k,
     job.movie_companies AS mc,
     job.movie_keyword AS mk,
     job.name AS n,
     job.title AS t
WHERE k.keyword ='character-name-in-title'
  AND n.name LIKE '%B%'
  AND n.id = ci.person_id
  AND ci.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND t.id = mc.movie_id
  AND mc.company_id = cn.id
  AND ci.movie_id = mc.movie_id
  AND ci.movie_id = mk.movie_id
  AND mc.movie_id = mk.movie_id;
