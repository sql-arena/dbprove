SELECT MIN(chn.name) AS character,
       MIN(t.title) AS movie_with_american_producer
FROM job.char_name AS chn,
     job.cast_info AS ci,
     job.company_name AS cn,
     job.company_type AS ct,
     job.movie_companies AS mc,
     job.role_type AS rt,
     job.title AS t
WHERE ci.note LIKE '%(producer)%'
  AND cn.country_code = '[us]'
  AND t.production_year > 1990
  AND t.id = mc.movie_id
  AND t.id = ci.movie_id
  AND ci.movie_id = mc.movie_id
  AND chn.id = ci.person_role_id
  AND rt.id = ci.role_id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id;
