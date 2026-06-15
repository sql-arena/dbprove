SELECT MIN(aka_t.title) AS aka_title,
       MIN(t.title) AS internet_movie_title
FROM job.aka_title AS aka_t,
     job.company_name AS cn,
     job.company_type AS ct,
     job.info_type AS it1,
     job.keyword AS k,
     job.movie_companies AS mc,
     job.movie_info AS mi,
     job.movie_keyword AS mk,
     job.title AS t
WHERE cn.country_code = '[us]'
  AND it1.info = 'release dates'
  AND mi.note LIKE '%internet%'
  AND t.production_year > 1990
  AND t.id = aka_t.movie_id
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id
  AND t.id = mc.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mc.movie_id
  AND mk.movie_id = aka_t.movie_id
  AND mi.movie_id = mc.movie_id
  AND mi.movie_id = aka_t.movie_id
  AND mc.movie_id = aka_t.movie_id
  AND k.id = mk.keyword_id
  AND it1.id = mi.info_type_id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id;
