SELECT MIN(cn1.name) AS first_company,
       MIN(cn.name) AS second_company,
       MIN(mi_idx1.info) AS first_rating,
       MIN(mi_idx.info) AS second_rating,
       MIN(t1.title) AS first_movie,
       MIN(t.title) AS second_movie
FROM company_name AS cn1,
     company_name AS cn,
     info_type AS it1,
     info_type AS it,
     kind_type AS kt1,
     kind_type AS kt,
     link_type AS lt,
     movie_companies AS mc1,
     movie_companies AS mc,
     movie_info_idx AS mi_idx1,
     movie_info_idx AS mi_idx,
     movie_link AS ml,
     title AS t1,
     title AS t
WHERE cn1.country_code != '[us]'
  AND it1.info = 'rating'
  AND it.info = 'rating'
  AND kt1.kind IN ('tv series',
                   'episode')
  AND kt.kind IN ('tv series',
                   'episode')
  AND lt.link IN ('sequel',
                  'follows',
                  'followed by')
  AND mi_idx.info < '3.5'
  AND t.production_year BETWEEN 2000 AND 2010
  AND lt.id = ml.link_type_id
  AND t1.id = ml.movie_id
  AND t.id = ml.linked_movie_id
  AND it1.id = mi_idx1.info_type_id
  AND t1.id = mi_idx1.movie_id
  AND kt1.id = t1.kind_id
  AND cn1.id = mc1.company_id
  AND t1.id = mc1.movie_id
  AND ml.movie_id = mi_idx1.movie_id
  AND ml.movie_id = mc1.movie_id
  AND mi_idx1.movie_id = mc1.movie_id
  AND it.id = mi_idx.info_type_id
  AND t.id = mi_idx.movie_id
  AND kt.id = t.kind_id
  AND cn.id = mc.company_id
  AND t.id = mc.movie_id
  AND ml.linked_movie_id = mi_idx.movie_id
  AND ml.linked_movie_id = mc.movie_id
  AND mi_idx.movie_id = mc.movie_id;

