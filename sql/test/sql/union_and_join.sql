SELECT id_dim1 AS v
FROM dim1 AS d1
UNION DISTINCT
SELECT F.id_dim2 AS v
FROM dim2 AS d2f
         JOIN fact F ON F.id_dim2 = d2f.id_dim2
UNION DISTINCT
SELECT DISTINCT id_dim2 AS v
FROM fact
ORDER BY v
    LIMIT 10