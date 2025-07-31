/* A sufficiently tricky query that shows basic operators in action */
SELECT s, d, n
FROM (SELECT F1.id_dim1, MIN(D1.s) AS s, SUM(F1.d) AS d
      FROM dim1 AS D1
               JOIN fact AS F1 ON D1.id_dim1 = F1.id_dim1
      GROUP BY F1.id_dim1) AS S1
         JOIN (SELECT D2.id_dim2, D2.n
               FROM dim2 AS D2
                        JOIN fact AS F2 ON D2.id_dim2 = F2.id_dim2
               ORDER BY D2.id_dim2 LIMIT 10) AS S2
              ON S1.id_dim1 = S2.id_dim2
;