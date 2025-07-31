SELECT D1.id_dim1, F1.d, COUNT(*), SUM(F1.d) AS sd
FROM dim1 AS D1
JOIN fact AS F1 ON D1.id_dim1 = F1.id_dim1
GROUP BY D1.id_dim1, F1.d;
