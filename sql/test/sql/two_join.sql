SELECT D1.id_dim1, F.d, D1.s, D2.n
FROM fact AS F
JOIN dim1 AS D1 ON F.id_dim1 = D1.id_dim1
JOIN dim2 AS D2 ON F.id_dim2 = D2.id_dim2
