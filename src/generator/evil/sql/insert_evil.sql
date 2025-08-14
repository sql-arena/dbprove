INSERT INTO evil (k, i_even_odd, i_skew, s_even_odd, s_skew)
SELECT S.n
    + 10 * P10.n
    + 100 * P100.n
    + 1000 * P1000.n
    + 10000 * P10000.n
    + 100000 * P100000.n
    + 1000000 * P1000000.n
    + 10000000 * P10000000.n
    + 100000000 * P100000000.n
     , S.i_even_odd
     , S.i_skew
     , S.s_even_odd
     , S.s_skew
FROM seed10 S
   , seed10 P10
   , seed10 P100
   , seed10 P1000
   , seed10 P10000
   , seed10 P100000
   , seed10 P1000000
   , seed10 P10000000
   , seed10 P100000000
;
