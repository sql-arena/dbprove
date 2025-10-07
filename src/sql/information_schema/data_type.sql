SELECT data_type
     , character_maximum_length
     , numeric_precision AS p
     , numeric_scale     AS s
FROM information_schema.columns
WHERE table_name = '{table_name}'
  AND table_schema = '{schema_name}'
ORDER BY ordinal_position
;
