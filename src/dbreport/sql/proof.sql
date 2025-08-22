DROP TABLE IF EXISTS proof;
CREATE TABLE proof (
    engine STRING
    , id BIGINT
    , theorem_type STRING
    , theorem STRING
    , description STRING
    , proof STRING
    , value STRING
    , unit STRING
);
