BEGIN TRANSACTION;

DROP TABLE IF EXISTS job.aka_name__old;
DROP TABLE IF EXISTS job.aka_title__old;
DROP TABLE IF EXISTS job.cast_info__old;
DROP TABLE IF EXISTS job.char_name__old;
DROP TABLE IF EXISTS job.comp_cast_type__old;
DROP TABLE IF EXISTS job.company_name__old;
DROP TABLE IF EXISTS job.company_type__old;
DROP TABLE IF EXISTS job.complete_cast__old;
DROP TABLE IF EXISTS job.info_type__old;
DROP TABLE IF EXISTS job.keyword__old;
DROP TABLE IF EXISTS job.kind_type__old;
DROP TABLE IF EXISTS job.link_type__old;
DROP TABLE IF EXISTS job.movie_companies__old;
DROP TABLE IF EXISTS job.movie_info__old;
DROP TABLE IF EXISTS job.movie_info_idx__old;
DROP TABLE IF EXISTS job.movie_keyword__old;
DROP TABLE IF EXISTS job.movie_link__old;
DROP TABLE IF EXISTS job.name__old;
DROP TABLE IF EXISTS job.person_info__old;
DROP TABLE IF EXISTS job.role_type__old;
DROP TABLE IF EXISTS job.title__old;

ALTER TABLE job.aka_name RENAME TO aka_name__old;
ALTER TABLE job.aka_title RENAME TO aka_title__old;
ALTER TABLE job.cast_info RENAME TO cast_info__old;
ALTER TABLE job.char_name RENAME TO char_name__old;
ALTER TABLE job.comp_cast_type RENAME TO comp_cast_type__old;
ALTER TABLE job.company_name RENAME TO company_name__old;
ALTER TABLE job.company_type RENAME TO company_type__old;
ALTER TABLE job.complete_cast RENAME TO complete_cast__old;
ALTER TABLE job.info_type RENAME TO info_type__old;
ALTER TABLE job.keyword RENAME TO keyword__old;
ALTER TABLE job.kind_type RENAME TO kind_type__old;
ALTER TABLE job.link_type RENAME TO link_type__old;
ALTER TABLE job.movie_companies RENAME TO movie_companies__old;
ALTER TABLE job.movie_info RENAME TO movie_info__old;
ALTER TABLE job.movie_info_idx RENAME TO movie_info_idx__old;
ALTER TABLE job.movie_keyword RENAME TO movie_keyword__old;
ALTER TABLE job.movie_link RENAME TO movie_link__old;
ALTER TABLE job.name RENAME TO name__old;
ALTER TABLE job.person_info RENAME TO person_info__old;
ALTER TABLE job.role_type RENAME TO role_type__old;
ALTER TABLE job.title RENAME TO title__old;

CREATE TABLE job.char_name (
    id integer NOT NULL PRIMARY KEY,
    name text NOT NULL,
    imdb_index character varying(12),
    imdb_id integer,
    name_pcode_nf character varying(5),
    surname_pcode character varying(5),
    md5sum character varying(32)
);
INSERT INTO job.char_name SELECT * FROM job.char_name__old;

CREATE TABLE job.comp_cast_type (
    id integer NOT NULL PRIMARY KEY,
    kind character varying(32) NOT NULL
);
INSERT INTO job.comp_cast_type SELECT * FROM job.comp_cast_type__old;

CREATE TABLE job.company_name (
    id integer NOT NULL PRIMARY KEY,
    name text NOT NULL,
    country_code character varying(255),
    imdb_id integer,
    name_pcode_nf character varying(5),
    name_pcode_sf character varying(5),
    md5sum character varying(32)
);
INSERT INTO job.company_name SELECT * FROM job.company_name__old;

CREATE TABLE job.company_type (
    id integer NOT NULL PRIMARY KEY,
    kind character varying(32) NOT NULL
);
INSERT INTO job.company_type SELECT * FROM job.company_type__old;

CREATE TABLE job.info_type (
    id integer NOT NULL PRIMARY KEY,
    info character varying(32) NOT NULL
);
INSERT INTO job.info_type SELECT * FROM job.info_type__old;

CREATE TABLE job.keyword (
    id integer NOT NULL PRIMARY KEY,
    keyword text NOT NULL,
    phonetic_code character varying(5)
);
INSERT INTO job.keyword SELECT * FROM job.keyword__old;

CREATE TABLE job.kind_type (
    id integer NOT NULL PRIMARY KEY,
    kind character varying(15) NOT NULL
);
INSERT INTO job.kind_type SELECT * FROM job.kind_type__old;

CREATE TABLE job.link_type (
    id integer NOT NULL PRIMARY KEY,
    link character varying(32) NOT NULL
);
INSERT INTO job.link_type SELECT * FROM job.link_type__old;

CREATE TABLE job.name (
    id integer NOT NULL PRIMARY KEY,
    name text NOT NULL,
    imdb_index character varying(12),
    imdb_id integer,
    gender character varying(1),
    name_pcode_cf character varying(5),
    name_pcode_nf character varying(5),
    surname_pcode character varying(5),
    md5sum character varying(32)
);
INSERT INTO job.name SELECT * FROM job.name__old;

CREATE TABLE job.role_type (
    id integer NOT NULL PRIMARY KEY,
    role character varying(32) NOT NULL
);
INSERT INTO job.role_type SELECT * FROM job.role_type__old;

CREATE TABLE job.title (
    id integer NOT NULL PRIMARY KEY,
    title text NOT NULL,
    imdb_index character varying(12),
    kind_id integer NOT NULL REFERENCES job.kind_type(id),
    production_year integer,
    imdb_id integer,
    phonetic_code character varying(5),
    episode_of_id integer,
    season_nr integer,
    episode_nr integer,
    series_years character varying(49),
    md5sum character varying(32)
);
INSERT INTO job.title SELECT * FROM job.title__old;

CREATE TABLE job.aka_name (
    id integer NOT NULL PRIMARY KEY,
    person_id integer NOT NULL REFERENCES job.name(id),
    name text NOT NULL,
    imdb_index character varying(12),
    name_pcode_cf character varying(5),
    name_pcode_nf character varying(5),
    surname_pcode character varying(5),
    md5sum character varying(32)
);
INSERT INTO job.aka_name SELECT * FROM job.aka_name__old;

CREATE TABLE job.aka_title (
    id integer NOT NULL PRIMARY KEY,
    movie_id integer NOT NULL REFERENCES job.title(id),
    title text NOT NULL,
    imdb_index character varying(12),
    kind_id integer NOT NULL REFERENCES job.kind_type(id),
    production_year integer,
    phonetic_code character varying(5),
    episode_of_id integer REFERENCES job.title(id),
    season_nr integer,
    episode_nr integer,
    note text,
    md5sum character varying(32)
);
INSERT INTO job.aka_title SELECT * FROM job.aka_title__old;

CREATE TABLE job.cast_info (
    id integer NOT NULL PRIMARY KEY,
    person_id integer NOT NULL REFERENCES job.name(id),
    movie_id integer NOT NULL REFERENCES job.title(id),
    person_role_id integer REFERENCES job.char_name(id),
    note text,
    nr_order integer,
    role_id integer NOT NULL REFERENCES job.role_type(id)
);
INSERT INTO job.cast_info SELECT * FROM job.cast_info__old;

CREATE TABLE job.complete_cast (
    id integer NOT NULL PRIMARY KEY,
    movie_id integer REFERENCES job.title(id),
    subject_id integer NOT NULL REFERENCES job.comp_cast_type(id),
    status_id integer NOT NULL REFERENCES job.comp_cast_type(id)
);
INSERT INTO job.complete_cast SELECT * FROM job.complete_cast__old;

CREATE TABLE job.movie_companies (
    id integer NOT NULL PRIMARY KEY,
    movie_id integer NOT NULL REFERENCES job.title(id),
    company_id integer NOT NULL REFERENCES job.company_name(id),
    company_type_id integer NOT NULL REFERENCES job.company_type(id),
    note text
);
INSERT INTO job.movie_companies SELECT * FROM job.movie_companies__old;

CREATE TABLE job.movie_info (
    id integer NOT NULL PRIMARY KEY,
    movie_id integer NOT NULL REFERENCES job.title(id),
    info_type_id integer NOT NULL REFERENCES job.info_type(id),
    info text NOT NULL,
    note text
);
INSERT INTO job.movie_info SELECT * FROM job.movie_info__old;

CREATE TABLE job.movie_info_idx (
    id integer NOT NULL PRIMARY KEY,
    movie_id integer NOT NULL REFERENCES job.title(id),
    info_type_id integer NOT NULL REFERENCES job.info_type(id),
    info text NOT NULL,
    note text
);
INSERT INTO job.movie_info_idx SELECT * FROM job.movie_info_idx__old;

CREATE TABLE job.movie_keyword (
    id integer NOT NULL PRIMARY KEY,
    movie_id integer NOT NULL REFERENCES job.title(id),
    keyword_id integer NOT NULL REFERENCES job.keyword(id)
);
INSERT INTO job.movie_keyword SELECT * FROM job.movie_keyword__old;

CREATE TABLE job.movie_link (
    id integer NOT NULL PRIMARY KEY,
    movie_id integer NOT NULL REFERENCES job.title(id),
    linked_movie_id integer NOT NULL REFERENCES job.title(id),
    link_type_id integer NOT NULL REFERENCES job.link_type(id)
);
INSERT INTO job.movie_link SELECT * FROM job.movie_link__old;

CREATE TABLE job.person_info (
    id integer NOT NULL PRIMARY KEY,
    person_id integer NOT NULL REFERENCES job.name(id),
    info_type_id integer NOT NULL REFERENCES job.info_type(id),
    info text NOT NULL,
    note text
);
INSERT INTO job.person_info SELECT * FROM job.person_info__old;

DROP TABLE job.person_info__old;
DROP TABLE job.movie_link__old;
DROP TABLE job.movie_keyword__old;
DROP TABLE job.movie_info_idx__old;
DROP TABLE job.movie_info__old;
DROP TABLE job.movie_companies__old;
DROP TABLE job.complete_cast__old;
DROP TABLE job.cast_info__old;
DROP TABLE job.aka_title__old;
DROP TABLE job.aka_name__old;
DROP TABLE job.title__old;
DROP TABLE job.role_type__old;
DROP TABLE job.name__old;
DROP TABLE job.link_type__old;
DROP TABLE job.kind_type__old;
DROP TABLE job.keyword__old;
DROP TABLE job.info_type__old;
DROP TABLE job.company_type__old;
DROP TABLE job.company_name__old;
DROP TABLE job.comp_cast_type__old;
DROP TABLE job.char_name__old;

COMMIT;
