/* PostgreSQL tuning script for JOB */
SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

DO $$
BEGIN
    -- Primary keys
    IF to_regclass('job.aka_name') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_pk_aka_name') THEN
        ALTER TABLE job.aka_name ADD CONSTRAINT dbprove_pk_aka_name PRIMARY KEY (id);
    END IF;

    IF to_regclass('job.aka_title') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_pk_aka_title') THEN
        ALTER TABLE job.aka_title ADD CONSTRAINT dbprove_pk_aka_title PRIMARY KEY (id);
    END IF;

    IF to_regclass('job.cast_info') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_pk_cast_info') THEN
        ALTER TABLE job.cast_info ADD CONSTRAINT dbprove_pk_cast_info PRIMARY KEY (id);
    END IF;

    IF to_regclass('job.char_name') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_pk_char_name') THEN
        ALTER TABLE job.char_name ADD CONSTRAINT dbprove_pk_char_name PRIMARY KEY (id);
    END IF;

    IF to_regclass('job.comp_cast_type') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_pk_comp_cast_type') THEN
        ALTER TABLE job.comp_cast_type ADD CONSTRAINT dbprove_pk_comp_cast_type PRIMARY KEY (id);
    END IF;

    IF to_regclass('job.company_name') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_pk_company_name') THEN
        ALTER TABLE job.company_name ADD CONSTRAINT dbprove_pk_company_name PRIMARY KEY (id);
    END IF;

    IF to_regclass('job.company_type') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_pk_company_type') THEN
        ALTER TABLE job.company_type ADD CONSTRAINT dbprove_pk_company_type PRIMARY KEY (id);
    END IF;

    IF to_regclass('job.complete_cast') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_pk_complete_cast') THEN
        ALTER TABLE job.complete_cast ADD CONSTRAINT dbprove_pk_complete_cast PRIMARY KEY (id);
    END IF;

    IF to_regclass('job.info_type') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_pk_info_type') THEN
        ALTER TABLE job.info_type ADD CONSTRAINT dbprove_pk_info_type PRIMARY KEY (id);
    END IF;

    IF to_regclass('job.keyword') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_pk_keyword') THEN
        ALTER TABLE job.keyword ADD CONSTRAINT dbprove_pk_keyword PRIMARY KEY (id);
    END IF;

    IF to_regclass('job.kind_type') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_pk_kind_type') THEN
        ALTER TABLE job.kind_type ADD CONSTRAINT dbprove_pk_kind_type PRIMARY KEY (id);
    END IF;

    IF to_regclass('job.link_type') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_pk_link_type') THEN
        ALTER TABLE job.link_type ADD CONSTRAINT dbprove_pk_link_type PRIMARY KEY (id);
    END IF;

    IF to_regclass('job.movie_companies') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_pk_movie_companies') THEN
        ALTER TABLE job.movie_companies ADD CONSTRAINT dbprove_pk_movie_companies PRIMARY KEY (id);
    END IF;

    IF to_regclass('job.movie_info') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_pk_movie_info') THEN
        ALTER TABLE job.movie_info ADD CONSTRAINT dbprove_pk_movie_info PRIMARY KEY (id);
    END IF;

    IF to_regclass('job.movie_info_idx') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_pk_movie_info_idx') THEN
        ALTER TABLE job.movie_info_idx ADD CONSTRAINT dbprove_pk_movie_info_idx PRIMARY KEY (id);
    END IF;

    IF to_regclass('job.movie_keyword') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_pk_movie_keyword') THEN
        ALTER TABLE job.movie_keyword ADD CONSTRAINT dbprove_pk_movie_keyword PRIMARY KEY (id);
    END IF;

    IF to_regclass('job.movie_link') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_pk_movie_link') THEN
        ALTER TABLE job.movie_link ADD CONSTRAINT dbprove_pk_movie_link PRIMARY KEY (id);
    END IF;

    IF to_regclass('job.name') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_pk_name') THEN
        ALTER TABLE job.name ADD CONSTRAINT dbprove_pk_name PRIMARY KEY (id);
    END IF;

    IF to_regclass('job.person_info') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_pk_person_info') THEN
        ALTER TABLE job.person_info ADD CONSTRAINT dbprove_pk_person_info PRIMARY KEY (id);
    END IF;

    IF to_regclass('job.role_type') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_pk_role_type') THEN
        ALTER TABLE job.role_type ADD CONSTRAINT dbprove_pk_role_type PRIMARY KEY (id);
    END IF;

    IF to_regclass('job.title') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_pk_title') THEN
        ALTER TABLE job.title ADD CONSTRAINT dbprove_pk_title PRIMARY KEY (id);
    END IF;

    -- Foreign keys
    IF to_regclass('job.aka_name') IS NOT NULL
       AND to_regclass('job.name') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_fk_aka_name_name') THEN
        ALTER TABLE job.aka_name
            ADD CONSTRAINT dbprove_fk_aka_name_name FOREIGN KEY (person_id) REFERENCES job.name (id);
    END IF;

    IF to_regclass('job.aka_title') IS NOT NULL
       AND to_regclass('job.title') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_fk_aka_title_title') THEN
        ALTER TABLE job.aka_title
            ADD CONSTRAINT dbprove_fk_aka_title_title FOREIGN KEY (movie_id) REFERENCES job.title (id);
    END IF;

    IF to_regclass('job.aka_title') IS NOT NULL
       AND to_regclass('job.kind_type') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_fk_aka_title_kind_type') THEN
        ALTER TABLE job.aka_title
            ADD CONSTRAINT dbprove_fk_aka_title_kind_type FOREIGN KEY (kind_id) REFERENCES job.kind_type (id);
    END IF;

    IF to_regclass('job.aka_title') IS NOT NULL
       AND to_regclass('job.title') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_fk_aka_title_episode_title') THEN
        ALTER TABLE job.aka_title
            ADD CONSTRAINT dbprove_fk_aka_title_episode_title FOREIGN KEY (episode_of_id) REFERENCES job.title (id);
    END IF;

    IF to_regclass('job.cast_info') IS NOT NULL
       AND to_regclass('job.name') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_fk_cast_info_name') THEN
        ALTER TABLE job.cast_info
            ADD CONSTRAINT dbprove_fk_cast_info_name FOREIGN KEY (person_id) REFERENCES job.name (id);
    END IF;

    IF to_regclass('job.cast_info') IS NOT NULL
       AND to_regclass('job.title') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_fk_cast_info_title') THEN
        ALTER TABLE job.cast_info
            ADD CONSTRAINT dbprove_fk_cast_info_title FOREIGN KEY (movie_id) REFERENCES job.title (id);
    END IF;

    IF to_regclass('job.cast_info') IS NOT NULL
       AND to_regclass('job.char_name') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_fk_cast_info_char_name') THEN
        ALTER TABLE job.cast_info
            ADD CONSTRAINT dbprove_fk_cast_info_char_name FOREIGN KEY (person_role_id) REFERENCES job.char_name (id);
    END IF;

    IF to_regclass('job.cast_info') IS NOT NULL
       AND to_regclass('job.role_type') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_fk_cast_info_role_type') THEN
        ALTER TABLE job.cast_info
            ADD CONSTRAINT dbprove_fk_cast_info_role_type FOREIGN KEY (role_id) REFERENCES job.role_type (id);
    END IF;

    IF to_regclass('job.complete_cast') IS NOT NULL
       AND to_regclass('job.title') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_fk_complete_cast_title') THEN
        ALTER TABLE job.complete_cast
            ADD CONSTRAINT dbprove_fk_complete_cast_title FOREIGN KEY (movie_id) REFERENCES job.title (id);
    END IF;

    IF to_regclass('job.complete_cast') IS NOT NULL
       AND to_regclass('job.comp_cast_type') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_fk_complete_cast_subject_type') THEN
        ALTER TABLE job.complete_cast
            ADD CONSTRAINT dbprove_fk_complete_cast_subject_type FOREIGN KEY (subject_id) REFERENCES job.comp_cast_type (id);
    END IF;

    IF to_regclass('job.complete_cast') IS NOT NULL
       AND to_regclass('job.comp_cast_type') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_fk_complete_cast_status_type') THEN
        ALTER TABLE job.complete_cast
            ADD CONSTRAINT dbprove_fk_complete_cast_status_type FOREIGN KEY (status_id) REFERENCES job.comp_cast_type (id);
    END IF;

    IF to_regclass('job.movie_companies') IS NOT NULL
       AND to_regclass('job.title') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_fk_movie_companies_title') THEN
        ALTER TABLE job.movie_companies
            ADD CONSTRAINT dbprove_fk_movie_companies_title FOREIGN KEY (movie_id) REFERENCES job.title (id);
    END IF;

    IF to_regclass('job.movie_companies') IS NOT NULL
       AND to_regclass('job.company_name') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_fk_movie_companies_company_name') THEN
        ALTER TABLE job.movie_companies
            ADD CONSTRAINT dbprove_fk_movie_companies_company_name FOREIGN KEY (company_id) REFERENCES job.company_name (id);
    END IF;

    IF to_regclass('job.movie_companies') IS NOT NULL
       AND to_regclass('job.company_type') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_fk_movie_companies_company_type') THEN
        ALTER TABLE job.movie_companies
            ADD CONSTRAINT dbprove_fk_movie_companies_company_type FOREIGN KEY (company_type_id) REFERENCES job.company_type (id);
    END IF;

    IF to_regclass('job.movie_info') IS NOT NULL
       AND to_regclass('job.title') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_fk_movie_info_title') THEN
        ALTER TABLE job.movie_info
            ADD CONSTRAINT dbprove_fk_movie_info_title FOREIGN KEY (movie_id) REFERENCES job.title (id);
    END IF;

    IF to_regclass('job.movie_info') IS NOT NULL
       AND to_regclass('job.info_type') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_fk_movie_info_info_type') THEN
        ALTER TABLE job.movie_info
            ADD CONSTRAINT dbprove_fk_movie_info_info_type FOREIGN KEY (info_type_id) REFERENCES job.info_type (id);
    END IF;

    IF to_regclass('job.movie_info_idx') IS NOT NULL
       AND to_regclass('job.title') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_fk_movie_info_idx_title') THEN
        ALTER TABLE job.movie_info_idx
            ADD CONSTRAINT dbprove_fk_movie_info_idx_title FOREIGN KEY (movie_id) REFERENCES job.title (id);
    END IF;

    IF to_regclass('job.movie_info_idx') IS NOT NULL
       AND to_regclass('job.info_type') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_fk_movie_info_idx_info_type') THEN
        ALTER TABLE job.movie_info_idx
            ADD CONSTRAINT dbprove_fk_movie_info_idx_info_type FOREIGN KEY (info_type_id) REFERENCES job.info_type (id);
    END IF;

    IF to_regclass('job.movie_keyword') IS NOT NULL
       AND to_regclass('job.title') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_fk_movie_keyword_title') THEN
        ALTER TABLE job.movie_keyword
            ADD CONSTRAINT dbprove_fk_movie_keyword_title FOREIGN KEY (movie_id) REFERENCES job.title (id);
    END IF;

    IF to_regclass('job.movie_keyword') IS NOT NULL
       AND to_regclass('job.keyword') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_fk_movie_keyword_keyword') THEN
        ALTER TABLE job.movie_keyword
            ADD CONSTRAINT dbprove_fk_movie_keyword_keyword FOREIGN KEY (keyword_id) REFERENCES job.keyword (id);
    END IF;

    IF to_regclass('job.movie_link') IS NOT NULL
       AND to_regclass('job.title') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_fk_movie_link_title') THEN
        ALTER TABLE job.movie_link
            ADD CONSTRAINT dbprove_fk_movie_link_title FOREIGN KEY (movie_id) REFERENCES job.title (id);
    END IF;

    IF to_regclass('job.movie_link') IS NOT NULL
       AND to_regclass('job.title') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_fk_movie_link_linked_title') THEN
        ALTER TABLE job.movie_link
            ADD CONSTRAINT dbprove_fk_movie_link_linked_title FOREIGN KEY (linked_movie_id) REFERENCES job.title (id);
    END IF;

    IF to_regclass('job.movie_link') IS NOT NULL
       AND to_regclass('job.link_type') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_fk_movie_link_link_type') THEN
        ALTER TABLE job.movie_link
            ADD CONSTRAINT dbprove_fk_movie_link_link_type FOREIGN KEY (link_type_id) REFERENCES job.link_type (id);
    END IF;

    IF to_regclass('job.person_info') IS NOT NULL
       AND to_regclass('job.name') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_fk_person_info_name') THEN
        ALTER TABLE job.person_info
            ADD CONSTRAINT dbprove_fk_person_info_name FOREIGN KEY (person_id) REFERENCES job.name (id);
    END IF;

    IF to_regclass('job.person_info') IS NOT NULL
       AND to_regclass('job.info_type') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_fk_person_info_info_type') THEN
        ALTER TABLE job.person_info
            ADD CONSTRAINT dbprove_fk_person_info_info_type FOREIGN KEY (info_type_id) REFERENCES job.info_type (id);
    END IF;

    IF to_regclass('job.title') IS NOT NULL
       AND to_regclass('job.kind_type') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_fk_title_kind_type') THEN
        ALTER TABLE job.title
            ADD CONSTRAINT dbprove_fk_title_kind_type FOREIGN KEY (kind_id) REFERENCES job.kind_type (id);
    END IF;

    IF to_regclass('job.title') IS NOT NULL
       AND to_regclass('job.title') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'dbprove_fk_title_episode_title') THEN
        ALTER TABLE job.title
            ADD CONSTRAINT dbprove_fk_title_episode_title FOREIGN KEY (episode_of_id) REFERENCES job.title (id);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS company_id_movie_companies ON job.movie_companies(company_id);
CREATE INDEX IF NOT EXISTS company_type_id_movie_companies ON job.movie_companies(company_type_id);
CREATE INDEX IF NOT EXISTS country_code_company_name ON job.company_name(country_code);
CREATE INDEX IF NOT EXISTS info_type_id_movie_info_idx ON job.movie_info_idx(info_type_id);
CREATE INDEX IF NOT EXISTS info_type_id_movie_info ON job.movie_info(info_type_id);
CREATE INDEX IF NOT EXISTS info_type_id_person_info ON job.person_info(info_type_id);
CREATE INDEX IF NOT EXISTS info_info_type ON job.info_type(info);
CREATE INDEX IF NOT EXISTS keyword_id_movie_keyword ON job.movie_keyword(keyword_id);
CREATE INDEX IF NOT EXISTS keyword_id_movie_id_movie_keyword ON job.movie_keyword(keyword_id, movie_id);
CREATE INDEX IF NOT EXISTS keyword_keyword ON job.keyword(keyword);
CREATE INDEX IF NOT EXISTS kind_id_aka_title ON job.aka_title(kind_id);
CREATE INDEX IF NOT EXISTS kind_id_title ON job.title(kind_id);
CREATE INDEX IF NOT EXISTS kind_company_type ON job.company_type(kind);
CREATE INDEX IF NOT EXISTS linked_movie_id_movie_link ON job.movie_link(linked_movie_id);
CREATE INDEX IF NOT EXISTS link_type_id_movie_link ON job.movie_link(link_type_id);
CREATE INDEX IF NOT EXISTS movie_id_aka_title ON job.aka_title(movie_id);
CREATE INDEX IF NOT EXISTS movie_id_cast_info ON job.cast_info(movie_id);
CREATE INDEX IF NOT EXISTS person_id_movie_id_cast_info ON job.cast_info(person_id, movie_id);
CREATE INDEX IF NOT EXISTS movie_id_complete_cast ON job.complete_cast(movie_id);
CREATE INDEX IF NOT EXISTS movie_id_movie_companies ON job.movie_companies(movie_id);
CREATE INDEX IF NOT EXISTS company_id_movie_id_movie_companies ON job.movie_companies(company_id, movie_id);
CREATE INDEX IF NOT EXISTS movie_id_movie_info_idx ON job.movie_info_idx(movie_id);
CREATE INDEX IF NOT EXISTS movie_id_movie_keyword ON job.movie_keyword(movie_id);
CREATE INDEX IF NOT EXISTS movie_id_movie_link ON job.movie_link(movie_id);
CREATE INDEX IF NOT EXISTS movie_id_movie_info ON job.movie_info(movie_id);
CREATE INDEX IF NOT EXISTS person_id_aka_name ON job.aka_name(person_id);
CREATE INDEX IF NOT EXISTS person_id_cast_info ON job.cast_info(person_id);
CREATE INDEX IF NOT EXISTS person_id_person_info ON job.person_info(person_id);
CREATE INDEX IF NOT EXISTS person_role_id_cast_info ON job.cast_info(person_role_id);
CREATE INDEX IF NOT EXISTS production_year_title ON job.title(production_year);
CREATE INDEX IF NOT EXISTS episode_nr_title ON job.title(episode_nr);
CREATE INDEX IF NOT EXISTS role_role_type ON job.role_type(role);
CREATE INDEX IF NOT EXISTS role_id_cast_info ON job.cast_info(role_id);
CREATE INDEX IF NOT EXISTS name_company_name_pattern ON job.company_name(name text_pattern_ops);
CREATE INDEX IF NOT EXISTS name_name_pattern ON job.name(name text_pattern_ops);
CREATE INDEX IF NOT EXISTS link_link_type_pattern ON job.link_type(link text_pattern_ops);
CREATE INDEX IF NOT EXISTS note_movie_companies ON job.movie_companies(note);

DO $$
DECLARE
    rec RECORD;
BEGIN
    -- Refresh planner stats when they are missing or stale after loading/tuning.
    FOR rec IN
        SELECT
            format('%I.%I', st.schemaname, st.relname) AS qualified_name
        FROM pg_stat_all_tables st
        WHERE st.schemaname = 'job'
          AND (
              COALESCE(st.last_analyze, st.last_autoanalyze) IS NULL
              OR st.n_mod_since_analyze > 0
          )
    LOOP
        EXECUTE 'ANALYZE ' || rec.qualified_name;
    END LOOP;
END $$;
