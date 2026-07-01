/* CedarDB tuning script for JOB
 *
 * CedarDB exclusively supports B-tree indexes. DO $$ blocks are not yet
 * supported; all statements are plain SQL. The tune script runs once on
 * freshly loaded tables, so no idempotency guards are needed.
 */

-- Primary keys
ALTER TABLE job.aka_name        ADD CONSTRAINT dbprove_pk_aka_name        PRIMARY KEY (id);
ALTER TABLE job.aka_title       ADD CONSTRAINT dbprove_pk_aka_title       PRIMARY KEY (id);
ALTER TABLE job.cast_info       ADD CONSTRAINT dbprove_pk_cast_info       PRIMARY KEY (id);
ALTER TABLE job.char_name       ADD CONSTRAINT dbprove_pk_char_name       PRIMARY KEY (id);
ALTER TABLE job.comp_cast_type  ADD CONSTRAINT dbprove_pk_comp_cast_type  PRIMARY KEY (id);
ALTER TABLE job.company_name    ADD CONSTRAINT dbprove_pk_company_name    PRIMARY KEY (id);
ALTER TABLE job.company_type    ADD CONSTRAINT dbprove_pk_company_type    PRIMARY KEY (id);
ALTER TABLE job.complete_cast   ADD CONSTRAINT dbprove_pk_complete_cast   PRIMARY KEY (id);
ALTER TABLE job.info_type       ADD CONSTRAINT dbprove_pk_info_type       PRIMARY KEY (id);
ALTER TABLE job.keyword         ADD CONSTRAINT dbprove_pk_keyword         PRIMARY KEY (id);
ALTER TABLE job.kind_type       ADD CONSTRAINT dbprove_pk_kind_type       PRIMARY KEY (id);
ALTER TABLE job.link_type       ADD CONSTRAINT dbprove_pk_link_type       PRIMARY KEY (id);
ALTER TABLE job.movie_companies ADD CONSTRAINT dbprove_pk_movie_companies PRIMARY KEY (id);
ALTER TABLE job.movie_info      ADD CONSTRAINT dbprove_pk_movie_info      PRIMARY KEY (id);
ALTER TABLE job.movie_info_idx  ADD CONSTRAINT dbprove_pk_movie_info_idx  PRIMARY KEY (id);
ALTER TABLE job.movie_keyword   ADD CONSTRAINT dbprove_pk_movie_keyword   PRIMARY KEY (id);
ALTER TABLE job.movie_link      ADD CONSTRAINT dbprove_pk_movie_link      PRIMARY KEY (id);
ALTER TABLE job.name            ADD CONSTRAINT dbprove_pk_name            PRIMARY KEY (id);
ALTER TABLE job.person_info     ADD CONSTRAINT dbprove_pk_person_info     PRIMARY KEY (id);
ALTER TABLE job.role_type       ADD CONSTRAINT dbprove_pk_role_type       PRIMARY KEY (id);
ALTER TABLE job.title           ADD CONSTRAINT dbprove_pk_title           PRIMARY KEY (id);

-- Foreign keys
ALTER TABLE job.aka_name
    ADD CONSTRAINT dbprove_fk_aka_name_name FOREIGN KEY (person_id) REFERENCES job.name (id);
ALTER TABLE job.aka_title
    ADD CONSTRAINT dbprove_fk_aka_title_title FOREIGN KEY (movie_id) REFERENCES job.title (id);
ALTER TABLE job.aka_title
    ADD CONSTRAINT dbprove_fk_aka_title_kind_type FOREIGN KEY (kind_id) REFERENCES job.kind_type (id);
ALTER TABLE job.aka_title
    ADD CONSTRAINT dbprove_fk_aka_title_episode_title FOREIGN KEY (episode_of_id) REFERENCES job.title (id);
ALTER TABLE job.cast_info
    ADD CONSTRAINT dbprove_fk_cast_info_name FOREIGN KEY (person_id) REFERENCES job.name (id);
ALTER TABLE job.cast_info
    ADD CONSTRAINT dbprove_fk_cast_info_title FOREIGN KEY (movie_id) REFERENCES job.title (id);
ALTER TABLE job.cast_info
    ADD CONSTRAINT dbprove_fk_cast_info_char_name FOREIGN KEY (person_role_id) REFERENCES job.char_name (id);
ALTER TABLE job.cast_info
    ADD CONSTRAINT dbprove_fk_cast_info_role_type FOREIGN KEY (role_id) REFERENCES job.role_type (id);
ALTER TABLE job.complete_cast
    ADD CONSTRAINT dbprove_fk_complete_cast_title FOREIGN KEY (movie_id) REFERENCES job.title (id);
ALTER TABLE job.complete_cast
    ADD CONSTRAINT dbprove_fk_complete_cast_subject_type FOREIGN KEY (subject_id) REFERENCES job.comp_cast_type (id);
ALTER TABLE job.complete_cast
    ADD CONSTRAINT dbprove_fk_complete_cast_status_type FOREIGN KEY (status_id) REFERENCES job.comp_cast_type (id);
ALTER TABLE job.movie_companies
    ADD CONSTRAINT dbprove_fk_movie_companies_title FOREIGN KEY (movie_id) REFERENCES job.title (id);
ALTER TABLE job.movie_companies
    ADD CONSTRAINT dbprove_fk_movie_companies_company_name FOREIGN KEY (company_id) REFERENCES job.company_name (id);
ALTER TABLE job.movie_companies
    ADD CONSTRAINT dbprove_fk_movie_companies_company_type FOREIGN KEY (company_type_id) REFERENCES job.company_type (id);
ALTER TABLE job.movie_info
    ADD CONSTRAINT dbprove_fk_movie_info_title FOREIGN KEY (movie_id) REFERENCES job.title (id);
ALTER TABLE job.movie_info
    ADD CONSTRAINT dbprove_fk_movie_info_info_type FOREIGN KEY (info_type_id) REFERENCES job.info_type (id);
ALTER TABLE job.movie_info_idx
    ADD CONSTRAINT dbprove_fk_movie_info_idx_title FOREIGN KEY (movie_id) REFERENCES job.title (id);
ALTER TABLE job.movie_info_idx
    ADD CONSTRAINT dbprove_fk_movie_info_idx_info_type FOREIGN KEY (info_type_id) REFERENCES job.info_type (id);
ALTER TABLE job.movie_keyword
    ADD CONSTRAINT dbprove_fk_movie_keyword_title FOREIGN KEY (movie_id) REFERENCES job.title (id);
ALTER TABLE job.movie_keyword
    ADD CONSTRAINT dbprove_fk_movie_keyword_keyword FOREIGN KEY (keyword_id) REFERENCES job.keyword (id);
ALTER TABLE job.movie_link
    ADD CONSTRAINT dbprove_fk_movie_link_title FOREIGN KEY (movie_id) REFERENCES job.title (id);
ALTER TABLE job.movie_link
    ADD CONSTRAINT dbprove_fk_movie_link_linked_title FOREIGN KEY (linked_movie_id) REFERENCES job.title (id);
ALTER TABLE job.movie_link
    ADD CONSTRAINT dbprove_fk_movie_link_link_type FOREIGN KEY (link_type_id) REFERENCES job.link_type (id);
ALTER TABLE job.person_info
    ADD CONSTRAINT dbprove_fk_person_info_name FOREIGN KEY (person_id) REFERENCES job.name (id);
ALTER TABLE job.person_info
    ADD CONSTRAINT dbprove_fk_person_info_info_type FOREIGN KEY (info_type_id) REFERENCES job.info_type (id);
ALTER TABLE job.title
    ADD CONSTRAINT dbprove_fk_title_kind_type FOREIGN KEY (kind_id) REFERENCES job.kind_type (id);
ALTER TABLE job.title
    ADD CONSTRAINT dbprove_fk_title_episode_title FOREIGN KEY (episode_of_id) REFERENCES job.title (id);

-- FK-backing B-tree indexes
CREATE INDEX company_id_movie_companies       ON job.movie_companies (company_id);
CREATE INDEX company_type_id_movie_companies  ON job.movie_companies (company_type_id);
CREATE INDEX info_type_id_movie_info_idx      ON job.movie_info_idx  (info_type_id);
CREATE INDEX info_type_id_movie_info          ON job.movie_info      (info_type_id);
CREATE INDEX info_type_id_person_info         ON job.person_info     (info_type_id);
CREATE INDEX keyword_id_movie_keyword         ON job.movie_keyword   (keyword_id);
CREATE INDEX kind_id_aka_title                ON job.aka_title       (kind_id);
CREATE INDEX kind_id_title                    ON job.title           (kind_id);
CREATE INDEX linked_movie_id_movie_link       ON job.movie_link      (linked_movie_id);
CREATE INDEX link_type_id_movie_link          ON job.movie_link      (link_type_id);
CREATE INDEX movie_id_aka_title               ON job.aka_title       (movie_id);
CREATE INDEX movie_id_cast_info               ON job.cast_info       (movie_id);
CREATE INDEX movie_id_complete_cast           ON job.complete_cast   (movie_id);
CREATE INDEX movie_id_movie_companies         ON job.movie_companies (movie_id);
CREATE INDEX movie_id_movie_info_idx          ON job.movie_info_idx  (movie_id);
CREATE INDEX movie_id_movie_keyword           ON job.movie_keyword   (movie_id);
CREATE INDEX movie_id_movie_link              ON job.movie_link      (movie_id);
CREATE INDEX movie_id_movie_info              ON job.movie_info      (movie_id);
CREATE INDEX person_id_aka_name               ON job.aka_name        (person_id);
CREATE INDEX person_id_cast_info              ON job.cast_info       (person_id);
CREATE INDEX person_id_person_info            ON job.person_info     (person_id);
CREATE INDEX person_role_id_cast_info         ON job.cast_info       (person_role_id);
CREATE INDEX role_id_cast_info                ON job.cast_info       (role_id);

ANALYZE job.aka_name;
ANALYZE job.aka_title;
ANALYZE job.cast_info;
ANALYZE job.char_name;
ANALYZE job.comp_cast_type;
ANALYZE job.company_name;
ANALYZE job.company_type;
ANALYZE job.complete_cast;
ANALYZE job.info_type;
ANALYZE job.keyword;
ANALYZE job.kind_type;
ANALYZE job.link_type;
ANALYZE job.movie_companies;
ANALYZE job.movie_info;
ANALYZE job.movie_info_idx;
ANALYZE job.movie_keyword;
ANALYZE job.movie_link;
ANALYZE job.name;
ANALYZE job.person_info;
ANALYZE job.role_type;
ANALYZE job.title;
