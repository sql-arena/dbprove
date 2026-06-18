-- Databricks tuning script for JOB key metadata.
-- Databricks constraints are informational only.

-- Drop foreign keys first to avoid dependency issues.
ALTER TABLE job.aka_name DROP CONSTRAINT IF EXISTS dbprove_fk_aka_name_name;
ALTER TABLE job.aka_title DROP CONSTRAINT IF EXISTS dbprove_fk_aka_title_title;
ALTER TABLE job.aka_title DROP CONSTRAINT IF EXISTS dbprove_fk_aka_title_kind_type;
ALTER TABLE job.aka_title DROP CONSTRAINT IF EXISTS dbprove_fk_aka_title_episode_title;
ALTER TABLE job.cast_info DROP CONSTRAINT IF EXISTS dbprove_fk_cast_info_name;
ALTER TABLE job.cast_info DROP CONSTRAINT IF EXISTS dbprove_fk_cast_info_title;
ALTER TABLE job.cast_info DROP CONSTRAINT IF EXISTS dbprove_fk_cast_info_char_name;
ALTER TABLE job.cast_info DROP CONSTRAINT IF EXISTS dbprove_fk_cast_info_role_type;
ALTER TABLE job.complete_cast DROP CONSTRAINT IF EXISTS dbprove_fk_complete_cast_title;
ALTER TABLE job.complete_cast DROP CONSTRAINT IF EXISTS dbprove_fk_complete_cast_subject_type;
ALTER TABLE job.complete_cast DROP CONSTRAINT IF EXISTS dbprove_fk_complete_cast_status_type;
ALTER TABLE job.movie_companies DROP CONSTRAINT IF EXISTS dbprove_fk_movie_companies_title;
ALTER TABLE job.movie_companies DROP CONSTRAINT IF EXISTS dbprove_fk_movie_companies_company_name;
ALTER TABLE job.movie_companies DROP CONSTRAINT IF EXISTS dbprove_fk_movie_companies_company_type;
ALTER TABLE job.movie_info DROP CONSTRAINT IF EXISTS dbprove_fk_movie_info_title;
ALTER TABLE job.movie_info DROP CONSTRAINT IF EXISTS dbprove_fk_movie_info_info_type;
ALTER TABLE job.movie_info_idx DROP CONSTRAINT IF EXISTS dbprove_fk_movie_info_idx_title;
ALTER TABLE job.movie_info_idx DROP CONSTRAINT IF EXISTS dbprove_fk_movie_info_idx_info_type;
ALTER TABLE job.movie_keyword DROP CONSTRAINT IF EXISTS dbprove_fk_movie_keyword_title;
ALTER TABLE job.movie_keyword DROP CONSTRAINT IF EXISTS dbprove_fk_movie_keyword_keyword;
ALTER TABLE job.movie_link DROP CONSTRAINT IF EXISTS dbprove_fk_movie_link_title;
ALTER TABLE job.movie_link DROP CONSTRAINT IF EXISTS dbprove_fk_movie_link_linked_title;
ALTER TABLE job.movie_link DROP CONSTRAINT IF EXISTS dbprove_fk_movie_link_link_type;
ALTER TABLE job.person_info DROP CONSTRAINT IF EXISTS dbprove_fk_person_info_name;
ALTER TABLE job.person_info DROP CONSTRAINT IF EXISTS dbprove_fk_person_info_info_type;
ALTER TABLE job.title DROP CONSTRAINT IF EXISTS dbprove_fk_title_kind_type;
ALTER TABLE job.title DROP CONSTRAINT IF EXISTS dbprove_fk_title_episode_title;

-- Drop primary keys after foreign keys are gone.
ALTER TABLE job.aka_name DROP CONSTRAINT IF EXISTS dbprove_pk_aka_name;
ALTER TABLE job.aka_title DROP CONSTRAINT IF EXISTS dbprove_pk_aka_title;
ALTER TABLE job.cast_info DROP CONSTRAINT IF EXISTS dbprove_pk_cast_info;
ALTER TABLE job.char_name DROP CONSTRAINT IF EXISTS dbprove_pk_char_name;
ALTER TABLE job.comp_cast_type DROP CONSTRAINT IF EXISTS dbprove_pk_comp_cast_type;
ALTER TABLE job.company_name DROP CONSTRAINT IF EXISTS dbprove_pk_company_name;
ALTER TABLE job.company_type DROP CONSTRAINT IF EXISTS dbprove_pk_company_type;
ALTER TABLE job.complete_cast DROP CONSTRAINT IF EXISTS dbprove_pk_complete_cast;
ALTER TABLE job.info_type DROP CONSTRAINT IF EXISTS dbprove_pk_info_type;
ALTER TABLE job.keyword DROP CONSTRAINT IF EXISTS dbprove_pk_keyword;
ALTER TABLE job.kind_type DROP CONSTRAINT IF EXISTS dbprove_pk_kind_type;
ALTER TABLE job.link_type DROP CONSTRAINT IF EXISTS dbprove_pk_link_type;
ALTER TABLE job.movie_companies DROP CONSTRAINT IF EXISTS dbprove_pk_movie_companies;
ALTER TABLE job.movie_info DROP CONSTRAINT IF EXISTS dbprove_pk_movie_info;
ALTER TABLE job.movie_info_idx DROP CONSTRAINT IF EXISTS dbprove_pk_movie_info_idx;
ALTER TABLE job.movie_keyword DROP CONSTRAINT IF EXISTS dbprove_pk_movie_keyword;
ALTER TABLE job.movie_link DROP CONSTRAINT IF EXISTS dbprove_pk_movie_link;
ALTER TABLE job.name DROP CONSTRAINT IF EXISTS dbprove_pk_name;
ALTER TABLE job.person_info DROP CONSTRAINT IF EXISTS dbprove_pk_person_info;
ALTER TABLE job.role_type DROP CONSTRAINT IF EXISTS dbprove_pk_role_type;
ALTER TABLE job.title DROP CONSTRAINT IF EXISTS dbprove_pk_title;

-- Add primary keys first.
ALTER TABLE job.aka_name ADD CONSTRAINT dbprove_pk_aka_name PRIMARY KEY (id) NOT ENFORCED;
ALTER TABLE job.aka_title ADD CONSTRAINT dbprove_pk_aka_title PRIMARY KEY (id) NOT ENFORCED;
ALTER TABLE job.cast_info ADD CONSTRAINT dbprove_pk_cast_info PRIMARY KEY (id) NOT ENFORCED;
ALTER TABLE job.char_name ADD CONSTRAINT dbprove_pk_char_name PRIMARY KEY (id) NOT ENFORCED;
ALTER TABLE job.comp_cast_type ADD CONSTRAINT dbprove_pk_comp_cast_type PRIMARY KEY (id) NOT ENFORCED;
ALTER TABLE job.company_name ADD CONSTRAINT dbprove_pk_company_name PRIMARY KEY (id) NOT ENFORCED;
ALTER TABLE job.company_type ADD CONSTRAINT dbprove_pk_company_type PRIMARY KEY (id) NOT ENFORCED;
ALTER TABLE job.complete_cast ADD CONSTRAINT dbprove_pk_complete_cast PRIMARY KEY (id) NOT ENFORCED;
ALTER TABLE job.info_type ADD CONSTRAINT dbprove_pk_info_type PRIMARY KEY (id) NOT ENFORCED;
ALTER TABLE job.keyword ADD CONSTRAINT dbprove_pk_keyword PRIMARY KEY (id) NOT ENFORCED;
ALTER TABLE job.kind_type ADD CONSTRAINT dbprove_pk_kind_type PRIMARY KEY (id) NOT ENFORCED;
ALTER TABLE job.link_type ADD CONSTRAINT dbprove_pk_link_type PRIMARY KEY (id) NOT ENFORCED;
ALTER TABLE job.movie_companies ADD CONSTRAINT dbprove_pk_movie_companies PRIMARY KEY (id) NOT ENFORCED;
ALTER TABLE job.movie_info ADD CONSTRAINT dbprove_pk_movie_info PRIMARY KEY (id) NOT ENFORCED;
ALTER TABLE job.movie_info_idx ADD CONSTRAINT dbprove_pk_movie_info_idx PRIMARY KEY (id) NOT ENFORCED;
ALTER TABLE job.movie_keyword ADD CONSTRAINT dbprove_pk_movie_keyword PRIMARY KEY (id) NOT ENFORCED;
ALTER TABLE job.movie_link ADD CONSTRAINT dbprove_pk_movie_link PRIMARY KEY (id) NOT ENFORCED;
ALTER TABLE job.name ADD CONSTRAINT dbprove_pk_name PRIMARY KEY (id) NOT ENFORCED;
ALTER TABLE job.person_info ADD CONSTRAINT dbprove_pk_person_info PRIMARY KEY (id) NOT ENFORCED;
ALTER TABLE job.role_type ADD CONSTRAINT dbprove_pk_role_type PRIMARY KEY (id) NOT ENFORCED;
ALTER TABLE job.title ADD CONSTRAINT dbprove_pk_title PRIMARY KEY (id) NOT ENFORCED;

-- Add foreign keys after parent primary keys exist.
ALTER TABLE job.aka_name
  ADD CONSTRAINT dbprove_fk_aka_name_name FOREIGN KEY (person_id)
  REFERENCES job.name (id) NOT ENFORCED;

ALTER TABLE job.aka_title
  ADD CONSTRAINT dbprove_fk_aka_title_title FOREIGN KEY (movie_id)
  REFERENCES job.title (id) NOT ENFORCED;

ALTER TABLE job.aka_title
  ADD CONSTRAINT dbprove_fk_aka_title_kind_type FOREIGN KEY (kind_id)
  REFERENCES job.kind_type (id) NOT ENFORCED;

ALTER TABLE job.aka_title
  ADD CONSTRAINT dbprove_fk_aka_title_episode_title FOREIGN KEY (episode_of_id)
  REFERENCES job.title (id) NOT ENFORCED;

ALTER TABLE job.cast_info
  ADD CONSTRAINT dbprove_fk_cast_info_name FOREIGN KEY (person_id)
  REFERENCES job.name (id) NOT ENFORCED;

ALTER TABLE job.cast_info
  ADD CONSTRAINT dbprove_fk_cast_info_title FOREIGN KEY (movie_id)
  REFERENCES job.title (id) NOT ENFORCED;

ALTER TABLE job.cast_info
  ADD CONSTRAINT dbprove_fk_cast_info_char_name FOREIGN KEY (person_role_id)
  REFERENCES job.char_name (id) NOT ENFORCED;

ALTER TABLE job.cast_info
  ADD CONSTRAINT dbprove_fk_cast_info_role_type FOREIGN KEY (role_id)
  REFERENCES job.role_type (id) NOT ENFORCED;

ALTER TABLE job.complete_cast
  ADD CONSTRAINT dbprove_fk_complete_cast_title FOREIGN KEY (movie_id)
  REFERENCES job.title (id) NOT ENFORCED;

ALTER TABLE job.complete_cast
  ADD CONSTRAINT dbprove_fk_complete_cast_subject_type FOREIGN KEY (subject_id)
  REFERENCES job.comp_cast_type (id) NOT ENFORCED;

ALTER TABLE job.complete_cast
  ADD CONSTRAINT dbprove_fk_complete_cast_status_type FOREIGN KEY (status_id)
  REFERENCES job.comp_cast_type (id) NOT ENFORCED;

ALTER TABLE job.movie_companies
  ADD CONSTRAINT dbprove_fk_movie_companies_title FOREIGN KEY (movie_id)
  REFERENCES job.title (id) NOT ENFORCED;

ALTER TABLE job.movie_companies
  ADD CONSTRAINT dbprove_fk_movie_companies_company_name FOREIGN KEY (company_id)
  REFERENCES job.company_name (id) NOT ENFORCED;

ALTER TABLE job.movie_companies
  ADD CONSTRAINT dbprove_fk_movie_companies_company_type FOREIGN KEY (company_type_id)
  REFERENCES job.company_type (id) NOT ENFORCED;

ALTER TABLE job.movie_info
  ADD CONSTRAINT dbprove_fk_movie_info_title FOREIGN KEY (movie_id)
  REFERENCES job.title (id) NOT ENFORCED;

ALTER TABLE job.movie_info
  ADD CONSTRAINT dbprove_fk_movie_info_info_type FOREIGN KEY (info_type_id)
  REFERENCES job.info_type (id) NOT ENFORCED;

ALTER TABLE job.movie_info_idx
  ADD CONSTRAINT dbprove_fk_movie_info_idx_title FOREIGN KEY (movie_id)
  REFERENCES job.title (id) NOT ENFORCED;

ALTER TABLE job.movie_info_idx
  ADD CONSTRAINT dbprove_fk_movie_info_idx_info_type FOREIGN KEY (info_type_id)
  REFERENCES job.info_type (id) NOT ENFORCED;

ALTER TABLE job.movie_keyword
  ADD CONSTRAINT dbprove_fk_movie_keyword_title FOREIGN KEY (movie_id)
  REFERENCES job.title (id) NOT ENFORCED;

ALTER TABLE job.movie_keyword
  ADD CONSTRAINT dbprove_fk_movie_keyword_keyword FOREIGN KEY (keyword_id)
  REFERENCES job.keyword (id) NOT ENFORCED;

ALTER TABLE job.movie_link
  ADD CONSTRAINT dbprove_fk_movie_link_title FOREIGN KEY (movie_id)
  REFERENCES job.title (id) NOT ENFORCED;

ALTER TABLE job.movie_link
  ADD CONSTRAINT dbprove_fk_movie_link_linked_title FOREIGN KEY (linked_movie_id)
  REFERENCES job.title (id) NOT ENFORCED;

ALTER TABLE job.movie_link
  ADD CONSTRAINT dbprove_fk_movie_link_link_type FOREIGN KEY (link_type_id)
  REFERENCES job.link_type (id) NOT ENFORCED;

ALTER TABLE job.person_info
  ADD CONSTRAINT dbprove_fk_person_info_name FOREIGN KEY (person_id)
  REFERENCES job.name (id) NOT ENFORCED;

ALTER TABLE job.person_info
  ADD CONSTRAINT dbprove_fk_person_info_info_type FOREIGN KEY (info_type_id)
  REFERENCES job.info_type (id) NOT ENFORCED;

ALTER TABLE job.title
  ADD CONSTRAINT dbprove_fk_title_kind_type FOREIGN KEY (kind_id)
  REFERENCES job.kind_type (id) NOT ENFORCED;

ALTER TABLE job.title
  ADD CONSTRAINT dbprove_fk_title_episode_title FOREIGN KEY (episode_of_id)
  REFERENCES job.title (id) NOT ENFORCED;
