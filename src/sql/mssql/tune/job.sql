-- SQL Server tuning script for JOB key metadata.

-- Primary keys
IF OBJECT_ID(N'job.aka_name', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'dbprove_pk_aka_name' AND type = 'PK')
    ALTER TABLE job.aka_name ADD CONSTRAINT dbprove_pk_aka_name PRIMARY KEY NONCLUSTERED (id);

IF OBJECT_ID(N'job.aka_title', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'dbprove_pk_aka_title' AND type = 'PK')
    ALTER TABLE job.aka_title ADD CONSTRAINT dbprove_pk_aka_title PRIMARY KEY NONCLUSTERED (id);

IF OBJECT_ID(N'job.cast_info', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'dbprove_pk_cast_info' AND type = 'PK')
    ALTER TABLE job.cast_info ADD CONSTRAINT dbprove_pk_cast_info PRIMARY KEY NONCLUSTERED (id);

IF OBJECT_ID(N'job.char_name', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'dbprove_pk_char_name' AND type = 'PK')
    ALTER TABLE job.char_name ADD CONSTRAINT dbprove_pk_char_name PRIMARY KEY NONCLUSTERED (id);

IF OBJECT_ID(N'job.comp_cast_type', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'dbprove_pk_comp_cast_type' AND type = 'PK')
    ALTER TABLE job.comp_cast_type ADD CONSTRAINT dbprove_pk_comp_cast_type PRIMARY KEY NONCLUSTERED (id);

IF OBJECT_ID(N'job.company_name', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'dbprove_pk_company_name' AND type = 'PK')
    ALTER TABLE job.company_name ADD CONSTRAINT dbprove_pk_company_name PRIMARY KEY NONCLUSTERED (id);

IF OBJECT_ID(N'job.company_type', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'dbprove_pk_company_type' AND type = 'PK')
    ALTER TABLE job.company_type ADD CONSTRAINT dbprove_pk_company_type PRIMARY KEY NONCLUSTERED (id);

IF OBJECT_ID(N'job.complete_cast', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'dbprove_pk_complete_cast' AND type = 'PK')
    ALTER TABLE job.complete_cast ADD CONSTRAINT dbprove_pk_complete_cast PRIMARY KEY NONCLUSTERED (id);

IF OBJECT_ID(N'job.info_type', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'dbprove_pk_info_type' AND type = 'PK')
    ALTER TABLE job.info_type ADD CONSTRAINT dbprove_pk_info_type PRIMARY KEY NONCLUSTERED (id);

IF OBJECT_ID(N'job.keyword', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'dbprove_pk_keyword' AND type = 'PK')
    ALTER TABLE job.keyword ADD CONSTRAINT dbprove_pk_keyword PRIMARY KEY NONCLUSTERED (id);

IF OBJECT_ID(N'job.kind_type', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'dbprove_pk_kind_type' AND type = 'PK')
    ALTER TABLE job.kind_type ADD CONSTRAINT dbprove_pk_kind_type PRIMARY KEY NONCLUSTERED (id);

IF OBJECT_ID(N'job.link_type', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'dbprove_pk_link_type' AND type = 'PK')
    ALTER TABLE job.link_type ADD CONSTRAINT dbprove_pk_link_type PRIMARY KEY NONCLUSTERED (id);

IF OBJECT_ID(N'job.movie_companies', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'dbprove_pk_movie_companies' AND type = 'PK')
    ALTER TABLE job.movie_companies ADD CONSTRAINT dbprove_pk_movie_companies PRIMARY KEY NONCLUSTERED (id);

IF OBJECT_ID(N'job.movie_info', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'dbprove_pk_movie_info' AND type = 'PK')
    ALTER TABLE job.movie_info ADD CONSTRAINT dbprove_pk_movie_info PRIMARY KEY NONCLUSTERED (id);

IF OBJECT_ID(N'job.movie_info_idx', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'dbprove_pk_movie_info_idx' AND type = 'PK')
    ALTER TABLE job.movie_info_idx ADD CONSTRAINT dbprove_pk_movie_info_idx PRIMARY KEY NONCLUSTERED (id);

IF OBJECT_ID(N'job.movie_keyword', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'dbprove_pk_movie_keyword' AND type = 'PK')
    ALTER TABLE job.movie_keyword ADD CONSTRAINT dbprove_pk_movie_keyword PRIMARY KEY NONCLUSTERED (id);

IF OBJECT_ID(N'job.movie_link', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'dbprove_pk_movie_link' AND type = 'PK')
    ALTER TABLE job.movie_link ADD CONSTRAINT dbprove_pk_movie_link PRIMARY KEY NONCLUSTERED (id);

IF OBJECT_ID(N'job.name', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'dbprove_pk_name' AND type = 'PK')
    ALTER TABLE job.name ADD CONSTRAINT dbprove_pk_name PRIMARY KEY NONCLUSTERED (id);

IF OBJECT_ID(N'job.person_info', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'dbprove_pk_person_info' AND type = 'PK')
    ALTER TABLE job.person_info ADD CONSTRAINT dbprove_pk_person_info PRIMARY KEY NONCLUSTERED (id);

IF OBJECT_ID(N'job.role_type', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'dbprove_pk_role_type' AND type = 'PK')
    ALTER TABLE job.role_type ADD CONSTRAINT dbprove_pk_role_type PRIMARY KEY NONCLUSTERED (id);

IF OBJECT_ID(N'job.title', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = N'dbprove_pk_title' AND type = 'PK')
    ALTER TABLE job.title ADD CONSTRAINT dbprove_pk_title PRIMARY KEY NONCLUSTERED (id);

-- Foreign keys
IF OBJECT_ID(N'job.aka_name', N'U') IS NOT NULL
   AND OBJECT_ID(N'job.name', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'dbprove_fk_aka_name_name')
    ALTER TABLE job.aka_name ADD CONSTRAINT dbprove_fk_aka_name_name FOREIGN KEY (person_id) REFERENCES job.name (id);

IF OBJECT_ID(N'job.aka_title', N'U') IS NOT NULL
   AND OBJECT_ID(N'job.title', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'dbprove_fk_aka_title_title')
    ALTER TABLE job.aka_title ADD CONSTRAINT dbprove_fk_aka_title_title FOREIGN KEY (movie_id) REFERENCES job.title (id);

IF OBJECT_ID(N'job.aka_title', N'U') IS NOT NULL
   AND OBJECT_ID(N'job.kind_type', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'dbprove_fk_aka_title_kind_type')
    ALTER TABLE job.aka_title ADD CONSTRAINT dbprove_fk_aka_title_kind_type FOREIGN KEY (kind_id) REFERENCES job.kind_type (id);

IF OBJECT_ID(N'job.aka_title', N'U') IS NOT NULL
   AND OBJECT_ID(N'job.title', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'dbprove_fk_aka_title_episode_title')
    ALTER TABLE job.aka_title ADD CONSTRAINT dbprove_fk_aka_title_episode_title FOREIGN KEY (episode_of_id) REFERENCES job.title (id);

IF OBJECT_ID(N'job.cast_info', N'U') IS NOT NULL
   AND OBJECT_ID(N'job.name', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'dbprove_fk_cast_info_name')
    ALTER TABLE job.cast_info ADD CONSTRAINT dbprove_fk_cast_info_name FOREIGN KEY (person_id) REFERENCES job.name (id);

IF OBJECT_ID(N'job.cast_info', N'U') IS NOT NULL
   AND OBJECT_ID(N'job.title', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'dbprove_fk_cast_info_title')
    ALTER TABLE job.cast_info ADD CONSTRAINT dbprove_fk_cast_info_title FOREIGN KEY (movie_id) REFERENCES job.title (id);

IF OBJECT_ID(N'job.cast_info', N'U') IS NOT NULL
   AND OBJECT_ID(N'job.char_name', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'dbprove_fk_cast_info_char_name')
    ALTER TABLE job.cast_info ADD CONSTRAINT dbprove_fk_cast_info_char_name FOREIGN KEY (person_role_id) REFERENCES job.char_name (id);

IF OBJECT_ID(N'job.cast_info', N'U') IS NOT NULL
   AND OBJECT_ID(N'job.role_type', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'dbprove_fk_cast_info_role_type')
    ALTER TABLE job.cast_info ADD CONSTRAINT dbprove_fk_cast_info_role_type FOREIGN KEY (role_id) REFERENCES job.role_type (id);

IF OBJECT_ID(N'job.complete_cast', N'U') IS NOT NULL
   AND OBJECT_ID(N'job.title', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'dbprove_fk_complete_cast_title')
    ALTER TABLE job.complete_cast ADD CONSTRAINT dbprove_fk_complete_cast_title FOREIGN KEY (movie_id) REFERENCES job.title (id);

IF OBJECT_ID(N'job.complete_cast', N'U') IS NOT NULL
   AND OBJECT_ID(N'job.comp_cast_type', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'dbprove_fk_complete_cast_subject_type')
    ALTER TABLE job.complete_cast ADD CONSTRAINT dbprove_fk_complete_cast_subject_type FOREIGN KEY (subject_id) REFERENCES job.comp_cast_type (id);

IF OBJECT_ID(N'job.complete_cast', N'U') IS NOT NULL
   AND OBJECT_ID(N'job.comp_cast_type', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'dbprove_fk_complete_cast_status_type')
    ALTER TABLE job.complete_cast ADD CONSTRAINT dbprove_fk_complete_cast_status_type FOREIGN KEY (status_id) REFERENCES job.comp_cast_type (id);

IF OBJECT_ID(N'job.movie_companies', N'U') IS NOT NULL
   AND OBJECT_ID(N'job.title', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'dbprove_fk_movie_companies_title')
    ALTER TABLE job.movie_companies ADD CONSTRAINT dbprove_fk_movie_companies_title FOREIGN KEY (movie_id) REFERENCES job.title (id);

IF OBJECT_ID(N'job.movie_companies', N'U') IS NOT NULL
   AND OBJECT_ID(N'job.company_name', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'dbprove_fk_movie_companies_company_name')
    ALTER TABLE job.movie_companies ADD CONSTRAINT dbprove_fk_movie_companies_company_name FOREIGN KEY (company_id) REFERENCES job.company_name (id);

IF OBJECT_ID(N'job.movie_companies', N'U') IS NOT NULL
   AND OBJECT_ID(N'job.company_type', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'dbprove_fk_movie_companies_company_type')
    ALTER TABLE job.movie_companies ADD CONSTRAINT dbprove_fk_movie_companies_company_type FOREIGN KEY (company_type_id) REFERENCES job.company_type (id);

IF OBJECT_ID(N'job.movie_info', N'U') IS NOT NULL
   AND OBJECT_ID(N'job.title', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'dbprove_fk_movie_info_title')
    ALTER TABLE job.movie_info ADD CONSTRAINT dbprove_fk_movie_info_title FOREIGN KEY (movie_id) REFERENCES job.title (id);

IF OBJECT_ID(N'job.movie_info', N'U') IS NOT NULL
   AND OBJECT_ID(N'job.info_type', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'dbprove_fk_movie_info_info_type')
    ALTER TABLE job.movie_info ADD CONSTRAINT dbprove_fk_movie_info_info_type FOREIGN KEY (info_type_id) REFERENCES job.info_type (id);

IF OBJECT_ID(N'job.movie_info_idx', N'U') IS NOT NULL
   AND OBJECT_ID(N'job.title', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'dbprove_fk_movie_info_idx_title')
    ALTER TABLE job.movie_info_idx ADD CONSTRAINT dbprove_fk_movie_info_idx_title FOREIGN KEY (movie_id) REFERENCES job.title (id);

IF OBJECT_ID(N'job.movie_info_idx', N'U') IS NOT NULL
   AND OBJECT_ID(N'job.info_type', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'dbprove_fk_movie_info_idx_info_type')
    ALTER TABLE job.movie_info_idx ADD CONSTRAINT dbprove_fk_movie_info_idx_info_type FOREIGN KEY (info_type_id) REFERENCES job.info_type (id);

IF OBJECT_ID(N'job.movie_keyword', N'U') IS NOT NULL
   AND OBJECT_ID(N'job.title', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'dbprove_fk_movie_keyword_title')
    ALTER TABLE job.movie_keyword ADD CONSTRAINT dbprove_fk_movie_keyword_title FOREIGN KEY (movie_id) REFERENCES job.title (id);

IF OBJECT_ID(N'job.movie_keyword', N'U') IS NOT NULL
   AND OBJECT_ID(N'job.keyword', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'dbprove_fk_movie_keyword_keyword')
    ALTER TABLE job.movie_keyword ADD CONSTRAINT dbprove_fk_movie_keyword_keyword FOREIGN KEY (keyword_id) REFERENCES job.keyword (id);

IF OBJECT_ID(N'job.movie_link', N'U') IS NOT NULL
   AND OBJECT_ID(N'job.title', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'dbprove_fk_movie_link_title')
    ALTER TABLE job.movie_link ADD CONSTRAINT dbprove_fk_movie_link_title FOREIGN KEY (movie_id) REFERENCES job.title (id);

IF OBJECT_ID(N'job.movie_link', N'U') IS NOT NULL
   AND OBJECT_ID(N'job.title', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'dbprove_fk_movie_link_linked_title')
    ALTER TABLE job.movie_link ADD CONSTRAINT dbprove_fk_movie_link_linked_title FOREIGN KEY (linked_movie_id) REFERENCES job.title (id);

IF OBJECT_ID(N'job.movie_link', N'U') IS NOT NULL
   AND OBJECT_ID(N'job.link_type', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'dbprove_fk_movie_link_link_type')
    ALTER TABLE job.movie_link ADD CONSTRAINT dbprove_fk_movie_link_link_type FOREIGN KEY (link_type_id) REFERENCES job.link_type (id);

IF OBJECT_ID(N'job.person_info', N'U') IS NOT NULL
   AND OBJECT_ID(N'job.name', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'dbprove_fk_person_info_name')
    ALTER TABLE job.person_info ADD CONSTRAINT dbprove_fk_person_info_name FOREIGN KEY (person_id) REFERENCES job.name (id);

IF OBJECT_ID(N'job.person_info', N'U') IS NOT NULL
   AND OBJECT_ID(N'job.info_type', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'dbprove_fk_person_info_info_type')
    ALTER TABLE job.person_info ADD CONSTRAINT dbprove_fk_person_info_info_type FOREIGN KEY (info_type_id) REFERENCES job.info_type (id);

IF OBJECT_ID(N'job.title', N'U') IS NOT NULL
   AND OBJECT_ID(N'job.kind_type', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'dbprove_fk_title_kind_type')
    ALTER TABLE job.title ADD CONSTRAINT dbprove_fk_title_kind_type FOREIGN KEY (kind_id) REFERENCES job.kind_type (id);

IF OBJECT_ID(N'job.title', N'U') IS NOT NULL
   AND OBJECT_ID(N'job.title', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'dbprove_fk_title_episode_title')
    ALTER TABLE job.title ADD CONSTRAINT dbprove_fk_title_episode_title FOREIGN KEY (episode_of_id) REFERENCES job.title (id);
