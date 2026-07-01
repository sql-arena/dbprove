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

-- FK-backing indexes
IF OBJECT_ID(N'job.aka_name', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'job.aka_name') AND name = N'person_id_aka_name')
    CREATE INDEX person_id_aka_name ON job.aka_name (person_id);

IF OBJECT_ID(N'job.aka_title', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'job.aka_title') AND name = N'movie_id_aka_title')
    CREATE INDEX movie_id_aka_title ON job.aka_title (movie_id);

IF OBJECT_ID(N'job.aka_title', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'job.aka_title') AND name = N'kind_id_aka_title')
    CREATE INDEX kind_id_aka_title ON job.aka_title (kind_id);

IF OBJECT_ID(N'job.cast_info', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'job.cast_info') AND name = N'person_id_cast_info')
    CREATE INDEX person_id_cast_info ON job.cast_info (person_id);

IF OBJECT_ID(N'job.cast_info', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'job.cast_info') AND name = N'movie_id_cast_info')
    CREATE INDEX movie_id_cast_info ON job.cast_info (movie_id);

IF OBJECT_ID(N'job.cast_info', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'job.cast_info') AND name = N'person_role_id_cast_info')
    CREATE INDEX person_role_id_cast_info ON job.cast_info (person_role_id);

IF OBJECT_ID(N'job.cast_info', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'job.cast_info') AND name = N'role_id_cast_info')
    CREATE INDEX role_id_cast_info ON job.cast_info (role_id);

IF OBJECT_ID(N'job.complete_cast', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'job.complete_cast') AND name = N'movie_id_complete_cast')
    CREATE INDEX movie_id_complete_cast ON job.complete_cast (movie_id);

IF OBJECT_ID(N'job.movie_companies', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'job.movie_companies') AND name = N'movie_id_movie_companies')
    CREATE INDEX movie_id_movie_companies ON job.movie_companies (movie_id);

IF OBJECT_ID(N'job.movie_companies', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'job.movie_companies') AND name = N'company_id_movie_companies')
    CREATE INDEX company_id_movie_companies ON job.movie_companies (company_id);

IF OBJECT_ID(N'job.movie_companies', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'job.movie_companies') AND name = N'company_type_id_movie_companies')
    CREATE INDEX company_type_id_movie_companies ON job.movie_companies (company_type_id);

IF OBJECT_ID(N'job.movie_info', N'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'job.movie_info') AND name = N'movie_id_movie_info' AND type_desc = 'NONCLUSTERED')
    DROP INDEX movie_id_movie_info ON job.movie_info;

IF OBJECT_ID(N'job.movie_info', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'job.movie_info') AND name = N'clustered_movie_id_movie_info')
    CREATE CLUSTERED INDEX clustered_movie_id_movie_info ON job.movie_info (movie_id);

IF OBJECT_ID(N'job.movie_info', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'job.movie_info') AND name = N'info_type_id_movie_info')
    CREATE INDEX info_type_id_movie_info ON job.movie_info (info_type_id);

IF OBJECT_ID(N'job.movie_info_idx', N'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'job.movie_info_idx') AND name = N'movie_id_movie_info_idx' AND type_desc = 'NONCLUSTERED')
    DROP INDEX movie_id_movie_info_idx ON job.movie_info_idx;

IF OBJECT_ID(N'job.movie_info_idx', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'job.movie_info_idx') AND name = N'clustered_movie_id_movie_info_idx')
    CREATE CLUSTERED INDEX clustered_movie_id_movie_info_idx ON job.movie_info_idx (movie_id);

IF OBJECT_ID(N'job.movie_info_idx', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'job.movie_info_idx') AND name = N'info_type_id_movie_info_idx')
    CREATE INDEX info_type_id_movie_info_idx ON job.movie_info_idx (info_type_id);

IF OBJECT_ID(N'job.movie_keyword', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'job.movie_keyword') AND name = N'movie_id_movie_keyword')
    CREATE INDEX movie_id_movie_keyword ON job.movie_keyword (movie_id);

IF OBJECT_ID(N'job.movie_keyword', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'job.movie_keyword') AND name = N'keyword_id_movie_keyword')
    CREATE INDEX keyword_id_movie_keyword ON job.movie_keyword (keyword_id);

IF OBJECT_ID(N'job.movie_link', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'job.movie_link') AND name = N'movie_id_movie_link')
    CREATE INDEX movie_id_movie_link ON job.movie_link (movie_id);

IF OBJECT_ID(N'job.movie_link', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'job.movie_link') AND name = N'linked_movie_id_movie_link')
    CREATE INDEX linked_movie_id_movie_link ON job.movie_link (linked_movie_id);

IF OBJECT_ID(N'job.movie_link', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'job.movie_link') AND name = N'link_type_id_movie_link')
    CREATE INDEX link_type_id_movie_link ON job.movie_link (link_type_id);

IF OBJECT_ID(N'job.person_info', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'job.person_info') AND name = N'person_id_person_info')
    CREATE INDEX person_id_person_info ON job.person_info (person_id);

IF OBJECT_ID(N'job.person_info', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'job.person_info') AND name = N'info_type_id_person_info')
    CREATE INDEX info_type_id_person_info ON job.person_info (info_type_id);

IF OBJECT_ID(N'job.title', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'job.title') AND name = N'kind_id_title')
    CREATE INDEX kind_id_title ON job.title (kind_id);

-- Refresh statistics for JOB tables when stats are missing or stale after loading/tuning.
DECLARE @JobTableName NVARCHAR(255);
DECLARE @JobSQL NVARCHAR(MAX);

DECLARE job_stats_cursor CURSOR FOR
SELECT t.name
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = 'job'
  AND t.name IN (
      'aka_name', 'aka_title', 'cast_info', 'char_name', 'comp_cast_type',
      'company_name', 'company_type', 'complete_cast', 'info_type', 'keyword',
      'kind_type', 'link_type', 'movie_companies', 'movie_info', 'movie_info_idx',
      'movie_keyword', 'movie_link', 'name', 'person_info', 'role_type', 'title'
  );

OPEN job_stats_cursor;
FETCH NEXT FROM job_stats_cursor INTO @JobTableName;

WHILE @@FETCH_STATUS = 0
BEGIN
    IF EXISTS (
        SELECT 1
        FROM sys.stats st
        JOIN sys.objects o ON o.object_id = st.object_id
        WHERE st.object_id = OBJECT_ID(N'job.' + @JobTableName)
          AND (
              STATS_DATE(st.object_id, st.stats_id) IS NULL
              OR STATS_DATE(st.object_id, st.stats_id) < o.modify_date
          )
    )
    BEGIN
        SET @JobSQL = 'UPDATE STATISTICS [job].' + QUOTENAME(@JobTableName);
        EXEC sp_executesql @JobSQL;
    END

    FETCH NEXT FROM job_stats_cursor INTO @JobTableName;
END

CLOSE job_stats_cursor;
DEALLOCATE job_stats_cursor;
