CREATE TABLE job.aka_name (
    id INT NOT NULL,
    person_id INT NOT NULL,
    name TEXT NOT NULL,
    imdb_index TEXT,
    name_pcode_cf TEXT,
    name_pcode_nf TEXT,
    surname_pcode TEXT,
    md5sum TEXT
);
