CREATE TABLE job.aka_title (
    id INT NOT NULL,
    movie_id INT NOT NULL,
    title TEXT NOT NULL,
    imdb_index TEXT,
    kind_id INT NOT NULL,
    production_year INT,
    phonetic_code TEXT,
    episode_of_id INT,
    season_nr INT,
    episode_nr INT,
    note TEXT,
    md5sum TEXT
);
