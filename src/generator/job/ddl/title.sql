CREATE TABLE job.title (
    id INT NOT NULL,
    title TEXT NOT NULL,
    imdb_index TEXT,
    kind_id INT NOT NULL,
    production_year INT,
    imdb_id INT,
    phonetic_code TEXT,
    episode_of_id INT,
    season_nr INT,
    episode_nr INT,
    series_years TEXT,
    md5sum TEXT
);
