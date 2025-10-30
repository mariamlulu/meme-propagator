-- 01_schema.sql
-- Schema definition for meme propagation analysis
-- Tables follow a raw → staging → analytics pattern

-- Raw data table: stores unprocessed NDJSON submissions
CREATE TABLE IF NOT EXISTS raw_submissions (
    id             VARCHAR,
    created_utc    BIGINT,
    subreddit      VARCHAR,
    title          VARCHAR,
    url            VARCHAR,
    author         VARCHAR,
    score          BIGINT,
    num_comments   BIGINT,
    post_hint      VARCHAR,
    is_self        BOOLEAN,
    over_18        BOOLEAN
);

-- Staging table: filtered image posts with normalized timestamps
CREATE TABLE IF NOT EXISTS staging_image_posts (
    id            VARCHAR PRIMARY KEY,
    created_utc   TIMESTAMP,
    subreddit     VARCHAR,
    title         VARCHAR,
    url           VARCHAR,
    author        VARCHAR,
    score         BIGINT,
    num_comments  BIGINT,
    image_host    VARCHAR
);

-- Intermediate table: tokenized words from titles (for fingerprinting)
CREATE TABLE IF NOT EXISTS title_words (
    id          VARCHAR,
    word        VARCHAR
);

-- Fingerprint lookup: maps post IDs to their normalized title fingerprints
CREATE TABLE IF NOT EXISTS title_fingerprints (
    id          VARCHAR PRIMARY KEY,
    fingerprint VARCHAR
);

-- Analytics table: aggregated virality metrics per fingerprint
CREATE TABLE IF NOT EXISTS fingerprint_stats (
    fingerprint      VARCHAR PRIMARY KEY,
    first_seen       TIMESTAMP,
    peak_time        TIMESTAMP,
    peak_score       BIGINT,
    time_to_peak_s   BIGINT,
    unique_subreddits BIGINT,
    appearances      BIGINT
);

-- Analytics table: tracks caption mutations (same image, different captions)
CREATE TABLE IF NOT EXISTS image_mutations (
    image_url       VARCHAR,
    id              VARCHAR,
    fingerprint     VARCHAR,
    title           VARCHAR,
    subreddit       VARCHAR,
    created_utc     TIMESTAMP
);

-- Performance indexes for frequently joined and filtered columns
CREATE INDEX IF NOT EXISTS idx_staging_url ON staging_image_posts(url);
CREATE INDEX IF NOT EXISTS idx_staging_created ON staging_image_posts(created_utc);
CREATE INDEX IF NOT EXISTS idx_staging_score ON staging_image_posts(score DESC);
CREATE INDEX IF NOT EXISTS idx_title_words_id ON title_words(id);
CREATE INDEX IF NOT EXISTS idx_fingerprints_fp ON title_fingerprints(fingerprint);
CREATE INDEX IF NOT EXISTS idx_mutations_url ON image_mutations(image_url);
CREATE INDEX IF NOT EXISTS idx_fingerprint_stats_appearances ON fingerprint_stats(appearances DESC);
