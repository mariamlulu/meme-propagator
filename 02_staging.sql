-- 02_staging.sql
-- ETL pipeline: Load NDJSON data and build staging tables with text processing
-- Make sure the filename below matches raw/submissions-2023-01-images.sample.ndjson

-- STEP 1: Load NDJSON into raw_submissions (read_json_auto exposes fields as columns)
INSERT INTO raw_submissions
SELECT
    COALESCE(id::VARCHAR, '') AS id,
    CAST(COALESCE(created_utc, '0') AS BIGINT) AS created_utc,
    COALESCE(subreddit::VARCHAR, '') AS subreddit,
    COALESCE(title::VARCHAR, '') AS title,
    COALESCE(url::VARCHAR, '') AS url,
    COALESCE(author::VARCHAR, '') AS author,
    CAST(COALESCE(score, 0) AS BIGINT) AS score,
    CAST(COALESCE(num_comments, 0) AS BIGINT) AS num_comments,
    COALESCE(post_hint::VARCHAR, '') AS post_hint,
    COALESCE(is_self, false) AS is_self,
    COALESCE(over_18, false) AS over_18
FROM read_json_auto('raw/submissions-2023-01-images.sample.ndjson');

-- STEP 2: Build staging_image_posts: filter for image hosts & normalize timestamp
-- Extracts only image posts from major hosting platforms
INSERT INTO staging_image_posts
SELECT
    id,
    TO_TIMESTAMP(CAST(created_utc AS BIGINT)) AS created_utc,
    subreddit,
    title,
    url,
    author,
    score,
    num_comments,
    lower(REGEXP_EXTRACT(url, 'https?://([^/]+)/')) AS image_host
FROM raw_submissions
WHERE url IS NOT NULL
  AND (
        lower(url) LIKE '%i.redd.it%' OR
        lower(url) LIKE '%imgur.com%' OR
        lower(url) LIKE '%imgflip%' OR
        lower(url) LIKE '%i.reddituploads%' OR
        post_hint = 'image'
  );

-- STEP 3: Tokenize titles into individual words (stopword filtering)
-- This creates the foundation for fingerprinting by breaking titles into normalized tokens
-- Stopwords (common words like "the", "and") are removed to focus on meaningful content
CREATE TEMPORARY TABLE IF NOT EXISTS stopwords(word VARCHAR);
DELETE FROM stopwords;
INSERT INTO stopwords VALUES
('the'),('and'),('for'),('a'),('an'),('to'),('of'),('in'),('on'),('is'),('it'),('that'),('this'),('with'),('as'),('are');

DELETE FROM title_words;
INSERT INTO title_words
SELECT
    id,
    lower(NULLIF(REGEXP_REPLACE(word, '^\\s+|\\s+$', ''), '')) AS word
FROM (
    SELECT id,
           regexp_split_to_table(
               REGEXP_REPLACE( lower(COALESCE(title, '')), '[^a-z0-9\\s]', ' ', 'g'),
               '\\s+'
           ) AS word
    FROM staging_image_posts
)
WHERE word IS NOT NULL AND length(word) > 2
  AND word NOT IN (SELECT word FROM stopwords);

-- STEP 4: Compute fingerprint: sorted distinct words concatenated
-- FINGERPRINTING ALGORITHM:
-- 1. Take all cleaned words for a post (from title_words)
-- 2. Remove duplicates (DISTINCT)
-- 3. Sort alphabetically (ORDER BY word)
-- 4. Concatenate into a single string (string_agg)
-- Result: "cat funny" and "funny cat" both become "cat funny"
-- This allows us to detect semantically similar memes regardless of word order
DELETE FROM title_fingerprints;
INSERT INTO title_fingerprints
SELECT id, string_agg(DISTINCT word, ' ' ORDER BY word) AS fingerprint
FROM title_words
GROUP BY id;
