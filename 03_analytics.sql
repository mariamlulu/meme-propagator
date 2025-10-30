-- 03_analytics.sql
-- Analytics layer: compute virality metrics, detect mutations, and export results

-- QUERY 1: Fingerprint virality statistics
-- Computes key metrics for each meme fingerprint:
-- - first_seen: when the meme first appeared
-- - peak_time: when it reached highest score
-- - peak_score: maximum score achieved
-- - time_to_peak_s: seconds from first appearance to peak (virality speed)
-- - unique_subreddits: cross-community spread
-- - appearances: total number of posts with this fingerprint
DELETE FROM fingerprint_stats;

WITH joined AS (
  SELECT tf.fingerprint, s.*, 
         ROW_NUMBER() OVER (PARTITION BY tf.fingerprint ORDER BY s.score DESC, s.created_utc ASC) AS rn
  FROM title_fingerprints tf
  JOIN staging_image_posts s ON s.id = tf.id
)
INSERT INTO fingerprint_stats
SELECT
  fingerprint,
  MIN(created_utc) AS first_seen,
  MAX(CASE WHEN rn = 1 THEN created_utc END) AS peak_time,
  MAX(CASE WHEN rn = 1 THEN score END) AS peak_score,
  CAST(EXTRACT(EPOCH FROM (MAX(CASE WHEN rn = 1 THEN created_utc END) - MIN(created_utc))) AS BIGINT) AS time_to_peak_s,
  COUNT(DISTINCT subreddit) AS unique_subreddits,
  COUNT(*) AS appearances
FROM joined
GROUP BY fingerprint
ORDER BY appearances DESC
LIMIT 10000;

-- QUERY 2: Image caption mutations
-- Identifies images that were reposted with different captions
-- This reveals how meme templates evolve as they spread
DELETE FROM image_mutations;

WITH url_multi AS (
  SELECT s.url
  FROM staging_image_posts s
  JOIN title_fingerprints tf ON s.id = tf.id
  GROUP BY s.url
  HAVING COUNT(DISTINCT tf.fingerprint) > 1
)
INSERT INTO image_mutations
SELECT s.url AS image_url, s.id, tf.fingerprint, s.title, s.subreddit, s.created_utc
FROM staging_image_posts s
JOIN title_fingerprints tf ON s.id = tf.id
JOIN url_multi um ON s.url = um.url
ORDER BY s.url, s.created_utc
LIMIT 2000;

-- QUERY 3: Subreddit meme adoption patterns (cohort analysis)
-- Advanced query showing which subreddits are meme "originators" vs "adopters"
-- Ranks subreddits by how quickly they pick up trending fingerprints
CREATE TABLE IF NOT EXISTS subreddit_adoption_stats AS
WITH fingerprint_timeline AS (
  -- Get first appearance time for each fingerprint globally
  SELECT 
    tf.fingerprint,
    MIN(s.created_utc) AS global_first_seen
  FROM title_fingerprints tf
  JOIN staging_image_posts s ON tf.id = s.id
  GROUP BY tf.fingerprint
  HAVING COUNT(*) >= 3  -- Only fingerprints with 3+ appearances
),
subreddit_posts AS (
  -- Get each subreddit's first post time for each fingerprint
  SELECT 
    s.subreddit,
    tf.fingerprint,
    MIN(s.created_utc) AS subreddit_first_seen,
    COUNT(*) AS posts_in_subreddit,
    MAX(s.score) AS best_score
  FROM staging_image_posts s
  JOIN title_fingerprints tf ON s.id = tf.id
  GROUP BY s.subreddit, tf.fingerprint
)
SELECT 
  sp.subreddit,
  COUNT(DISTINCT sp.fingerprint) AS unique_fingerprints,
  -- How many times was this subreddit first to post a fingerprint?
  SUM(CASE WHEN sp.subreddit_first_seen = ft.global_first_seen THEN 1 ELSE 0 END) AS originated_count,
  -- Average delay in hours from global first appearance to subreddit adoption
  AVG(EXTRACT(EPOCH FROM (sp.subreddit_first_seen - ft.global_first_seen)) / 3600.0) AS avg_adoption_delay_hours,
  -- Total posts and average score
  SUM(sp.posts_in_subreddit) AS total_posts,
  AVG(sp.best_score) AS avg_best_score,
  -- Engagement rate: originated / total unique fingerprints
  ROUND(100.0 * SUM(CASE WHEN sp.subreddit_first_seen = ft.global_first_seen THEN 1 ELSE 0 END) / COUNT(DISTINCT sp.fingerprint), 2) AS originator_rate_pct
FROM subreddit_posts sp
JOIN fingerprint_timeline ft ON sp.fingerprint = ft.fingerprint
GROUP BY sp.subreddit
HAVING COUNT(DISTINCT sp.fingerprint) >= 5  -- Subreddits with 5+ different fingerprints
ORDER BY originated_count DESC, unique_fingerprints DESC;

-- QUERY 4: Export results to CSV files in results/
COPY (SELECT * FROM fingerprint_stats ORDER BY appearances DESC LIMIT 1000) TO 'results/fingerprint_stats.csv' (HEADER, DELIMITER ',');
COPY (SELECT * FROM image_mutations ORDER BY image_url LIMIT 2000) TO 'results/image_mutations.csv' (HEADER, DELIMITER ',');
COPY (SELECT * FROM subreddit_adoption_stats ORDER BY originated_count DESC) TO 'results/subreddit_adoption_stats.csv' (HEADER, DELIMITER ',');
