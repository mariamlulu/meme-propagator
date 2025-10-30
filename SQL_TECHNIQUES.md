# SQL Techniques Demonstrated

This document highlights the SQL skills showcased in this project for portfolio/interview purposes.

## 1. Schema Design & DDL
**File**: `sql/01_schema.sql`

- Table creation with appropriate data types
- Primary keys for data integrity
- Strategic indexes on frequently queried columns
- Comments documenting table purposes

```sql
CREATE TABLE IF NOT EXISTS fingerprint_stats (
    fingerprint      VARCHAR PRIMARY KEY,
    first_seen       TIMESTAMP,
    peak_time        TIMESTAMP,
    ...
);

CREATE INDEX IF NOT EXISTS idx_staging_url ON staging_image_posts(url);
```

## 2. Data Ingestion & Type Casting
**File**: `sql/02_staging.sql`

- Reading semi-structured data (NDJSON)
- Type casting and coercion
- NULL handling with COALESCE
- Data validation during load

```sql
SELECT
    COALESCE(id::VARCHAR, '') AS id,
    CAST(COALESCE(created_utc, '0') AS BIGINT) AS created_utc,
    TO_TIMESTAMP(CAST(created_utc AS BIGINT)) AS created_utc
FROM read_json_auto('raw/submissions-2023-01-images.sample.ndjson');
```

## 3. Text Processing & Regex
**File**: `sql/02_staging.sql`

- Pattern extraction with REGEXP_EXTRACT
- Text cleaning with REGEXP_REPLACE
- String splitting with regexp_split_to_table
- Case normalization

```sql
lower(REGEXP_EXTRACT(url, 'https?://([^/]+)/')) AS image_host

regexp_split_to_table(
    REGEXP_REPLACE(lower(COALESCE(title, '')), '[^a-z0-9\\s]', ' ', 'g'),
    '\\s+'
) AS word
```

## 4. Aggregation & String Functions
**File**: `sql/02_staging.sql`

- String aggregation with ordering
- DISTINCT within aggregates
- Custom fingerprinting algorithm

```sql
SELECT id, string_agg(DISTINCT word, ' ' ORDER BY word) AS fingerprint
FROM title_words
GROUP BY id;
```

## 5. Window Functions
**File**: `sql/03_analytics.sql`

- ROW_NUMBER for ranking
- PARTITION BY for grouped calculations
- Identifying peak values within groups

```sql
WITH joined AS (
  SELECT tf.fingerprint, s.*, 
         ROW_NUMBER() OVER (PARTITION BY tf.fingerprint 
                           ORDER BY s.score DESC, s.created_utc ASC) AS rn
  FROM title_fingerprints tf
  JOIN staging_image_posts s ON s.id = tf.id
)
```

## 6. Common Table Expressions (CTEs)
**File**: `sql/03_analytics.sql`

- Multi-level CTEs for complex logic
- Reusable subqueries
- Improved readability

```sql
WITH fingerprint_timeline AS (
  SELECT fingerprint, MIN(created_utc) AS global_first_seen
  FROM ...
  GROUP BY fingerprint
),
subreddit_posts AS (
  SELECT subreddit, fingerprint, MIN(created_utc) AS subreddit_first_seen
  FROM ...
  GROUP BY subreddit, fingerprint
)
SELECT ... FROM subreddit_posts sp JOIN fingerprint_timeline ft ...
```

## 7. Conditional Aggregation
**File**: `sql/03_analytics.sql`

- CASE statements within aggregates
- Calculating multiple metrics in one pass
- Conditional counting

```sql
SELECT
  MIN(created_utc) AS first_seen,
  MAX(CASE WHEN rn = 1 THEN created_utc END) AS peak_time,
  MAX(CASE WHEN rn = 1 THEN score END) AS peak_score,
  SUM(CASE WHEN subreddit_first_seen = global_first_seen THEN 1 ELSE 0 END) AS originated_count
FROM ...
```

## 8. Date/Time Manipulation
**File**: `sql/03_analytics.sql`

- EXTRACT for date parts
- EPOCH conversion for time differences
- Timestamp arithmetic

```sql
CAST(EXTRACT(EPOCH FROM (MAX(peak_time) - MIN(first_seen))) AS BIGINT) AS time_to_peak_s

AVG(EXTRACT(EPOCH FROM (subreddit_first_seen - global_first_seen)) / 3600.0) AS avg_adoption_delay_hours
```

## 9. Subqueries & Filtering
**File**: `sql/03_analytics.sql`

- Subqueries in WHERE clauses
- HAVING for post-aggregation filtering
- NOT IN for exclusion logic

```sql
WHERE word NOT IN (SELECT word FROM stopwords)

HAVING COUNT(DISTINCT tf.fingerprint) > 1

HAVING COUNT(*) >= 3  -- Only fingerprints with 3+ appearances
```

## 10. Multiple JOINs
**File**: `sql/03_analytics.sql`

- Multi-table joins
- Join conditions with multiple predicates
- Self-referential patterns

```sql
FROM staging_image_posts s
JOIN title_fingerprints tf ON s.id = tf.id
JOIN url_multi um ON s.url = um.url
```

## 11. Cohort Analysis
**File**: `sql/03_analytics.sql`

- Time-based cohort grouping
- Originator vs adopter classification
- Lag/lead analysis patterns

```sql
-- Calculate origination rate
ROUND(100.0 * SUM(CASE WHEN sp.subreddit_first_seen = ft.global_first_seen THEN 1 ELSE 0 END) 
      / COUNT(DISTINCT sp.fingerprint), 2) AS originator_rate_pct
```

## 12. Data Export
**File**: `sql/03_analytics.sql`

- COPY command for CSV export
- Formatted output with headers
- Result materialization

```sql
COPY (SELECT * FROM fingerprint_stats ORDER BY appearances DESC LIMIT 1000) 
TO 'results/fingerprint_stats.csv' (HEADER, DELIMITER ',');
```

---

## Interview Talking Points

1. **Problem-Solving**: "I designed a fingerprinting algorithm using sorted word aggregation to detect similar memes regardless of word order"

2. **Performance**: "I added indexes on frequently joined columns like url and fingerprint, reducing query time for the mutation detection query"

3. **Complex Analytics**: "I built a cohort analysis showing which subreddits originate memes vs adopt them, using CTEs and time-lag calculations"

4. **Data Quality**: "I implemented NULL handling and type validation during the ETL process to ensure data integrity"

5. **Real-World Application**: "This pipeline processes social media data to measure virality metrics like time-to-peak and cross-community spread"
