import duckdb, pandas as pd, os

con = duckdb.connect('memeprop.duckdb')

# top mutated images (how many distinct captions per image)
df = con.execute("""
  SELECT s.url AS image_url,
         COUNT(DISTINCT tf.fingerprint) AS distinct_captions,
         MIN(s.created_utc) AS first_seen,
         COUNT(*) AS total_posts
  FROM staging_image_posts s
  JOIN title_fingerprints tf ON s.id = tf.id
  GROUP BY s.url
  HAVING COUNT(DISTINCT tf.fingerprint) > 1
  ORDER BY distinct_captions DESC, total_posts DESC
  LIMIT 10
""").fetchdf()

print("Top mutated images (url, distinct_captions, first_seen, total_posts):")
print(df.to_string(index=False))

if not df.empty:
    url = df.loc[0,'image_url']
    print(f"\nSample posts for top mutated image: {url}\n")
    sample = con.execute(f"""
      SELECT s.created_utc, s.subreddit, s.title, s.url
      FROM staging_image_posts s
      JOIN title_fingerprints tf ON s.id = tf.id
      WHERE s.url = '{url.replace("'", "''")}'
      ORDER BY s.created_utc
      LIMIT 12
    """).fetchdf()
    print(sample.to_string(index=False))
else:
    print("No mutated images found in this sample.")
