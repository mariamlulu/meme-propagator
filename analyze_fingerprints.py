import duckdb, pandas as pd, sys, os

con = duckdb.connect('memeprop.duckdb')
print("Top 10 fingerprints by appearances:")
df1 = con.execute("""
SELECT fingerprint, appearances, unique_subreddits, peak_score, time_to_peak_s
FROM fingerprint_stats
ORDER BY appearances DESC
LIMIT 10
""").fetchdf()
print(df1.to_string(index=False))

print("\nTop 10 by unique_subreddits (spread across communities):")
df2 = con.execute("""
SELECT fingerprint, appearances, unique_subreddits, peak_score
FROM fingerprint_stats
ORDER BY unique_subreddits DESC, appearances DESC
LIMIT 10
""").fetchdf()
print(df2.to_string(index=False))

print("\nTop 10 fastest-to-peak among posts with peak_score > 100:")
df3 = con.execute("""
SELECT fingerprint, time_to_peak_s, peak_score, appearances
FROM fingerprint_stats
WHERE peak_score > 100
ORDER BY time_to_peak_s ASC
LIMIT 10
""").fetchdf()
print(df3.to_string(index=False))

row = con.execute("SELECT fingerprint, appearances, unique_subreddits, peak_score, time_to_peak_s FROM fingerprint_stats ORDER BY appearances DESC LIMIT 1").fetchone()
if row:
    fp = row[0]
    print('\nTop fingerprint to inspect (most appearances):', fp)
    # write timeline CSV for that fingerprint
    con.execute(f"""
    COPY (
        SELECT s.created_utc, s.score, s.subreddit, s.title, s.url
        FROM title_fingerprints tf
        JOIN staging_image_posts s ON tf.id = s.id
        WHERE tf.fingerprint = '{fp.replace("'", "''")}'
        ORDER BY s.created_utc
    ) TO 'results/top_fingerprint_timeline.csv' (HEADER, DELIMITER ',');
    """)
    print("Saved timeline CSV -> results/top_fingerprint_timeline.csv")
else:
    print("No fingerprints found in fingerprint_stats table.")

# show a few rows of the timeline CSV if created
if os.path.exists('results/top_fingerprint_timeline.csv'):
    print("\nPreview of results/top_fingerprint_timeline.csv:")
    print(pd.read_csv('results/top_fingerprint_timeline.csv').head(8).to_string(index=False))
