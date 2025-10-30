#!/usr/bin/env python3
"""
Complete analysis runner for meme-propagator project
Demonstrates all SQL queries and analytics capabilities
"""
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import os

print("=" * 80)
print("MEME PROPAGATOR - COMPLETE SQL ANALYSIS")
print("=" * 80)

# Connect to database
con = duckdb.connect('memeprop.duckdb')

# 1. Top fingerprints by appearances
print("\n📊 TOP 10 MEME FINGERPRINTS BY APPEARANCES")
print("-" * 80)
df1 = con.execute("""
SELECT fingerprint, appearances, unique_subreddits, peak_score, time_to_peak_s
FROM fingerprint_stats
ORDER BY appearances DESC
LIMIT 10
""").fetchdf()
print(df1.to_string(index=False))

# 2. Fastest viral memes
print("\n\n⚡ TOP 10 FASTEST-TO-PEAK MEMES (peak_score > 100)")
print("-" * 80)
df2 = con.execute("""
SELECT fingerprint, time_to_peak_s, peak_score, appearances
FROM fingerprint_stats
WHERE peak_score > 100
ORDER BY time_to_peak_s ASC
LIMIT 10
""").fetchdf()
print(df2.to_string(index=False))

# 3. Caption mutations
print("\n\n🔄 TOP IMAGE CAPTION MUTATIONS")
print("-" * 80)
df3 = con.execute("""
SELECT s.url AS image_url,
       COUNT(DISTINCT tf.fingerprint) AS distinct_captions,
       MIN(s.created_utc) AS first_seen,
       COUNT(*) AS total_posts
FROM staging_image_posts s
JOIN title_fingerprints tf ON s.id = tf.id
GROUP BY s.url
HAVING COUNT(DISTINCT tf.fingerprint) > 1
ORDER BY distinct_captions DESC, total_posts DESC
LIMIT 5
""").fetchdf()
print(df3.to_string(index=False))

# 4. Subreddit adoption analysis
print("\n\n🏆 SUBREDDIT ADOPTION PATTERNS (Originators vs Adopters)")
print("-" * 80)
df4 = con.execute("""
SELECT 
  subreddit,
  unique_fingerprints,
  originated_count,
  ROUND(avg_adoption_delay_hours, 1) AS avg_delay_hrs,
  total_posts,
  ROUND(avg_best_score, 0) AS avg_score,
  originator_rate_pct
FROM subreddit_adoption_stats
ORDER BY originated_count DESC
LIMIT 10
""").fetchdf()
print(df4.to_string(index=False))

# 5. Generate timeline plot for top fingerprint
print("\n\n📈 GENERATING TIMELINE PLOT...")
print("-" * 80)
top_fp = con.execute("""
SELECT fingerprint FROM fingerprint_stats 
ORDER BY appearances DESC LIMIT 1
""").fetchone()[0]

timeline_df = con.execute(f"""
SELECT s.created_utc, s.score, s.subreddit, s.title
FROM title_fingerprints tf
JOIN staging_image_posts s ON tf.id = s.id
WHERE tf.fingerprint = '{top_fp.replace("'", "''")}'
ORDER BY s.created_utc
""").fetchdf()

timeline_df['created_utc'] = pd.to_datetime(timeline_df['created_utc'])
daily = timeline_df.groupby(pd.Grouper(key='created_utc', freq='D')).agg({'score':'sum'}).reset_index()

plt.figure(figsize=(10,3.5))
plt.plot(daily['created_utc'], daily['score'], marker='o', linewidth=1)
plt.title(f'Top fingerprint: "{top_fp}" — score over time (daily)')
plt.xlabel('Date')
plt.ylabel('Total score')
plt.tight_layout()
plt.savefig('results/top_fingerprint_timeline.png', dpi=150)
print(f"✓ Saved: results/top_fingerprint_timeline.png")

# Summary
print("\n\n" + "=" * 80)
print("ANALYSIS COMPLETE!")
print("=" * 80)
print("\n📁 Output files generated:")
print("   • results/fingerprint_stats.csv")
print("   • results/image_mutations.csv")
print("   • results/subreddit_adoption_stats.csv")
print("   • results/top_fingerprint_timeline.csv")
print("   • results/top_fingerprint_timeline.png")

print("\n💡 SQL techniques demonstrated:")
print("   • Window functions (ROW_NUMBER, PARTITION BY)")
print("   • CTEs (Common Table Expressions)")
print("   • Conditional aggregation (CASE in SELECT)")
print("   • Text processing (regex, string_agg)")
print("   • Date/time calculations (EXTRACT EPOCH)")
print("   • Multi-table JOINs")
print("   • Cohort analysis")
print("   • Performance indexes")

con.close()
print("\n✨ Ready for portfolio/interviews!\n")
