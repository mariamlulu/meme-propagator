import duckdb, pandas as pd

con = duckdb.connect('memeprop.duckdb')

print("=" * 80)
print("SUBREDDIT ADOPTION ANALYSIS")
print("=" * 80)
print("\nThis analysis shows which subreddits are meme 'originators' vs 'adopters'")
print("- originated_count: how many times this subreddit posted a meme FIRST")
print("- avg_adoption_delay_hours: average time lag from global first post")
print("- originator_rate_pct: percentage of memes this subreddit originated\n")

df = con.execute("""
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
  LIMIT 15
""").fetchdf()

if not df.empty:
    print(df.to_string(index=False))
    print("\n" + "=" * 80)
    print("INSIGHTS:")
    print("=" * 80)
    
    top_originator = df.iloc[0]
    print(f"\n🔥 Top Originator: r/{top_originator['subreddit']}")
    print(f"   - Posted {int(top_originator['originated_count'])} memes FIRST")
    print(f"   - {top_originator['originator_rate_pct']}% origination rate")
    
    if len(df) > 1:
        fastest_adopter = df.loc[df['avg_delay_hrs'] > 0].nsmallest(1, 'avg_delay_hrs')
        if not fastest_adopter.empty:
            adopter = fastest_adopter.iloc[0]
            print(f"\n⚡ Fastest Adopter: r/{adopter['subreddit']}")
            print(f"   - Average adoption delay: {adopter['avg_delay_hrs']} hours")
            print(f"   - Quickly picks up trending memes from other communities")
    
    highest_engagement = df.nlargest(1, 'avg_score').iloc[0]
    print(f"\n📈 Highest Engagement: r/{highest_engagement['subreddit']}")
    print(f"   - Average best score: {int(highest_engagement['avg_score'])}")
    
else:
    print("No subreddit adoption data found. Need more data with 3+ appearances per fingerprint.")

con.close()
