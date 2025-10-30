import pandas as pd, matplotlib.pyplot as plt, os, sys

fn = 'results/top_fingerprint_timeline.csv'
if not os.path.exists(fn):
    print("ERROR: timeline CSV not found:", fn); sys.exit(1)

df = pd.read_csv(fn)

# convert created_utc to datetime (handles unix seconds or readable string)
try:
    df['created_utc'] = pd.to_datetime(df['created_utc'].astype(int), unit='s')
except Exception:
    df['created_utc'] = pd.to_datetime(df['created_utc'], errors='coerce')

# aggregate to daily total score
daily = df.groupby(pd.Grouper(key='created_utc', freq='D')).agg({'score':'sum'}).reset_index()

plt.figure(figsize=(10,3.5))
plt.plot(daily['created_utc'], daily['score'], marker='o', linewidth=1)
plt.title('Top fingerprint — score over time (daily)')
plt.xlabel('Date'); plt.ylabel('Total score')
plt.tight_layout()

out='results/top_fingerprint_timeline.png'
plt.savefig(out, dpi=150)
print("Saved:", out)

# on mac open automatically
try:
    os.system(f"open {out}")
except:
    pass
