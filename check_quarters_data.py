import pandas as pd

df = pd.read_csv('data/raw/games_nba_api.csv')

print("All 42 columns:")
for i, col in enumerate(df.columns, 1):
    print(f"{i:2}. {col}")

print(f"\n\nQuarter data completeness:")
quarter_cols = ['home_q1', 'home_q2', 'home_q3', 'home_q4', 'visitor_q1', 'visitor_q2', 'visitor_q3', 'visitor_q4']
for col in quarter_cols:
    if col in df.columns:
        non_null = df[col].notna().sum()
        pct = non_null / len(df) * 100
        print(f"{col:15} : {non_null:,} / {len(df):,} ({pct:.1f}% complete)")
    else:
        print(f"{col:15} : ❌ NOT FOUND")

print(f"\n\nHalves data completeness:")
halves_cols = ['home_h1', 'home_h2', 'visitor_h1', 'visitor_h2']
for col in halves_cols:
    if col in df.columns:
        non_null = df[col].notna().sum()
        pct = non_null / len(df) * 100
        print(f"{col:15} : {non_null:,} / {len(df):,} ({pct:.1f}% complete)")
    else:
        print(f"{col:15} : ❌ NOT FOUND")

print(f"\n\nSample game with quarter scores:")
sample = df[df['home_q1'].notna()].iloc[0]
print(f"Date: {sample['date']}")
print(f"{sample['visitor_team']} @ {sample['home_team']}")
print(f"\nQuarters:")
print(f"  Q1: {sample.get('visitor_q1', 'N/A')} - {sample.get('home_q1', 'N/A')}")
print(f"  Q2: {sample.get('visitor_q2', 'N/A')} - {sample.get('home_q2', 'N/A')}")
print(f"  Q3: {sample.get('visitor_q3', 'N/A')} - {sample.get('home_q3', 'N/A')}")
print(f"  Q4: {sample.get('visitor_q4', 'N/A')} - {sample.get('home_q4', 'N/A')}")
print(f"Final: {sample.get('visitor_pts', 'N/A')} - {sample.get('home_pts', 'N/A')}")
