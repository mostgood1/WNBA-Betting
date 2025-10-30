import pandas as pd
from pathlib import Path
p = Path(__file__).resolve().parent.parent / 'data' / 'processed' / 'schedule_2025_26.csv'
df = pd.read_csv(p)
df['date_utc'] = pd.to_datetime(df['date_utc'], errors='coerce').dt.strftime('%Y-%m-%d')
sub = df[df['date_utc'].isin(['2025-10-23','2025-10-24','2025-10-25','2025-10-26'])]
print(sub[['game_id','date_utc','home_tricode','away_tricode']].head(30).to_string(index=False))
print('n rows:', len(sub))
