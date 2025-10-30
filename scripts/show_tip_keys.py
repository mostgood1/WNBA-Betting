import pandas as pd
from pathlib import Path

def norm_gid(x)->str:
    s=str(x).strip()
    if len(s)==8 and s.startswith('2250'):
        s='00'+s
    return s.zfill(10) if s.isdigit() else s

root= Path(__file__).resolve().parent.parent / 'data' / 'processed'
ds='2025-10-24'
tip= pd.read_csv(root / f'tip_winner_probs_{ds}.csv')
rec= pd.read_csv(root / f'pbp_reconcile_{ds}.csv')
rec['game_id_norm']=rec['game_id'].astype(str).str.strip().map(norm_gid)

tip_keys= set(tip['game_id'].astype(str).map(norm_gid))
rec_keys= set(rec['game_id_norm'].astype(str))
print('tip keys sample:', sorted(list(tip_keys))[:5])
print('rec keys sample:', sorted(list(rec_keys))[:5])
print('intersection size:', len(tip_keys & rec_keys))
