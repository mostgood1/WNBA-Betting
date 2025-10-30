import pandas as pd
from pathlib import Path

def norm_gid(x)->str:
    s=str(x).strip()
    if len(s)==8 and s.startswith('2250'):
        s='00'+s
    return s.zfill(10) if s.isdigit() else s

root= Path(__file__).resolve().parent.parent / 'data' / 'processed'
for ds in ['2025-10-23','2025-10-24','2025-10-25','2025-10-26']:
    tip= pd.read_csv(root / f'tip_winner_probs_{ds}.csv')
    rec= pd.read_csv(root / f'pbp_reconcile_{ds}.csv')
    rec['game_id_norm']=rec['game_id'].astype(str).map(norm_gid)
    m= rec.dropna(subset=['tip_outcome_home']).set_index('game_id_norm')
    tot= len(tip)
    cnt= sum(1 for _,r in tip.iterrows() if norm_gid(r['game_id']) in m.index)
    print(ds, 'tip rows', tot, 'reconcile tip rows', m.index.nunique(), 'matches', cnt)
