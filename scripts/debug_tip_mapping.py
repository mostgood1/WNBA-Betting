import pandas as pd
from pathlib import Path
from nba_betting.pbp_markets import _jump_ball_event as _jb
import re

def norm_gid(x:str)->str:
    s=str(x).strip()
    if len(s)==8 and s.startswith('2250'):
        s='00'+s
    return s.zfill(10) if s.isdigit() else s

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / 'data' / 'processed'

for ds in ['2025-10-23','2025-10-24','2025-10-25','2025-10-26']:
    tip_path=DATA / f'tip_winner_probs_{ds}.csv'
    if not tip_path.exists():
        print(ds, 'no tip file')
        continue
    tip=pd.read_csv(tip_path)
    # schedule map
    sched=DATA / 'schedule_2025_26.csv'
    sch=pd.read_csv(sched)
    sch['date_utc']=pd.to_datetime(sch['date_utc'],errors='coerce').dt.strftime('%Y-%m-%d')
    sm=sch[sch['date_utc']==ds].set_index('game_id')[['home_tricode','away_tricode']].to_dict('index')
    # pbp map
    dpg=DATA / 'pbp'
    pbp_map={}
    for f in dpg.glob('pbp_*.csv'):
        gid=f.stem.replace('pbp_','')
        pbp_map[norm_gid(gid)]=pd.read_csv(f)
    ok=0; total=0
    for _,r in tip.iterrows():
        gidn=norm_gid(r['game_id'])
        if gidn not in pbp_map: continue
        total+=1
        ev=_jb(pbp_map[gidn])
        if not ev: continue
        winner=(ev.get('winner_text') or '').strip().lower()
        if not winner: continue
        # get home/away
        raw=gidn.lstrip('0')
        pair=sm.get(raw)
        if not pair: continue
        home=pair['home_tricode']; away=pair['away_tricode']
        # rosterless fallback via pbp
        df=pbp_map[gidn]
        name=None
        for c in ('playerName','PLAYER1_NAME','player1_name'):
            if c in df.columns:
                name=df[c].astype(str).str.lower(); break
        team=None
        for c in ('teamTricode','PLAYER1_TEAM_ABBREVIATION','team_abbr'):
            if c in df.columns:
                team=df[c].astype(str).str.upper(); break
        if name is None or team is None: continue
        toks=[t for t in re.split(r'\s+', winner) if t]
        pat='|'.join([re.escape(t) for t in toks[-2:]]) if toks else None
        if not pat: continue
        m=team[name.str.contains(pat, regex=True)].value_counts()
        if m.empty: continue
        wtri=m.index[0]
        out= 1.0 if wtri.upper()==str(home).upper() else (0.0 if wtri.upper()==str(away).upper() else None)
        if out is not None:
            ok+=1
    print(ds, 'mapped', ok, 'of', total)
