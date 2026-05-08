import pandas as pd
from pathlib import Path

def main():
    proc_dir = Path('c:/Users/mostg/OneDrive/Coding/WNBA-Betting/data/processed')
    pth = proc_dir / 'recon_props_2026-01-15.csv'
    df = pd.read_csv(pth)
    cols = {c.lower(): c for c in df.columns}
    tcol = cols.get('team') or cols.get('team_abbr')
    cand_names = ['PTS','REB','AST','THREES','PRA']
    cand = [cols.get(c.lower()) for c in cand_names if cols.get(c.lower())]
    print('cand:', cand)
    acc = {}
    if not (tcol and cand and len(cand) >= 2):
        print('missing tcol or cand')
        return
    for team, grp in df[[tcol] + cand].groupby(tcol):
        mat = grp[cand].apply(pd.to_numeric, errors='coerce').dropna(how='all')
        if mat is None or mat.empty:
            continue
        corr = mat.corr(method='pearson').fillna(0.0)
        for a in cand:
            for b in cand:
                if a == b:
                    continue
                r = float(corr.loc[a, b]) if (a in corr.index and b in corr.columns) else 0.0
                acc.setdefault((a,b), []).append(abs(r))
    print('pairs collected:', len(acc))
    pens = {k: max(0.0, min(0.20, 0.18 * float(pd.Series(v).mean()))) for k,v in acc.items()}
    nz = sum(1 for p in pens.values() if p>0)
    print('nonzero pens:', nz)
    print('sample:', list(pens.items())[:5])

if __name__ == '__main__':
    main()
