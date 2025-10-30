import pandas as pd, os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_PROCESSED = ROOT / "data" / "processed"

for ds in ["2025-10-23","2025-10-24","2025-10-25","2025-10-26","2025-10-28","2025-10-29"]:
    tip_path = DATA_PROCESSED / f"tip_winner_probs_{ds}.csv"
    fb_path = DATA_PROCESSED / f"first_basket_probs_{ds}.csv"
    thr_path = DATA_PROCESSED / f"early_threes_{ds}.csv"
    dpg = DATA_PROCESSED / "pbp"
    ids_tip = set(); ids_fb = set(); ids_thr = set(); ids_pbp = set()
    if tip_path.exists():
        try:
            t = pd.read_csv(tip_path)
            ids_tip = set(str(x).strip() for x in t.get("game_id", pd.Series(dtype=str)).astype(str).tolist())
        except Exception: pass
    if fb_path.exists():
        try:
            f = pd.read_csv(fb_path)
            ids_fb = set(str(x).strip() for x in f.get("game_id", pd.Series(dtype=str)).astype(str).tolist())
        except Exception: pass
    if thr_path.exists():
        try:
            r = pd.read_csv(thr_path)
            ids_thr = set(str(x).strip() for x in r.get("game_id", pd.Series(dtype=str)).astype(str).tolist())
        except Exception: pass
    if dpg.exists():
        for p in dpg.glob("pbp_*.csv"):
            gid = p.stem.replace("pbp_","" ).strip()
            ids_pbp.add(gid)
            ids_pbp.add(gid.zfill(10))
    print(ds, {
        "has_tip": tip_path.exists(),
        "has_fb": fb_path.exists(),
        "has_thr": thr_path.exists(),
        "n_tip_ids": len(ids_tip),
        "n_fb_ids": len(ids_fb),
        "n_thr_ids": len(ids_thr),
        "sample_tip": list(sorted(list(ids_tip)))[:3],
        "sample_fb": list(sorted(list(ids_fb)))[:3],
        "overlap_tip_pbp": len(ids_tip & ids_pbp),
        "overlap_fb_pbp": len(ids_fb & ids_pbp),
        "overlap_thr_pbp": len(ids_thr & ids_pbp),
    })
