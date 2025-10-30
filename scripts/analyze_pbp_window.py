import pandas as pd, numpy as np, os, sys
from datetime import date, timedelta

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: analyze_pbp_window.py START_DATE END_DATE", file=sys.stderr)
        sys.exit(2)
    start = pd.to_datetime(sys.argv[1]).date()
    end = pd.to_datetime(sys.argv[2]).date()
    root = os.path.join(os.path.dirname(__file__), "..", "data", "processed")
    root = os.path.abspath(root)
    rows = []
    d = start
    while d <= end:
        p = os.path.join(root, f"pbp_reconcile_{d.isoformat()}.csv")
        if os.path.exists(p):
            try:
                df = pd.read_csv(p)
                df["__date"] = d.isoformat()
                rows.append(df)
            except Exception:
                pass
        d += timedelta(days=1)
    if not rows:
        print({"n_files": 0}); sys.exit(0)
    allr = pd.concat(rows, ignore_index=True)
    # First-basket metrics
    fb_top1 = pd.to_numeric(allr.get("first_basket_hit_top1"), errors='coerce')
    fb_top5 = pd.to_numeric(allr.get("first_basket_hit_top5"), errors='coerce')
    fb_pact = pd.to_numeric(allr.get("first_basket_prob_actual"), errors='coerce')
    fb_top1_acc = float(fb_top1.dropna().mean()) if fb_top1 is not None and fb_top1.notna().any() else float('nan')
    fb_top5_cov = float(fb_top5.dropna().mean()) if fb_top5 is not None and fb_top5.notna().any() else float('nan')
    fb_mean_pact = float(fb_pact.dropna().mean()) if fb_pact is not None and fb_pact.notna().any() else float('nan')
    # Tip metrics
    p = pd.to_numeric(allr.get("tip_prob_home"), errors='coerce')
    y = pd.to_numeric(allr.get("tip_outcome_home"), errors='coerce')
    mask = p.notna() & y.notna()
    if mask.any():
        brier = float(((p[mask]-y[mask])**2).mean())
        import math
        logloss = float(-(y[mask]*np.log(np.clip(p[mask],1e-9,1-1e-9)) + (1-y[mask])*np.log(np.clip(1-p[mask],1e-9,1-1e-9))).mean())
        n_tip = int(mask.sum())
    else:
        brier = float('nan'); logloss = float('nan'); n_tip = 0
    # Per-date breakdown for sanity
    by_date = allr.groupby("__date").agg({
        "first_basket_hit_top1": lambda s: pd.to_numeric(s, errors='coerce').mean(),
        "first_basket_hit_top5": lambda s: pd.to_numeric(s, errors='coerce').mean(),
        "first_basket_prob_actual": lambda s: pd.to_numeric(s, errors='coerce').mean(),
        "tip_prob_home": lambda s: pd.to_numeric(s, errors='coerce').count(),
        "tip_outcome_home": lambda s: pd.to_numeric(s, errors='coerce').count(),
    }).reset_index()
    print({
        "window": [start.isoformat(), end.isoformat()],
        "fb_top1_acc": fb_top1_acc,
        "fb_top5_cov": fb_top5_cov,
        "fb_mean_prob_actual": fb_mean_pact,
        "tip_brier": brier,
        "tip_logloss": logloss,
        "tip_n": n_tip,
        "files": len(rows),
    })
    try:
        print("by_date:")
        print(by_date.to_string(index=False))
    except Exception:
        pass
