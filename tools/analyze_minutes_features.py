from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[1]
_DATA_ROOT_ENV = (os.environ.get("NBA_BETTING_DATA_ROOT") or "").strip()
DATA_ROOT = Path(_DATA_ROOT_ENV).expanduser().resolve() if _DATA_ROOT_ENV else (BASE_DIR / "data")
PROC_DIR = DATA_ROOT / "processed"


def _norm_player_key(x: Any) -> str:
    try:
        import re
        import unicodedata

        t = str(x or "").strip()
        if not t:
            return ""
        if "(" in t:
            t = t.split("(", 1)[0]
        t = t.replace("-", " ")
        t = re.sub(r"[\.,']", "", t)
        t = t.replace(",", " ")
        t = " ".join(t.split())
        u = t.upper()
        u = re.sub(r"\s+(JR|SR|II|III|IV)$", "", u).strip()
        try:
            u = unicodedata.normalize("NFKD", u)
            u = "".join(ch for ch in u if not unicodedata.combining(ch))
            u = u.encode("ascii", "ignore").decode("ascii")
        except Exception:
            pass
        return " ".join(u.split())
    except Exception:
        return ""


def _corr(x: pd.Series, y: pd.Series) -> float | None:
    x = pd.to_numeric(x, errors="coerce")
    y = pd.to_numeric(y, errors="coerce")
    m = x.notna() & y.notna()
    if int(m.sum()) < 5:
        return None
    try:
        return float(np.corrcoef(x[m].to_numpy(float), y[m].to_numpy(float))[0, 1])
    except Exception:
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--k", type=int, default=5)
    args = ap.parse_args()

    games_path = PROC_DIR / f"connected_realism_games_{args.start}_{args.end}.csv"
    players_path = PROC_DIR / f"connected_realism_players_{args.start}_{args.end}.csv"

    G = pd.read_csv(games_path)
    P = pd.read_csv(players_path)

    G["away_min_corr_topk"] = pd.to_numeric(G.get("away_min_corr_topk"), errors="coerce")
    worst = (
        G.sort_values("away_min_corr_topk", ascending=True)
        .head(int(args.k))
        .loc[:, ["date", "game_id", "home_tri", "away_tri", "away_min_corr_topk"]]
    )

    print("Worst away minutes-corr games (from evaluator):")
    print(worst.to_string(index=False))

    P["min_act"] = pd.to_numeric(P.get("min_act"), errors="coerce")

    for _, r in worst.iterrows():
        date_s = str(r.get("date") or "").strip()
        gid = str(r.get("game_id") or "").strip()
        team = str(r.get("away_tri") or "").strip().upper()
        opp = str(r.get("home_tri") or "").strip().upper()

        fp = PROC_DIR / f"props_predictions_{date_s}.csv"
        if not fp.exists():
            print(f"\n[{gid} {team} @ {opp}] missing props file: {fp}")
            continue

        feats = pd.read_csv(fp)
        if "team" in feats.columns:
            feats["team"] = feats["team"].astype(str).str.upper().str.strip()
        if "opponent" in feats.columns:
            feats["opponent"] = feats["opponent"].astype(str).str.upper().str.strip()

        if ("team" in feats.columns) and ("opponent" in feats.columns):
            sub = feats[(feats["team"] == team) & (feats["opponent"] == opp)].copy()
        elif "team" in feats.columns:
            sub = feats[feats["team"] == team].copy()
        else:
            sub = feats.copy()

        if "player_name" not in sub.columns:
            print(f"\n[{gid} {team} @ {opp}] props file missing player_name")
            continue

        act = P[(P["game_id"].astype(str) == gid) & (P["team"].astype(str).str.upper().str.strip() == team)].copy()
        act = act[["player_name", "min_act"]].copy()
        act["k"] = act["player_name"].map(_norm_player_key)

        sub["k"] = sub["player_name"].map(_norm_player_key)

        j = act.merge(sub, on="k", how="left", suffixes=("_act", "_feat"))

        cols = [c for c in ["roll5_min", "roll10_min", "pred_min"] if c in j.columns]
        print(f"\n=== {date_s} GID {gid} AWAY {team} vs {opp} ===")
        print(f"act_rows={len(act)} feat_rows={len(sub)} joined={len(j)}")
        for c in cols:
            cc = _corr(j[c], j["min_act"])
            cov = int(pd.to_numeric(j[c], errors="coerce").notna().sum())
            print(f"{c}: coverage={cov} corr_with_act={cc}")

        show = [c for c in ["player_name_act", "min_act"] + cols if c in j.columns]
        top = j.sort_values("min_act", ascending=False).head(10)
        print(top[show].to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
