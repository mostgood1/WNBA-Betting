from __future__ import annotations

import argparse
from pathlib import Path
from datetime import datetime, timedelta
import json
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
PROC = ROOT / "data" / "processed"

NEEDED = {
    "gameId": {"gameId", "GAME_ID", "game_id"},
    "team": {"teamTricode", "TEAM_ABBREVIATION", "team", "slugTeam"},
    "pts": {"points", "PTS"},
    "reb": {"reboundsTotal", "REB", "TREB"},
    "ast": {"assists", "AST"},
    "threes": {"threePointersMade", "FG3M"},
}


def _col(df: pd.DataFrame, keys: set[str]) -> str | None:
    cols = {c.lower(): c for c in df.columns}
    for k in keys:
        if k.lower() in cols:
            return cols[k.lower()]
    return None


def _date_from_filename(path: Path) -> datetime | None:
    name = path.stem.replace("boxscores_", "")
    try:
        return datetime.fromisoformat(name)
    except Exception:
        try:
            return datetime.strptime(name, "%Y-%m-%d")
        except Exception:
            return None


def build_opponent_splits(date_str: str, days_back: int = 21) -> tuple[pd.DataFrame, dict]:
    cutoff = datetime.fromisoformat(date_str)
    start = cutoff - timedelta(days=days_back)
    per_game: dict[str, dict[str, dict[str, float]]] = {}

    for p in sorted(PROC.glob("boxscores_*.csv")):
        dval = _date_from_filename(p)
        if dval is None or not (start <= dval <= cutoff):
            continue
        try:
            df = pd.read_csv(p)
        except Exception:
            continue
        gid = _col(df, NEEDED["gameId"]) ; team = _col(df, NEEDED["team"]) ;
        c_pts = _col(df, NEEDED["pts"]) ; c_reb = _col(df, NEEDED["reb"]) ; c_ast = _col(df, NEEDED["ast"]) ; c_3 = _col(df, NEEDED["threes"]) ;
        if not gid or not team:
            continue
        use_cols = [x for x in [gid, team, c_pts, c_reb, c_ast, c_3] if x]
        tmp = df[use_cols].copy()
        for c in [c_pts, c_reb, c_ast, c_3]:
            if c and c in tmp.columns:
                tmp[c] = pd.to_numeric(tmp[c], errors="coerce")
        # Sum per team per game
        grp = tmp.groupby([gid, team], as_index=False).sum()
        for gk, sub in grp.groupby(gid):
            rows = sub.to_dict(orient="records")
            if len(rows) < 2:
                continue
            # Attribute opponent totals as allowed to the other team
            if len(rows) >= 2:
                a, b = rows[0], rows[1]
                ta = str(a.get(team) or "").strip().upper()
                tb = str(b.get(team) or "").strip().upper()
                if ta:
                    per_game.setdefault(ta, {})[gk] = {
                        "pts": float(b.get(c_pts, 0.0) or 0.0) if c_pts else np.nan,
                        "reb": float(b.get(c_reb, 0.0) or 0.0) if c_reb else np.nan,
                        "ast": float(b.get(c_ast, 0.0) or 0.0) if c_ast else np.nan,
                        "threes": float(b.get(c_3, 0.0) or 0.0) if c_3 else np.nan,
                    }
                if tb:
                    per_game.setdefault(tb, {})[gk] = {
                        "pts": float(a.get(c_pts, 0.0) or 0.0) if c_pts else np.nan,
                        "reb": float(a.get(c_reb, 0.0) or 0.0) if c_reb else np.nan,
                        "ast": float(a.get(c_ast, 0.0) or 0.0) if c_ast else np.nan,
                        "threes": float(a.get(c_3, 0.0) or 0.0) if c_3 else np.nan,
                    }
    # Aggregate averages
    rows = []
    for team_key, game_map in per_game.items():
        vals = pd.DataFrame(list(game_map.values()))
        out = {
            "team": team_key,
            "games": len(vals),
            "pts_allowed": float(vals["pts"].mean()) if "pts" in vals.columns else np.nan,
            "reb_allowed": float(vals["reb"].mean()) if "reb" in vals.columns else np.nan,
            "ast_allowed": float(vals["ast"].mean()) if "ast" in vals.columns else np.nan,
            "threes_allowed": float(vals["threes"].mean()) if "threes" in vals.columns else np.nan,
        }
        rows.append(out)
    avg_df = pd.DataFrame(rows)
    # Ranks (higher allowed => higher rank, favorable for overs)
    ranks = {}
    for mk, col in [("pts","pts_allowed"),("reb","reb_allowed"),("ast","ast_allowed"),("threes","threes_allowed")]:
        if col in avg_df.columns and not avg_df.empty:
            s = avg_df[["team", col]].dropna()
            s[col] = pd.to_numeric(s[col], errors="coerce")
            s = s.dropna()
            if not s.empty:
                ord = s[col].rank(method="min", ascending=True)
                max_rank = int(ord.max()) if len(ord) > 0 else 0
                for i, (_, row) in enumerate(s.iterrows()):
                    t = str(row["team"]).strip().upper()
                    ranks.setdefault(t, {})[mk] = int(ord.iat[i])
    return avg_df, ranks


def main():
    ap = argparse.ArgumentParser(description="Build opponent splits (allowed stats) over recent window")
    ap.add_argument("--date", required=True, help="YYYY-MM-DD cutoff date")
    ap.add_argument("--days", type=int, default=21, help="Lookback window in days")
    ap.add_argument("--out-csv", default=None, help="Explicit CSV output path")
    args = ap.parse_args()
    avg_df, ranks = build_opponent_splits(args.date, args.days)
    PROC.mkdir(parents=True, exist_ok=True)
    out_csv = Path(args.out_csv) if args.out_csv else (PROC / f"opponent_splits_{args.date}.csv")
    out_json = PROC / f"opponent_splits_{args.date}.json"
    try:
        avg_df.to_csv(out_csv, index=False)
    except Exception as e:
        print(f"ERROR: write_csv: {e}")
    try:
        with open(out_json, "w", encoding="utf-8") as f:
            json.dump({"date": args.date, "ranks": ranks}, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"ERROR: write_json: {e}")
    print("OK:opponent_splits")


if __name__ == "__main__":
    main()
