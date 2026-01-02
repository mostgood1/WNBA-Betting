from __future__ import annotations

import argparse
from pathlib import Path
import json
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
PROC = ROOT / "data" / "processed"

EXCLUDE_STATUSES = {"OUT","DOUBTFUL","SUSPENDED","INACTIVE","REST"}


def _excluded_status(u: str) -> bool:
    try:
        u = str(u).upper()
    except Exception:
        return False
    if u in EXCLUDE_STATUSES:
        return True
    if ("OUT" in u and ("SEASON" in u or "INDEFINITE" in u)) or ("SEASON-ENDING" in u):
        return True
    return False


def build_injuries_snapshot(date_str: str) -> tuple[dict, pd.DataFrame]:
    path = RAW / "injuries.csv"
    team_counts = {}
    excluded_players = []
    team_key_outs: dict[str, list[str]] = {}
    team_impact: dict[str, float] = {}
    if not path.exists():
        return {"date": date_str, "team_counts": team_counts, "players": excluded_players, "team_key_outs": team_key_outs, "team_impact": team_impact}, pd.DataFrame()
    inj = pd.read_csv(path)
    if "date" in inj.columns:
        inj["date"] = pd.to_datetime(inj["date"], errors="coerce").dt.date
        inj = inj[inj["date"].notna()]
        inj = inj[inj["date"] <= pd.to_datetime(date_str).date()].copy()
    sort_cols = [c for c in ["date"] if c in inj.columns]
    if sort_cols:
        inj = inj.sort_values(sort_cols)
    grp_cols = [c for c in ["player","team"] if c in inj.columns] or ["player"]
    latest = inj.groupby(grp_cols, as_index=False).tail(1).copy()
    latest.loc[:, "status_norm"] = latest["status"].astype(str).str.upper()
    filt = latest[latest["status_norm"].map(_excluded_status)].copy()
    # Team counts
    if "team" in filt.columns:
        cnt = filt.groupby("team").size()
        for t, n in cnt.items():
            tk = str(t or "").strip().upper()
            if tk:
                team_counts[tk] = int(n)
    # Player list
    for _, row in filt.iterrows():
        excluded_players.append({
            "player": str(row.get("player") or ""),
            "team": str(row.get("team") or ""),
            "status": str(row.get("status") or "")
        })

    # Identity-aware impact: use player_logs over last 30 days to score outs
    try:
        logs_p = PROC / "player_logs.csv"
        if logs_p.exists():
            logs = pd.read_csv(logs_p)
            # Normalize columns
            cols = {c.lower(): c for c in logs.columns}
            c_date = cols.get("game_date")
            c_name = cols.get("player_name")
            c_team = cols.get("team_abbreviation")
            c_min = cols.get("min")
            c_fpts = cols.get("fantasy_pts")
            # Prepare window
            cutoff = pd.to_datetime(date_str, errors="coerce")
            if pd.isna(cutoff):
                from datetime import date as _date
                cutoff = pd.Timestamp(_date.today())
            start = cutoff - pd.Timedelta(days=30)

            if c_date and c_name and c_team and (c_min or c_fpts):
                tmp = logs[[c_date, c_name, c_team] + ([c_min] if c_min else []) + ([c_fpts] if c_fpts else [])].copy()
                tmp[c_date] = pd.to_datetime(tmp[c_date], errors="coerce")
                tmp = tmp[tmp[c_date].notna()]
                tmp = tmp[(tmp[c_date] >= start) & (tmp[c_date] <= cutoff)]

                # Normalize player names (ASCII, upper, drop suffixes)
                def _norm_player_name(t: str) -> str:
                    try:
                        s = str(t or "")
                    except Exception:
                        s = ""
                    s = s.replace("-", " ").replace(".", "").replace("'", "").replace(",", " ").strip()
                    for suf in [" JR", " SR", " II", " III", " IV"]:
                        if s.upper().endswith(suf):
                            s = s[: -len(suf)]
                    try:
                        s = s.encode("ascii", "ignore").decode("ascii")
                    except Exception:
                        pass
                    return s.upper().strip()

                tmp["name_norm"] = tmp[c_name].astype(str).map(_norm_player_name)
                # Aggregate per player (mean mins, mean fantasy points)
                agg = tmp.groupby(["name_norm", c_team], as_index=False).agg({
                    (c_min or "MIN"): ("mean" if c_min else np.nan),
                    (c_fpts or "FANTASY_PTS"): ("mean" if c_fpts else np.nan)
                })
                # Determine most recent/most frequent team per player over window
                try:
                    freq = tmp.groupby(["name_norm", c_team], as_index=False).size().rename(columns={"size": "cnt"})
                except Exception:
                    freq = pd.DataFrame(columns=["name_norm", c_team, "cnt"])  # empty fallback
                prefer_team_map: dict[str, str] = {}
                try:
                    for name, grp in freq.groupby("name_norm"):
                        # choose team with highest count; tie-break by latest date
                        g2 = grp.sort_values(["cnt"], ascending=False)
                        tval = str(g2.iloc[0].get(c_team) or "").strip().upper()
                        if tval:
                            prefer_team_map[str(name)] = tval
                except Exception:
                    prefer_team_map = {}
                # Build quick lookup
                def _get_mean(row_name: str, row_team: str) -> tuple[float, float]:
                    try:
                        rn = _norm_player_name(row_name)
                        rt = str(row_team or "").strip().upper()
                        sub = agg[(agg["name_norm"] == rn) & (agg[c_team] == rt)]
                        if sub.empty:
                            # allow team-agnostic match if team changed
                            sub = agg[agg["name_norm"] == rn]
                        if sub.empty:
                            return (0.0, 0.0)
                        mv = float(sub.iloc[0].get(c_min or "MIN", 0.0) or 0.0)
                        fv = float(sub.iloc[0].get(c_fpts or "FANTASY_PTS", 0.0) or 0.0)
                        return (mv, fv)
                    except Exception:
                        return (0.0, 0.0)

                # Score excluded players and pick top per team
                scored: dict[str, list[tuple[str, float, float]]] = {}
                for _, row in filt.iterrows():
                    pname = str(row.get("player") or "")
                    pteam_inj = str(row.get("team") or "").strip().upper()
                    # Prefer team from logs window if available, else injuries team
                    pteam_pref = prefer_team_map.get(_norm_player_name(pname), pteam_inj)
                    mv, fv = _get_mean(pname, pteam_pref)
                    # Impact score: prefer fantasy points, fallback to minutes
                    impact = fv if (fv and np.isfinite(fv) and fv > 0) else (mv if (mv and np.isfinite(mv)) else 0.0)
                    scored.setdefault(pteam_pref, []).append((pname, impact, mv))

                for team_key, lst in scored.items():
                    if not lst:
                        continue
                    # Sort by impact desc and take top 3 names
                    lst_sorted = sorted(lst, key=lambda x: float(x[1] or 0.0), reverse=True)
                    names = [x[0] for x in lst_sorted[:3]]
                    total_impact = float(sum(float(x[1] or 0.0) for x in lst_sorted[:3]))
                    if names:
                        team_key_outs[team_key] = names
                    if total_impact > 0:
                        team_impact[team_key] = total_impact
    except Exception:
        # Non-fatal; keep counts-only
        pass
    # As CSV for convenience
    df_counts = pd.DataFrame([{"team": k, "outs": v} for k, v in team_counts.items()]).sort_values(["outs"], ascending=False)
    snapshot = {
        "date": date_str,
        "team_counts": team_counts,
        "players": excluded_players,
        "team_key_outs": team_key_outs,
        "team_impact": team_impact,
    }
    return snapshot, df_counts


def main():
    ap = argparse.ArgumentParser(description="Snapshot injuries to JSON + CSV counts")
    ap.add_argument("--date", required=True, help="YYYY-MM-DD date for snapshot cutoff")
    args = ap.parse_args()
    snapshot, df_counts = build_injuries_snapshot(args.date)
    PROC.mkdir(parents=True, exist_ok=True)
    out_json = PROC / f"injuries_counts_{args.date}.json"
    out_csv = PROC / f"injuries_counts_{args.date}.csv"
    try:
        with open(out_json, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"ERROR: write_json: {e}")
    try:
        if not df_counts.empty:
            df_counts.to_csv(out_csv, index=False)
    except Exception as e:
        print(f"ERROR: write_csv: {e}")
    print("OK:injuries_snapshot")


if __name__ == "__main__":
    main()
