from __future__ import annotations

import argparse
from pathlib import Path
import json
import pandas as pd
import numpy as np
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
PROC = ROOT / "data" / "processed"

# Ensure src/ is importable so we can reuse team normalizers.
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

try:
    from nba_betting.teams import to_tricode as _to_tri  # type: ignore
except Exception:
    _to_tri = None  # type: ignore

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

    # Prefer generating the snapshot from the processed league_status for the date.
    # This keeps injuries_counts aligned with the actual availability pool used by downstream
    # predictions/sims (and avoids mis-tagged raw injury rows).
    ls_path = PROC / f"league_status_{date_str}.csv"
    try:
        if ls_path.exists():
            ls = pd.read_csv(ls_path)
            if ls is not None and (not ls.empty):
                cols = {c.lower(): c for c in ls.columns}
                name_c = cols.get("player_name") or cols.get("player")
                team_c = cols.get("team")
                status_c = cols.get("injury_status") or cols.get("status")
                on_slate_c = cols.get("team_on_slate")
                if name_c and team_c and status_c:
                    filt = ls[[name_c, team_c, status_c] + ([on_slate_c] if on_slate_c else [])].copy()
                    filt = filt.rename(columns={name_c: "player", team_c: "team", status_c: "status"})
                    # Focus on slate teams if that flag exists; otherwise keep all.
                    if on_slate_c and on_slate_c in filt.columns:
                        try:
                            filt = filt[filt[on_slate_c].fillna(False).astype(bool)].copy()
                        except Exception:
                            pass
                    filt["status_norm"] = filt["status"].astype(str).str.upper()
                    filt = filt[filt["status_norm"].map(_excluded_status)].copy()
                    filt = filt.drop(columns=[on_slate_c], errors="ignore")

                    # Team counts
                    if "team" in filt.columns:
                        cnt = filt.groupby("team").size()
                        for t, n in cnt.items():
                            tk = str(t or "").strip().upper()
                            if tk:
                                team_counts[tk] = int(n)

                    for _, row in filt.iterrows():
                        excluded_players.append({
                            "player": str(row.get("player") or ""),
                            "team": str(row.get("team") or ""),
                            "status": str(row.get("status") or ""),
                        })

                    # Identity-aware impact: use player_logs over last 30 days to score outs
                    try:
                        logs_p = PROC / "player_logs.csv"
                        if logs_p.exists():
                            logs = pd.read_csv(logs_p)
                            cols2 = {c.lower(): c for c in logs.columns}
                            c_date = cols2.get("game_date")
                            c_name = cols2.get("player_name")
                            c_team = cols2.get("team_abbreviation")
                            c_min = cols2.get("min")
                            c_fpts = cols2.get("fantasy_pts")
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
                                agg = tmp.groupby(["name_norm", c_team], as_index=False).agg({
                                    (c_min or "MIN"): ("mean" if c_min else np.nan),
                                    (c_fpts or "FANTASY_PTS"): ("mean" if c_fpts else np.nan),
                                })
                                try:
                                    freq = tmp.groupby(["name_norm", c_team], as_index=False).size().rename(columns={"size": "cnt"})
                                except Exception:
                                    freq = pd.DataFrame(columns=["name_norm", c_team, "cnt"])
                                prefer_team_map: dict[str, str] = {}
                                try:
                                    for name, grp in freq.groupby("name_norm"):
                                        g2 = grp.sort_values(["cnt"], ascending=False)
                                        tval = str(g2.iloc[0].get(c_team) or "").strip().upper()
                                        if tval:
                                            prefer_team_map[str(name)] = tval
                                except Exception:
                                    prefer_team_map = {}

                                def _get_mean(row_name: str, row_team: str) -> tuple[float, float]:
                                    try:
                                        rn = _norm_player_name(row_name)
                                        rt = str(row_team or "").strip().upper()
                                        sub = agg[(agg["name_norm"] == rn) & (agg[c_team] == rt)]
                                        if sub.empty:
                                            sub = agg[agg["name_norm"] == rn]
                                        if sub.empty:
                                            return (0.0, 0.0)
                                        mv = float(sub.iloc[0].get(c_min or "MIN", 0.0) or 0.0)
                                        fv = float(sub.iloc[0].get(c_fpts or "FANTASY_PTS", 0.0) or 0.0)
                                        return (mv, fv)
                                    except Exception:
                                        return (0.0, 0.0)

                                scored: dict[str, list[tuple[str, float, float]]] = {}
                                for _, row in filt.iterrows():
                                    pname = str(row.get("player") or "")
                                    pteam = str(row.get("team") or "").strip().upper()
                                    pteam_pref = prefer_team_map.get(_norm_player_name(pname), pteam)
                                    mv, fv = _get_mean(pname, pteam_pref)
                                    impact = fv if (fv and np.isfinite(fv) and fv > 0) else (mv if (mv and np.isfinite(mv)) else 0.0)
                                    scored.setdefault(pteam_pref, []).append((pname, impact, mv))

                                for team_key, lst in scored.items():
                                    if not lst:
                                        continue
                                    lst_sorted = sorted(lst, key=lambda x: float(x[1] or 0.0), reverse=True)
                                    names = [x[0] for x in lst_sorted[:3]]
                                    total_impact = float(sum(float(x[1] or 0.0) for x in lst_sorted[:3]))
                                    if names:
                                        team_key_outs[team_key] = names
                                    if total_impact > 0:
                                        team_impact[team_key] = total_impact
                    except Exception:
                        pass

                    _rows = [{"team": k, "outs": v} for k, v in team_counts.items()]
                    if _rows:
                        df_counts = pd.DataFrame(_rows).sort_values(["outs"], ascending=False)
                    else:
                        df_counts = pd.DataFrame(columns=["team", "outs"])
                    snapshot = {
                        "date": date_str,
                        "team_counts": team_counts,
                        "players": excluded_players,
                        "team_key_outs": team_key_outs,
                        "team_impact": team_impact,
                    }
                    return snapshot, df_counts
    except Exception:
        # Fall back to raw injuries feed below.
        pass
    if not path.exists():
        return {"date": date_str, "team_counts": team_counts, "players": excluded_players, "team_key_outs": team_key_outs, "team_impact": team_impact}, pd.DataFrame()
    inj = pd.read_csv(path)
    if "date" in inj.columns:
        inj["date"] = pd.to_datetime(inj["date"], errors="coerce").dt.date
        inj = inj[inj["date"].notna()]
        inj = inj[inj["date"] <= pd.to_datetime(date_str).date()].copy()

        # Recency guardrail: avoid treating very old OUT rows as current.
        # Keep only rows within the last ~30 days unless the status looks season-ending/indefinite.
        try:
            cutoff = pd.to_datetime(date_str, errors="coerce").date()
            fresh_cutoff = (pd.Timestamp(cutoff) - pd.Timedelta(days=30)).date()
            st = inj.get("status", "").astype(str).str.upper()
            season_out = (
                (st.str.contains("OUT") & (st.str.contains("SEASON") | st.str.contains("INDEFINITE")))
                | st.str.contains("SEASON-ENDING")
            )
            inj = inj[(inj["date"] >= fresh_cutoff) | season_out].copy()
        except Exception:
            pass
    sort_cols = [c for c in ["date"] if c in inj.columns]
    if sort_cols:
        inj = inj.sort_values(sort_cols)
    grp_cols = [c for c in ["player","team"] if c in inj.columns] or ["player"]
    latest = inj.groupby(grp_cols, as_index=False).tail(1).copy()
    latest.loc[:, "status_norm"] = latest["status"].astype(str).str.upper()
    filt = latest[latest["status_norm"].map(_excluded_status)].copy()

    # Drop stale injury exclusions when we have evidence the player played AFTER the injury row date.
    # This protects against persistent/mis-tagged OUT rows in the injuries feed.
    try:
        if (not filt.empty) and ("date" in filt.columns) and ("player" in filt.columns):
            logs_p = PROC / "player_logs.csv"
            if logs_p.exists():
                logs = pd.read_csv(logs_p)
                if logs is not None and (not logs.empty):
                    cols = {c.upper(): c for c in logs.columns}
                    c_name = cols.get("PLAYER_NAME")
                    c_date = cols.get("GAME_DATE")
                    c_min = cols.get("MIN")
                    if c_name and c_date and c_min:
                        tmp = logs[[c_name, c_date, c_min]].copy()
                        tmp[c_date] = pd.to_datetime(tmp[c_date], errors="coerce").dt.date
                        tmp = tmp[tmp[c_date].notna()]
                        try:
                            cutoff = pd.to_datetime(date_str, errors="coerce").date()
                            tmp = tmp[tmp[c_date] <= cutoff]
                        except Exception:
                            pass
                        tmp[c_min] = pd.to_numeric(tmp[c_min], errors="coerce").fillna(0.0)
                        tmp = tmp[tmp[c_min] > 0].copy()

                        def _norm_name_key(s: str) -> str:
                            t = (s or "").strip().lower()
                            try:
                                import unicodedata as _ud
                                t = _ud.normalize("NFKD", t)
                                t = t.encode("ascii", "ignore").decode("ascii")
                            except Exception:
                                pass
                            t = re.sub(r"[^a-z0-9\s]", "", t)
                            t = re.sub(r"\s+", " ", t).strip()
                            toks = [x for x in t.split(" ") if x and x not in {"jr", "sr", "ii", "iii", "iv", "v"}]
                            return " ".join(toks)

                        tmp["_pkey"] = tmp[c_name].astype(str).map(_norm_name_key)
                        last_game = tmp.groupby("_pkey")[c_date].max().to_dict()
                        if last_game:
                            ff = filt.copy()
                            ff["_pkey"] = ff["player"].astype(str).map(_norm_name_key)
                            ff["_last_game"] = ff["_pkey"].map(last_game)
                            # If player played after the injury row date, treat the OUT as stale.
                            mask_stale = ff["_last_game"].notna() & ff["date"].notna() & (ff["_last_game"] > ff["date"])
                            filt = filt.loc[~mask_stale.values].copy()
    except Exception:
        pass

    # Correct team assignments using processed rosters for the season. This prevents
    # mis-tagged injury rows (e.g., trades/feed glitches) from being attributed to the wrong team.
    try:
        if not filt.empty and ("player" in filt.columns):
            # Determine season roster file
            d = pd.to_datetime(date_str, errors="coerce")
            start_year = int(d.year) if (d is not None and not pd.isna(d) and int(d.month) >= 7) else (int(d.year) - 1 if (d is not None and not pd.isna(d)) else None)
            season = f"{start_year}-{str(start_year+1)[-2:]}" if start_year is not None else None
            cand = (PROC / f"rosters_{season}.csv") if season else None
            roster_file = cand if (cand is not None and cand.exists()) else None
            if roster_file is None:
                files = list(PROC.glob("rosters_*.csv"))
                season_files = [f for f in files if "-" in f.stem]
                if season_files:
                    season_files.sort(key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)
                    roster_file = season_files[0]
                elif files:
                    files.sort(key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)
                    roster_file = files[0]

            roster_map: dict[str, str] = {}
            if roster_file is not None and roster_file.exists():
                rdf = pd.read_csv(roster_file)
                if rdf is not None and not rdf.empty:
                    cols = {c.upper(): c for c in rdf.columns}
                    name_col = cols.get("PLAYER")
                    tri_col = cols.get("TEAM_ABBREVIATION")
                    if name_col and tri_col:
                        def _norm_key(s: str) -> str:
                            t = (s or "").strip().lower()
                            try:
                                import unicodedata as _ud
                                t = _ud.normalize("NFKD", t)
                                t = t.encode("ascii", "ignore").decode("ascii")
                            except Exception:
                                pass
                            t = re.sub(r"[^a-z0-9\s]", "", t)
                            t = re.sub(r"\s+", " ", t).strip()
                            toks = [x for x in t.split(" ") if x and x not in {"jr", "sr", "ii", "iii", "iv", "v"}]
                            return " ".join(toks)

                        for _, rr in rdf[[name_col, tri_col]].dropna().iterrows():
                            try:
                                pk = _norm_key(str(rr.get(name_col) or ""))
                                raw_tri = str(rr.get(tri_col) or "").strip().upper()
                                tri = (str(_to_tri(raw_tri) or raw_tri).strip().upper()) if _to_tri else raw_tri
                                if pk and tri:
                                    roster_map[pk] = tri
                            except Exception:
                                continue

            if roster_map:
                def _norm_key_player(s: str) -> str:
                    t = (s or "").strip().lower()
                    try:
                        import unicodedata as _ud
                        t = _ud.normalize("NFKD", t)
                        t = t.encode("ascii", "ignore").decode("ascii")
                    except Exception:
                        pass
                    t = re.sub(r"[^a-z0-9\s]", "", t)
                    t = re.sub(r"\s+", " ", t).strip()
                    toks = [x for x in t.split(" ") if x and x not in {"jr", "sr", "ii", "iii", "iv", "v"}]
                    return " ".join(toks)

                # Apply corrections
                filt = filt.copy()
                filt["_pkey"] = filt["player"].astype(str).map(_norm_key_player)
                if "team" in filt.columns:
                    filt["team"] = filt["team"].astype(str).map(lambda x: (str(_to_tri(str(x)) or "").strip().upper()) if _to_tri else str(x or "").strip().upper())
                filt["team"] = filt.get("team", "").astype(str)
                filt["team_roster"] = filt["_pkey"].map(lambda k: roster_map.get(str(k) or ""))
                filt["team"] = filt.apply(lambda r: (str(r.get("team_roster") or "").strip().upper() or str(r.get("team") or "").strip().upper()), axis=1)
                filt = filt.drop(columns=["_pkey", "team_roster"], errors="ignore")
    except Exception:
        pass
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
    _rows = [{"team": k, "outs": v} for k, v in team_counts.items()]
    if _rows:
        df_counts = pd.DataFrame(_rows).sort_values(["outs"], ascending=False)
    else:
        df_counts = pd.DataFrame(columns=["team", "outs"])
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
