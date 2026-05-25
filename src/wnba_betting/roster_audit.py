from __future__ import annotations
import pandas as pd
from typing import Tuple
from .config import paths
from .teams import to_tricode
from .boxscores import fetch_boxscores_for_date
from .league_status import build_league_status
import re as _re


def _norm_name(s: str) -> str:
    s = (s or '').strip().lower()
    s = _re.sub(r"[^a-z0-9\s]", "", s)
    s = _re.sub(r"\s+", " ", s).strip()
    toks = [t for t in s.split(' ') if t not in {'jr','sr','ii','iii','iv','v'}]
    return ' '.join(toks)


def audit_roster_for_date(date_str: str) -> Tuple[pd.DataFrame, dict]:
    """Compare league_status vs boxscores for a date.

    Produces roster_audit_<date>.csv with columns: player_id, player_name, team_ls, team_box, match.
    Returns (audit_df, summary_dict).
    """
    # Ensure league_status exists
    ls = build_league_status(date_str)
    if ls is None or ls.empty:
        return pd.DataFrame(), {"error": "league_status empty"}
    ls = ls.copy()
    # Normalize
    if "player_id" in ls.columns:
        ls["player_id"] = pd.to_numeric(ls["player_id"], errors="coerce")
    ls["team_ls"] = ls.get("team", "").astype(str).map(lambda x: (to_tricode(str(x)) or str(x).strip().upper()))
    ls["name_key"] = ls.get("player_name", "").astype(str).map(_norm_name)

    # Ensure team_on_slate is available using ScoreboardV2 if missing or empty
    if "team_on_slate" not in ls.columns or (ls.get("team_on_slate").isna().all() if "team_on_slate" in ls.columns else True):
        try:
            from nba_api.stats.endpoints import scoreboardv2
            sb = scoreboardv2.ScoreboardV2(game_date=date_str, day_offset=0, timeout=30)
            nd = sb.get_normalized_dict()
            ls_df = pd.DataFrame(nd.get("LineScore", []))
            teams_on = set()
            if not ls_df.empty:
                cu2 = {c.upper(): c for c in ls_df.columns}
                if "TEAM_ABBREVIATION" in cu2:
                    teams_on = set(str(x).strip().upper() for x in ls_df[cu2["TEAM_ABBREVIATION"]].dropna().astype(str))
            if teams_on:
                ls["team_on_slate"] = ls["team_ls"].astype(str).str.upper().isin(teams_on)
        except Exception:
            pass

    # Load or fetch boxscores for date
    box_path = paths.data_processed / f"boxscores_{date_str}.csv"
    if box_path.exists():
        box = pd.read_csv(box_path)
    else:
        box, _ = fetch_boxscores_for_date(date_str, only_final=True)
    if box is None or box.empty:
        return pd.DataFrame(), {"error": "boxscores empty"}
    cu = {c.upper(): c for c in box.columns}
    # Support modern v3 field names and classic v2 names
    pid = cu.get("PLAYER_ID") or cu.get("PERSONID")
    pname = cu.get("PLAYER_NAME") or cu.get("PLAYER")
    tcol = cu.get("TEAM_ABBREVIATION") or cu.get("TEAMTRICODE") or cu.get("TEAM_ABBR")
    part = box.copy()
    # Build missing fields if necessary
    if not pname:
        fn = cu.get("FIRSTNAME"); ln = cu.get("FAMILYNAME") or cu.get("LASTNAME")
        if fn and ln:
            part["_pname"] = part[fn].astype(str).str.strip() + " " + part[ln].astype(str).str.strip()
            pname = "_pname"
    keep_cols = [x for x in [pid, pname, tcol] if x]
    part = part[keep_cols].copy()
    if pid and pid in part.columns:
        part[pid] = pd.to_numeric(part[pid], errors="coerce")
    # Normalize box team and name key
    if tcol and tcol in part.columns:
        part["team_box"] = part[tcol].astype(str).map(lambda x: (to_tricode(str(x)) or str(x).strip().upper()))
    else:
        part["team_box"] = None
    if pname and pname in part.columns:
        part["name_key"] = part[pname].astype(str).map(_norm_name)
    else:
        part["name_key"] = None

    # Join by player_id primarily, then by name_key fallback
    audit = None
    if pid and pid in part.columns and "player_id" in ls.columns:
        audit = ls.merge(part[[pid, "team_box"]].rename(columns={pid: "player_id"}), on="player_id", how="left")
    else:
        audit = ls.merge(part[["name_key", "team_box"]], on="name_key", how="left")
    audit = audit[[x for x in ["player_id","player_name","team_ls","team_box","team_on_slate","injury_status"] if x in audit.columns]].copy()
    # Consider only players on slate for mismatch rate (uses computed scoreboard flags when available)
    on_slate = audit[audit.get("team_on_slate", False) == True].copy() if "team_on_slate" in audit.columns else audit.copy()
    audit["match"] = (audit["team_ls"].astype(str).str.upper() == audit["team_box"].astype(str).str.upper())
    on_slate["match"] = (on_slate["team_ls"].astype(str).str.upper() == on_slate["team_box"].astype(str).str.upper())

    # Summary
    total = int(len(audit))
    total_on = int(len(on_slate))
    matched = int(audit["match"].sum()) if "match" in audit.columns else 0
    matched_on = int(on_slate["match"].sum()) if "match" in on_slate.columns else 0
    summary = {
        "date": date_str,
        "rows": total,
        "rows_on_slate": total_on,
        "match_all_pct": (matched/total) if total else None,
        "match_on_slate_pct": (matched_on/total_on) if total_on else None,
        "mismatches": int(total - matched),
        "mismatches_on_slate": int(total_on - matched_on),
    }

    # Write audit CSV
    out = paths.data_processed / f"roster_audit_{date_str}.csv"
    audit.to_csv(out, index=False)
    return audit, summary
