from __future__ import annotations

from collections import defaultdict
import argparse
import os
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from nba_betting.player_priors import _norm_player_key  # type: ignore
from nba_betting.teams import to_tricode


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit stale injury exclusions for a date.")
    ap.add_argument("--date", type=str, default=None, help="YYYY-MM-DD (default: $NBA_DATE or today UTC)")
    args = ap.parse_args()

    date_str = (args.date or os.environ.get("NBA_DATE") or "").strip()
    if not date_str:
        date_str = datetime.utcnow().date().isoformat()

    props_path = Path(f"data/processed/props_predictions_{date_str}.csv")
    inj_excl_path = Path(f"data/processed/injuries_excluded_{date_str}.csv")
    raw_inj_path = Path("data/raw/injuries.csv")

    if not props_path.exists():
        raise SystemExit(f"missing {props_path}")

    props = pd.read_csv(props_path)

    # Build allowlist (players marked playing_today)
    allow: dict[str, set[str]] = defaultdict(set)
    if {"team", "player_name"}.issubset(set(props.columns)):
        tmp = props[["team", "player_name"] + (["playing_today"] if "playing_today" in props.columns else [])].copy()
        tmp["team"] = tmp["team"].astype(str).str.strip().str.upper()
        tmp["player_name"] = tmp["player_name"].astype(str).str.strip()
        if "playing_today" in tmp.columns:
            pt = tmp["playing_today"].astype(str).str.lower().str.strip()
            tmp = tmp[~pt.isin(["false", "0", "no", "n"])].copy()
        tmp = tmp[tmp["player_name"].ne("")]
        for _, r in tmp.iterrows():
            tri = str(to_tricode(r.get("team")) or r.get("team") or "").strip().upper()
            k = str(_norm_player_key(r.get("player_name")) or "").strip().upper()
            if tri and k:
                allow[tri].add(k)

    # Build exclusions from processed injuries_excluded + raw injuries latest (no recency window), to detect staleness.
    excl: dict[str, set[str]] = defaultdict(set)

    if inj_excl_path.exists():
        df = pd.read_csv(inj_excl_path)
        tcol = "team_tri" if "team_tri" in df.columns else ("team" if "team" in df.columns else None)
        ncol = "player" if "player" in df.columns else ("player_name" if "player_name" in df.columns else None)
        if tcol and ncol:
            for _, r in df[[tcol, ncol]].dropna().iterrows():
                tri = str(to_tricode(r.get(tcol)) or r.get(tcol) or "").strip().upper()
                k = str(_norm_player_key(r.get(ncol)) or "").strip().upper()
                if tri and k:
                    excl[tri].add(k)

    if raw_inj_path.exists():
        df = pd.read_csv(raw_inj_path)
        if {"team", "player", "status", "date"}.issubset(set(df.columns)):
            df = df.copy()
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df[df["date"].notna()].copy()
            cutoff = pd.to_datetime(date_str, errors="coerce")
            if pd.notna(cutoff):
                df = df[df["date"] <= cutoff].copy()
            df = df.sort_values(["date"]).groupby(["player", "team"], as_index=False).tail(1)
            st = df["status"].astype(str).str.upper().str.strip()
            EXCL = {"OUT", "DOUBTFUL", "SUSPENDED", "INACTIVE", "REST"}
            season_out = (st.str.contains("SEASON", na=False) & st.str.contains("OUT", na=False)) | st.str.contains("INDEFINITE", na=False) | st.str.contains("SEASON-ENDING", na=False)

            # Apply the same recency gating used elsewhere in the pipeline:
            # don't treat OUT/DOUBTFUL/etc as actionable if the injury row is stale.
            # This avoids false conflicts when the raw feed isn't updated promptly.
            try:
                if pd.notna(cutoff):
                    days_old = (cutoff - df["date"]).dt.days
                    stale_excl = st.isin(EXCL) & (~season_out) & (days_old > 3)
                    df = df[~stale_excl].copy()
                    st = df["status"].astype(str).str.upper().str.strip()
                    season_out = (
                        (st.str.contains("SEASON", na=False) & st.str.contains("OUT", na=False))
                        | st.str.contains("INDEFINITE", na=False)
                        | st.str.contains("SEASON-ENDING", na=False)
                    )
            except Exception:
                pass

            df = df[st.isin(EXCL) | season_out].copy()
            for _, r in df.iterrows():
                tri = str(to_tricode(r.get("team")) or r.get("team") or "").strip().upper()
                k = str(_norm_player_key(r.get("player")) or "").strip().upper()
                if tri and k:
                    excl[tri].add(k)

    # Find conflicts: excluded but playing_today
    conflicts = []
    for tri, keys in excl.items():
        bad = sorted(list(keys.intersection(allow.get(tri, set()))))
        if bad:
            conflicts.append((tri, len(bad)))

    conflicts.sort(key=lambda x: (-x[1], x[0]))
    out = {"date": date_str, "teams_with_conflicts": len(conflicts), "conflicts": conflicts[:15]}
    print(json.dumps(out, indent=2))

    # Non-zero exit so VS Code tasks fail loudly.
    if conflicts:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
