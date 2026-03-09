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


BASE_DIR = Path(__file__).resolve().parents[1]
_DATA_ROOT_ENV = (os.environ.get("NBA_BETTING_DATA_ROOT") or "").strip()
DATA_ROOT = Path(_DATA_ROOT_ENV).expanduser().resolve() if _DATA_ROOT_ENV else (BASE_DIR / "data")
PROC_DIR = DATA_ROOT / "processed"
RAW_DIR = DATA_ROOT / "raw"

EXCLUDE_STATUSES = {"OUT", "DOUBTFUL", "SUSPENDED", "INACTIVE", "REST"}


def _norm_bool(v: object) -> bool | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in {"true", "1", "yes", "y"}:
        return True
    if s in {"false", "0", "no", "n"}:
        return False
    return None


def _is_season_exclusion(status_series: pd.Series) -> pd.Series:
    st = status_series.astype(str).str.upper().str.strip()
    return (
        (st.str.contains("SEASON", na=False) & st.str.contains("OUT", na=False))
        | st.str.contains("INDEFINITE", na=False)
        | st.str.contains("SEASON-ENDING", na=False)
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit stale injury exclusions for a date.")
    ap.add_argument("--date", type=str, default=None, help="YYYY-MM-DD (default: $NBA_DATE or today UTC)")
    args = ap.parse_args()

    date_str = (args.date or os.environ.get("NBA_DATE") or "").strip()
    if not date_str:
        date_str = datetime.utcnow().date().isoformat()

    props_path = PROC_DIR / f"props_predictions_{date_str}.csv"
    inj_excl_path = PROC_DIR / f"injuries_excluded_{date_str}.csv"
    raw_inj_path = RAW_DIR / "injuries.csv"

    if not props_path.exists():
        raise SystemExit(f"missing {props_path}")

    props = pd.read_csv(props_path)

    # Build allowlist (players explicitly marked playing_today == True)
    allow: dict[str, set[str]] = defaultdict(set)
    if {"team", "player_name"}.issubset(set(props.columns)):
        tmp = props[["team", "player_name"] + (["playing_today"] if "playing_today" in props.columns else [])].copy()
        tmp["team"] = tmp["team"].astype(str).str.strip().str.upper()
        tmp["player_name"] = tmp["player_name"].astype(str).str.strip()
        if "playing_today" in tmp.columns:
            tmp["playing_today_norm"] = tmp["playing_today"].map(_norm_bool)
            tmp = tmp[tmp["playing_today_norm"] == True].copy()  # noqa: E712
        else:
            tmp = tmp.iloc[0:0].copy()
        tmp = tmp[tmp["player_name"].ne("")]
        for _, r in tmp.iterrows():
            tri = str(to_tricode(r.get("team")) or r.get("team") or "").strip().upper()
            k = str(_norm_player_key(r.get("player_name")) or "").strip().upper()
            if tri and k:
                allow[tri].add(k)

    # Build exclusions from processed injuries_excluded + raw injuries latest.
    excl: dict[str, set[str]] = defaultdict(set)

    if inj_excl_path.exists():
        df = pd.read_csv(inj_excl_path)
        tcol = "team_tri" if "team_tri" in df.columns else ("team" if "team" in df.columns else None)
        ncol = "player" if "player" in df.columns else ("player_name" if "player_name" in df.columns else None)
        if tcol and ncol:
            df = df.copy()
            if "status" in df.columns:
                st = df["status"].astype(str).str.upper().str.strip()
                season_out = _is_season_exclusion(df["status"])
                df = df[st.isin(EXCLUDE_STATUSES) | season_out].copy()
            cutoff = pd.to_datetime(date_str, errors="coerce")
            if pd.notna(cutoff) and "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
                df = df[df["date"].notna() & (df["date"] <= cutoff.date())].copy()
                fresh_cutoff = (cutoff - pd.Timedelta(days=30)).date()
                season_out = _is_season_exclusion(df.get("status", pd.Series(dtype=str)))
                df = df[(df["date"] >= fresh_cutoff) | season_out].copy()
                df = df.sort_values(["date"]).groupby([ncol, tcol], as_index=False).tail(1)
            for _, r in df[[c for c in [tcol, ncol] if c in df.columns]].dropna().iterrows():
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
            season_out = _is_season_exclusion(df["status"])

            # Apply the same recency gating used elsewhere in the pipeline:
            # don't treat OUT/DOUBTFUL/etc as actionable if the injury row is stale.
            # This avoids false conflicts when the raw feed isn't updated promptly.
            try:
                if pd.notna(cutoff):
                    days_old = (cutoff - df["date"]).dt.days
                    stale_excl = st.isin(EXCLUDE_STATUSES) & (~season_out) & (days_old > 3)
                    df = df[~stale_excl].copy()
                    st = df["status"].astype(str).str.upper().str.strip()
                    season_out = _is_season_exclusion(df["status"])
            except Exception:
                pass

            df = df[st.isin(EXCLUDE_STATUSES) | season_out].copy()
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
