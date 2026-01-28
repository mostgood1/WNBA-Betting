from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PROC = ROOT / "data" / "processed"
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

try:
    from nba_betting.teams import to_tricode as _to_tri  # type: ignore
except Exception:
    _to_tri = None  # type: ignore


def _season_for_date(date_str: str) -> str:
    d = pd.to_datetime(date_str, errors="coerce")
    if d is None or pd.isna(d):
        # default: current year season
        from datetime import date as _date

        today = _date.today()
        start_year = today.year if today.month >= 7 else today.year - 1
    else:
        start_year = int(d.year) if int(d.month) >= 7 else int(d.year) - 1
    return f"{start_year}-{str(start_year + 1)[-2:]}"


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


def _load_roster_map(date_str: str) -> dict[str, str]:
    season = _season_for_date(date_str)
    cand = PROC / f"rosters_{season}.csv"
    roster_file = cand if cand.exists() else None
    if roster_file is None:
        files = list(PROC.glob("rosters_*.csv"))
        season_files = [f for f in files if "-" in f.stem]
        if season_files:
            season_files.sort(key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)
            roster_file = season_files[0]
        elif files:
            files.sort(key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)
            roster_file = files[0]

    if roster_file is None or not roster_file.exists():
        return {}

    df = pd.read_csv(roster_file)
    if df is None or df.empty:
        return {}

    cols = {c.upper(): c for c in df.columns}
    name_col = cols.get("PLAYER")
    tri_col = cols.get("TEAM_ABBREVIATION")
    if not name_col or not tri_col:
        return {}

    out: dict[str, str] = {}
    for _, r in df[[name_col, tri_col]].dropna().iterrows():
        try:
            pk = _norm_key(str(r.get(name_col) or ""))
            raw = str(r.get(tri_col) or "").strip().upper()
            tri = (str(_to_tri(raw) or raw).strip().upper()) if _to_tri else raw
            if pk and tri:
                out[pk] = tri
        except Exception:
            continue
    return out


def repair_injuries_excluded(date_str: str) -> tuple[Path, int, int]:
    p = PROC / f"injuries_excluded_{date_str}.csv"
    if not p.exists():
        raise FileNotFoundError(p)

    df = pd.read_csv(p)
    if df is None or df.empty or "player" not in df.columns:
        return p, 0, 0

    roster_map = _load_roster_map(date_str)
    if not roster_map:
        return p, 0, 0

    df = df.copy()
    if "team_tri" not in df.columns:
        df["team_tri"] = ""

    before_bad = 0
    fixed = 0
    for i, r in df.iterrows():
        try:
            pk = _norm_key(str(r.get("player") or ""))
            corr = roster_map.get(pk)
            if not corr:
                continue
            tri = str(r.get("team_tri") or "").strip().upper()
            if tri and tri != corr:
                before_bad += 1
            if (not tri) or (tri != corr):
                df.at[i, "team_tri"] = corr
                if "team" in df.columns:
                    df.at[i, "team"] = corr
                fixed += 1
        except Exception:
            continue

    df.to_csv(p, index=False)
    return p, before_bad, fixed


def main() -> None:
    ap = argparse.ArgumentParser(description="Repair injuries_excluded_<date>.csv team assignments using processed rosters")
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    out, before_bad, fixed = repair_injuries_excluded(args.date)
    print(f"OK: {out} bad_team_rows_before={before_bad} rows_updated={fixed}")


if __name__ == "__main__":
    main()
