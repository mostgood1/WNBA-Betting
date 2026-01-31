from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PROC = ROOT / "data" / "processed"


EXCLUDE_STATUSES = {"OUT", "DOUBTFUL", "SUSPENDED", "INACTIVE", "REST"}


def _norm_name_key(name: Any) -> str:
    s = str(name or "").strip().lower()
    if not s:
        return ""
    try:
        import unicodedata as _ud

        s = _ud.normalize("NFKD", s)
        s = s.encode("ascii", "ignore").decode("ascii")
    except Exception:
        pass
    import re

    s = s.replace("-", " ")
    s = re.sub(r"[^a-z0-9\s]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    toks = [t for t in s.split(" ") if t and t not in {"jr", "sr", "ii", "iii", "iv", "v"}]
    return " ".join(toks)


def _norm_bool(v: Any) -> Optional[bool]:
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


@dataclass
class DateReport:
    date: str
    injuries_counts_path: str
    league_status_path: Optional[str]
    props_predictions_path: Optional[str]
    injuries_n: int
    conflicts_n: int
    conflicts: list[dict[str, Any]]


def _load_injuries_counts(date_str: str) -> pd.DataFrame:
    fp = PROC / f"injuries_counts_{date_str}.json"
    if not fp.exists():
        return pd.DataFrame()
    with open(fp, "r", encoding="utf-8") as f:
        obj = json.load(f)
    players = (obj or {}).get("players") or []
    df = pd.DataFrame(players)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["team"] = df.get("team", "").astype(str).str.upper().str.strip()
    df["status"] = df.get("status", "").astype(str).str.upper().str.strip()
    df["player"] = df.get("player", "").astype(str).str.strip()
    df["_pkey"] = df["player"].map(_norm_name_key)
    # Only consider the strong exclude statuses (matches snapshot_injuries intent)
    df = df[df["status"].isin(EXCLUDE_STATUSES)].copy()
    return df


def _load_league_status(date_str: str) -> tuple[Optional[pd.DataFrame], Optional[str]]:
    fp = PROC / f"league_status_{date_str}.csv"
    if not fp.exists():
        return None, None
    df = pd.read_csv(fp)
    if df is None or df.empty:
        return df, str(fp)
    df = df.copy()
    df["team"] = df.get("team", "").astype(str).str.upper().str.strip()
    df["player_name"] = df.get("player_name", "").astype(str).str.strip()
    df["injury_status"] = df.get("injury_status", "").astype(str).str.upper().str.strip()
    if "playing_today" in df.columns:
        df["playing_today_norm"] = df["playing_today"].map(_norm_bool)
    else:
        df["playing_today_norm"] = None
    df["_pkey"] = df["player_name"].map(_norm_name_key)
    df = df[df["_pkey"].astype(str).str.len() > 0].copy()
    return df, str(fp)


def _load_props_predictions(date_str: str) -> tuple[Optional[pd.DataFrame], Optional[str]]:
    fp = PROC / f"props_predictions_{date_str}.csv"
    if not fp.exists():
        return None, None
    df = pd.read_csv(fp)
    if df is None or df.empty:
        return df, str(fp)
    df = df.copy()
    df["team"] = df.get("team", "").astype(str).str.upper().str.strip()
    df["player_name"] = df.get("player_name", "").astype(str).str.strip()
    df["injury_status"] = df.get("injury_status", "").astype(str).str.upper().str.strip()
    if "playing_today" in df.columns:
        df["playing_today_norm"] = df["playing_today"].map(_norm_bool)
    else:
        df["playing_today_norm"] = None
    df["_pkey"] = df["player_name"].map(_norm_name_key)
    df = df[df["_pkey"].astype(str).str.len() > 0].copy()
    return df, str(fp)


def audit_date(date_str: str) -> DateReport:
    inj = _load_injuries_counts(date_str)
    ls, ls_path = _load_league_status(date_str)
    pp, pp_path = _load_props_predictions(date_str)

    conflicts: list[dict[str, Any]] = []

    # Compare injuries_counts OUT list vs league_status/props playing_today
    if not inj.empty:
        if ls is not None and (not ls.empty):
            # Join by (team, normalized name) with fallback to name-only.
            merged = inj.merge(
                ls[["team", "_pkey", "playing_today_norm", "injury_status"]],
                on=["team", "_pkey"],
                how="left",
                suffixes=("", "_ls"),
            )
            missing = merged["playing_today_norm"].isna()
            if bool(missing.any()):
                # name-only fallback
                merged2 = merged.loc[missing].drop(columns=["playing_today_norm", "injury_status"], errors="ignore").merge(
                    ls[["_pkey", "playing_today_norm", "injury_status"]].drop_duplicates(subset=["_pkey"], keep="last"),
                    on=["_pkey"],
                    how="left",
                    suffixes=("", "_ls_byname"),
                )
                merged.loc[missing, "playing_today_norm"] = merged2["playing_today_norm"].values
                merged.loc[missing, "injury_status"] = merged2["injury_status"].values

            bad = merged[merged["playing_today_norm"] == True].copy()  # noqa: E712
            for _, r in bad.iterrows():
                conflicts.append(
                    {
                        "date": date_str,
                        "source": "injuries_counts",
                        "player": str(r.get("player") or ""),
                        "team": str(r.get("team") or ""),
                        "injuries_status": str(r.get("status") or ""),
                        "league_status_injury_status": str(r.get("injury_status") or ""),
                        "league_status_playing_today": True,
                        "reason": "injuries_counts_marks_out_but_league_status_playing_today_true",
                    }
                )

        if pp is not None and (not pp.empty):
            merged = inj.merge(
                pp[["team", "_pkey", "playing_today_norm", "injury_status"]],
                on=["team", "_pkey"],
                how="left",
                suffixes=("", "_pp"),
            )
            missing = merged["playing_today_norm"].isna()
            if bool(missing.any()):
                merged2 = merged.loc[missing].drop(columns=["playing_today_norm", "injury_status"], errors="ignore").merge(
                    pp[["_pkey", "playing_today_norm", "injury_status"]].drop_duplicates(subset=["_pkey"], keep="last"),
                    on=["_pkey"],
                    how="left",
                    suffixes=("", "_pp_byname"),
                )
                merged.loc[missing, "playing_today_norm"] = merged2["playing_today_norm"].values
                merged.loc[missing, "injury_status"] = merged2["injury_status"].values

            bad = merged[merged["playing_today_norm"] == True].copy()  # noqa: E712
            for _, r in bad.iterrows():
                conflicts.append(
                    {
                        "date": date_str,
                        "source": "injuries_counts",
                        "player": str(r.get("player") or ""),
                        "team": str(r.get("team") or ""),
                        "injuries_status": str(r.get("status") or ""),
                        "props_injury_status": str(r.get("injury_status") or ""),
                        "props_playing_today": True,
                        "reason": "injuries_counts_marks_out_but_props_predictions_playing_today_true",
                    }
                )

    return DateReport(
        date=date_str,
        injuries_counts_path=str(PROC / f"injuries_counts_{date_str}.json"),
        league_status_path=ls_path,
        props_predictions_path=pp_path,
        injuries_n=int(len(inj)) if inj is not None else 0,
        conflicts_n=len(conflicts),
        conflicts=conflicts,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit injuries_counts vs league_status/props_predictions consistency")
    ap.add_argument("--date", help="YYYY-MM-DD")
    ap.add_argument("--start", help="YYYY-MM-DD")
    ap.add_argument("--end", help="YYYY-MM-DD")
    args = ap.parse_args()

    if not args.date and not (args.start and args.end):
        ap.error("Provide --date or --start/--end")

    dates: list[str] = []
    if args.date:
        dates = [args.date]
    else:
        start = pd.to_datetime(args.start).date()
        end = pd.to_datetime(args.end).date()
        d = start
        while d <= end:
            dates.append(str(d))
            d = (pd.Timestamp(d) + pd.Timedelta(days=1)).date()

    reports: list[dict[str, Any]] = []
    conflicts_total = 0

    for ds in dates:
        fp = PROC / f"injuries_counts_{ds}.json"
        if not fp.exists():
            continue
        rep = audit_date(ds)
        reports.append(
            {
                "date": rep.date,
                "injuries_n": rep.injuries_n,
                "conflicts_n": rep.conflicts_n,
                "injuries_counts_path": rep.injuries_counts_path,
                "league_status_path": rep.league_status_path,
                "props_predictions_path": rep.props_predictions_path,
                "conflicts": rep.conflicts,
            }
        )
        conflicts_total += rep.conflicts_n

    out = {
        "dates_checked": len(reports),
        "conflicts_total": conflicts_total,
        "reports": reports,
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))

    # Fail loudly if we found contradictions.
    return 0 if conflicts_total == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
