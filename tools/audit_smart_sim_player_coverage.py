from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.nba_betting.teams import to_tricode


def _norm_player_key(name) -> str:
    text = str(name or "").strip().upper()
    if not text:
        return ""
    for token in (".", ",", "'", "-"):
        text = text.replace(token, " ")
    parts = [part for part in text.split() if part]
    drop = {"JR", "SR", "II", "III", "IV", "V"}
    parts = [part for part in parts if part not in drop]
    return " ".join(parts)


def _norm_bool_str(x) -> str:
    try:
        s = str(x).strip().lower()
    except Exception:
        return ""
    if s in {"true", "1", "yes", "y"}:
        return "true"
    if s in {"false", "0", "no", "n"}:
        return "false"
    return s


def _load_expected_players(
    props_path: Path,
    team_tri: str,
    opp_tri: str | None,
    allowed_names: set[str] | None = None,
) -> set[str]:
    df = pd.read_csv(props_path)
    if df is None or df.empty:
        return set()
    if "team" not in df.columns or "player_name" not in df.columns:
        return set()

    tmp = df.copy()
    tmp["team"] = tmp["team"].astype(str).str.upper().str.strip()
    tmp["player_name"] = tmp["player_name"].astype(str).str.strip()
    tmp = tmp[tmp["player_name"].ne("")].copy()

    t = str(team_tri).strip().upper()
    tmp = tmp[tmp["team"] == t].copy()

    # prefer matchup-specific filter if opponent is present (but don’t require it)
    if opp_tri and "opponent" in tmp.columns:
        try:
            tmp["opponent"] = tmp["opponent"].astype(str).str.upper().str.strip()
            mm = tmp[tmp["opponent"] == str(opp_tri).strip().upper()]
            if len(mm) >= 8:
                tmp = mm
        except Exception:
            pass

    if "playing_today" in tmp.columns:
        pt = tmp["playing_today"].map(_norm_bool_str)
        tmp = tmp[~pt.eq("false")].copy()

    if "team_on_slate" in tmp.columns:
        try:
            tos = tmp["team_on_slate"].map(_norm_bool_str)
            # if column is present, require not false
            tmp = tmp[~tos.eq("false")].copy()
        except Exception:
            pass

    if allowed_names is not None:
        allowed = {str(name).strip() for name in allowed_names if str(name).strip()}
        if allowed:
            tmp = tmp[tmp["player_name"].isin(allowed)].copy()

    return set(tmp["player_name"].astype(str).tolist())


def _load_market_players_by_matchup(processed: Path, ds: str) -> dict[tuple[str, str], set[str]]:
    candidates = [
        processed.parent / "raw" / f"odds_nba_player_props_{ds}.csv",
        processed / f"oddsapi_player_props_{ds}.csv",
    ]
    snapshot_path = next((path for path in candidates if path.exists()), None)
    if snapshot_path is None:
        return {}

    try:
        odds = pd.read_csv(snapshot_path)
    except Exception:
        return {}
    if odds is None or odds.empty:
        return {}
    required = {"home_team", "away_team", "player_name"}
    if not required.issubset(set(odds.columns)):
        return {}

    tmp = odds.copy()
    tmp["home_tri"] = tmp["home_team"].astype(str).map(lambda value: (to_tricode(str(value or "")) or str(value or "").strip().upper()))
    tmp["away_tri"] = tmp["away_team"].astype(str).map(lambda value: (to_tricode(str(value or "")) or str(value or "").strip().upper()))
    tmp["player_name"] = tmp["player_name"].astype(str).str.strip()
    tmp = tmp[
        tmp["home_tri"].astype(str).str.len().gt(0)
        & tmp["away_tri"].astype(str).str.len().gt(0)
        & tmp["player_name"].ne("")
    ].copy()
    if tmp.empty:
        return {}

    market_players: dict[tuple[str, str], set[str]] = {}
    for row in tmp[["home_tri", "away_tri", "player_name"]].drop_duplicates().itertuples(index=False):
        key = (str(row.home_tri).upper().strip(), str(row.away_tri).upper().strip())
        market_players.setdefault(key, set()).add(str(row.player_name).strip())
    return market_players


def _load_smartsim_names(smartsim_path: Path) -> tuple[set[str], set[str]]:
    obj = json.loads(smartsim_path.read_text(encoding="utf-8"))
    players = obj.get("players") or {}
    home = players.get("home") or []
    away = players.get("away") or []

    def _names(arr):
        out = set()
        for r in arr:
            if isinstance(r, dict) and r.get("player_name"):
                out.add(str(r.get("player_name")).strip())
        return out

    return _names(home), _names(away)


def _load_smartsim_excluded_keys(smartsim_path: Path) -> dict[str, set[str]]:
    obj = json.loads(smartsim_path.read_text(encoding="utf-8"))
    excluded = ((obj.get("context") or {}).get("excluded_players") or {})
    return {
        str(team).strip().upper(): {
            key
            for key in (_norm_player_key(name) for name in (names or []))
            if key
        }
        for team, names in excluded.items()
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit SmartSim JSON player coverage vs props_predictions pool.")
    ap.add_argument("--date", type=str, required=True, help="YYYY-MM-DD")
    ap.add_argument("--max-games", type=int, default=50)
    args = ap.parse_args()

    ds = args.date.strip()
    repo_root = Path(__file__).resolve().parent.parent
    _DATA_ROOT = os.environ.get("NBA_BETTING_DATA_ROOT")
    data_root = Path(_DATA_ROOT).expanduser() if _DATA_ROOT else (repo_root / "data")
    processed = data_root / "processed"
    props_path = processed / f"props_predictions_{ds}.csv"
    if not props_path.exists():
        raise SystemExit(f"missing {props_path}")
    market_players_by_matchup = _load_market_players_by_matchup(processed, ds)

    files = sorted(processed.glob(f"smart_sim_{ds}_*.json"))
    files = files[: int(args.max_games)]

    findings: list[dict] = []
    for fp in files:
        stem = fp.stem
        parts = stem.split("_")
        # smart_sim_<date>_<HOME>_<AWAY>
        if len(parts) < 5:
            continue
        home_tri = parts[-2].strip().upper()
        away_tri = parts[-1].strip().upper()

        try:
            home_names, away_names = _load_smartsim_names(fp)
            excluded_keys = _load_smartsim_excluded_keys(fp)
        except Exception as e:
            findings.append({"file": str(fp), "error": repr(e)})
            continue

        market_names = market_players_by_matchup.get((home_tri, away_tri))
        exp_home = _load_expected_players(props_path, team_tri=home_tri, opp_tri=away_tri, allowed_names=market_names)
        exp_away = _load_expected_players(props_path, team_tri=away_tri, opp_tri=home_tri, allowed_names=market_names)

        home_name_keys = {_norm_player_key(name) for name in home_names}
        away_name_keys = {_norm_player_key(name) for name in away_names}
        home_excluded_keys = excluded_keys.get(home_tri, set())
        away_excluded_keys = excluded_keys.get(away_tri, set())

        miss_home = sorted(
            name
            for name in exp_home
            if (key := _norm_player_key(name))
            and key not in home_name_keys
            and key not in home_excluded_keys
        )
        miss_away = sorted(
            name
            for name in exp_away
            if (key := _norm_player_key(name))
            and key not in away_name_keys
            and key not in away_excluded_keys
        )

        if miss_home or miss_away:
            findings.append(
                {
                    "file": str(fp),
                    "home": home_tri,
                    "away": away_tri,
                    "expected_home_n": len(exp_home),
                    "expected_away_n": len(exp_away),
                    "smartsim_home_n": len(home_names),
                    "smartsim_away_n": len(away_names),
                    "missing_home": miss_home[:25],
                    "missing_away": miss_away[:25],
                }
            )

    out = {
        "date": ds,
        "props_predictions": str(props_path),
        "smartsim_files": len(files),
        "issues": findings,
        "issues_n": len(findings),
        "ran_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    print(json.dumps(out, indent=2))

    # Non-zero exit so CI/tasks can fail loudly.
    if findings:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
