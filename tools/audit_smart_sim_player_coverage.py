from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

import pandas as pd


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


def _load_expected_players(props_path: Path, team_tri: str, opp_tri: str | None) -> set[str]:
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

    return set(tmp["player_name"].astype(str).tolist())


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


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit SmartSim JSON player coverage vs props_predictions pool.")
    ap.add_argument("--date", type=str, required=True, help="YYYY-MM-DD")
    ap.add_argument("--max-games", type=int, default=50)
    args = ap.parse_args()

    ds = args.date.strip()
    processed = Path("data/processed")
    props_path = processed / f"props_predictions_{ds}.csv"
    if not props_path.exists():
        raise SystemExit(f"missing {props_path}")

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
        except Exception as e:
            findings.append({"file": str(fp), "error": repr(e)})
            continue

        exp_home = _load_expected_players(props_path, team_tri=home_tri, opp_tri=away_tri)
        exp_away = _load_expected_players(props_path, team_tri=away_tri, opp_tri=home_tri)

        miss_home = sorted(list(exp_home - home_names))
        miss_away = sorted(list(exp_away - away_names))

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
