"""Build per-player reconciliation: SmartSim player means vs actual boxscore stats.

Output: data/processed/recon_players_<YYYY-MM-DD>.csv

Joins:
- SmartSim: data/processed/smart_sim_<date>_*.json (players.home/players.away)
- Actuals:  data/processed/boxscores/boxscore_<gameId>.csv (personId, points, reboundsTotal, assists, threePointersMade, minutes)

This is intended for historical validation and debugging blanks/mismatches.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[1]
PROC_DIR = BASE_DIR / "data" / "processed"
BOXSCORES_DIR = PROC_DIR / "boxscores"


def _canon_nba_game_id(game_id: Any) -> str:
    try:
        raw = str(game_id or "").strip()
    except Exception:
        return ""
    digits = "".join([c for c in raw if c.isdigit()])
    if len(digits) == 8:
        return "00" + digits
    if len(digits) == 9:
        return "0" + digits
    return digits


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, str) and not x.strip():
            return None
        v = float(x)
        if pd.isna(v):
            return None
        return v
    except Exception:
        return None


def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        if isinstance(x, str) and not x.strip():
            return None
        return int(float(x))
    except Exception:
        return None


def _parse_minutes_to_float(v: Any) -> Optional[float]:
    """Parse NBA minutes strings like '24:49' to decimal minutes."""
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    s = str(v).strip()
    if not s:
        return None
    if ":" not in s:
        return _safe_float(s)
    try:
        mm, ss = s.split(":", 1)
        m = int(mm)
        sec = int(ss)
        return float(m) + float(sec) / 60.0
    except Exception:
        return None


@dataclass
class BoxscoreIndex:
    game_id: str
    df: pd.DataFrame

    @classmethod
    def load(cls, game_id: str) -> Optional["BoxscoreIndex"]:
        # SmartSim often stores game_id without leading zeros (8 digits like 22500762).
        # Cached NBA boxscores are stored as 10-digit IDs like 0022500762.
        gid = _canon_nba_game_id(game_id)
        if not gid:
            return None
        fp = BOXSCORES_DIR / f"boxscore_{gid}.csv"
        if not fp.exists():
            return None
        try:
            df = pd.read_csv(fp)
        except Exception:
            return None
        return cls(game_id=gid, df=df)

    def row_for_player(self, person_id: int) -> Optional[dict[str, Any]]:
        try:
            if "personId" not in self.df.columns:
                return None
            dfp = self.df[self.df["personId"] == person_id]
            if dfp.empty:
                return None
            # Expect 1 row; take first.
            return dfp.iloc[0].to_dict()
        except Exception:
            return None


def build_recon_players(date_str: str) -> pd.DataFrame:
    files = sorted(PROC_DIR.glob(f"smart_sim_{date_str}_*.json"))
    rows: list[dict[str, Any]] = []

    for fp in files:
        try:
            obj = json.loads(fp.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue

        game_id_raw = str(obj.get("game_id") or "").strip()
        game_id = _canon_nba_game_id(game_id_raw)
        home_tri = str(obj.get("home") or "").strip().upper()
        away_tri = str(obj.get("away") or "").strip().upper()
        players = obj.get("players") if isinstance(obj.get("players"), dict) else {}

        bs = BoxscoreIndex.load(game_id)

        for side in ("home", "away"):
            team_tri = home_tri if side == "home" else away_tri
            opp_tri = away_tri if side == "home" else home_tri
            parr = players.get(side) if isinstance(players, dict) else []
            if not isinstance(parr, list):
                continue

            for p in parr:
                if not isinstance(p, dict):
                    continue
                pid = _safe_int(p.get("player_id"))
                pname = str(p.get("player_name") or "").strip()

                sim_min = _safe_float(p.get("min_mean"))
                sim_pts = _safe_float(p.get("pts_mean"))
                sim_reb = _safe_float(p.get("reb_mean"))
                sim_ast = _safe_float(p.get("ast_mean"))
                sim_3pm = _safe_float(p.get("threes_mean"))
                sim_pra = _safe_float(p.get("pra_mean"))
                sim_stl = _safe_float(p.get("stl_mean"))
                sim_blk = _safe_float(p.get("blk_mean"))
                sim_tov = _safe_float(p.get("tov_mean"))

                actual_min = None
                actual_pts = None
                actual_reb = None
                actual_ast = None
                actual_3pm = None
                actual_3pa = None
                actual_fgm = None
                actual_fga = None
                actual_ftm = None
                actual_fta = None
                actual_2pm = None
                actual_2pa = None
                actual_pra = None
                actual_stl = None
                actual_blk = None
                actual_tov = None
                actual_pf = None
                actual_oreb = None
                actual_dreb = None
                actual_pm = None
                missing_actual = True

                if bs is not None and pid is not None:
                    r = bs.row_for_player(pid)
                    if r is not None:
                        missing_actual = False
                        actual_min = _parse_minutes_to_float(r.get("minutes"))
                        actual_pts = _safe_float(r.get("points"))
                        actual_reb = _safe_float(r.get("reboundsTotal"))
                        actual_ast = _safe_float(r.get("assists"))
                        actual_3pm = _safe_float(r.get("threePointersMade"))
                        actual_3pa = _safe_float(r.get("threePointersAttempted"))
                        actual_fgm = _safe_float(r.get("fieldGoalsMade"))
                        actual_fga = _safe_float(r.get("fieldGoalsAttempted"))
                        actual_ftm = _safe_float(r.get("freeThrowsMade"))
                        actual_fta = _safe_float(r.get("freeThrowsAttempted"))
                        actual_stl = _safe_float(r.get("steals"))
                        actual_blk = _safe_float(r.get("blocks"))
                        actual_tov = _safe_float(r.get("turnovers"))
                        actual_pf = _safe_float(r.get("foulsPersonal"))
                        actual_oreb = _safe_float(r.get("reboundsOffensive"))
                        actual_dreb = _safe_float(r.get("reboundsDefensive"))
                        actual_pm = _safe_float(r.get("plusMinusPoints"))

                        if actual_fgm is not None and actual_3pm is not None:
                            actual_2pm = actual_fgm - actual_3pm
                        if actual_fga is not None and actual_3pa is not None:
                            actual_2pa = actual_fga - actual_3pa
                        if actual_pts is not None and actual_reb is not None and actual_ast is not None:
                            actual_pra = actual_pts + actual_reb + actual_ast

                def err(sim: Optional[float], act: Optional[float]) -> Optional[float]:
                    if sim is None or act is None:
                        return None
                    return sim - act

                rows.append(
                    {
                        "date": date_str,
                        "game_id": game_id,
                        "game_id_raw": game_id_raw,
                        "home_tri": home_tri,
                        "away_tri": away_tri,
                        "side": side,
                        "team_tri": team_tri,
                        "opp_tri": opp_tri,
                        "player_id": pid,
                        "player_name": pname,
                        "sim_min": sim_min,
                        "sim_pts": sim_pts,
                        "sim_reb": sim_reb,
                        "sim_ast": sim_ast,
                        "sim_3pm": sim_3pm,
                        "sim_pra": sim_pra,
                        "sim_stl": sim_stl,
                        "sim_blk": sim_blk,
                        "sim_tov": sim_tov,
                        "actual_min": actual_min,
                        "actual_pts": actual_pts,
                        "actual_reb": actual_reb,
                        "actual_ast": actual_ast,
                        "actual_3pm": actual_3pm,
                        "actual_3pa": actual_3pa,
                        "actual_fgm": actual_fgm,
                        "actual_fga": actual_fga,
                        "actual_ftm": actual_ftm,
                        "actual_fta": actual_fta,
                        "actual_2pm": actual_2pm,
                        "actual_2pa": actual_2pa,
                        "actual_stl": actual_stl,
                        "actual_blk": actual_blk,
                        "actual_tov": actual_tov,
                        "actual_pf": actual_pf,
                        "actual_oreb": actual_oreb,
                        "actual_dreb": actual_dreb,
                        "actual_pm": actual_pm,
                        "actual_pra": actual_pra,
                        "err_min": err(sim_min, actual_min),
                        "err_pts": err(sim_pts, actual_pts),
                        "err_reb": err(sim_reb, actual_reb),
                        "err_ast": err(sim_ast, actual_ast),
                        "err_3pm": err(sim_3pm, actual_3pm),
                        "err_pra": err(sim_pra, actual_pra),
                        "err_stl": err(sim_stl, actual_stl),
                        "err_blk": err(sim_blk, actual_blk),
                        "err_tov": err(sim_tov, actual_tov),
                        "abs_err_pts": (abs(err(sim_pts, actual_pts)) if err(sim_pts, actual_pts) is not None else None),
                        "abs_err_pra": (abs(err(sim_pra, actual_pra)) if err(sim_pra, actual_pra) is not None else None),
                        "missing_actual": bool(missing_actual),
                    }
                )

    df = pd.DataFrame(rows)
    # Stable ordering for easy diffs
    if not df.empty:
        df = df.sort_values(["game_id", "team_tri", "sim_min"], ascending=[True, True, False])
    return df


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument(
        "--out",
        default=None,
        help="Output CSV path (default: data/processed/recon_players_<date>.csv)",
    )
    args = ap.parse_args()

    d = str(args.date).strip()
    out = Path(args.out) if args.out else (PROC_DIR / f"recon_players_{d}.csv")
    out.parent.mkdir(parents=True, exist_ok=True)

    df = build_recon_players(d)
    df.to_csv(out, index=False)

    # Basic console summary
    n_rows = int(len(df))
    n_missing = int(df["missing_actual"].sum()) if (not df.empty and "missing_actual" in df.columns) else 0
    n_games = int(df["game_id"].nunique()) if (not df.empty and "game_id" in df.columns) else 0
    print(f"wrote {out}")
    print(f"rows={n_rows} games={n_games} missing_actual_rows={n_missing}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
