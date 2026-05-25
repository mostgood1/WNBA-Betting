from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from .config import paths
from .league import LEAGUE, season_year_from_date
from .teams import TEAM_TRICODES


@dataclass(frozen=True)
class TeamGameTotals:
    team: str
    opp: str
    pts_for: float
    pts_against: float
    fgm: float
    fga: float
    tpm: float
    tpa: float
    fta: float
    oreb: float
    dreb: float
    tov: float
    ast: float
    opp_dreb: float
    poss_for: float
    poss_against: float
    pace_game: float


def _season_key_from_game_id(game_id: str) -> str:
    gid = str(game_id or "").strip()
    if len(gid) >= 5 and gid.isdigit():
        return gid[3:5]
    return ""


def _iter_boxscore_files(boxscores_dir: Path) -> Iterable[Path]:
    if not boxscores_dir.exists():
        return []
    return sorted(boxscores_dir.glob("boxscore_*.csv"))


def _coerce_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(0.0)


def _possessions(fga: float, fta: float, oreb: float, tov: float) -> float:
    # Standard possessions approximation (Dean Oliver)
    return float(fga + 0.44 * fta - oreb + tov)


def compute_team_advanced_stats_from_boxscores(
    season: int,
    boxscores_dir: Path | None = None,
    min_games: int = 10,
    as_of: str | date | None = None,
    games_path: Path | None = None,
) -> pd.DataFrame:
    """Compute team pace/ratings/four-factors from cached boxscore CSVs.

    Notes:
    - Uses player-level boxscore totals aggregated to team totals.
    - Prefers date-based season classification so ESPN/WNBA game ids are included.
    - Falls back to NBA-style gameId season keys when no date mapping is available.
    """

    if boxscores_dir is None:
        boxscores_dir = paths.data_processed / "boxscores"

    if games_path is None:
        games_path = paths.data_raw / "games_nba_api.csv"

    season_key = f"{(int(season) - 1) % 100:02d}"

    as_of_ts = None
    try:
        if as_of is not None:
            as_of_ts = pd.to_datetime(as_of).normalize()
    except Exception:
        as_of_ts = None

    gid_to_date: dict[str, pd.Timestamp] = {}
    mapping_paths = [
        games_path,
        paths.data_processed / "boxscores_history.parquet",
        paths.data_processed / "boxscores_history.csv",
    ]
    for mapping_path in mapping_paths:
        try:
            if mapping_path is None or not mapping_path.exists():
                continue
            if mapping_path.suffix.lower() == ".parquet":
                gdf = pd.read_parquet(mapping_path)
            else:
                gdf = pd.read_csv(mapping_path, dtype={"game_id": str, "GAME_ID": str})
            if gdf is None or gdf.empty:
                continue
            game_id_col = "game_id" if "game_id" in gdf.columns else ("GAME_ID" if "GAME_ID" in gdf.columns else None)
            date_col = "date" if "date" in gdf.columns else ("GAME_DATE" if "GAME_DATE" in gdf.columns else None)
            if game_id_col is None or date_col is None:
                continue
            gdf = gdf[[game_id_col, date_col]].copy()
            gdf[game_id_col] = gdf[game_id_col].astype(str).str.strip()
            gdf[date_col] = pd.to_datetime(gdf[date_col], errors="coerce").dt.normalize()
            gdf = gdf.dropna(subset=[date_col])
            for _, rr in gdf.iterrows():
                gid = str(rr.get(game_id_col) or "").strip()
                dt = rr.get(date_col)
                if gid and isinstance(dt, pd.Timestamp) and not pd.isna(dt):
                    gid_to_date[gid] = dt
        except Exception:
            continue

    team_games: list[TeamGameTotals] = []

    for fp in _iter_boxscore_files(boxscores_dir):
        stem = fp.stem
        if not stem.startswith("boxscore_"):
            continue
        game_id = stem.replace("boxscore_", "", 1)
        game_dt = gid_to_date.get(str(game_id))
        if game_dt is not None:
            if season_year_from_date(game_dt.date()) != int(season):
                continue
        elif LEAGUE.code != "nba":
            continue
        elif _season_key_from_game_id(game_id) != season_key:
            continue

        # No-leakage filter: only include games on/before as_of
        if as_of_ts is not None:
            if game_dt is None:
                # If we can't date this game, skip to avoid accidental leakage.
                continue
            if game_dt > as_of_ts:
                continue

        try:
            df = pd.read_csv(fp)
        except Exception:
            continue

        if df is None or df.empty:
            continue

        cols = set(df.columns)
        # Core required stats
        core_required = {
            "teamTricode",
            "points",
            "fieldGoalsMade",
            "fieldGoalsAttempted",
            "threePointersMade",
            "freeThrowsAttempted",
            "reboundsOffensive",
            "reboundsDefensive",
            "turnovers",
        }
        if not core_required.issubset(cols):
            continue

        # Aggregate player stats -> team totals.
        want_cols = list(core_required)
        if "threePointersAttempted" in cols:
            want_cols.append("threePointersAttempted")
        if "assists" in cols:
            want_cols.append("assists")

        tmp = df[[c for c in df.columns if c in want_cols]].copy()
        tmp["teamTricode"] = tmp["teamTricode"].astype(str).str.upper().str.strip()
        if LEAGUE.code != "nba":
            tmp = tmp[tmp["teamTricode"].isin(set(TEAM_TRICODES))].copy()
            if tmp.empty:
                continue
        for c in [
            "points",
            "fieldGoalsMade",
            "fieldGoalsAttempted",
            "threePointersMade",
            "threePointersAttempted",
            "freeThrowsAttempted",
            "reboundsOffensive",
            "reboundsDefensive",
            "turnovers",
            "assists",
        ]:
            if c in tmp.columns:
                tmp[c] = _coerce_num(tmp[c])

        group_cols = [
            "points",
            "fieldGoalsMade",
            "fieldGoalsAttempted",
            "threePointersMade",
            "freeThrowsAttempted",
            "reboundsOffensive",
            "reboundsDefensive",
            "turnovers",
        ]
        if "threePointersAttempted" in tmp.columns:
            group_cols.append("threePointersAttempted")
        if "assists" in tmp.columns:
            group_cols.append("assists")

        g = tmp.groupby("teamTricode", as_index=False)[group_cols].sum()

        if g is None or len(g) != 2:
            continue

        t0 = g.iloc[0]
        t1 = g.iloc[1]
        team0 = str(t0["teamTricode"]).upper().strip()
        team1 = str(t1["teamTricode"]).upper().strip()
        if not team0 or not team1 or team0 == team1:
            continue

        poss0 = _possessions(
            fga=float(t0["fieldGoalsAttempted"]),
            fta=float(t0["freeThrowsAttempted"]),
            oreb=float(t0["reboundsOffensive"]),
            tov=float(t0["turnovers"]),
        )
        poss1 = _possessions(
            fga=float(t1["fieldGoalsAttempted"]),
            fta=float(t1["freeThrowsAttempted"]),
            oreb=float(t1["reboundsOffensive"]),
            tov=float(t1["turnovers"]),
        )
        pace_game = 0.5 * (poss0 + poss1)

        team_games.append(
            TeamGameTotals(
                team=team0,
                opp=team1,
                pts_for=float(t0["points"]),
                pts_against=float(t1["points"]),
                fgm=float(t0["fieldGoalsMade"]),
                fga=float(t0["fieldGoalsAttempted"]),
                tpm=float(t0["threePointersMade"]),
                tpa=float(t0["threePointersAttempted"]) if "threePointersAttempted" in g.columns else float("nan"),
                fta=float(t0["freeThrowsAttempted"]),
                oreb=float(t0["reboundsOffensive"]),
                dreb=float(t0["reboundsDefensive"]),
                tov=float(t0["turnovers"]),
                ast=float(t0["assists"]) if "assists" in g.columns else float("nan"),
                opp_dreb=float(t1["reboundsDefensive"]),
                poss_for=poss0,
                poss_against=poss1,
                pace_game=pace_game,
            )
        )
        team_games.append(
            TeamGameTotals(
                team=team1,
                opp=team0,
                pts_for=float(t1["points"]),
                pts_against=float(t0["points"]),
                fgm=float(t1["fieldGoalsMade"]),
                fga=float(t1["fieldGoalsAttempted"]),
                tpm=float(t1["threePointersMade"]),
                tpa=float(t1["threePointersAttempted"]) if "threePointersAttempted" in g.columns else float("nan"),
                fta=float(t1["freeThrowsAttempted"]),
                oreb=float(t1["reboundsOffensive"]),
                dreb=float(t1["reboundsDefensive"]),
                tov=float(t1["turnovers"]),
                ast=float(t1["assists"]) if "assists" in g.columns else float("nan"),
                opp_dreb=float(t0["reboundsDefensive"]),
                poss_for=poss1,
                poss_against=poss0,
                pace_game=pace_game,
            )
        )

    if not team_games:
        return pd.DataFrame()

    tg = pd.DataFrame([t.__dict__ for t in team_games])

    # Aggregate to team-season totals
    agg = tg.groupby("team", as_index=False).agg(
        games=("team", "size"),
        pts_for=("pts_for", "sum"),
        pts_against=("pts_against", "sum"),
        fgm=("fgm", "sum"),
        fga=("fga", "sum"),
        tpm=("tpm", "sum"),
        tpa=("tpa", lambda s: s.sum(min_count=1)),
        fta=("fta", "sum"),
        oreb=("oreb", "sum"),
        dreb=("dreb", "sum"),
        tov=("tov", "sum"),
        ast=("ast", lambda s: s.sum(min_count=1)),
        opp_dreb=("opp_dreb", "sum"),
        poss_for=("poss_for", "sum"),
        poss_against=("poss_against", "sum"),
        pace=("pace_game", "mean"),
    )

    # Filter low-sample teams (helps avoid preseason/partial data weirdness)
    agg = agg[agg["games"] >= int(min_games)].copy()
    if agg.empty:
        return pd.DataFrame()

    # Ratings & four factors
    eps = 1e-9
    agg["off_rtg"] = 100.0 * agg["pts_for"] / (agg["poss_for"] + eps)
    agg["def_rtg"] = 100.0 * agg["pts_against"] / (agg["poss_against"] + eps)

    agg["efg_pct"] = (agg["fgm"] + 0.5 * agg["tpm"]) / (agg["fga"] + eps)
    agg["tov_pct"] = agg["tov"] / (agg["fga"] + 0.44 * agg["fta"] + agg["tov"] + eps)
    agg["orb_pct"] = agg["oreb"] / (agg["oreb"] + agg["opp_dreb"] + eps)
    agg["ft_rate"] = agg["fta"] / (agg["fga"] + eps)

    agg["fg3a_rate"] = agg["tpa"] / (agg["fga"] + eps)
    agg["fg3_pct"] = agg["tpm"] / (agg["tpa"] + eps)
    agg["ts_pct"] = agg["pts_for"] / (2.0 * (agg["fga"] + 0.44 * agg["fta"]) + eps)
    agg["ast_per_100"] = 100.0 * agg["ast"] / (agg["poss_for"] + eps)

    # Keep output schema consistent with Basketball Reference scraper output
    out = agg[
        [
            "team",
            "pace",
            "off_rtg",
            "def_rtg",
            "efg_pct",
            "tov_pct",
            "orb_pct",
            "ft_rate",
            "fg3a_rate",
            "fg3_pct",
            "ts_pct",
            "ast_per_100",
            "games",
        ]
    ].copy()
    out["source"] = "boxscores"

    # Clean up numeric types
    for c in [
        "pace",
        "off_rtg",
        "def_rtg",
        "efg_pct",
        "tov_pct",
        "orb_pct",
        "ft_rate",
        "fg3a_rate",
        "fg3_pct",
        "ts_pct",
        "ast_per_100",
    ]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out = out.replace([np.inf, -np.inf], np.nan).dropna(subset=["pace", "off_rtg", "def_rtg"])
    out = out.sort_values("team").reset_index(drop=True)
    return out
