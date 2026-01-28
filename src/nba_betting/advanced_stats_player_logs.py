from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from .config import paths


@dataclass(frozen=True)
class TeamGameTotals:
    team: str
    opp: str
    pts_for: float
    pts_against: float
    fgm: float
    fga: float
    tpm: float
    fta: float
    oreb: float
    dreb: float
    tov: float
    opp_dreb: float
    poss_for: float
    poss_against: float
    pace_game: float


def _coerce_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(0.0)


def _possessions(fga: float, fta: float, oreb: float, tov: float) -> float:
    # Standard possessions approximation (Dean Oliver)
    return float(fga + 0.44 * fta - oreb + tov)


def _season_label(season: int) -> str:
    # season=2026 -> 2025-26
    return f"{int(season) - 1}-{str(int(season))[-2:]}"


def compute_team_advanced_stats_from_player_logs(
    season: int,
    player_logs_path: Path | None = None,
    min_games: int = 10,
    as_of: str | date | None = None,
) -> pd.DataFrame:
    """Compute team pace/ratings/four-factors from cached player game logs.

    This is a no-network fallback when per-game boxscore cache files are absent.

    Uses the same output schema as :func:`nba_betting.advanced_stats_boxscores.compute_team_advanced_stats_from_boxscores`.
    """

    if player_logs_path is None:
        player_logs_path = paths.data_processed / "player_logs.csv"

    if not player_logs_path.exists():
        return pd.DataFrame()

    try:
        df = pd.read_csv(player_logs_path, dtype={"GAME_ID": str})
    except Exception:
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    required_cols = {
        "SEASON",
        "TEAM_ABBREVIATION",
        "GAME_ID",
        "GAME_DATE",
        "FGM",
        "FGA",
        "FG3M",
        "FTA",
        "OREB",
        "DREB",
        "TOV",
        "PTS",
    }
    if not required_cols.issubset(set(df.columns)):
        return pd.DataFrame()

    # Season filter
    s_label = _season_label(int(season))
    df["SEASON"] = df["SEASON"].astype(str).str.strip()
    df = df[df["SEASON"] == s_label].copy()
    if df.empty:
        return pd.DataFrame()

    # No-leakage filter
    as_of_ts = None
    try:
        if as_of is not None:
            as_of_ts = pd.to_datetime(as_of).normalize()
    except Exception:
        as_of_ts = None

    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce").dt.normalize()
    df = df.dropna(subset=["GAME_DATE"])
    if as_of_ts is not None:
        df = df[df["GAME_DATE"] <= as_of_ts].copy()

    if df.empty:
        return pd.DataFrame()

    # Aggregate player rows -> team totals per game.
    tmp = df[
        [
            "GAME_ID",
            "TEAM_ABBREVIATION",
            "PTS",
            "FGM",
            "FGA",
            "FG3M",
            "FTA",
            "OREB",
            "DREB",
            "TOV",
        ]
    ].copy()

    tmp["GAME_ID"] = tmp["GAME_ID"].astype(str).str.strip()
    tmp["TEAM_ABBREVIATION"] = tmp["TEAM_ABBREVIATION"].astype(str).str.upper().str.strip()

    for c in ["PTS", "FGM", "FGA", "FG3M", "FTA", "OREB", "DREB", "TOV"]:
        tmp[c] = _coerce_num(tmp[c])

    g = (
        tmp.groupby(["GAME_ID", "TEAM_ABBREVIATION"], as_index=False)
        .agg(
            pts_for=("PTS", "sum"),
            fgm=("FGM", "sum"),
            fga=("FGA", "sum"),
            tpm=("FG3M", "sum"),
            fta=("FTA", "sum"),
            oreb=("OREB", "sum"),
            dreb=("DREB", "sum"),
            tov=("TOV", "sum"),
        )
        .rename(columns={"TEAM_ABBREVIATION": "team", "GAME_ID": "game_id"})
    )

    if g.empty:
        return pd.DataFrame()

    # Self-join to attach opponent totals.
    opp = g.rename(
        columns={
            "team": "opp",
            "pts_for": "pts_against",
            "fgm": "opp_fgm",
            "fga": "opp_fga",
            "tpm": "opp_tpm",
            "fta": "opp_fta",
            "oreb": "opp_oreb",
            "dreb": "opp_dreb",
            "tov": "opp_tov",
        }
    )

    m = g.merge(opp, on="game_id", how="inner")
    m = m[m["team"] != m["opp"]].copy()

    if m.empty:
        return pd.DataFrame()

    # Ensure exactly one opponent per team/game.
    m = m.drop_duplicates(subset=["game_id", "team"]).copy()

    team_games: list[TeamGameTotals] = []
    for _, r in m.iterrows():
        poss_for = _possessions(float(r["fga"]), float(r["fta"]), float(r["oreb"]), float(r["tov"]))
        poss_against = _possessions(float(r["opp_fga"]), float(r["opp_fta"]), float(r["opp_oreb"]), float(r["opp_tov"]))
        pace_game = 0.5 * (poss_for + poss_against)

        team_games.append(
            TeamGameTotals(
                team=str(r["team"]),
                opp=str(r["opp"]),
                pts_for=float(r["pts_for"]),
                pts_against=float(r["pts_against"]),
                fgm=float(r["fgm"]),
                fga=float(r["fga"]),
                tpm=float(r["tpm"]),
                fta=float(r["fta"]),
                oreb=float(r["oreb"]),
                dreb=float(r["dreb"]),
                tov=float(r["tov"]),
                opp_dreb=float(r["opp_dreb"]),
                poss_for=poss_for,
                poss_against=poss_against,
                pace_game=pace_game,
            )
        )

    if not team_games:
        return pd.DataFrame()

    tg = pd.DataFrame([t.__dict__ for t in team_games])

    agg = tg.groupby("team", as_index=False).agg(
        games=("team", "size"),
        pts_for=("pts_for", "sum"),
        pts_against=("pts_against", "sum"),
        fgm=("fgm", "sum"),
        fga=("fga", "sum"),
        tpm=("tpm", "sum"),
        fta=("fta", "sum"),
        oreb=("oreb", "sum"),
        dreb=("dreb", "sum"),
        tov=("tov", "sum"),
        opp_dreb=("opp_dreb", "sum"),
        poss_for=("poss_for", "sum"),
        poss_against=("poss_against", "sum"),
        pace=("pace_game", "mean"),
    )

    agg = agg[agg["games"] >= int(min_games)].copy()
    if agg.empty:
        return pd.DataFrame()

    eps = 1e-9
    agg["off_rtg"] = 100.0 * agg["pts_for"] / (agg["poss_for"] + eps)
    agg["def_rtg"] = 100.0 * agg["pts_against"] / (agg["poss_against"] + eps)

    agg["efg_pct"] = (agg["fgm"] + 0.5 * agg["tpm"]) / (agg["fga"] + eps)
    agg["tov_pct"] = agg["tov"] / (agg["fga"] + 0.44 * agg["fta"] + agg["tov"] + eps)
    agg["orb_pct"] = agg["oreb"] / (agg["oreb"] + agg["opp_dreb"] + eps)
    agg["ft_rate"] = agg["fta"] / (agg["fga"] + eps)

    out = agg[["team", "pace", "off_rtg", "def_rtg", "efg_pct", "tov_pct", "orb_pct", "ft_rate", "games"]].copy()
    out["source"] = "player_logs"

    for c in ["pace", "off_rtg", "def_rtg", "efg_pct", "tov_pct", "orb_pct", "ft_rate"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out = out.replace([np.inf, -np.inf], np.nan).dropna(subset=["pace", "off_rtg", "def_rtg"])
    out = out.sort_values("team").reset_index(drop=True)
    return out
