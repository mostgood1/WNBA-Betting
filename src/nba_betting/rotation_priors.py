from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd

from .config import paths
from .league import LEAGUE


PERIOD_SECONDS_DEFAULT = LEAGUE.regulation_period_seconds


def _optional_text(value: Any) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _required_text(value: Any) -> str:
    text = _optional_text(value)
    return text or ""


@dataclass(frozen=True)
class FirstBenchSubIn:
    game_id: str
    team: str
    enter_player_id: Optional[str]
    enter_player_name: Optional[str]
    exit_player_id: Optional[str]
    exit_player_name: Optional[str]
    period: int
    clock: str
    elapsed_sec: Optional[int]


def _read_pbp_espn_history() -> pd.DataFrame:
    parquet = paths.data_processed / "pbp_espn_history.parquet"
    csv = paths.data_processed / "pbp_espn_history.csv"

    if parquet.exists():
        try:
            return pd.read_parquet(parquet)
        except Exception:
            pass

    if csv.exists():
        try:
            return pd.read_csv(
                csv,
                dtype={
                    "date": "string",
                    "game_id": "string",
                    "event_id": "string",
                    "play_id": "string",
                    "team": "string",
                    "type": "string",
                    "clock": "string",
                    "enter_player_id": "string",
                    "exit_player_id": "string",
                    "enter_player_name": "string",
                    "exit_player_name": "string",
                },
            )
        except Exception:
            pass

    return pd.DataFrame()


def compute_first_bench_sub_in_for_team_game(
    pbp_game: pd.DataFrame,
    team_tricode: str,
    period: int = 1,
    period_seconds: int = PERIOD_SECONDS_DEFAULT,
) -> Optional[FirstBenchSubIn]:
    """Compute the first substitution where a player enters for `team_tricode` in a game."""
    if pbp_game is None or pbp_game.empty:
        return None

    df = pbp_game.copy()

    for c in ["type", "team", "period"]:
        if c not in df.columns:
            return None

    df = df[df["type"].astype(str).str.lower() == "substitution"]
    df = df[df["team"].astype(str).str.upper() == team_tricode.upper()]

    # ESPN uses numeric period numbers (1..4, 5+ OT)
    try:
        df = df[df["period"].astype(int) == int(period)]
    except Exception:
        return None

    if "clock_sec_remaining" not in df.columns:
        return None

    df["clock_sec_remaining"] = pd.to_numeric(df["clock_sec_remaining"], errors="coerce")
    df = df.dropna(subset=["clock_sec_remaining"]).copy()
    if df.empty:
        return None

    df["elapsed_sec"] = (period_seconds - df["clock_sec_remaining"]).clip(lower=0).astype(int)

    # First substitution in period => minimal elapsed seconds (closest to tip-off)
    sort_cols = ["elapsed_sec"]
    if "sequence" in df.columns:
        sort_cols.append("sequence")

    df = df.sort_values(sort_cols, ascending=True, kind="stable")
    row = df.iloc[0].to_dict()

    game_id = _required_text(row.get("game_id"))
    return FirstBenchSubIn(
        game_id=game_id,
        team=team_tricode.upper(),
        enter_player_id=_optional_text(row.get("enter_player_id")),
        enter_player_name=_optional_text(row.get("enter_player_name")),
        exit_player_id=_optional_text(row.get("exit_player_id")),
        exit_player_name=_optional_text(row.get("exit_player_name")),
        period=int(period),
        clock=_required_text(row.get("clock")),
        elapsed_sec=int(row.get("elapsed_sec")) if row.get("elapsed_sec") is not None else None,
    )


def compute_first_bench_sub_in_dataset(
    pbp: pd.DataFrame,
    period: int = 1,
    period_seconds: int = PERIOD_SECONDS_DEFAULT,
) -> pd.DataFrame:
    """Return a per-(date, game_id, team) dataset of first bench sub-in events."""
    if pbp is None or pbp.empty:
        return pd.DataFrame()

    required = {"game_id", "team", "type", "period", "clock_sec_remaining"}
    if not required.issubset(set(pbp.columns)):
        return pd.DataFrame()

    df = pbp.copy()
    df["team"] = df["team"].astype(str).str.upper()

    out_rows: list[dict[str, Any]] = []

    for (date, game_id, team), group in df.groupby(["date", "game_id", "team"], dropna=False):
        gid = _required_text(game_id)
        if not gid:
            continue
        team_code = _required_text(team)
        if not team_code:
            continue
        event = compute_first_bench_sub_in_for_team_game(
            group,
            team_tricode=team_code,
            period=period,
            period_seconds=period_seconds,
        )
        if event is None:
            continue
        out_rows.append(
            {
                "date": date,
                "game_id": gid,
                "team": event.team,
                "period": event.period,
                "clock": event.clock,
                "elapsed_sec": event.elapsed_sec,
                "enter_player_id": event.enter_player_id,
                "enter_player_name": event.enter_player_name,
                "exit_player_id": event.exit_player_id,
                "exit_player_name": event.exit_player_name,
            }
        )

    return pd.DataFrame(out_rows)


def compute_rotation_priors(
    lookback_days: int = 60,
    min_games: int = 10,
    period: int = 1,
    period_seconds: int = PERIOD_SECONDS_DEFAULT,
) -> pd.DataFrame:
    """Compute team-level priors for first bench sub-in timing.

    Returns one row per team with summary statistics + most common first-sub entrant.
    """
    pbp = _read_pbp_espn_history()
    if pbp is None or pbp.empty:
        return pd.DataFrame()

    if "date" not in pbp.columns:
        return pd.DataFrame()

    pbp = pbp.copy()
    pbp["date"] = pd.to_datetime(pbp["date"], errors="coerce")
    pbp = pbp.dropna(subset=["date"])
    if pbp.empty:
        return pd.DataFrame()

    max_date = pbp["date"].max()
    start = max_date - pd.Timedelta(days=int(lookback_days))
    pbp = pbp[pbp["date"] >= start]

    first_subs = compute_first_bench_sub_in_dataset(pbp, period=period, period_seconds=period_seconds)
    if first_subs.empty:
        return pd.DataFrame()

    first_subs["elapsed_sec"] = pd.to_numeric(first_subs["elapsed_sec"], errors="coerce")
    first_subs = first_subs.dropna(subset=["elapsed_sec"]).copy()

    rows: list[dict[str, Any]] = []

    for team, g in first_subs.groupby("team"):
        if len(g) < min_games:
            continue

        elapsed = g["elapsed_sec"].astype(float)
        mean = float(elapsed.mean())
        median = float(elapsed.median())
        p25 = float(elapsed.quantile(0.25))
        p75 = float(elapsed.quantile(0.75))
        std = float(elapsed.std(ddof=1)) if len(elapsed) > 1 else 0.0

        # Most common first-sub entrant for the team
        enter = g["enter_player_id"].astype(str).replace({"": np.nan})
        top_enter_id = None
        top_enter_name = None
        top_enter_share = None
        if enter.notna().any():
            counts = enter.value_counts(dropna=True)
            top_enter_id = str(counts.index[0])
            top_enter_share = float(counts.iloc[0] / counts.sum())
            # Pick a representative name for the ID
            try:
                top_enter_name = (
                    g[g["enter_player_id"].astype(str) == top_enter_id]["enter_player_name"].dropna().astype(str).head(1).iloc[0]
                )
            except Exception:
                top_enter_name = None

        rows.append(
            {
                "team": str(team),
                "n_games": int(len(g)),
                "elapsed_sec_mean": mean,
                "elapsed_sec_median": median,
                "elapsed_sec_p25": p25,
                "elapsed_sec_p75": p75,
                "elapsed_sec_std": std,
                "top_enter_player_id": top_enter_id,
                "top_enter_player_name": top_enter_name,
                "top_enter_share": top_enter_share,
                "window_days": int(lookback_days),
                "asof_date": max_date.date().isoformat(),
            }
        )

    return pd.DataFrame(rows).sort_values(["n_games", "team"], ascending=[False, True], kind="stable")


def write_rotation_priors(
    lookback_days: int = 60,
    min_games: int = 10,
    out_path: Optional[Path] = None,
) -> dict[str, Any]:
    priors = compute_rotation_priors(lookback_days=lookback_days, min_games=min_games)
    if out_path is None:
        out_path = paths.data_processed / "rotation_priors_first_bench_sub_in.csv"

    if priors is None or priors.empty:
        return {"wrote": None, "rows": 0, "path": str(out_path)}

    priors.to_csv(out_path, index=False)
    return {"wrote": str(out_path), "rows": int(len(priors)), "path": str(out_path)}
