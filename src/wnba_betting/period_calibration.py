"""
Period (halves/quarters) calibration utilities.

Goal: improve per-quarter/half totals and margins by blending model outputs
with data-driven team tendencies and enforcing natural constraints:
- Quarter totals sum to game total
- H1 = Q1 + Q2, H2 = Q3 + Q4
- Quarter margins sum to game spread margin

Approach:
- Compute per-team quarter scoring shares from historical games (last ~2 seasons)
- Blend share-based projections with model outputs (configurable weight)
- Renormalize to respect constraints

This module is inference-only (no sklearn), ARM64-friendly.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Dict, List, Tuple, Optional
import pandas as pd
import numpy as np

from .config import paths


@dataclass
class CalibrationConfig:
    # Weight of share-based estimate vs model output for totals
    totals_blend_weight: float = 0.8  # 0.0 -> model only, 1.0 -> shares only
    # How many games worth of prior to shrink team shares toward league averages
    share_prior_games: float = 20.0
    # Minimum games required to trust team-specific shares before heavy shrinkage
    min_team_games: int = 10

    def __post_init__(self):
        """Allow overriding totals_blend_weight via environment variable.

        Set NBA_CALIB_TOTALS_WEIGHT to a float in [0,1] to override the default.
        """
        env_val = os.getenv("NBA_CALIB_TOTALS_WEIGHT")
        if env_val is not None:
            try:
                v = float(env_val)
                # clamp to [0,1]
                if not np.isfinite(v):
                    return
                v = max(0.0, min(1.0, v))
                self.totals_blend_weight = v
            except Exception:
                # Ignore invalid env values
                pass


def _load_raw_games() -> pd.DataFrame:
    """Load raw games with quarter line scores if available."""
    # Prefer parquet if readable, else CSV
    parq = paths.data_raw / "games_nba_api.parquet"
    csv = paths.data_raw / "games_nba_api.csv"
    df = None
    if parq.exists():
        try:
            df = pd.read_parquet(parq)
        except Exception:
            df = None
    if df is None and csv.exists():
        df = pd.read_csv(csv)
    if df is None or df.empty:
            raise FileNotFoundError("Raw games file not found (games_nba_api.{parquet|csv}).")
    return df


def _compute_league_shares(df: pd.DataFrame) -> np.ndarray:
    """Compute league-average quarter shares of total points (Q1..Q4 sum to 1)."""
    # Filter rows with complete quarters
    mask = df[["home_q1", "home_q2", "home_q3", "home_q4",
               "visitor_q1", "visitor_q2", "visitor_q3", "visitor_q4"]].notna().all(axis=1)
    d = df.loc[mask].copy()
    if d.empty:
        # fallback to even split
        return np.array([0.25, 0.25, 0.25, 0.25], dtype=float)
    # Per-game quarter totals
    q_totals = np.stack([
        (d["home_q1"].values + d["visitor_q1"].values),
        (d["home_q2"].values + d["visitor_q2"].values),
        (d["home_q3"].values + d["visitor_q3"].values),
        (d["home_q4"].values + d["visitor_q4"].values),
    ], axis=1)
    game_totals = q_totals.sum(axis=1, keepdims=True)
    # Avoid division by zero
    game_totals[game_totals == 0] = 1.0
    shares = q_totals / game_totals
    return shares.mean(axis=0)


def _compute_team_shares(df: pd.DataFrame) -> pd.DataFrame:
    """Compute per-team quarter scoring shares (Q1..Q4 of team total points)."""
    required_cols = [
        "home_team", "visitor_team",
        "home_q1", "home_q2", "home_q3", "home_q4",
        "visitor_q1", "visitor_q2", "visitor_q3", "visitor_q4",
    ]
    for c in required_cols:
        if c not in df.columns:
            # Return empty; caller will fallback to league shares
            return pd.DataFrame(columns=["team", "games", "q1", "q2", "q3", "q4"])

    records: List[Dict] = []
    # Build long-form per-team rows
    # Home side
    home = df[[
        "home_team", "home_q1", "home_q2", "home_q3", "home_q4"
    ]].dropna()
    home = home.rename(columns={
        "home_team": "team", "home_q1": "q1", "home_q2": "q2",
        "home_q3": "q3", "home_q4": "q4"
    })
    # Visitor side
    away = df[[
        "visitor_team", "visitor_q1", "visitor_q2", "visitor_q3", "visitor_q4"
    ]].dropna()
    away = away.rename(columns={
        "visitor_team": "team", "visitor_q1": "q1", "visitor_q2": "q2",
        "visitor_q3": "q3", "visitor_q4": "q4"
    })
    all_rows = pd.concat([home, away], ignore_index=True)
    if all_rows.empty:
        return pd.DataFrame(columns=["team", "games", "q1", "q2", "q3", "q4"])
    all_rows["total"] = all_rows[["q1", "q2", "q3", "q4"]].sum(axis=1)
    # Filter non-positive totals (shouldn't happen but be safe)
    all_rows = all_rows[all_rows["total"] > 0]
    # Shares per row
    for q in ("q1", "q2", "q3", "q4"):
        all_rows[q] = all_rows[q] / all_rows["total"]
    grp = all_rows.groupby("team")
    shares = grp[["q1", "q2", "q3", "q4"]].mean().reset_index()
    shares["games"] = grp.size().values
    return shares[["team", "games", "q1", "q2", "q3", "q4"]]


def load_or_build_team_period_shares(force_recompute: bool = False) -> Tuple[pd.DataFrame, np.ndarray]:
    """Load cached team quarter shares (data/processed), or compute from raw.

    Returns:
        (team_shares_df, league_share_vector)
    """
    out_csv = paths.data_processed / "team_period_shares.csv"
    if out_csv.exists() and not force_recompute:
        try:
            df = pd.read_csv(out_csv)
            # League average saved as special row? Compute fresh to be safe
            raw = _load_raw_games()
            league = _compute_league_shares(raw)
            return df, league
        except Exception:
            pass
    # Compute
    raw = _load_raw_games()
    team_df = _compute_team_shares(raw)
    league = _compute_league_shares(raw)
    paths.data_processed.mkdir(parents=True, exist_ok=True)
    try:
        team_df.to_csv(out_csv, index=False)
    except Exception:
        pass
    return team_df, league


def _shrink_team_vector(vec: np.ndarray, team_games: int, league_vec: np.ndarray, prior_games: float) -> np.ndarray:
    """Empirical-Bayes shrinkage of team quarter share vector toward league average."""
    w = float(team_games) / (float(team_games) + float(prior_games))
    return w * vec + (1.0 - w) * league_vec


def calibrate_periods_for_row(
    row: pd.Series,
    team_shares: pd.DataFrame,
    league_share: np.ndarray,
    cfg: CalibrationConfig | None = None,
) -> pd.Series:
    """Given a prediction row, calibrate quarter/half totals and margins.

    Expected columns present in row:
    - pred_total (game total prediction)
    - spread_margin (game margin prediction)
    - quarters_q{1..4}_total (optional model outputs)
    - quarters_q{1..4}_margin (optional model outputs)
    - halves_h{1,2}_total (optional), will be recomputed from quarters

    Returns updated row.
    """
    if cfg is None:
        cfg = CalibrationConfig()

    # If required core predictions missing, nothing to do
    if pd.isna(row.get("totals")) and pd.isna(row.get("pred_total")):
        return row
    game_total = float(row.get("totals") if pd.notna(row.get("totals")) else row.get("pred_total"))
    game_margin = float(row.get("spread_margin", 0.0))

    # Fetch team share vectors (with shrinkage)
    th = str(row.get("home_team"))
    tv = str(row.get("visitor_team"))
    sh = team_shares[team_shares["team"] == th]
    sv = team_shares[team_shares["team"] == tv]
    if sh.empty or sv.empty:
        # Fallback: use league shares for both
        comb = league_share.copy()
    else:
        vh = sh[["q1", "q2", "q3", "q4"]].values.astype(float)[0]
        vv = sv[["q1", "q2", "q3", "q4"]].values.astype(float)[0]
        gh = int(sh["games"].values[0])
        gv = int(sv["games"].values[0])
        vh = _shrink_team_vector(vh, gh, league_share, cfg.share_prior_games)
        vv = _shrink_team_vector(vv, gv, league_share, cfg.share_prior_games)
        comb = (vh + vv) / 2.0
    # Normalize just in case
    comb = np.clip(comb, 1e-4, 1.0)
    comb = comb / comb.sum()

    # Model quarter totals if available
    model_q_totals = []
    for i in range(1, 5):
        model_q_totals.append(float(row.get(f"quarters_q{i}_total", np.nan)))
    model_q_totals = np.array(model_q_totals, dtype=float)

    # Share-based quarter totals
    share_q_totals = comb * game_total

    # Blend totals and renormalize to match game total
    if np.isfinite(model_q_totals).sum() >= 2:
        # Where model has NaN, use share value
        filled_model = np.where(np.isfinite(model_q_totals), model_q_totals, share_q_totals)
        blended = cfg.totals_blend_weight * share_q_totals + (1.0 - cfg.totals_blend_weight) * filled_model
    else:
        blended = share_q_totals
    # Enforce sum constraint
    s = blended.sum()
    if s > 0:
        blended = blended * (game_total / s)

    # Update quarter totals
    for i in range(1, 5):
        row[f"quarters_q{i}_total"] = float(blended[i - 1])

    # Halves from quarters
    row["halves_h1_total"] = float(blended[0] + blended[1])
    row["halves_h2_total"] = float(blended[2] + blended[3])

    # Margins: scale model quarter margins to sum to game margin; fallback uniform
    model_q_margins = []
    for i in range(1, 5):
        model_q_margins.append(float(row.get(f"quarters_q{i}_margin", np.nan)))
    model_q_margins = np.array(model_q_margins, dtype=float)
    if np.isfinite(model_q_margins).sum() >= 2 and abs(np.nansum(model_q_margins)) > 1e-6:
        sm = float(np.nansum(model_q_margins))
        scaled = model_q_margins * (game_margin / sm)
    else:
        # Uniform distribution of margin across quarters
        scaled = np.array([0.25, 0.25, 0.25, 0.25], dtype=float) * game_margin
    for i in range(1, 5):
        row[f"quarters_q{i}_margin"] = float(scaled[i - 1])

    # Derive half margins from quarter margins
    row["halves_h1_margin"] = float(scaled[0] + scaled[1])
    row["halves_h2_margin"] = float(scaled[2] + scaled[3])

    return row


def calibrate_period_predictions(pred_df: pd.DataFrame, cfg: CalibrationConfig | None = None) -> pd.DataFrame:
    """Calibrate period predictions for every row of predictions dataframe.

    pred_df should include columns produced by predict_games_npu (home_team, visitor_team,
    totals/pred_total, spread_margin, quarters_* and halves_* columns when enabled).
    """
    team_shares, league_share = load_or_build_team_period_shares(force_recompute=False)
    # Apply row-wise
    return pred_df.apply(lambda r: calibrate_periods_for_row(r, team_shares, league_share, cfg), axis=1)
