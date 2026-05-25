from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .config import paths
from .league import LEAGUE
from .league import season_label_from_date


def _parse_date(value: str) -> pd.Timestamp | None:
    try:
        ts = pd.to_datetime(value, errors="coerce")
        return ts.normalize() if pd.notna(ts) else None
    except Exception:
        return None


def _winsorized_mean(values: pd.Series, lo: float = 0.05, hi: float = 0.95) -> float:
    series = pd.to_numeric(values, errors="coerce").dropna()
    if series.empty:
        return 0.0
    q_lo = float(series.quantile(lo))
    q_hi = float(series.quantile(hi))
    return float(series.clip(lower=q_lo, upper=q_hi).mean())


def _pick_margin_col(df: pd.DataFrame) -> str | None:
    for col in ("pred_margin", "spread_margin", "margin_pred"):
        if col in df.columns:
            return col
    return None


@lru_cache(maxsize=16)
def _schedule_phase_by_date(season_label: str) -> dict[str, str]:
    try:
        fp = paths.data_processed / f"schedule_{str(season_label).strip()}.csv"
        if not fp.exists():
            return {}
        df = pd.read_csv(fp, usecols=["date_est", "game_label"])
    except Exception:
        return {}
    if df is None or df.empty:
        return {}
    try:
        df = df.copy()
        df["date_est"] = pd.to_datetime(df["date_est"], errors="coerce").dt.strftime("%Y-%m-%d")
        df["game_label"] = df["game_label"].astype(str).str.strip()
        df = df.dropna(subset=["date_est", "game_label"]).drop_duplicates(subset=["date_est"], keep="last")
        return dict(zip(df["date_est"].tolist(), df["game_label"].tolist()))
    except Exception:
        return {}


def _calibration_weight_for_date(date_str: str) -> float:
    ts = _parse_date(date_str)
    if ts is None:
        return 1.0
    try:
        phase_map = _schedule_phase_by_date(season_label_from_date(ts.date()))
        phase = str(phase_map.get(ts.strftime("%Y-%m-%d")) or "").strip().lower()
    except Exception:
        phase = ""
    if phase == "preseason":
        return 0.0
    return 1.0


def _load_prediction_recon_rows(date_str: str, processed_dir: Path) -> pd.DataFrame:
    pred_path = processed_dir / f"predictions_{date_str}.csv"
    recon_path = processed_dir / f"recon_games_{date_str}.csv"
    if not pred_path.exists() or not recon_path.exists():
        return pd.DataFrame()

    try:
        preds = pd.read_csv(pred_path)
        recon = pd.read_csv(recon_path)
    except Exception:
        return pd.DataFrame()
    if preds.empty or recon.empty:
        return pd.DataFrame()

    margin_col = _pick_margin_col(preds)
    if margin_col is None:
        return pd.DataFrame()

    for frame in (preds, recon):
        frame["home_team"] = frame.get("home_team", "").astype(str).str.strip()
        frame["visitor_team"] = frame.get("visitor_team", "").astype(str).str.strip()
        frame["date"] = pd.to_datetime(frame.get("date"), errors="coerce")

    merged = recon.merge(preds, on=["date", "home_team", "visitor_team"], how="inner", suffixes=("_recon", ""))
    if merged.empty:
        return pd.DataFrame()

    merged[margin_col] = pd.to_numeric(merged.get(margin_col), errors="coerce")
    merged["home_pts"] = pd.to_numeric(merged.get("home_pts"), errors="coerce")
    merged["visitor_pts"] = pd.to_numeric(merged.get("visitor_pts"), errors="coerce")
    merged = merged.dropna(subset=[margin_col, "home_pts", "visitor_pts"], how="any")
    if merged.empty:
        return pd.DataFrame()

    merged = merged[["date", "home_team", "visitor_team", margin_col, "home_pts", "visitor_pts"]].copy()
    merged = merged.rename(columns={margin_col: "pred_margin_raw"})
    merged["actual_margin"] = merged["home_pts"] - merged["visitor_pts"]
    merged["margin_error"] = merged["actual_margin"] - merged["pred_margin_raw"]
    return merged[["date", "home_team", "visitor_team", "pred_margin_raw", "actual_margin", "margin_error"]]


@lru_cache(maxsize=128)
def compute_game_biases(anchor_date: str, window_days: int = 30, prior_games: float = 10.0) -> dict[str, Any]:
    anchor = _parse_date(anchor_date)
    if anchor is None:
        return {}
    window_days = max(1, int(window_days))
    prior_games = max(1.0, float(prior_games))

    rows: list[pd.DataFrame] = []
    for d in pd.date_range(anchor - pd.Timedelta(days=window_days), anchor - pd.Timedelta(days=1), freq="D"):
        part = _load_prediction_recon_rows(d.strftime("%Y-%m-%d"), paths.data_processed)
        if not part.empty:
            rows.append(part)

    if not rows:
        return {}

    hist = pd.concat(rows, ignore_index=True)
    if hist.empty:
        return {}

    global_bias = _winsorized_mean(hist["margin_error"])
    home_team_bias: dict[str, float] = {}
    home_team_games: dict[str, int] = {}
    for team, grp in hist.groupby("home_team"):
        n_games = int(len(grp))
        if n_games <= 0:
            continue
        team_mean = _winsorized_mean(grp["margin_error"])
        weight = float(n_games) / float(n_games + prior_games)
        home_team_bias[str(team)] = float((weight * team_mean) + ((1.0 - weight) * global_bias))
        home_team_games[str(team)] = n_games

    return {
        "anchor": str(anchor.date()),
        "window_days": int(window_days),
        "prior_games": float(prior_games),
        "global": {
            "margin_bias": float(global_bias),
            "games": int(len(hist)),
        },
        "home_team": home_team_bias,
        "home_team_games": home_team_games,
    }


def save_game_calibration(biases: dict[str, Any], anchor_date: str) -> Path | None:
    if not biases:
        return None
    out_path = paths.data_processed / f"calibration_games_{anchor_date}.json"
    try:
        out_path.write_text(pd.Series(biases).to_json(indent=2), encoding="utf-8")
        return out_path
    except Exception:
        return None


def apply_game_biases(pred_df: pd.DataFrame, biases: dict[str, Any]) -> pd.DataFrame:
    if pred_df is None or pred_df.empty or not biases:
        return pred_df

    out = pred_df.copy()
    margin_col = _pick_margin_col(out)
    if margin_col is None:
        return out

    out[margin_col] = pd.to_numeric(out.get(margin_col), errors="coerce")
    if "pred_margin_raw" not in out.columns:
        out["pred_margin_raw"] = out[margin_col]

    team_bias = biases.get("home_team") if isinstance(biases, dict) else None
    if not isinstance(team_bias, dict):
        team_bias = {}
    global_bias = 0.0
    try:
        global_bias = float(((biases.get("global") or {}).get("margin_bias")) or 0.0)
    except Exception:
        global_bias = 0.0

    weight = 1.0
    try:
        first_date = pd.to_datetime(out.get("date"), errors="coerce").dropna().dt.strftime("%Y-%m-%d").iloc[0]
        weight = float(_calibration_weight_for_date(str(first_date)))
    except Exception:
        weight = 1.0

    out["pred_margin_calibration"] = (
        out.get("home_team", "")
        .astype(str)
        .map(lambda team: float(team_bias.get(str(team).strip(), global_bias)))
    )
    out["pred_margin_calibration_weight"] = float(weight)
    out[margin_col] = pd.to_numeric(out[margin_col], errors="coerce") + (pd.to_numeric(out["pred_margin_calibration"], errors="coerce").fillna(0.0) * float(weight))
    if margin_col != "pred_margin":
        out["pred_margin"] = out[margin_col]

    sigma = float(LEAGUE.spread_winprob_sigma)
    spread_prob = 1.0 / (1.0 + np.exp(-(pd.to_numeric(out[margin_col], errors="coerce") / max(1e-6, sigma))))
    spread_prob = np.clip(spread_prob, 0.001, 0.999)

    if "home_win_prob" in out.columns:
        if "home_win_prob_raw" not in out.columns:
            out["home_win_prob_raw"] = pd.to_numeric(out.get("home_win_prob"), errors="coerce")
        raw_prob = pd.to_numeric(out.get("home_win_prob_raw"), errors="coerce").clip(lower=0.001, upper=0.999)
        out["home_win_prob_from_spread"] = spread_prob
        out["home_win_prob"] = ((0.8 * spread_prob) + (0.2 * raw_prob)).clip(lower=0.001, upper=0.999)
    else:
        out["home_win_prob_from_spread"] = spread_prob
        out["home_win_prob"] = spread_prob

    return out
