from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .config import paths
from .league import LEAGUE
from .league_status import build_league_status
from .player_names import normalize_player_name_key
from .props_features import build_features_for_date


def _truthy_mask(values: pd.Series | Any) -> pd.Series:
    try:
        ser = values if isinstance(values, pd.Series) else pd.Series(values)
    except Exception:
        return pd.Series(dtype=bool)
    if len(ser) == 0:
        return pd.Series(dtype=bool)
    txt = ser.astype(str).str.strip().str.lower()
    out = txt.isin({"1", "true", "t", "yes", "y"})
    try:
        num = pd.to_numeric(ser, errors="coerce")
        out = out | ((num > 0.5) & num.notna())
    except Exception:
        pass
    return out.astype(bool)


def _norm_name(value: Any) -> str:
    return str(normalize_player_name_key(value, case="upper") or "").strip().upper()


def _load_rotation_priors() -> pd.DataFrame:
    fp = paths.data_processed / "rotation_priors_first_bench_sub_in.csv"
    if not fp.exists():
        return pd.DataFrame()
    try:
        out = pd.read_csv(fp)
    except Exception:
        return pd.DataFrame()
    if out is None or out.empty:
        return pd.DataFrame()
    if "team" in out.columns:
        out["team"] = out["team"].astype(str).str.upper().str.strip()
    if "elapsed_sec_mean" in out.columns:
        out["elapsed_sec_mean"] = pd.to_numeric(out["elapsed_sec_mean"], errors="coerce")
    return out


def _rotation_target_share(team: str, priors: pd.DataFrame) -> float:
    team_code = str(team or "").strip().upper()
    if priors is None or priors.empty or "team" not in priors.columns:
        return 0.70
    row = priors[priors["team"] == team_code]
    if row.empty:
        return 0.70
    sec_f = pd.to_numeric(row.iloc[0].get("elapsed_sec_mean"), errors="coerce")
    if pd.isna(sec_f):
        return 0.70
    early = 120.0
    late = 360.0
    z = float(np.clip((float(sec_f) - early) / max(1e-6, late - early), 0.0, 1.0))
    return float(0.66 + 0.08 * z)


def _load_league_status_for_date(date_str: str) -> pd.DataFrame:
    fp = paths.data_processed / f"league_status_{str(date_str).strip()}.csv"
    if fp.exists():
        try:
            out = pd.read_csv(fp)
        except Exception:
            out = pd.DataFrame()
    else:
        try:
            out = build_league_status(str(date_str))
        except Exception:
            out = pd.DataFrame()
    if out is None or out.empty:
        return pd.DataFrame()

    cols_lower = {c.lower(): c for c in out.columns}
    name_col = cols_lower.get("player_name") or cols_lower.get("player")
    team_col = cols_lower.get("team") or cols_lower.get("team_tri") or cols_lower.get("team_abbreviation")
    pid_col = cols_lower.get("player_id")
    if not (name_col and team_col):
        return pd.DataFrame()

    keep_cols = [team_col, name_col]
    if pid_col:
        keep_cols.append(pid_col)
    for optional in ("team_on_slate", "playing_today"):
        col = cols_lower.get(optional)
        if col:
            keep_cols.append(col)

    out = out[keep_cols].copy()
    rename_map = {team_col: "team_tri", name_col: "player_name"}
    if pid_col:
        rename_map[pid_col] = "player_id"
    out = out.rename(columns=rename_map)
    out["team_tri"] = out["team_tri"].astype(str).str.upper().str.strip()
    out["player_name"] = out["player_name"].astype(str).str.strip()
    if "player_id" in out.columns:
        out["player_id"] = pd.to_numeric(out["player_id"], errors="coerce")
    if "team_on_slate" in out.columns:
        out = out[_truthy_mask(out["team_on_slate"]).reindex(out.index, fill_value=False)].copy()
    if "playing_today" in out.columns:
        playing_false = (~_truthy_mask(out["playing_today"])) & out["playing_today"].notna()
        out = out[~playing_false.reindex(out.index, fill_value=False)].copy()
    out = out[(out["team_tri"].str.len() == 3) & (out["player_name"].str.len() > 0)].copy()
    out["player_key"] = out["player_name"].map(_norm_name)
    return out.drop_duplicates(subset=[c for c in ["team_tri", "player_id", "player_key"] if c in out.columns], keep="last")


def _compute_base_minutes(features: pd.DataFrame) -> pd.DataFrame:
    out = features.copy()
    for col in ("lag1_min", "roll3_min", "roll5_min", "roll10_min", "roll5_min_std", "roll10_min_std"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
        else:
            out[col] = np.nan

    weighted_sum = (
        out["lag1_min"].fillna(0.0) * 0.20
        + out["roll3_min"].fillna(0.0) * 0.25
        + out["roll5_min"].fillna(0.0) * 0.35
        + out["roll10_min"].fillna(0.0) * 0.20
    )
    weight_total = (
        out["lag1_min"].notna().astype(float) * 0.20
        + out["roll3_min"].notna().astype(float) * 0.25
        + out["roll5_min"].notna().astype(float) * 0.35
        + out["roll10_min"].notna().astype(float) * 0.20
    )
    out["exp_min_mean"] = np.where(weight_total > 0.0, weighted_sum / weight_total, np.nan)
    out["exp_min_sd"] = np.where(
        out["roll5_min_std"].notna() | out["roll10_min_std"].notna(),
        0.6 * out["roll5_min_std"].fillna(out["roll10_min_std"])
        + 0.4 * out["roll10_min_std"].fillna(out["roll5_min_std"]),
        np.nan,
    )
    return out


def _build_minutes_rows_for_team(team_df: pd.DataFrame, priors: pd.DataFrame) -> pd.DataFrame:
    if team_df is None or team_df.empty:
        return pd.DataFrame()
    out = team_df.copy().reset_index(drop=True)
    out["exp_min_mean"] = pd.to_numeric(out.get("exp_min_mean"), errors="coerce")
    out["exp_min_sd"] = pd.to_numeric(out.get("exp_min_sd"), errors="coerce")

    starter_order = out["exp_min_mean"].fillna(-1.0).rank(method="first", ascending=False)
    out["is_starter"] = starter_order <= 5

    known = out["exp_min_mean"].dropna()
    starter_fill = float(np.clip(known.nlargest(min(5, len(known))).mean() if not known.empty else 30.0, 24.0, 34.0))
    bench_fill = float(np.clip(known.nsmallest(max(1, min(5, len(known)))).mean() if not known.empty else 10.0, 6.0, 18.0))
    out.loc[out["exp_min_mean"].isna() & out["is_starter"], "exp_min_mean"] = starter_fill
    out.loc[out["exp_min_mean"].isna() & (~out["is_starter"]), "exp_min_mean"] = bench_fill

    out.loc[out["is_starter"], "exp_min_mean"] = out.loc[out["is_starter"], "exp_min_mean"].clip(lower=18.0, upper=38.0)
    out.loc[~out["is_starter"], "exp_min_mean"] = out.loc[~out["is_starter"], "exp_min_mean"].clip(lower=2.0, upper=28.0)

    target_share = _rotation_target_share(str(out["team_tri"].iloc[0]), priors)
    starter_mask = out["is_starter"].astype(bool)
    starter_total = float(out.loc[starter_mask, "exp_min_mean"].sum())
    bench_total = float(out.loc[~starter_mask, "exp_min_mean"].sum())
    total = starter_total + bench_total
    if starter_total > 0.0 and bench_total > 0.0 and total > 0.0 and (1.0 - target_share) > 1e-6:
        factor = (target_share * bench_total) / (starter_total * (1.0 - target_share))
        factor = float(np.clip(factor, 0.75, 1.25))
        out.loc[starter_mask, "exp_min_mean"] = out.loc[starter_mask, "exp_min_mean"] * factor

    total = float(out["exp_min_mean"].sum())
    if total <= 0.0:
        return pd.DataFrame()
    out["exp_min_mean"] = out["exp_min_mean"] * (float(LEAGUE.regulation_team_minutes) / total)

    missing_sd = out["exp_min_sd"].isna()
    if bool(missing_sd.any()):
        out.loc[missing_sd, "exp_min_sd"] = np.where(out.loc[missing_sd, "is_starter"], 3.0, 4.5)
    out["exp_min_sd"] = out["exp_min_sd"].clip(lower=1.5, upper=8.0)
    out["exp_min_cap"] = np.where(
        out["is_starter"],
        np.minimum(40.0, out["exp_min_mean"] + np.maximum(3.0, 1.15 * out["exp_min_sd"])),
        np.minimum(30.0, out["exp_min_mean"] + np.maximum(2.5, 1.25 * out["exp_min_sd"])),
    )

    out = out.sort_values(["is_starter", "exp_min_mean", "player_name"], ascending=[False, False, True], kind="stable").reset_index(drop=True)
    out["starter_prob"] = 0.08
    starter_idx = out.index[out["is_starter"].astype(bool)].tolist()
    starter_probs = [0.92, 0.86, 0.80, 0.74, 0.68]
    for i, idx in enumerate(starter_idx[:5]):
        out.loc[idx, "starter_prob"] = starter_probs[i]
    if len(out) > 5:
        out.loc[out.index[5:min(len(out), 8)], "starter_prob"] = 0.20

    return out


def write_pregame_expected_minutes(date_str: str, out_path: Path | None = None) -> dict[str, Any]:
    ds = str(date_str).strip()
    if not ds:
        raise ValueError("date_str is required")

    slate = _load_league_status_for_date(ds)
    if slate.empty:
        return {"wrote": None, "rows": 0, "date": ds, "reason": "empty_league_status"}

    feats = build_features_for_date(ds)
    if feats is None:
        feats = pd.DataFrame()
    feats = feats.copy()
    if not feats.empty:
        if "team" in feats.columns and "team_tri" not in feats.columns:
            feats = feats.rename(columns={"team": "team_tri"})
        if "team_tri" in feats.columns:
            feats["team_tri"] = feats["team_tri"].astype(str).str.upper().str.strip()
        if "player_name" in feats.columns:
            feats["player_name"] = feats["player_name"].astype(str).str.strip()
            feats["player_key"] = feats["player_name"].map(_norm_name)
        else:
            feats["player_key"] = ""
        if "player_id" in feats.columns:
            feats["player_id"] = pd.to_numeric(feats["player_id"], errors="coerce")
        feats = _compute_base_minutes(feats)

    merged = slate.copy()
    if not feats.empty and "player_id" in merged.columns and "player_id" in feats.columns:
        feats_pid = feats[feats["player_id"].notna()].copy()
        if not feats_pid.empty:
            feats_pid = feats_pid.sort_values(["player_id", "exp_min_mean"], ascending=[True, False], kind="stable")
            feats_pid = feats_pid.drop_duplicates(subset=["player_id"], keep="first")
            merged = merged.merge(
                feats_pid[[c for c in ["player_id", "team_tri", "player_key", "lag1_min", "roll3_min", "roll5_min", "roll10_min", "roll5_min_std", "roll10_min_std", "exp_min_mean", "exp_min_sd"] if c in feats_pid.columns]],
                on="player_id",
                how="left",
                suffixes=("", "_feat"),
            )
    if "exp_min_mean" not in merged.columns or merged["exp_min_mean"].isna().all():
        feats_pid = pd.DataFrame()

    need_name = merged.get("exp_min_mean") if "exp_min_mean" in merged.columns else pd.Series([np.nan] * len(merged), index=merged.index)
    if not feats.empty and need_name.isna().any():
        feats_name = feats.sort_values(["team_tri", "player_key", "exp_min_mean"], ascending=[True, True, False], kind="stable")
        feats_name = feats_name.drop_duplicates(subset=["team_tri", "player_key"], keep="first")
        fill_cols = [c for c in ["lag1_min", "roll3_min", "roll5_min", "roll10_min", "roll5_min_std", "roll10_min_std", "exp_min_mean", "exp_min_sd"] if c in feats_name.columns]
        if fill_cols:
            lookup = feats_name.set_index(["team_tri", "player_key"])[fill_cols]
            need_mask = need_name.isna()
            need_keys = pd.MultiIndex.from_frame(merged.loc[need_mask, ["team_tri", "player_key"]])
            matched = lookup.reindex(need_keys)
            matched.index = merged.index[need_mask]
            for col in fill_cols:
                merged.loc[need_mask, col] = matched[col].to_numpy()

    priors = _load_rotation_priors()
    rows: list[pd.DataFrame] = []
    asof_ts = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    for team_code, team_df in merged.groupby("team_tri", dropna=False):
        built = _build_minutes_rows_for_team(team_df, priors)
        if built.empty:
            continue
        built["date"] = ds
        built["exp_asof_ts"] = asof_ts
        built["exp_min_source"] = "props_features+league_status"
        rows.append(built)

    if not rows:
        return {"wrote": None, "rows": 0, "date": ds, "reason": "no_team_rows"}

    out = pd.concat(rows, ignore_index=True)
    keep = [
        "date",
        "team_tri",
        "player_id",
        "player_name",
        "starter_prob",
        "is_starter",
        "exp_min_mean",
        "exp_min_sd",
        "exp_min_cap",
        "exp_asof_ts",
        "exp_min_source",
        "lag1_min",
        "roll3_min",
        "roll5_min",
        "roll10_min",
    ]
    keep = [c for c in keep if c in out.columns]
    out = out[keep].copy()
    out = out.sort_values(["team_tri", "is_starter", "exp_min_mean", "player_name"], ascending=[True, False, False, True], kind="stable")

    out_csv = out_path or (paths.data_processed / f"pregame_expected_minutes_{ds}.csv")
    out_parquet = out_csv.with_suffix(".parquet")
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_csv, index=False)
    parquet_written = False
    try:
        out.to_parquet(out_parquet, index=False)
        parquet_written = True
    except Exception:
        parquet_written = False

    return {
        "wrote": str(out_csv),
        "parquet": str(out_parquet) if parquet_written else None,
        "rows": int(len(out)),
        "teams": int(out["team_tri"].nunique()) if "team_tri" in out.columns else 0,
        "date": ds,
    }