from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np

from .config import paths


STAT_KEYS = ["pts", "reb", "ast", "threes", "pra"]
PRED_COLS = {
    "pts": "pred_pts",
    "reb": "pred_reb",
    "ast": "pred_ast",
    "threes": "pred_threes",
    "pra": "pred_pra",
}


def _list_recent_dates(anchor_date: str, days: int) -> List[str]:
    d = pd.to_datetime(anchor_date).date()
    out: List[str] = []
    for i in range(1, days + 1):  # lookback excludes anchor (today)
        out.append(str(d - pd.Timedelta(days=i)))
    return out


def _read_predictions_for_date(date_str: str) -> Optional[pd.DataFrame]:
    p = paths.data_processed / f"props_predictions_{date_str}.csv"
    if not p.exists():
        return None
    try:
        return pd.read_csv(p)
    except Exception:
        return None


def _read_recon_for_date(date_str: str) -> Optional[pd.DataFrame]:
    p = paths.data_processed / f"recon_props_{date_str}.csv"
    if not p.exists():
        return None
    try:
        return pd.read_csv(p)
    except Exception:
        return None


def compute_biases(anchor_date: str, window_days: int = 7, min_pairs: int = 50) -> Dict[str, float]:
    """Compute per-stat intercept bias using last N days of (actual - predicted).

    - anchor_date: YYYY-MM-DD (today) – we look back 1..N days
    - window_days: lookback horizon
    - min_pairs: require at least this many matched rows overall to apply calibration

    Returns dict like {"pts": +0.3, "reb": -0.1, ...}. Missing stats default to 0.0.
    """
    dates = _list_recent_dates(anchor_date, window_days)
    errs: Dict[str, List[float]] = {k: [] for k in STAT_KEYS}
    total_pairs = 0
    for ds in dates:
        pred = _read_predictions_for_date(ds)
        recon = _read_recon_for_date(ds)
        if pred is None or recon is None or pred.empty or recon.empty:
            continue
        # Prefer joining by player_id if available; else by player_name (+ team if present)
        join_keys: List[Tuple[str, str]] = []
        if "player_id" in pred.columns and "player_id" in recon.columns:
            join_keys = [("player_id", "player_id")]
        elif "player_name" in pred.columns and "player_name" in recon.columns:
            join_keys = [("player_name", "player_name")]
            if "team" in pred.columns and "team_abbr" in recon.columns:
                join_keys.append(("team", "team_abbr"))
        else:
            continue
        left_on = [a for a, _ in join_keys]
        right_on = [b for _, b in join_keys]
        try:
            merged = pred.merge(recon, left_on=left_on, right_on=right_on, how="inner", suffixes=("_pred", "_act"))
        except Exception:
            continue
        if merged.empty:
            continue
        # Collect errors actual - predicted per stat
        for stat in STAT_KEYS:
            pc = PRED_COLS.get(stat)
            if pc in merged.columns and stat in merged.columns:
                try:
                    a = pd.to_numeric(merged[stat], errors="coerce")
                    p = pd.to_numeric(merged[pc], errors="coerce")
                    diff = (a - p).astype(float)
                    # Drop extreme outliers to keep it stable
                    q1, q99 = np.nanpercentile(diff.dropna(), [1, 99]) if diff.notna().sum() > 10 else (np.nan, np.nan)
                    if not np.isnan(q1) and not np.isnan(q99):
                        mask = (diff >= q1) & (diff <= q99)
                        vals = diff[mask].tolist()
                    else:
                        vals = diff.tolist()
                    errs[stat].extend([x for x in vals if np.isfinite(x)])
                except Exception:
                    continue
        total_pairs += int(len(merged))
    # If not enough data, return zeros
    if total_pairs < min_pairs:
        return {k: 0.0 for k in STAT_KEYS}
    # Use median error (robust) as intercept
    biases: Dict[str, float] = {}
    for stat in STAT_KEYS:
        arr = np.array(errs.get(stat, []), dtype=float)
        if arr.size == 0:
            biases[stat] = 0.0
            continue
        b = float(np.nanmedian(arr))
        # Light safety caps by stat
        caps = {"pts": 2.5, "reb": 1.5, "ast": 1.5, "threes": 0.8, "pra": 3.5}
        cap = caps.get(stat, 3.0)
        if b > cap:
            b = cap
        if b < -cap:
            b = -cap
        biases[stat] = b
    return biases


def apply_biases(preds: pd.DataFrame, biases: Dict[str, float]) -> pd.DataFrame:
    out = preds.copy()
    for stat, pc in PRED_COLS.items():
        if pc in out.columns and stat in biases:
            try:
                out[pc] = pd.to_numeric(out[pc], errors="coerce") + float(biases[stat])
            except Exception:
                continue
    return out


def save_calibration(biases: Dict[str, float], anchor_date: str, window_days: int) -> Path:
    obj = {
        "date": anchor_date,
        "window_days": int(window_days),
        "biases": {k: float(v) for k, v in biases.items()},
    }
    # Write dated calibration for reproducibility only
    out = paths.data_processed / f"props_calibration_{anchor_date}.json"
    try:
        out.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    except Exception:
        pass
    return out


# ------------------------
# Per-player calibration
# ------------------------

def _merge_pred_recon_for_date(date_str: str) -> Optional[pd.DataFrame]:
    """Helper to merge predictions and recon for a single date.

    Returns merged df with columns including player_id, player_name, and stat columns.
    """
    pred = _read_predictions_for_date(date_str)
    recon = _read_recon_for_date(date_str)
    if pred is None or recon is None or pred.empty or recon.empty:
        return None
    # Determine best join keys
    join_keys: List[Tuple[str, str]] = []
    if "player_id" in pred.columns and "player_id" in recon.columns:
        join_keys = [("player_id", "player_id")]
    elif "player_name" in pred.columns and "player_name" in recon.columns:
        join_keys = [("player_name", "player_name")]
        if "team" in pred.columns and "team_abbr" in recon.columns:
            join_keys.append(("team", "team_abbr"))
    else:
        return None
    left_on = [a for a, _ in join_keys]
    right_on = [b for _, b in join_keys]
    try:
        merged = pred.merge(recon, left_on=left_on, right_on=right_on, how="inner", suffixes=("_pred", "_act"))
    except Exception:
        return None
    if merged.empty:
        return None
    return merged


def compute_player_biases(
    anchor_date: str,
    window_days: int = 30,
    min_pairs_per_player: int = 6,
    shrink_k: float = 8.0,
    shrink_k_by_stat: dict[str, float] | None = None,
    min_pairs_by_stat: dict[str, int] | None = None,
) -> pd.DataFrame:
    """Compute per-player per-stat median error (actual - predicted) over a rolling window.

    - anchor_date: YYYY-MM-DD (today) – we look back 1..N days
    - window_days: lookback horizon
    - min_pairs_per_player: require at least this many matched rows per player/stat
    - shrink_k: Empirical-Bayes shrinkage strength; effective adj = bias * n/(n+K)

    Returns DataFrame columns: [player_id, player_name, stat, bias, n, adj]
    where 'adj' is the shrunken bias to apply.
    """
    dates = _list_recent_dates(anchor_date, window_days)
    # Collect diffs per (player_id, stat)
    rows: List[Tuple[int | None, str | None, str, float]] = []
    for ds in dates:
        merged = _merge_pred_recon_for_date(ds)
        if merged is None or merged.empty:
            continue
        pid_col = "player_id" if "player_id" in merged.columns else None
        pname_col = "player_name_pred" if "player_name_pred" in merged.columns else ("player_name" if "player_name" in merged.columns else None)
        for stat in STAT_KEYS:
            pc = PRED_COLS.get(stat)
            if pc in merged.columns and stat in merged.columns:
                try:
                    a = pd.to_numeric(merged[stat], errors="coerce")
                    p = pd.to_numeric(merged[pc], errors="coerce")
                    diff = (a - p).astype(float)
                    # Clip outliers lightly (per-date) for robustness
                    vals = diff.to_numpy()
                    vals = vals[np.isfinite(vals)]
                    if vals.size == 0:
                        continue
                    if vals.size > 10:
                        q1, q99 = np.nanpercentile(vals, [1, 99])
                        mask = (diff >= q1) & (diff <= q99)
                    else:
                        mask = diff.notna()
                    tmp = merged.loc[mask, [c for c in [pid_col, pname_col] if c] + [stat, pc]].copy()
                    tmp["_err"] = pd.to_numeric(tmp[stat], errors="coerce") - pd.to_numeric(tmp[pc], errors="coerce")
                    for _, r in tmp.iterrows():
                        pid = int(r[pid_col]) if pid_col and pd.notna(r[pid_col]) else None
                        pname = str(r[pname_col]) if pname_col and pd.notna(r[pname_col]) else None
                        e = float(r["_err"]) if pd.notna(r["_err"]) else np.nan
                        if np.isfinite(e):
                            rows.append((pid, pname, stat, e))
                except Exception:
                    continue
    if not rows:
        return pd.DataFrame(columns=["player_id","player_name","stat","bias","n","adj"])
    df = pd.DataFrame(rows, columns=["player_id","player_name","stat","err"])  # type: ignore[arg-type]
    # Aggregate by player/stat
    agg = df.groupby(["player_id","player_name","stat"]).agg(
        n=("err","count"),
        bias=("err", lambda x: float(np.nanmedian(np.asarray(list(x), dtype=float))))
    ).reset_index()
    # Caps per stat
    caps = {"pts": 3.0, "reb": 1.8, "ast": 1.8, "threes": 1.0, "pra": 4.5}
    agg["bias"] = agg.apply(lambda r: max(-caps.get(str(r["stat"]) ,3.0), min(caps.get(str(r["stat"]) ,3.0), float(r["bias"]))), axis=1)
    # Minimum pairs filter (allow per-stat overrides)
    if min_pairs_by_stat:
        try:
            def _ok_min(row):
                s = str(row["stat"])
                req = int(min_pairs_by_stat.get(s, min_pairs_per_player))  # type: ignore[arg-type]
                return int(row["n"]) >= req
            agg = agg[agg.apply(_ok_min, axis=1)].copy()
        except Exception:
            agg = agg[agg["n"] >= int(min_pairs_per_player)].copy()
    else:
        agg = agg[agg["n"] >= int(min_pairs_per_player)].copy()
    if agg.empty:
        return pd.DataFrame(columns=["player_id","player_name","stat","bias","n","adj"])
    # Shrinkage toward zero (allow per-stat overrides for K)
    if shrink_k_by_stat:
        def _adj_row(r):
            try:
                s = str(r["stat"]) if pd.notna(r["stat"]) else None
                k = float(shrink_k_by_stat.get(s, shrink_k))  # type: ignore[arg-type]
            except Exception:
                k = float(shrink_k)
            n = float(r["n"]) if pd.notna(r["n"]) else 0.0
            b = float(r["bias"]) if pd.notna(r["bias"]) else 0.0
            return b * (n / (n + k)) if (n + k) > 0 else 0.0
        agg["adj"] = agg.apply(_adj_row, axis=1)
    else:
        agg["adj"] = agg.apply(lambda r: float(r["bias"]) * (float(r["n"]) / (float(r["n"]) + float(shrink_k))), axis=1)
    return agg.sort_values(["stat","n"], ascending=[True, False]).reset_index(drop=True)


def apply_player_biases(preds: pd.DataFrame, player_biases: pd.DataFrame) -> pd.DataFrame:
    """Apply per-player per-stat adjustments to prediction columns in preds.

    Expects player_biases columns: [player_id, stat, adj] (player_name optional).
    """
    if preds is None or preds.empty or player_biases is None or player_biases.empty:
        return preds.copy()
    out = preds.copy()
    if "player_id" not in out.columns:
        # Without player_id, skip (name-based matching can be noisy)
        return out
    # Merge once per stat to avoid wide pivoting
    for stat, pc in PRED_COLS.items():
        if pc not in out.columns:
            continue
        sub = player_biases[player_biases["stat"] == stat][["player_id","adj"]].copy()
        if sub.empty:
            continue
        sub = sub.rename(columns={"adj": f"adj_{stat}"})
        out = out.merge(sub, on="player_id", how="left")
        adj_col = f"adj_{stat}"
        if adj_col in out.columns:
            try:
                out[pc] = pd.to_numeric(out[pc], errors="coerce") + pd.to_numeric(out[adj_col], errors="coerce").fillna(0.0)
            except Exception:
                pass
            out.drop(columns=[adj_col], inplace=True, errors="ignore")
    return out


def save_player_calibration(player_biases: pd.DataFrame, anchor_date: str, window_days: int) -> Path:
    """Save per-player calibration to processed folder as CSV.

    File name: props_player_calibration_<anchor>.csv
    """
    out = paths.data_processed / f"props_player_calibration_{anchor_date}.csv"
    try:
        player_biases = player_biases.copy()
        player_biases["date"] = str(anchor_date)
        player_biases["window_days"] = int(window_days)
        player_biases.to_csv(out, index=False)
    except Exception:
        pass
    return out
