from __future__ import annotations

import pandas as pd
import numpy as np
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Tuple
import os

import unicodedata

from .config import paths
# from .props_train import predict_props  # MOVED TO CONDITIONAL - requires sklearn
from .props_calibration import compute_biases as _compute_biases, apply_biases as _apply_biases
from .props_features import build_features_for_date
# from .props_train import _load_features as _load_props_features  # MOVED TO CONDITIONAL - requires sklearn
from .odds_api import OddsApiConfig, backfill_player_props, fetch_player_props_current
from .teams import to_tricode as _tri, normalize_team as _norm_team
from .odds_bovada import fetch_bovada_player_props_current


_PROPS_PROB_CALIB_CACHE: dict[str, object] | None = None
_PROPS_PROB_CALIB_ALPHA: float | None = None


def _env_bool(name: str, default: bool = True) -> bool:
    try:
        raw = os.environ.get(name)
        if raw is None:
            return bool(default)
        s = str(raw).strip().lower()
        if s in {"1", "true", "yes", "y", "on"}:
            return True
        if s in {"0", "false", "no", "n", "off"}:
            return False
        return bool(default)
    except Exception:
        return bool(default)


def _env_float(name: str, default: float, lo: float | None = None, hi: float | None = None) -> float:
    try:
        raw = os.environ.get(name)
        v = float(default) if raw is None or str(raw).strip() == "" else float(str(raw).strip())
        if lo is not None:
            v = max(float(lo), v)
        if hi is not None:
            v = min(float(hi), v)
        return float(v)
    except Exception:
        return float(default)


_PROPS_OPENING_CACHE: dict[str, pd.DataFrame] = {}


def _load_opening_props_odds_for_date(date: datetime) -> pd.DataFrame:
    """Return opening (earliest snapshot) prop lines/prices for the slate date.

    This is a lightweight "sentiment" proxy: if the market has moved meaningfully
    since the earliest saved snapshot, we shrink model probabilities toward the
    market break-even.

    Source: data/raw/odds_nba_player_props*.parquet/csv (OddsAPI snapshots).
    """
    day_str = pd.to_datetime(date).date().isoformat()
    if day_str in _PROPS_OPENING_CACHE:
        return _PROPS_OPENING_CACHE[day_str]

    # Prefer a per-day "opening" snapshot first (small, avoids loading an all-day history),
    # then fall back to per-day history, then the per-day latest snapshot, then the cumulative history file.
    raw_open_pq = paths.data_raw / f"odds_nba_player_props_opening_{day_str}.parquet"
    raw_open_csv = paths.data_raw / f"odds_nba_player_props_opening_{day_str}.csv"
    raw_hist_pq = paths.data_raw / f"odds_nba_player_props_history_{day_str}.parquet"
    raw_hist_csv = paths.data_raw / f"odds_nba_player_props_history_{day_str}.csv"
    raw_day_pq = paths.data_raw / f"odds_nba_player_props_{day_str}.parquet"
    raw_day_csv = paths.data_raw / f"odds_nba_player_props_{day_str}.csv"
    raw_all_pq = paths.data_raw / "odds_nba_player_props.parquet"
    raw_all_csv = paths.data_raw / "odds_nba_player_props.csv"
    candidates = [raw_open_pq, raw_open_csv, raw_hist_pq, raw_hist_csv, raw_day_pq, raw_day_csv, raw_all_pq, raw_all_csv]

    usecols = [
        "snapshot_ts",
        "event_id",
        "commence_time",
        "bookmaker",
        "market",
        "outcome_name",
        "player_name",
        "point",
        "price",
    ]

    df = None
    for p in candidates:
        if not p.exists():
            continue
        try:
            if str(p).lower().endswith(".parquet"):
                df_try = pd.read_parquet(p, columns=usecols)
            else:
                df_try = pd.read_csv(p, usecols=usecols)
        except Exception:
            continue

        if df_try is None or df_try.empty:
            continue

        if "snapshot_ts" not in df_try.columns or "commence_time" not in df_try.columns:
            continue

        # Filter to this slate date using the event commence_time (ET calendar day).
        try:
            dt_utc = pd.to_datetime(df_try["commence_time"], errors="coerce", utc=True)
            try:
                et_dates = dt_utc.dt.tz_convert("America/New_York").dt.date
            except Exception:
                try:
                    et_dates = dt_utc.dt.tz_convert("US/Eastern").dt.date
                except Exception:
                    et_dates = dt_utc.dt.date
            df_try = df_try.loc[et_dates == pd.to_datetime(date).date()].copy()
        except Exception:
            df_try = pd.DataFrame()

        if df_try is None or df_try.empty:
            # This file doesn't contain the slate; try the next candidate.
            continue

        df = df_try
        break

    if df is None or df.empty:
        out = pd.DataFrame()
        _PROPS_OPENING_CACHE[day_str] = out
        return out

    # (Filter to slate date was applied during candidate selection.)

    # Normalize and map side.
    def _map_side(x: str) -> Optional[str]:
        u = str(x).upper()
        if "OVER" in u:
            return "OVER"
        if "UNDER" in u:
            return "UNDER"
        if u in ("YES", "Y"):
            return "YES"
        if u in ("NO", "N"):
            return "NO"
        return None

    try:
        df["name_key"] = df["player_name"].astype(str).map(_norm_name)
        df["side"] = df["outcome_name"].astype(str).map(_map_side)
    except Exception:
        out = pd.DataFrame()
        _PROPS_OPENING_CACHE[day_str] = out
        return out

    # Keep only rows we can join.
    need = ["snapshot_ts", "event_id", "bookmaker", "market", "name_key", "side", "point", "price"]
    df = df[[c for c in need if c in df.columns]].copy()
    df = df[df["event_id"].notna() & df["bookmaker"].notna() & df["market"].notna() & df["name_key"].notna() & df["side"].notna()].copy()
    if df.empty:
        out = pd.DataFrame()
        _PROPS_OPENING_CACHE[day_str] = out
        return out

    # Opening snapshot per prop outcome = earliest snapshot_ts.
    df["snapshot_ts_dt"] = pd.to_datetime(df["snapshot_ts"], errors="coerce", utc=True)
    df = df[df["snapshot_ts_dt"].notna()].copy()
    if df.empty:
        out = pd.DataFrame()
        _PROPS_OPENING_CACHE[day_str] = out
        return out

    # Ladder-safe opening selection:
    # - First pick the earliest snapshot per (event, book, market, player, side)
    # - Then, within that snapshot (which may include many ladder points), pick a
    #   canonical "main" line by choosing the row whose implied probability is
    #   closest to the -110 break-even (implied ~= 0.5238).
    keys = ["event_id", "bookmaker", "market", "name_key", "side"]
    df["price_num"] = pd.to_numeric(df.get("price"), errors="coerce")
    df["point_num"] = pd.to_numeric(df.get("point"), errors="coerce")

    # Vectorized implied probability from American odds
    ip = pd.Series(np.nan, index=df.index, dtype="float64")
    m_pos = df["price_num"] > 0
    if m_pos.any():
        ip.loc[m_pos] = 100.0 / (df.loc[m_pos, "price_num"] + 100.0)
    m_neg = df["price_num"] < 0
    if m_neg.any():
        ip.loc[m_neg] = (-df.loc[m_neg, "price_num"]) / ((-df.loc[m_neg, "price_num"]) + 100.0)
    df["_open_implied"] = ip
    target_ip = 110.0 / (110.0 + 100.0)  # implied prob for -110
    df["_open_metric"] = (df["_open_implied"] - float(target_ip)).abs()
    # Avoid NaNs breaking idxmin
    df["_open_metric"] = df["_open_metric"].fillna(1e9)

    # Earliest snapshot_ts per group
    first_ts = df.groupby(keys)["snapshot_ts_dt"].transform("min")
    df0 = df[df["snapshot_ts_dt"] == first_ts].copy()
    if df0.empty:
        out = pd.DataFrame()
        _PROPS_OPENING_CACHE[day_str] = out
        return out

    try:
        idx = df0.groupby(keys)["_open_metric"].idxmin()
        opening = df0.loc[idx].copy()
    except Exception:
        # Conservative fallback: deterministic earliest row per group
        opening = df0.sort_values(["snapshot_ts_dt"]).groupby(keys, as_index=False).first()

    opening = opening.rename(
        columns={
            "point_num": "open_line",
            "price_num": "open_price",
            "snapshot_ts": "open_snapshot_ts",
        }
    )
    keep = ["event_id", "bookmaker", "market", "name_key", "side", "open_line", "open_price", "open_snapshot_ts"]
    opening = opening[[c for c in keep if c in opening.columns]].copy()

    _PROPS_OPENING_CACHE[day_str] = opening
    return opening


def _get_props_prob_calib_alpha() -> float:
    """Return runtime blend strength for probability calibration.

    This blends between identity and the saved monotone calibration curve:
      p_final = (1-alpha) * p_raw + alpha * p_curve

    Set via env var PROPS_PROB_CALIB_ALPHA in [0, 1]. Default: 1.
    """
    global _PROPS_PROB_CALIB_ALPHA
    if _PROPS_PROB_CALIB_ALPHA is not None:
        return float(_PROPS_PROB_CALIB_ALPHA)
    raw = (os.environ.get("PROPS_PROB_CALIB_ALPHA") or "").strip()
    if not raw:
        _PROPS_PROB_CALIB_ALPHA = 1.0
        return 1.0
    try:
        v = float(raw)
        v = float(max(0.0, min(1.0, v)))
        _PROPS_PROB_CALIB_ALPHA = v
        return v
    except Exception:
        _PROPS_PROB_CALIB_ALPHA = 1.0
        return 1.0


def _is_sane_prob_calibration(xs: list[float], ys: list[float]) -> bool:
    """Basic validation to avoid applying a broken calibration curve.

    We expect a monotone mapping raw_prob -> calibrated_prob that roughly behaves
    like a probability (spans around 0.5 in the mid-range).
    """
    try:
        if not (isinstance(xs, list) and isinstance(ys, list)):
            return False
        if len(xs) < 2 or len(xs) != len(ys):
            return False
        xs_f = [float(v) for v in xs]
        ys_f = [float(v) for v in ys]
        # xs strictly increasing (or at least non-decreasing with unique endpoints)
        if xs_f[0] >= xs_f[-1]:
            return False
        if any((xs_f[i + 1] < xs_f[i]) for i in range(len(xs_f) - 1)):
            return False
        # y in [0,1] and monotone non-decreasing
        if any((y < -1e-9 or y > 1.0 + 1e-9) for y in ys_f):
            return False
        if any((ys_f[i + 1] + 1e-6 < ys_f[i]) for i in range(len(ys_f) - 1)):
            return False

        def _interp(xv: float) -> float:
            xv = float(xv)
            if xv <= xs_f[0]:
                return float(ys_f[0])
            if xv >= xs_f[-1]:
                return float(ys_f[-1])
            for i in range(len(xs_f) - 1):
                x0, x1 = xs_f[i], xs_f[i + 1]
                if x0 <= xv <= x1:
                    y0, y1 = ys_f[i], ys_f[i + 1]
                    t = 0.0 if x1 == x0 else (xv - x0) / (x1 - x0)
                    return float((1.0 - t) * y0 + t * y1)
            return float(ys_f[-1])

        y10 = _interp(0.10)
        y50 = _interp(0.50)
        y90 = _interp(0.90)

        # Midpoint should stay near 0.5.
        if not (0.40 <= float(y50) <= 0.60):
            return False

        # In this project, prop win probabilities often live in a narrow band around 0.5
        # (especially after guardrails), so requiring a huge [0.1..0.9] spread can
        # incorrectly reject valid shrinkage curves. We only require that the mapping
        # isn't effectively constant.
        if float(y90) - float(y10) < 0.05:
            return False

        return True
    except Exception:
        return False


def _load_props_prob_calibration() -> dict[str, object] | None:
    """Load piecewise-linear probability calibration for props.

    Supported JSON formats:
      - Legacy global: {"x": [...], "y": [...]}
      - Per-stat: {"global": {"x": [...], "y": [...]}, "per_stat": {"pts": {"x": [...], "y": [...]}, ...}}

    Primary lookup is per-stat, with fallback to global.
    """
    global _PROPS_PROB_CALIB_CACHE
    if _PROPS_PROB_CALIB_CACHE is not None:
        return _PROPS_PROB_CALIB_CACHE
    try:
        import json

        def _curve(obj: object) -> dict[str, list[float]] | None:
            try:
                if not isinstance(obj, dict):
                    return None
                xs = obj.get("x") or []
                ys = obj.get("y") or []
                if not (isinstance(xs, list) and isinstance(ys, list) and len(xs) >= 2 and len(xs) == len(ys)):
                    return None
                xs_f = [float(v) for v in xs]
                ys_f = [float(v) for v in ys]
                if not _is_sane_prob_calibration(xs_f, ys_f):
                    return None
                return {"x": xs_f, "y": ys_f}
            except Exception:
                return None

        fp_by = paths.data_processed / "props_prob_calibration_by_stat.json"
        fp_global = paths.data_processed / "props_prob_calibration.json"

        # Prefer per-stat calibration when available.
        if fp_by.exists():
            obj = json.loads(fp_by.read_text(encoding="utf-8"))
            if isinstance(obj, dict):
                global_curve = _curve(obj.get("global") if isinstance(obj.get("global"), dict) else obj)
                per_stat: dict[str, dict[str, list[float]]] = {}
                ps = obj.get("per_stat")
                if isinstance(ps, dict):
                    for k, v in ps.items():
                        cv = _curve(v)
                        if cv is None:
                            continue
                        per_stat[str(k).strip().lower()] = cv
                if global_curve is not None or per_stat:
                    _PROPS_PROB_CALIB_CACHE = {"global": global_curve, "per_stat": per_stat}
                    return _PROPS_PROB_CALIB_CACHE

        # Fall back to legacy global calibration.
        if fp_global.exists():
            obj = json.loads(fp_global.read_text(encoding="utf-8"))
            global_curve = _curve(obj)
            if global_curve is not None:
                _PROPS_PROB_CALIB_CACHE = {"global": global_curve, "per_stat": {}}
                return _PROPS_PROB_CALIB_CACHE

        _PROPS_PROB_CALIB_CACHE = None
        return None
    except Exception:
        _PROPS_PROB_CALIB_CACHE = None
        return None


def _apply_props_prob_calibration(p: float, stat: str | None = None) -> float:
    """Calibrate/shrink prop win probabilities.

        If a saved isotonic calibration curve exists and passes sanity checks,
        we apply it.

        If no curve exists, we do NOT apply a generic shrink. A prior linear shrink
        collapsed probabilities into a narrow band around 0.5, which badly distorted
        EV (especially for longshot markets). We instead rely on:
            - sigma inflation,
            - market-blend guardrails,
            - and sanity checks on projections
        downstream in edge computation.
    """
    cal = _load_props_prob_calibration()
    try:
        pv = float(max(0.0, min(1.0, float(p))))
        if cal is None:
            return pv

        curve = None
        try:
            st = str(stat or "").strip().lower()
            if st:
                ps = cal.get("per_stat") if isinstance(cal, dict) else None
                if isinstance(ps, dict):
                    curve = ps.get(st)
        except Exception:
            curve = None
        if curve is None:
            curve = cal.get("global") if isinstance(cal, dict) else None
        if not isinstance(curve, dict):
            return pv

        xs = curve.get("x") or []
        ys = curve.get("y") or []
        if not (isinstance(xs, list) and isinstance(ys, list) and len(xs) >= 2 and len(xs) == len(ys)):
            return pv

        ycal: float
        if pv <= xs[0]:
            ycal = float(ys[0])
        elif pv >= xs[-1]:
            ycal = float(ys[-1])
        else:
            ycal = pv
            for i in range(len(xs) - 1):
                x0 = float(xs[i]); x1 = float(xs[i + 1])
                if x0 <= pv <= x1:
                    y0 = float(ys[i]); y1 = float(ys[i + 1])
                    t = 0.0 if x1 == x0 else (pv - x0) / (x1 - x0)
                    ycal = float((1.0 - t) * y0 + t * y1)
                    break

        a = _get_props_prob_calib_alpha()
        if a >= 0.999:
            return float(max(0.0, min(1.0, ycal)))
        return float(max(0.0, min(1.0, (1.0 - a) * pv + a * ycal)))
        return pv
    except Exception:
        try:
            return float(max(0.0, min(1.0, float(p))))
        except Exception:
            return 0.5


# Map OddsAPI player markets to our prediction columns
MARKET_TO_STAT = {
    "player_points": "pts",
    "player_rebounds": "reb",
    "player_assists": "ast",
    "player_threes": "threes",
    "player_points_rebounds_assists": "pra",
    # Additional OddsAPI markets
    "player_points_rebounds": "pr",
    "player_points_assists": "pa",
    "player_rebounds_assists": "ra",
    "player_steals": "stl",
    "player_blocks": "blk",
    "player_turnovers": "tov",
    "player_double_double": "dd",
    "player_triple_double": "td",
}


def _norm_name(s: str) -> str:
    if s is None:
        return ""
    t = str(s)
    # strip team suffixes in parentheses if present
    if "(" in t:
        t = t.split("(", 1)[0]
    # normalize punctuation/hyphens and quotes
    t = t.replace("-", " ")
    t = t.replace(".", "").replace("'", "").replace(",", " ").strip()
    # remove common suffix tokens
    for suf in [" JR", " SR", " II", " III", " IV"]:
        if t.upper().endswith(suf):
            t = t[: -len(suf)]
    try:
        # Convert diacritics (e.g., Dončić -> Doncic) instead of dropping letters.
        t = unicodedata.normalize("NFKD", t)
        t = "".join(ch for ch in t if not unicodedata.combining(ch))
        t = t.encode("ascii", "ignore").decode("ascii")
    except Exception:
        pass
    return t.upper().strip()


def _short_key(s: str) -> str:
    """Lightweight key: LASTNAME + FIRST_INITIAL, uppercase, ascii-only."""
    if not s:
        return ""
    s2 = _norm_name(s)
    parts = [p for p in s2.replace("-", " ").split() if p]
    if not parts:
        return s2
    last = parts[-1]
    first_initial = parts[0][0] if parts and parts[0] else ""
    return f"{last}{first_initial}"


def _american_implied_prob(price: float) -> float:
    try:
        a = float(price)
    except Exception:
        return np.nan
    if a > 0:
        return 100.0 / (a + 100.0)
    else:
        return (-a) / ((-a) + 100.0)


def _ev_per_unit(price: float, win_prob: float) -> float:
    # Expected value per 1 unit stake using American odds
    try:
        a = float(price)
        p = float(win_prob)
    except Exception:
        return np.nan
    if a > 0:
        return p * (a / 100.0) - (1 - p) * 1.0
    else:
        return p * (100.0 / (-a)) - (1 - p) * 1.0


@dataclass
class SigmaConfig:
    pts: float = 7.5
    reb: float = 3.0
    ast: float = 2.5
    threes: float = 1.3
    pra: float = 9.0
    # Optional defaults for additional stats
    stl: float = 1.2
    blk: float = 1.3
    tov: float = 1.5


def _odds_for_date_from_saved(date: datetime) -> pd.DataFrame:
    # Load saved odds and filter to commence_time date.
    # Prefer per-date snapshots (created by our daily pipeline) when available.
    date_str = pd.to_datetime(date).date().isoformat()
    # Keep memory bounded: only load the columns we actually use downstream.
    usecols = [
        "snapshot_ts",
        "event_id",
        "commence_time",
        "bookmaker",
        "bookmaker_title",
        "market",
        "outcome_name",
        "player_name",
        "point",
        "price",
        "home_team",
        "away_team",
    ]
    usecols_set = set(usecols)

    def _read_parquet(fp: Path) -> pd.DataFrame | None:
        try:
            return pd.read_parquet(fp, columns=usecols)
        except Exception:
            try:
                return pd.read_parquet(fp)
            except Exception:
                return None

    def _read_csv(fp: Path) -> pd.DataFrame | None:
        try:
            return pd.read_csv(fp, usecols=lambda c: c in usecols_set)
        except Exception:
            try:
                return pd.read_csv(fp)
            except Exception:
                return None

    raw_pq = paths.data_raw / f"odds_nba_player_props_{date_str}.parquet"
    raw_csv = paths.data_raw / f"odds_nba_player_props_{date_str}.csv"
    raw_all_pq = paths.data_raw / "odds_nba_player_props.parquet"
    raw_all_csv = paths.data_raw / "odds_nba_player_props.csv"
    df = None
    if raw_pq.exists():
        try:
            df = _read_parquet(raw_pq)
        except Exception:
            df = None
    if df is None and raw_csv.exists():
        try:
            df = _read_csv(raw_csv)
        except Exception:
            df = None
    if df is None and raw_all_pq.exists():
        try:
            df = _read_parquet(raw_all_pq)
        except Exception:
            df = None
    if df is None and raw_all_csv.exists():
        try:
            df = _read_csv(raw_all_csv)
        except Exception:
            df = None
    if df is None or df.empty:
        return pd.DataFrame()
    # Filter by commence_time date
    if "commence_time" in df.columns:
        dt_utc = pd.to_datetime(df["commence_time"], errors="coerce", utc=True)
        # Convert to US/Eastern date for correct slate filtering (vectorized).
        try:
            et_dates = dt_utc.dt.tz_convert("America/New_York").dt.date
        except Exception:
            try:
                et_dates = dt_utc.dt.tz_convert("US/Eastern").dt.date
            except Exception:
                et_dates = dt_utc.dt.date
        df = df.loc[et_dates == pd.to_datetime(date).date()].copy()
    return df


def _fetch_odds_for_date(date: datetime, mode: str, api_key: Optional[str]) -> pd.DataFrame:
    if not api_key:
        return pd.DataFrame()
    cfg = OddsApiConfig(api_key=api_key)
    # Try historical first with a late timestamp on that date (UTC)
    if mode in ("auto", "historical"):
        # Use a late-evening Eastern timestamp for the snapshot to ensure late games are included
        base = pd.Timestamp(pd.to_datetime(date).date())
        try:
            # Localize to US/Eastern then convert to UTC for the historical API 'date' parameter
            ts_et = base.tz_localize("America/New_York").replace(hour=23, minute=59, second=0)
        except Exception:
            try:
                ts_et = base.tz_localize("US/Eastern").replace(hour=23, minute=59, second=0)
            except Exception:
                # Fallback naive -> approximate by subtracting 4h (DST months) or 5h otherwise
                month = int(base.month)
                offset = 4 if 3 <= month <= 11 else 5
                ts_et = (base.replace(hour=23, minute=59, second=0) - pd.Timedelta(hours=offset)).tz_localize("UTC")
        ts = ts_et.tz_convert("UTC").to_pydatetime()
        try:
            df = backfill_player_props(cfg, ts, verbose=False)
            # Filter to date in commence_time in case file had other days
            if df is not None and not df.empty:
                dt_utc = pd.to_datetime(df["commence_time"], errors="coerce", utc=True)
                def _to_et_date(ts):
                    try:
                        return ts.tz_convert("America/New_York").date()
                    except Exception:
                        try:
                            return ts.tz_convert("US/Eastern").date()
                        except Exception:
                            month = int(ts.month)
                            offset = 4 if 3 <= month <= 11 else 5
                            return (ts - pd.Timedelta(hours=offset)).date()
                et_dates = dt_utc.map(_to_et_date)
                df = df.loc[et_dates == pd.to_datetime(date).date()].copy()
                if not df.empty:
                    return df
        except Exception:
            pass
        if mode == "historical":
            return pd.DataFrame()
    # Fallback to current event odds for the calendar date
    if mode in ("auto", "current"):
        try:
            df = fetch_player_props_current(cfg, pd.to_datetime(date))
            return df if df is not None else pd.DataFrame()
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


def compute_props_edges(
    date: str,
    sigma: SigmaConfig,
    use_saved: bool = True,
    mode: str = "auto",
    api_key: Optional[str] = None,
    source: str = "auto",
    predictions_path: Optional[str] = None,
    from_file_only: bool = False,
    exclude_injured: bool = True,
    calibrate_prob: bool = False,
) -> pd.DataFrame:
    """Compute model edges/EV against OddsAPI player props for a given date.

    Returns a DataFrame with: date, player_name, stat, side, line, price, implied_prob, model_prob, edge, ev, bookmaker.
    """
    target_date = pd.to_datetime(date)
    # Load predictions for slate, preferring precomputed CSV when available
    preds: pd.DataFrame
    if predictions_path is None:
        # default location written by predict-props CLI
        predictions_path = str(paths.data_processed / f"props_predictions_{pd.to_datetime(target_date).date()}.csv")
    preds = pd.DataFrame()
    try:
        p = Path(predictions_path)
        if not p.is_absolute():
            p = paths.root / p
        if p.exists():
            preds = pd.read_csv(p)
    except Exception:
        preds = pd.DataFrame()
    if preds is None or preds.empty:
        # If we are restricted to file-only mode, do NOT run models server-side
        if from_file_only:
            return pd.DataFrame()
        # Otherwise compute predictions locally using pure ONNX path (no sklearn)
        from .props_onnx_pure import predict_props_pure_onnx  # Pure ONNX inference
        feats = build_features_for_date(target_date)
        preds = predict_props_pure_onnx(feats)
        # Light bias calibration based on recent recon (safe default)
        try:
            biases = _compute_biases(anchor_date=str(pd.to_datetime(target_date).date()), window_days=7)
            preds = _apply_biases(preds, biases)
        except Exception:
            pass
        # Persist predictions for downstream consumers (e.g., recommendations export)
        try:
            out_p = paths.data_processed / f"props_predictions_{pd.to_datetime(target_date).date()}.csv"
            out_p.parent.mkdir(parents=True, exist_ok=True)
            preds.to_csv(out_p, index=False)
        except Exception:
            pass
    # Prepare prediction columns.
    # If present, prefer mean_* columns (SmartSim-enhanced); otherwise fall back to pred_*.
    base_map = {
        "pts": ("mean_pts", "pred_pts"),
        "reb": ("mean_reb", "pred_reb"),
        "ast": ("mean_ast", "pred_ast"),
        "threes": ("mean_threes", "pred_threes"),
        "pra": ("mean_pra", "pred_pra"),
        # newly supported
        "stl": ("mean_stl", "pred_stl"),
        "blk": ("mean_blk", "pred_blk"),
        "tov": ("mean_tov", "pred_tov"),
    }
    pred_map: dict[str, str] = {}
    for stat, (mean_col, pred_col) in base_map.items():
        if mean_col in preds.columns:
            pred_map[stat] = mean_col
        else:
            pred_map[stat] = pred_col

    for need in ["player_id", "player_name"] + list(pred_map.values()):
        if need not in preds.columns:
            raise ValueError(f"Predictions missing column: {need}")

    preds = preds.copy()

    # If predictions include slate/availability flags, enforce them here.
    # This prevents stale rows (or players not expected to play) from generating
    # extreme "auto-under" edges.
    try:
        if "team_on_slate" in preds.columns:
            preds = preds[pd.to_numeric(preds["team_on_slate"], errors="coerce").fillna(0).astype(bool)].copy()
        if "playing_today" in preds.columns:
            preds = preds[pd.to_numeric(preds["playing_today"], errors="coerce").fillna(0).astype(bool)].copy()
    except Exception:
        pass

    preds["name_key"] = preds["player_name"].astype(str).map(_norm_name)
    preds["short_key"] = preds["player_name"].astype(str).map(_short_key)

    # Load odds (OddsAPI or Bovada based on source)
    odds = pd.DataFrame()
    src = (source or "auto").lower()
    if src == "oddsapi":
        if use_saved:
            odds = _odds_for_date_from_saved(target_date)
        if odds is None or odds.empty:
            fetched = _fetch_odds_for_date(target_date, mode=mode, api_key=api_key)
            odds = fetched if fetched is not None else pd.DataFrame()
    elif src == "bovada":
        odds = fetch_bovada_player_props_current(target_date)
    else:
        # auto: try saved/current OddsAPI first, then fall back to Bovada
        if use_saved:
            odds = _odds_for_date_from_saved(target_date)
        if odds is None or odds.empty:
            fetched = _fetch_odds_for_date(target_date, mode=mode, api_key=api_key)
            odds = fetched if fetched is not None else pd.DataFrame()
        if odds is None or odds.empty:
            odds = fetch_bovada_player_props_current(target_date)
    if odds is None or odds.empty:
        return pd.DataFrame()

    # Optional: exclude injured players from consideration before merging
    if exclude_injured:
        try:
            inj_path = paths.data_raw / "injuries.csv"
            if inj_path.exists():
                inj = pd.read_csv(inj_path)
                # Normalize date and take latest status per player/team up to target date
                if "date" in inj.columns:
                    inj["date"] = pd.to_datetime(inj["date"], errors="coerce").dt.date
                    cutoff = pd.to_datetime(target_date).date()
                    inj = inj[inj["date"].notna()]
                    inj = inj[inj["date"] <= cutoff].copy()
                # Keep relevant columns
                keep = [c for c in ["player","team","status","date"] if c in inj.columns]
                inj = inj[keep].copy() if keep else pd.DataFrame()
                if not inj.empty and "player" in inj.columns and "status" in inj.columns:
                    # Latest record per player+team if team exists, else per player
                    sort_cols = [c for c in ["date"] if c in inj.columns]
                    if sort_cols:
                        inj = inj.sort_values(sort_cols)
                    grp_cols = [c for c in ["player","team"] if c in inj.columns]
                    if not grp_cols:
                        grp_cols = ["player"]
                    inj_latest = inj.groupby(grp_cols, as_index=False).tail(1).copy()
                    # Normalize names and statuses (use .loc to avoid SettingWithCopyWarning)
                    def _norm(s: str) -> str:
                        return _norm_name(s)
                    inj_latest.loc[:, "name_key"] = inj_latest["player"].astype(str).map(_norm)
                    inj_latest.loc[:, "short_key"] = inj_latest["player"].astype(str).map(_short_key)
                    inj_latest.loc[:, "status_norm"] = inj_latest["status"].astype(str).str.upper()
                    # Exclusion logic: exact statuses plus season-long/indefinite phrasing
                    EXCLUDE_STATUSES = {"OUT","DOUBTFUL","SUSPENDED","INACTIVE","REST"}
                    def _excluded_status(u: str) -> bool:
                        try:
                            u = str(u).upper()
                        except Exception:
                            return False
                        if u in EXCLUDE_STATUSES:
                            return True
                        # Season-long and indefinite patterns
                        if ("OUT" in u and ("SEASON" in u or "INDEFINITE" in u)) or ("SEASON-ENDING" in u):
                            return True
                        return False
                    bad = inj_latest[inj_latest["status_norm"].map(_excluded_status)].copy()
                    if not bad.empty:
                        bad_name_keys = set(bad["name_key"].dropna().astype(str))
                        bad_short_keys = set(bad["short_key"].dropna().astype(str))
                        # We'll apply once odds are normalized with name_key/short_key
                        _inj_filter = (bad_name_keys, bad_short_keys)
                    else:
                        _inj_filter = None
                else:
                    _inj_filter = None
            else:
                _inj_filter = None
        except Exception:
            _inj_filter = None

    # Normalize odds
    keep_cols = [
        "snapshot_ts", "event_id",
        "bookmaker", "bookmaker_title", "market", "outcome_name", "player_name", "point", "price", "commence_time",
        "home_team", "away_team",
    ]
    odds = odds[[c for c in keep_cols if c in odds.columns]].copy()
    # outcome_name is Over/Under, player_name may be in description
    if "player_name" not in odds.columns and "outcome_name" in odds.columns:
        # Some payloads put the player in outcome_name; try to salvage
        odds["player_name"] = odds["outcome_name"].astype(str)
    odds["name_key"] = odds["player_name"].astype(str).map(_norm_name)
    odds["short_key"] = odds["player_name"].astype(str).map(_short_key)
    # Apply injuries filter now that keys exist
    try:
        if exclude_injured and ('_inj_filter' in locals()) and (_inj_filter is not None):
            bad_name_keys, bad_short_keys = _inj_filter
            if bad_name_keys or bad_short_keys:
                odds = odds[~(odds["name_key"].isin(bad_name_keys) | odds["short_key"].isin(bad_short_keys))].copy()
    except Exception:
        pass
    # Side: OVER/UNDER for most markets; YES/NO for double-double/triple-double
    def _map_side(x: str) -> Optional[str]:
        u = str(x).upper()
        if "OVER" in u:
            return "OVER"
        if "UNDER" in u:
            return "UNDER"
        if u in ("YES", "Y"):
            return "YES"
        if u in ("NO", "N"):
            return "NO"
        return None
    odds["side"] = odds["outcome_name"].astype(str).map(_map_side)
    # Ensure team columns exist for event context
    for col in ("home_team","away_team"):
        if col not in odds.columns:
            odds[col] = None
    # Map markets to stat
    odds["stat"] = odds["market"].map(MARKET_TO_STAT)
    # Keep rows depending on market type: dd/td have no point/line.
    # Avoid DataFrame.apply(axis=1) here: it allocates per-row Series objects and can
    # blow up memory on large slates.
    base_ok = odds["name_key"].notna() & odds["stat"].notna() & odds["side"].notna()
    is_yesno = odds["stat"].isin(["dd", "td"])  # YES/NO markets (no point)
    price_ok = odds["price"].notna()
    point_ok = odds["point"].notna()
    odds = odds.loc[base_ok & price_ok & (is_yesno | point_ok)].copy()

    # Attach opening snapshot (sentiment proxy) when we have saved OddsAPI snapshots.
    # This is intentionally conservative: if no opening snapshot is available for a row,
    # we do nothing.
    if _env_bool("PROPS_SENTIMENT_ENABLE", True):
        try:
            if "event_id" in odds.columns and odds["event_id"].notna().any():
                opening = _load_opening_props_odds_for_date(target_date)
                if opening is not None and not opening.empty:
                    join_keys = ["event_id", "bookmaker", "market", "name_key", "side"]
                    ok_keys = [k for k in join_keys if k in odds.columns and k in opening.columns]
                    if len(ok_keys) == len(join_keys):
                        odds = odds.merge(opening, on=join_keys, how="left")
        except Exception:
            pass

    # Merge odds with predictions on name_key
    # Include optional per-player uncertainty columns (from SmartSim) when present.
    extra_cols = [c for c in ("sd_pts","sd_reb","sd_ast","sd_threes","sd_pra","sd_stl","sd_blk","sd_tov") if c in preds.columns]
    base_pred_cols = [pred_map[k] for k in ("pts", "reb", "ast", "threes", "pra", "stl", "blk", "tov") if pred_map.get(k) in preds.columns]
    # Include light context columns for projection sanity checks when available.
    sanity_cols = [
        c
        for c in (
            "injury_status",
            "lag1_min",
            "roll10_min",
            "roll10_pts",
            "roll10_reb",
            "roll10_ast",
            "roll10_threes",
        )
        if c in preds.columns
    ]
    merged = odds.merge(
        preds[["name_key", "player_id", "player_name", "team", *base_pred_cols, *extra_cols, *sanity_cols]],
        on="name_key", how="left", suffixes=("", "_pred")
    )
    # Second-pass resolve using short key for any unmatched players
    unmatched = merged[merged["player_id"].isna()].copy()
    if not unmatched.empty:
        # Merge by short key; manage name collisions with suffixes and prefer prediction player_name
        alt = odds.merge(
            preds[["short_key", "player_id", "player_name", "team", *base_pred_cols]].rename(columns={"short_key": "short_key_pred"}),
            left_on="short_key", right_on="short_key_pred", how="left", suffixes=("", "_pred")
        )
        # Consolidate player_name from predictions when available, else keep odds name
        if "player_name_pred" in alt.columns:
            alt["player_name_join"] = alt["player_name_pred"].fillna(alt.get("player_name"))
        else:
            alt["player_name_join"] = alt.get("player_name")
        keep = [
            "short_key", "player_id", "player_name_join", "team",
            *base_pred_cols,
        ]
        keep = [c for c in keep if c in alt.columns]
        alt = alt[keep].copy()
        alt = alt.rename(columns={"player_name_join": "player_name"})
        merged = merged.merge(alt.add_suffix("_alt"), left_on="short_key", right_on="short_key_alt", how="left")
        # Fill missing fields from alt
        for col in ["player_id", "player_name", "team", "model_mean"]:
            if col == "model_mean":
                # will be computed after selecting stat
                continue
            base_col = col
            alt_col = f"{col}_alt"
            if base_col in merged.columns and alt_col in merged.columns:
                merged[base_col] = merged[base_col].fillna(merged[alt_col])

    # Third-pass: roster-assisted resolution by event team context -> join predictions by player_id
    try:
        still_unmatched = merged[merged["player_id"].isna()].copy()
        if not still_unmatched.empty:
            # Load latest rosters file from processed
            roster = pd.DataFrame()
            try:
                proc = paths.data_processed
                # Prefer files like rosters_*.csv; pick most recent by modified time
                cands = sorted(proc.glob("rosters_*.csv"), key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)
                if not cands:
                    cands = sorted(proc.glob("*roster*.csv"), key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)
                if cands:
                    roster = pd.read_csv(cands[0])
            except Exception:
                roster = pd.DataFrame()
            if roster is not None and not roster.empty:
                # Normalize roster columns heuristically
                def _pick(col_opts):
                    for c in col_opts:
                        if c in roster.columns:
                            return c
                    return None
                name_col = _pick(["PLAYER","player","Player","NAME","name"]) or "PLAYER"
                id_col = _pick(["PLAYER_ID","player_id","PlayerID","id","ID"])
                team_col = _pick(["TEAM","team","Team","TEAM_ABBREVIATION","TEAM_ABBR","tricode","TRICODE","TEAM_TRICODE"])            
                cols = {}
                if name_col and name_col in roster.columns:
                    cols["player_name"] = roster[name_col].astype(str)
                if id_col and id_col in roster.columns:
                    cols["player_id"] = pd.to_numeric(roster[id_col], errors="coerce")
                if team_col and team_col in roster.columns:
                    cols["team"] = roster[team_col].astype(str)
                r = pd.DataFrame(cols)
                if not r.empty and ("player_name" in r.columns):
                    r["name_key"] = r["player_name"].astype(str).map(_norm_name)
                    r["short_key"] = r["player_name"].astype(str).map(_short_key)
                    # Join by name_key first
                    enrich = still_unmatched.merge(r[["name_key","player_id","team"]], on="name_key", how="left", suffixes=("","_r"))
                    # For remaining, try short_key
                    rem = enrich[enrich["player_id"].isna()].copy()
                    if not rem.empty:
                        enrich2 = rem.merge(r[["short_key","player_id","team"]].rename(columns={"short_key":"short_key_r"}), left_on="short_key", right_on="short_key_r", how="left")
                        for col in ("player_id","team"):
                            col_r = f"{col}_y"
                            if col in enrich.columns and col_r in enrich2.columns:
                                enrich.loc[rem.index, col] = enrich.loc[rem.index, col].fillna(enrich2[col_r])
                    # Team-tricode alignment: prefer roster match that aligns with event teams
                    def _to_tri_team(x: str | None) -> str:
                        try:
                            return _tri(_norm_team(str(x))) if x is not None else ""
                        except Exception:
                            return (str(x).strip().upper() if x else "")
                    if "home_team" in enrich.columns and "away_team" in enrich.columns:
                        enrich["home_tri"] = enrich["home_team"].astype(str).map(_to_tri_team)
                        enrich["away_tri"] = enrich["away_team"].astype(str).map(_to_tri_team)
                        enrich["team_tri_r"] = enrich.get("team").astype(str).map(_to_tri_team)
                        # In cases where roster team doesn't match either event team, null it out to avoid mis-join
                        ok_mask = (enrich["team_tri_r"] == enrich["home_tri"]) | (enrich["team_tri_r"] == enrich["away_tri"]) | (enrich["team_tri_r"] == "")
                        enrich.loc[~ok_mask, ["player_id","team"]] = np.nan
                    # Fill merged with roster-assisted ids/teams
                    for col in ["player_id","team"]:
                        if col in enrich.columns:
                            merged[col] = merged[col].fillna(enrich[col])
                    # If we resolved player_id, bring in prediction columns via id join
                    need = merged[merged["player_id"].notna()].index
                    if len(need) > 0:
                        pred_cols = ["player_id", "player_name", "team", *base_pred_cols]
                        pred_cols = [c for c in pred_cols if c in preds.columns]
                        by_id = preds[pred_cols].drop_duplicates("player_id")
                        merged = merged.merge(by_id.add_suffix("_pid"), left_on="player_id", right_on="player_id_pid", how="left")
                        # Backfill any missing prediction fields from the _pid columns
                        for col in ["player_name", "team", *base_pred_cols]:
                            base = col
                            aux = f"{col}_pid"
                            if base in merged.columns and aux in merged.columns:
                                merged[base] = merged[base].fillna(merged[aux])
    except Exception:
        pass
    # Choose model mean based on stat.
    # Avoid DataFrame.apply(axis=1): it allocates per-row Series objects and can
    # blow up memory on large slates.
    stat_s = merged.get("stat").astype(str).str.lower() if "stat" in merged.columns else pd.Series("", index=merged.index)

    def _num_col(col_name: str | None) -> pd.Series:
        if (not col_name) or (col_name not in merged.columns):
            return pd.Series(np.nan, index=merged.index, dtype="float64")
        return pd.to_numeric(merged[col_name], errors="coerce")

    model_mean = pd.Series(np.nan, index=merged.index, dtype="float64")

    # Derived combos from base predictions
    pts_v = _num_col(pred_map.get("pts"))
    reb_v = _num_col(pred_map.get("reb"))
    ast_v = _num_col(pred_map.get("ast"))
    m_pr = stat_s == "pr"
    if m_pr.any():
        model_mean.loc[m_pr] = (pts_v + reb_v).loc[m_pr]
    m_pa = stat_s == "pa"
    if m_pa.any():
        model_mean.loc[m_pa] = (pts_v + ast_v).loc[m_pa]
    m_ra = stat_s == "ra"
    if m_ra.any():
        model_mean.loc[m_ra] = (reb_v + ast_v).loc[m_ra]

    # Base stats: prefer direct prediction columns, with _alt fallback when present.
    for st in ("pts", "reb", "ast", "threes", "pra", "stl", "blk", "tov"):
        col = pred_map.get(st)
        base = _num_col(col)
        if col and (f"{col}_alt" in merged.columns):
            base = base.fillna(pd.to_numeric(merged[f"{col}_alt"], errors="coerce"))
        mask = stat_s == st
        if mask.any():
            model_mean.loc[mask] = base.loc[mask]

    merged["model_mean"] = model_mean

    # Projection sanity check: suppress clearly corrupted projections.
    # If the model mean is wildly inconsistent with recent rolling production given
    # non-trivial minutes, treat it as invalid (skip the prop).
    try:
        stat_s = merged.get("stat").astype(str).str.lower() if "stat" in merged.columns else None
        if stat_s is not None and "model_mean" in merged.columns:
            mm = pd.to_numeric(merged["model_mean"], errors="coerce")
            m10 = pd.to_numeric(merged.get("roll10_min"), errors="coerce") if "roll10_min" in merged.columns else None

            r10_pts = pd.to_numeric(merged.get("roll10_pts"), errors="coerce") if "roll10_pts" in merged.columns else None
            r10_reb = pd.to_numeric(merged.get("roll10_reb"), errors="coerce") if "roll10_reb" in merged.columns else None
            r10_ast = pd.to_numeric(merged.get("roll10_ast"), errors="coerce") if "roll10_ast" in merged.columns else None
            r10_threes = pd.to_numeric(merged.get("roll10_threes"), errors="coerce") if "roll10_threes" in merged.columns else None

            ref = pd.Series(np.nan, index=merged.index)
            if r10_pts is not None:
                ref.loc[stat_s == "pts"] = r10_pts.loc[stat_s == "pts"]
            if r10_reb is not None:
                ref.loc[stat_s == "reb"] = r10_reb.loc[stat_s == "reb"]
            if r10_ast is not None:
                ref.loc[stat_s == "ast"] = r10_ast.loc[stat_s == "ast"]
            if r10_threes is not None:
                ref.loc[stat_s == "threes"] = r10_threes.loc[stat_s == "threes"]
            if r10_pts is not None and r10_reb is not None:
                ref.loc[stat_s == "pr"] = (r10_pts + r10_reb).loc[stat_s == "pr"]
            if r10_pts is not None and r10_ast is not None:
                ref.loc[stat_s == "pa"] = (r10_pts + r10_ast).loc[stat_s == "pa"]
            if r10_reb is not None and r10_ast is not None:
                ref.loc[stat_s == "ra"] = (r10_reb + r10_ast).loc[stat_s == "ra"]
            if r10_pts is not None and r10_reb is not None and r10_ast is not None:
                ref.loc[stat_s == "pra"] = (r10_pts + r10_reb + r10_ast).loc[stat_s == "pra"]

            minutes_ok = pd.Series(True, index=merged.index)
            if m10 is not None:
                minutes_ok = m10.fillna(0.0) >= 10.0

            # Only enforce when recent production is meaningful.
            ref_ok = ref.fillna(0.0) >= 4.0
            insane = minutes_ok & ref_ok & (mm.fillna(0.0) < (0.35 * ref.fillna(0.0)))
            if insane.any():
                merged.loc[insane, "model_mean"] = np.nan
    except Exception:
        pass
    # Team-consistency guard: ensure the prediction team matches the event's home or away team (by tricode)
    try:
        def _to_tri_team(x: str | None) -> str:
            try:
                return _tri(_norm_team(str(x))) if x is not None else ""
            except Exception:
                return (str(x).strip().upper() if x else "")
        merged["team_tri"] = merged.get("team").astype(str).map(lambda x: _to_tri_team(x))
        merged["home_tri"] = merged.get("home_team").astype(str).map(lambda x: _to_tri_team(x))
        merged["away_tri"] = merged.get("away_team").astype(str).map(lambda x: _to_tri_team(x))
        # Only filter when we have a non-empty team_tri
        mask_ok = (merged["team_tri"] == "") | (merged["team_tri"].isna()) | (
            (merged["team_tri"] == merged["home_tri"]) | (merged["team_tri"] == merged["away_tri"]) )
        merged = merged[mask_ok].copy()
    except Exception:
        pass
    # Sigma by stat; combos derived assuming independence of components
    stat_s = merged.get("stat").astype(str).str.lower() if "stat" in merged.columns else pd.Series("", index=merged.index)

    def _safe_sd_series(col_name: str) -> pd.Series:
        if (not col_name) or (col_name not in merged.columns):
            return pd.Series(np.nan, index=merged.index, dtype="float64")
        s = pd.to_numeric(merged[col_name], errors="coerce").astype(float)
        s = s.where(np.isfinite(s))
        s = s.where((s > 0.05) & (s < 50.0))
        return s

    sd_pts = _safe_sd_series("sd_pts")
    sd_reb = _safe_sd_series("sd_reb")
    sd_ast = _safe_sd_series("sd_ast")
    sd_threes = _safe_sd_series("sd_threes")
    sd_pra = _safe_sd_series("sd_pra")
    sd_stl = _safe_sd_series("sd_stl")
    sd_blk = _safe_sd_series("sd_blk")
    sd_tov = _safe_sd_series("sd_tov")

    fallback_sig = {
        "pts": float(sigma.pts),
        "reb": float(sigma.reb),
        "ast": float(sigma.ast),
        "threes": float(sigma.threes),
        "pra": float(sigma.pra),
        "stl": float(sigma.stl),
        "blk": float(sigma.blk),
        "tov": float(sigma.tov),
        "pr": float(np.sqrt(float(sigma.pts) ** 2 + float(sigma.reb) ** 2)),
        "pa": float(np.sqrt(float(sigma.pts) ** 2 + float(sigma.ast) ** 2)),
        "ra": float(np.sqrt(float(sigma.reb) ** 2 + float(sigma.ast) ** 2)),
    }
    sig = stat_s.map(fallback_sig).astype(float)

    # Base stats: prefer simulated SDs when present/valid.
    for st, sd in (
        ("pts", sd_pts),
        ("reb", sd_reb),
        ("ast", sd_ast),
        ("threes", sd_threes),
        ("pra", sd_pra),
        ("stl", sd_stl),
        ("blk", sd_blk),
        ("tov", sd_tov),
    ):
        m = stat_s == st
        if m.any():
            sig.loc[m] = sd.loc[m].fillna(sig.loc[m])

    # Derived combos: combine component SDs (sim or fallback).
    m_pr = stat_s == "pr"
    if m_pr.any():
        s1 = sd_pts.fillna(float(sigma.pts))
        s2 = sd_reb.fillna(float(sigma.reb))
        sig.loc[m_pr] = np.sqrt(np.square(s1.loc[m_pr]) + np.square(s2.loc[m_pr]))
    m_pa = stat_s == "pa"
    if m_pa.any():
        s1 = sd_pts.fillna(float(sigma.pts))
        s2 = sd_ast.fillna(float(sigma.ast))
        sig.loc[m_pa] = np.sqrt(np.square(s1.loc[m_pa]) + np.square(s2.loc[m_pa]))
    m_ra = stat_s == "ra"
    if m_ra.any():
        s1 = sd_reb.fillna(float(sigma.reb))
        s2 = sd_ast.fillna(float(sigma.ast))
        sig.loc[m_ra] = np.sqrt(np.square(s1.loc[m_ra]) + np.square(s2.loc[m_ra]))

    merged["sigma"] = sig

    # Extra variance safety for higher-variance markets (empirically pts/pra have
    # been more overconfident recently). Inflating sigma shrinks probabilities
    # toward 0.5 in a line-aware way (less aggressive than a hard clamp).
    try:
        stat_s = merged.get("stat").astype(str).str.lower() if "stat" in merged.columns else None
        if stat_s is not None and "sigma" in merged.columns:
            sig = pd.to_numeric(merged["sigma"], errors="coerce")
            m_pts = stat_s == "pts"
            m_pra = stat_s == "pra"
            if m_pts.any():
                sig.loc[m_pts] = sig.loc[m_pts] * 1.12
            if m_pra.any():
                sig.loc[m_pra] = sig.loc[m_pra] * 1.15
            merged["sigma"] = sig
    except Exception:
        pass

    # Model probability for Over: P(X > line) under Normal(mean, sigma)
    merged["line"] = pd.to_numeric(merged.get("point"), errors="coerce")
    merged["price"] = pd.to_numeric(merged.get("price"), errors="coerce")
    # Compute model probability (vectorized); special handling for YES/NO markets (double/triple-double)
    stat_s = merged.get("stat").astype(str).str.strip().str.lower() if "stat" in merged.columns else pd.Series("", index=merged.index)
    side_s = merged.get("side").astype(str).str.strip().str.upper() if "side" in merged.columns else pd.Series("", index=merged.index)

    def _norm_cdf_vec(x: np.ndarray) -> np.ndarray:
        """Vectorized standard Normal CDF.

        Uses a fast erf approximation (Numerical Recipes) to avoid SciPy imports.
        Accuracy is sufficient for probability/EV calculations.
        """
        x = np.asarray(x, dtype=float)
        z = x / np.sqrt(2.0)
        t = 1.0 / (1.0 + 0.5 * np.abs(z))
        tau = t * np.exp(
            -z * z
            - 1.26551223
            + t
            * (
                1.00002368
                + t
                * (
                    0.37409196
                    + t
                    * (
                        0.09678418
                        + t
                        * (
                            -0.18628806
                            + t
                            * (
                                0.27886807
                                + t
                                * (
                                    -1.13520398
                                    + t
                                    * (
                                        1.48851587 + t * (-0.82215223 + t * 0.17087277)
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
        erf_approx = np.sign(z) * (1.0 - tau)
        cdf = 0.5 * (1.0 + erf_approx)
        return np.clip(cdf, 0.0, 1.0)

    n = int(len(merged))
    prob = np.full(n, np.nan, dtype=float)

    # OVER/UNDER markets
    mean_arr = pd.to_numeric(merged.get("model_mean"), errors="coerce").to_numpy(dtype=float, na_value=np.nan)
    sig_arr = pd.to_numeric(merged.get("sigma"), errors="coerce").to_numpy(dtype=float, na_value=np.nan)
    line_arr = pd.to_numeric(merged.get("line"), errors="coerce").to_numpy(dtype=float, na_value=np.nan)
    over_arr = np.full(n, np.nan, dtype=float)
    valid = np.isfinite(mean_arr) & np.isfinite(sig_arr) & np.isfinite(line_arr) & (sig_arr > 0)
    if bool(valid.any()):
        z = (line_arr[valid] - mean_arr[valid]) / sig_arr[valid]
        over_arr[valid] = 1.0 - _norm_cdf_vec(z)
    over_arr = np.clip(over_arr, 0.0, 1.0)

    is_yesno = stat_s.isin(["dd", "td"]).to_numpy(dtype=bool)
    side_over = (side_s == "OVER").to_numpy(dtype=bool)
    side_under = (side_s == "UNDER").to_numpy(dtype=bool)

    mask_ou = ~is_yesno
    if bool((mask_ou & side_over).any()):
        prob[mask_ou & side_over] = over_arr[mask_ou & side_over]
    if bool((mask_ou & side_under).any()):
        prob[mask_ou & side_under] = 1.0 - over_arr[mask_ou & side_under]

    # YES/NO markets (double-double / triple-double)
    if bool(is_yesno.any()):
        mean_pts = pd.to_numeric(merged.get(pred_map.get("pts") or ""), errors="coerce").to_numpy(dtype=float, na_value=np.nan)
        mean_reb = pd.to_numeric(merged.get(pred_map.get("reb") or ""), errors="coerce").to_numpy(dtype=float, na_value=np.nan)
        mean_ast = pd.to_numeric(merged.get(pred_map.get("ast") or ""), errors="coerce").to_numpy(dtype=float, na_value=np.nan)

        p10_pts = np.full(n, np.nan, dtype=float)
        p10_reb = np.full(n, np.nan, dtype=float)
        p10_ast = np.full(n, np.nan, dtype=float)

        if float(sigma.pts) > 0:
            ok = np.isfinite(mean_pts)
            if bool(ok.any()):
                z = (10.0 - mean_pts[ok]) / float(sigma.pts)
                p10_pts[ok] = 1.0 - _norm_cdf_vec(z)
        if float(sigma.reb) > 0:
            ok = np.isfinite(mean_reb)
            if bool(ok.any()):
                z = (10.0 - mean_reb[ok]) / float(sigma.reb)
                p10_reb[ok] = 1.0 - _norm_cdf_vec(z)
        if float(sigma.ast) > 0:
            ok = np.isfinite(mean_ast)
            if bool(ok.any()):
                z = (10.0 - mean_ast[ok]) / float(sigma.ast)
                p10_ast[ok] = 1.0 - _norm_cdf_vec(z)

        p_td_yes = p10_pts * p10_reb * p10_ast
        p_dd_yes = (p10_pts * p10_reb) + (p10_pts * p10_ast) + (p10_reb * p10_ast) - (p10_pts * p10_reb * p10_ast)

        stat_arr = stat_s.to_numpy(dtype=str)
        p_yes = np.full(n, np.nan, dtype=float)
        m_td = stat_arr == "td"
        if bool(m_td.any()):
            p_yes[m_td] = p_td_yes[m_td]
        m_dd = stat_arr == "dd"
        if bool(m_dd.any()):
            p_yes[m_dd] = p_dd_yes[m_dd]

        side_yes = (side_s == "YES").to_numpy(dtype=bool)
        side_no = (side_s == "NO").to_numpy(dtype=bool)
        if bool((is_yesno & side_yes).any()):
            prob[is_yesno & side_yes] = p_yes[is_yesno & side_yes]
        if bool((is_yesno & side_no).any()):
            prob[is_yesno & side_no] = 1.0 - p_yes[is_yesno & side_no]

    merged["model_prob"] = prob
    # Optional: calibrate probabilities using reliability bins / isotonic mapping.
    # This is intentionally opt-in because our default Normal(mean, sigma) probabilities
    # are already a derived distribution; applying a generic calibration curve can easily
    # distort the signal and wipe out edges.
    merged["model_prob_raw"] = merged["model_prob"]
    merged["_prob_calibrated"] = False
    if calibrate_prob:
        try:
            mp = pd.to_numeric(merged["model_prob"], errors="coerce")
            st = merged.get("stat").astype(str).str.lower() if "stat" in merged.columns else None
            cal = _load_props_prob_calibration() or {}
            by_stat_exists = bool((paths.data_processed / "props_prob_calibration_by_stat.json").exists())

            if st is not None and isinstance(cal, dict):
                out = mp.copy()
                applied = pd.Series(False, index=out.index)
                ps = cal.get("per_stat") if isinstance(cal.get("per_stat"), dict) else {}
                global_curve = cal.get("global") if isinstance(cal.get("global"), dict) else None

                for mk in sorted(set(st.dropna().astype(str).tolist())):
                    mk2 = str(mk).strip().lower()
                    if not mk2:
                        continue
                    mask = st == mk2
                    if not bool(mask.any()):
                        continue

                    used_per_stat = False
                    curve = ps.get(mk2) if isinstance(ps, dict) else None
                    if isinstance(curve, dict):
                        used_per_stat = True
                    else:
                        curve = global_curve
                    if not isinstance(curve, dict):
                        continue

                    xs = curve.get("x") or []
                    ys = curve.get("y") or []
                    if not (isinstance(xs, list) and isinstance(ys, list) and len(xs) >= 2 and len(xs) == len(ys)):
                        continue
                    x = np.asarray(xs, dtype=float)
                    y = np.asarray(ys, dtype=float)

                    vals = pd.to_numeric(out.loc[mask], errors="coerce").to_numpy(dtype=float)
                    ok = np.isfinite(vals)
                    if ok.any():
                        vals2 = np.clip(vals, 0.0, 1.0)
                        vals2[ok] = np.interp(vals2[ok], x, y)
                        out.loc[mask] = vals2
                        # Only mark as calibrated for the purpose of skipping additional guardrails
                        # when we're actually using the by-stat calibration artifact. When we're
                        # falling back to the legacy global calibration file, keep guardrails on.
                        if used_per_stat or bool(by_stat_exists):
                            applied.loc[mask] = True

                merged["model_prob"] = out
                merged.loc[applied.index, "_prob_calibrated"] = applied
            else:
                # Scalar fallback (should be rare): apply global curve only.
                merged["model_prob"] = mp.apply(
                    lambda v: _apply_props_prob_calibration(float(v)) if np.isfinite(float(v)) else np.nan
                )
                merged["_prob_calibrated"] = bool(by_stat_exists) and (merged["model_prob"].notna() & merged["model_prob_raw"].notna())
        except Exception:
            pass

    # Stat-specific safety: pts/pra have shown weaker separation recently; apply an
    # additional shrink toward 0.5 to avoid overstating edges in these markets.
    try:
        stat_s = merged.get("stat").astype(str).str.lower() if "stat" in merged.columns else None
        if stat_s is not None:
            k_by_stat = {"pts": 0.45, "pra": 0.50}
            for st, k in k_by_stat.items():
                mask = (stat_s == st) & (~pd.to_numeric(merged.get("_prob_calibrated"), errors="coerce").fillna(False).astype(bool))
                if mask.any():
                    p = pd.to_numeric(merged.loc[mask, "model_prob"], errors="coerce")
                    merged.loc[mask, "model_prob"] = (0.5 + float(k) * (p - 0.5)).clip(lower=0.0, upper=1.0)
    except Exception:
        pass

    # Hard safety clamp to avoid extreme/invalid probabilities dominating EV.
    try:
        merged["model_prob"] = pd.to_numeric(merged["model_prob"], errors="coerce").clip(lower=0.01, upper=0.99)
    except Exception:
        pass
    # Implied probability from American odds (vectorized).
    price_num = pd.to_numeric(merged.get("price"), errors="coerce")
    implied = pd.Series(np.nan, index=merged.index, dtype="float64")
    m_pos = price_num > 0
    if m_pos.any():
        implied.loc[m_pos] = 100.0 / (price_num.loc[m_pos] + 100.0)
    m_neg = price_num < 0
    if m_neg.any():
        implied.loc[m_neg] = (-price_num.loc[m_neg]) / ((-price_num.loc[m_neg]) + 100.0)
    merged["implied_prob"] = implied

    # Market-blend guardrail: for the combo markets we actually recommend most often,
    # shrink the model probability toward the market break-even probability.
    # This reduces blow-ups when the mean/variance estimate is off.
    try:
        stat_s = merged.get("stat").astype(str).str.lower() if "stat" in merged.columns else None
        if stat_s is not None:
            pm = pd.to_numeric(merged.get("model_prob"), errors="coerce")
            pi = pd.to_numeric(merged.get("implied_prob"), errors="coerce")
            if pm is not None and pi is not None:
                w_by_stat = {
                    "pa": 0.22,
                    "pr": 0.22,
                    "ra": 0.22,
                    # Light shrink for base ast/reb as a general safety.
                    "ast": 0.15,
                    "reb": 0.15,
                }
                for st, w in w_by_stat.items():
                    mask = (stat_s == st) & (~pd.to_numeric(merged.get("_prob_calibrated"), errors="coerce").fillna(False).astype(bool))
                    if mask.any():
                        pmm = pm.loc[mask].clip(0.0, 1.0)
                        pii = pi.loc[mask].clip(0.0, 1.0)
                        merged.loc[mask, "model_prob"] = (pii + float(w) * (pmm - pii)).clip(0.0, 1.0)
    except Exception:
        pass

    # PRA tuning: shrink toward the market break-even probability. Recent backtests
    # show PRA is often overconfident; this keeps signal but reduces edge magnitude.
    try:
        stat_s = merged.get("stat").astype(str).str.lower() if "stat" in merged.columns else None
        if stat_s is not None:
            mask = (stat_s == "pra") & (~pd.to_numeric(merged.get("_prob_calibrated"), errors="coerce").fillna(False).astype(bool))
            if mask.any():
                pm = pd.to_numeric(merged.loc[mask, "model_prob"], errors="coerce").clip(0.0, 1.0)
                pi = pd.to_numeric(merged.loc[mask, "implied_prob"], errors="coerce").clip(0.0, 1.0)
                w = 0.35
                merged.loc[mask, "model_prob"] = (pi + float(w) * (pm - pi)).clip(0.0, 1.0)
    except Exception:
        pass

    # --- Market movement "sentiment" shrink (pregame props) ---
    # If we have an opening snapshot for the same prop outcome and the market has
    # moved materially (line and/or price), shrink model_prob toward implied_prob.
    # This does NOT add alpha; it reduces overconfidence in high-information markets.
    merged["sentiment_strength"] = np.nan
    merged["sentiment_w"] = np.nan
    merged["sentiment_applied"] = False
    try:
        if _env_bool("PROPS_SENTIMENT_ENABLE", True) and ("open_price" in merged.columns or "open_line" in merged.columns):
            alpha = _env_float("PROPS_SENTIMENT_ALPHA", 0.22, lo=0.0, hi=1.0)
            w_max = _env_float("PROPS_SENTIMENT_MAX_SHRINK", 0.35, lo=0.0, hi=1.0)
            line_scale = _env_float("PROPS_SENTIMENT_LINE_SCALE", 1.5, lo=0.1, hi=10.0)
            prob_scale = _env_float("PROPS_SENTIMENT_PROB_SCALE", 0.06, lo=0.005, hi=0.5)

            mp = pd.to_numeric(merged.get("model_prob"), errors="coerce")
            ip = pd.to_numeric(merged.get("implied_prob"), errors="coerce")

            ol = pd.to_numeric(merged.get("open_line"), errors="coerce") if "open_line" in merged.columns else pd.Series(np.nan, index=merged.index)
            op = pd.to_numeric(merged.get("open_price"), errors="coerce") if "open_price" in merged.columns else pd.Series(np.nan, index=merged.index)
            if "open_price" in merged.columns:
                oip = pd.Series(np.nan, index=merged.index, dtype="float64")
                m_pos = op > 0
                if m_pos.any():
                    oip.loc[m_pos] = 100.0 / (op.loc[m_pos] + 100.0)
                m_neg = op < 0
                if m_neg.any():
                    oip.loc[m_neg] = (-op.loc[m_neg]) / ((-op.loc[m_neg]) + 100.0)
            else:
                oip = pd.Series(np.nan, index=merged.index)

            lm = (pd.to_numeric(merged.get("line"), errors="coerce") - ol) if "open_line" in merged.columns else pd.Series(np.nan, index=merged.index)
            pm = (ip - oip) if "open_price" in merged.columns else pd.Series(np.nan, index=merged.index)

            # Strength combines line and probability movement; NaNs contribute 0.
            strength = (lm.abs() / float(line_scale)).fillna(0.0) + (pm.abs() / float(prob_scale)).fillna(0.0)
            w = (float(alpha) * strength).clip(lower=0.0, upper=float(w_max))

            ok = mp.notna() & ip.notna() & (w > 1e-9)
            if ok.any():
                merged.loc[ok, "model_prob"] = (ip.loc[ok] + (1.0 - w.loc[ok]) * (mp.loc[ok] - ip.loc[ok])).clip(0.0, 1.0)
                merged.loc[ok, "sentiment_applied"] = True
            merged["sentiment_strength"] = strength
            merged["sentiment_w"] = w
            # Keep raw movement columns when available (useful for debugging / analysis)
            if "open_price" in merged.columns:
                merged["open_implied_prob"] = oip
            if "open_line" in merged.columns:
                merged["line_move"] = lm
            if "open_price" in merged.columns:
                merged["implied_move"] = pm
    except Exception:
        pass

    merged["edge"] = merged["model_prob"] - merged["implied_prob"]
    # EV per 1u stake (vectorized).
    try:
        price_num = pd.to_numeric(merged.get("price"), errors="coerce")
        mp = pd.to_numeric(merged.get("model_prob"), errors="coerce")
        ev = pd.Series(np.nan, index=merged.index, dtype="float64")
        m_pos = price_num > 0
        if m_pos.any():
            ev.loc[m_pos] = mp.loc[m_pos] * (price_num.loc[m_pos] / 100.0) - (1.0 - mp.loc[m_pos])
        m_neg = price_num < 0
        if m_neg.any():
            ev.loc[m_neg] = mp.loc[m_neg] * (100.0 / (-price_num.loc[m_neg])) - (1.0 - mp.loc[m_neg])
        merged["ev"] = ev
    except Exception:
        merged["ev"] = merged.apply(lambda r: _ev_per_unit(r["price"], r["model_prob"]), axis=1)

    # Ensure we have a player_name column available; prefer odds name, then prediction name
    if "player_name" not in merged.columns:
        if "player_name_pred" in merged.columns:
            merged["player_name"] = merged["player_name_pred"]
        elif "player_name_alt" in merged.columns:
            merged["player_name"] = merged["player_name_alt"]
        else:
            merged["player_name"] = None

    # Default bookmaker_title if missing
    if "bookmaker_title" not in merged.columns and "bookmaker" in merged.columns:
        merged["bookmaker_title"] = merged["bookmaker"].map(lambda b: "Bovada" if str(b).lower()=="bovada" else None)

    desired_cols = [
        # Odds snapshot timestamps (useful for documenting opening + movement)
        "snapshot_ts",
        "open_snapshot_ts",
        "player_id", "player_name", "team", "stat", "side", "line", "price", "implied_prob", "model_prob", "model_prob_raw", "edge", "ev", "bookmaker", "bookmaker_title", "commence_time",
        "home_team", "away_team",
        # Optional sentiment / market-movement columns (present when saved snapshots exist)
        "open_line", "open_price", "open_implied_prob", "line_move", "implied_move", "sentiment_strength", "sentiment_w", "sentiment_applied",
    ]
    out_cols = [c for c in desired_cols if c in merged.columns]
    if not out_cols:
        # If somehow nothing matches, return empty DataFrame to avoid exceptions
        return pd.DataFrame()
    out = merged[out_cols].copy()
    out.insert(0, "date", pd.to_datetime(target_date).date())
    # De-duplicate final edges across identical player/stat/side/line/price/bookmaker
    dedup_keys = [
        "date", "player_id", "player_name", "team", "stat", "side", "line", "price", "bookmaker", "bookmaker_title", "commence_time"
    ]
    keys = [c for c in dedup_keys if c in out.columns]
    if keys:
        out = out.drop_duplicates(subset=keys, keep="first").reset_index(drop=True)
    out.sort_values(["stat", "edge"], ascending=[True, False], inplace=True)
    return out


def calibrate_sigma_for_date(date: str, window_days: int = 30, min_rows: int = 200, defaults: Optional[SigmaConfig] = None) -> SigmaConfig:
    """Estimate sigma per stat from recent residuals in props_features.

    Uses saved features with actual targets (t_*) over [date - window_days, date - 1],
    predicts with current models, and computes stddev of residuals per stat.
    Falls back to defaults if not enough rows.
    """
    if defaults is None:
        defaults = SigmaConfig()
    try:
        from .props_train import _load_features as _load_props_features  # Optional; may not exist without sklearn
        df = _load_props_features().copy()
    except Exception:
        return defaults
    if "date" not in df.columns:
        return defaults
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    end = pd.to_datetime(date)
    start = end - pd.Timedelta(days=window_days)
    hist = df[(df["date"] >= start) & (df["date"] < end)].copy()
    if hist.empty:
        return defaults
    # Predict on this window
    try:
        from .props_onnx_pure import predict_props_pure_onnx
        preds = predict_props_pure_onnx(hist)
    except Exception:
        return defaults
    stats = {
        "pts": ("t_pts", "pred_pts"),
        "reb": ("t_reb", "pred_reb"),
        "ast": ("t_ast", "pred_ast"),
        "threes": ("t_threes", "pred_threes"),
        "pra": ("t_pra", "pred_pra"),
        # Additional stats if available
        "stl": ("t_stl", "pred_stl"),
        "blk": ("t_blk", "pred_blk"),
        "tov": ("t_tov", "pred_tov"),
    }
    sig = {}
    for k, (tgt, pr) in stats.items():
        if tgt in preds.columns and pr in preds.columns:
            y = pd.to_numeric(preds[tgt], errors="coerce")
            p = pd.to_numeric(preds[pr], errors="coerce")
            m = y.notna() & p.notna()
            if m.sum() >= min_rows:
                sig[k] = float((y[m] - p[m]).std(ddof=1))
    return SigmaConfig(
        pts=sig.get("pts", defaults.pts),
        reb=sig.get("reb", defaults.reb),
        ast=sig.get("ast", defaults.ast),
        threes=sig.get("threes", defaults.threes),
        pra=sig.get("pra", defaults.pra),
        stl=sig.get("stl", defaults.stl),
        blk=sig.get("blk", defaults.blk),
        tov=sig.get("tov", defaults.tov),
    )
