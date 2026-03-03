import argparse
import json
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
_DATA_ROOT = os.environ.get("NBA_BETTING_DATA_ROOT")
DATA_ROOT = Path(_DATA_ROOT).expanduser() if _DATA_ROOT else (BASE_DIR / "data")
PROC_DIR = DATA_ROOT / "processed"

PRICE_MIN = -400.0
PRICE_MAX = 400.0

SUPPORTED_STATS = [
    "pts",
    "reb",
    "ast",
    "threes",
    "pra",
    "pr",
    "pa",
    "ra",
]


def _logloss(p: np.ndarray, y: np.ndarray) -> float:
    p = np.asarray(p, dtype=float)
    y = np.asarray(y, dtype=float)
    m = np.isfinite(p) & np.isfinite(y)
    if int(m.sum()) == 0:
        return float("nan")
    pp = np.clip(p[m], 1e-6, 1.0 - 1e-6)
    yy = y[m]
    return float(np.mean(-(yy * np.log(pp) + (1.0 - yy) * np.log(1.0 - pp))))


def _brier(p: np.ndarray, y: np.ndarray) -> float:
    p = np.asarray(p, dtype=float)
    y = np.asarray(y, dtype=float)
    m = np.isfinite(p) & np.isfinite(y)
    if int(m.sum()) == 0:
        return float("nan")
    pp = np.clip(p[m], 0.0, 1.0)
    yy = y[m]
    return float(np.mean((pp - yy) ** 2))


def _profit_per_unit(price: float) -> float:
    price = float(price)
    return (price / 100.0) if price > 0 else (100.0 / abs(price))


def _load_actuals() -> pd.DataFrame:
    act_parq = PROC_DIR / "props_actuals.parquet"
    if act_parq.exists():
        try:
            df = pd.read_parquet(act_parq)
            if isinstance(df, pd.DataFrame) and not df.empty:
                return df
        except Exception:
            pass
    frames: list[pd.DataFrame] = []
    for p in sorted(PROC_DIR.glob("props_actuals_*.csv")):
        try:
            df = pd.read_csv(p)
            if isinstance(df, pd.DataFrame) and not df.empty:
                frames.append(df)
        except Exception:
            continue
    if frames:
        return pd.concat(frames, ignore_index=True)

    # Fallback: daily reconciliation outputs
    for p in sorted(PROC_DIR.glob("recon_props_*.csv")):
        try:
            df = pd.read_csv(p)
            if isinstance(df, pd.DataFrame) and not df.empty:
                frames.append(df)
        except Exception:
            continue
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _iter_window_dates(days: int, end: date) -> list[date]:
    start = end - timedelta(days=days - 1)
    out: list[date] = []
    d = start
    while d <= end:
        out.append(d)
        d = d + timedelta(days=1)
    return out


def _actual_val(df: pd.DataFrame) -> pd.Series:
    stat = df["stat"].astype(str).str.lower().str.strip()
    out = pd.Series(np.nan, index=df.index, dtype=float)

    def _num(col: str) -> pd.Series:
        return pd.to_numeric(df.get(col), errors="coerce")

    if "pts" in df.columns:
        out.loc[stat == "pts"] = _num("pts")[stat == "pts"]
    if "reb" in df.columns:
        out.loc[stat == "reb"] = _num("reb")[stat == "reb"]
    if "ast" in df.columns:
        out.loc[stat == "ast"] = _num("ast")[stat == "ast"]
    if "threes" in df.columns:
        out.loc[stat == "threes"] = _num("threes")[stat == "threes"]
    if "pra" in df.columns:
        out.loc[stat == "pra"] = _num("pra")[stat == "pra"]

    # composites
    try:
        mask = stat == "pr"
        if mask.any() and {"pts", "reb"}.issubset(set(df.columns)):
            out.loc[mask] = _num("pts")[mask] + _num("reb")[mask]
    except Exception:
        pass
    try:
        mask = stat == "pa"
        if mask.any() and {"pts", "ast"}.issubset(set(df.columns)):
            out.loc[mask] = _num("pts")[mask] + _num("ast")[mask]
    except Exception:
        pass
    try:
        mask = stat == "ra"
        if mask.any() and {"reb", "ast"}.issubset(set(df.columns)):
            out.loc[mask] = _num("reb")[mask] + _num("ast")[mask]
    except Exception:
        pass

    return out


@dataclass
class Curve:
    x: np.ndarray
    y: np.ndarray


def _load_calibration(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _curve_from(obj: Any) -> Optional[Curve]:
    if not isinstance(obj, dict):
        return None
    xs = obj.get("x")
    ys = obj.get("y")
    if not (isinstance(xs, list) and isinstance(ys, list) and len(xs) >= 2 and len(xs) == len(ys)):
        return None
    try:
        x = np.asarray([float(v) for v in xs], dtype=float)
        y = np.asarray([float(v) for v in ys], dtype=float)
        if not (np.all(np.isfinite(x)) and np.all(np.isfinite(y))):
            return None
        if not (x[0] < x[-1]):
            return None
        if np.any(np.diff(x) < 0):
            return None
        return Curve(x=x, y=np.clip(y, 0.0, 1.0))
    except Exception:
        return None


def _apply_curve(p: np.ndarray, curve: Optional[Curve]) -> np.ndarray:
    pp = np.clip(np.asarray(p, dtype=float), 0.0, 1.0)
    if curve is None:
        return pp
    try:
        return np.clip(np.interp(pp, curve.x, curve.y), 0.0, 1.0)
    except Exception:
        return pp


def _merge_edges_actuals(*, edges: pd.DataFrame, actuals_day: pd.DataFrame) -> pd.DataFrame:
    if edges is None or edges.empty:
        return pd.DataFrame()

    need_base = {"date", "player_id", "stat", "side", "line", "price"}
    if not need_base.issubset(set(edges.columns)):
        return pd.DataFrame()

    prob_col = "model_prob_raw" if "model_prob_raw" in edges.columns else "model_prob"
    if prob_col not in edges.columns:
        return pd.DataFrame()

    e = edges.copy()
    e["date"] = pd.to_datetime(e["date"], errors="coerce").dt.date
    e["player_id"] = pd.to_numeric(e["player_id"], errors="coerce")
    e["stat"] = e["stat"].astype(str).str.lower().str.strip()
    e["side"] = e["side"].astype(str).str.upper().str.strip()
    e["line"] = pd.to_numeric(e["line"], errors="coerce")
    e["price"] = pd.to_numeric(e["price"], errors="coerce")
    e["p_raw"] = pd.to_numeric(e[prob_col], errors="coerce")

    e = e[(e["price"] >= PRICE_MIN) & (e["price"] <= PRICE_MAX)].copy()
    e = e[e["stat"].isin(SUPPORTED_STATS)].copy()
    e = e.dropna(subset=["date", "player_id", "stat", "side", "line", "price", "p_raw"]).copy()
    if e.empty:
        return pd.DataFrame()

    a = actuals_day.copy()
    a["date"] = pd.to_datetime(a["date"], errors="coerce").dt.date
    if "player_id" in a.columns:
        a["player_id"] = pd.to_numeric(a["player_id"], errors="coerce")

    merged = e.merge(a, on=["date", "player_id"], how="left", suffixes=("", "_act"))
    if merged is None or merged.empty:
        return pd.DataFrame()

    merged["actual_val"] = _actual_val(merged)
    merged = merged.dropna(subset=["actual_val"]).copy()
    if merged.empty:
        return pd.DataFrame()

    merged["hit"] = np.where(
        (merged["side"] == "OVER") & (merged["actual_val"] > merged["line"]),
        1,
        np.where(
            (merged["side"] == "UNDER") & (merged["actual_val"] < merged["line"]),
            1,
            np.where((merged["actual_val"] == merged["line"]), np.nan, 0),
        ),
    )
    merged = merged.dropna(subset=["hit"]).copy()
    if merged.empty:
        return pd.DataFrame()

    merged["unit_profit"] = merged["price"].astype(float).map(_profit_per_unit)
    merged["roi"] = np.where(merged["hit"] == 1, merged["unit_profit"], -1.0)

    return merged[["stat", "p_raw", "hit", "roi"]].copy()


def main() -> int:
    ap = argparse.ArgumentParser(description="Sweep runtime PROPS_PROB_CALIB_ALPHA against settled props")
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--end", type=str, default="")
    ap.add_argument("--step", type=float, default=0.05)
    ap.add_argument("--by-stat", action="store_true")
    ap.add_argument(
        "--calibration",
        type=str,
        default=str(PROC_DIR / "props_prob_calibration_by_stat.json"),
        help="Calibration JSON to evaluate (per-stat preferred).",
    )
    args = ap.parse_args()

    if str(args.end).strip():
        end = datetime.strptime(str(args.end).strip(), "%Y-%m-%d").date()
    else:
        end = date.today() - timedelta(days=1)

    cal_obj = _load_calibration(Path(str(args.calibration)))
    if not cal_obj:
        raise SystemExit("no-calibration")

    global_curve = _curve_from(cal_obj.get("global") if isinstance(cal_obj.get("global"), dict) else cal_obj)
    per_stat_curves: dict[str, Curve] = {}
    ps = cal_obj.get("per_stat")
    if isinstance(ps, dict):
        for k, v in ps.items():
            cv = _curve_from(v)
            if cv is not None:
                per_stat_curves[str(k).strip().lower()] = cv

    actuals = _load_actuals()
    if actuals is None or actuals.empty:
        raise SystemExit("no-actuals")
    actuals["date"] = pd.to_datetime(actuals["date"], errors="coerce").dt.date
    if "player_id" in actuals.columns:
        actuals["player_id"] = pd.to_numeric(actuals["player_id"], errors="coerce")

    rows: list[pd.DataFrame] = []
    for d in _iter_window_dates(int(args.days), end=end):
        ef = PROC_DIR / f"props_edges_{d}.csv"
        if not ef.exists():
            continue
        try:
            edges = pd.read_csv(ef)
        except Exception:
            continue
        a_day = actuals[actuals["date"] == d].copy()
        if a_day.empty:
            continue
        m = _merge_edges_actuals(edges=edges, actuals_day=a_day)
        if m is not None and not m.empty:
            rows.append(m)

    if not rows:
        print("NO_ROWS")
        return 0

    df = pd.concat(rows, ignore_index=True)
    df["stat"] = df["stat"].astype(str).str.lower().str.strip()

    # Compute fully calibrated probability (from curve) per row.
    p_raw = pd.to_numeric(df["p_raw"], errors="coerce").to_numpy(dtype=float)
    stats = df["stat"].astype(str).to_numpy()

    p_cal = np.clip(p_raw, 0.0, 1.0)
    if per_stat_curves:
        for st, cv in per_stat_curves.items():
            mask = stats == st
            if np.any(mask):
                p_cal[mask] = _apply_curve(p_raw[mask], cv)

    # Global fallback
    if global_curve is not None:
        mask = np.ones(len(df), dtype=bool)
        if per_stat_curves:
            mask = np.array([s not in per_stat_curves for s in stats], dtype=bool)
        if np.any(mask):
            p_cal[mask] = _apply_curve(p_raw[mask], global_curve)

    y = pd.to_numeric(df["hit"], errors="coerce").to_numpy(dtype=float)

    alphas = np.round(np.arange(0.0, 1.0 + 1e-9, float(args.step)), 4)

    def _eval(p: np.ndarray) -> dict[str, float]:
        return {
            "logloss": _logloss(p, y),
            "brier": _brier(p, y),
        }

    print(f"rows={len(df)} window_days={int(args.days)} end={end.isoformat()} step={float(args.step)}")
    base = _eval(p_raw)
    full = _eval(p_cal)
    print(f"raw:  logloss={base['logloss']:.6f} brier={base['brier']:.6f}")
    print(f"cal1: logloss={full['logloss']:.6f} brier={full['brier']:.6f}")

    best_a = None
    best_ll = None
    for a in alphas:
        p = (1.0 - float(a)) * p_raw + float(a) * p_cal
        ll = _logloss(p, y)
        if best_ll is None or ll < best_ll:
            best_ll = float(ll)
            best_a = float(a)

    print(f"best_alpha(identity_vs_curve)={float(best_a):.3f} best_logloss={float(best_ll):.6f}")

    if bool(args.by_stat):
        for st, g in df.groupby("stat", dropna=False):
            st_s = str(st).strip().lower()
            if not st_s:
                continue
            if int(len(g)) < 400:
                continue
            gi = g.index.to_numpy(dtype=int)
            y_g = y[gi]
            pr = p_raw[gi]
            pc = p_cal[gi]
            best_a_s = None
            best_ll_s = None
            for a in alphas:
                p = (1.0 - float(a)) * pr + float(a) * pc
                ll = _logloss(p, y_g)
                if best_ll_s is None or ll < best_ll_s:
                    best_ll_s = float(ll)
                    best_a_s = float(a)
            print(f"stat={st_s:8s} n={int(len(g)):6d} best_a={float(best_a_s):.2f} best_ll={float(best_ll_s):.5f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
