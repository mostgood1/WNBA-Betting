"""Compare two connected-realism evaluation runs.

Usage (example):
  python tools/compare_connected_realism.py \
    --base-json data/processed/connected_realism_summary_..._rotshock065_gate2b.json \
    --new-json  data/processed/connected_realism_summary_..._rotshock065_garbage100_tight4.json \
    --new-games-csv data/processed/connected_realism_games_..._rotshock065_garbage100_tight4.csv

Prints:
- mean metric deltas (new - base)
- garbage-time trigger counts inferred from new-games CSV, if provided
- guardrails activation/scale stats inferred from new-games CSV, if provided
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _print_means_delta(base: dict[str, Any], new: dict[str, Any]) -> None:
    base_means = base.get("means", {}) or {}
    new_means = new.get("means", {}) or {}
    keys = sorted(set(base_means) | set(new_means))

    print("MEANS delta (new - base):")
    any_diff = False
    for key in keys:
        vb = base_means.get(key)
        vn = new_means.get(key)
        if isinstance(vb, (int, float)) and isinstance(vn, (int, float)):
            delta = float(vn) - float(vb)
            if abs(delta) > 1e-12:
                any_diff = True
                print(f"  {key:32s} {delta:+.6f}  (base {vb:.6f} -> {vn:.6f})")

    if not any_diff:
        print("  (no diffs)")


def _print_garbage_triggers(new_games_csv: Path) -> None:
    applied_games = 0
    applied_sides = 0
    total_games = 0

    with new_games_csv.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_games += 1
            hs = float(row.get("home_garbage_time_shift_minutes") or 0.0)
            a_s = float(row.get("away_garbage_time_shift_minutes") or 0.0)
            if hs > 1e-9:
                applied_sides += 1
            if a_s > 1e-9:
                applied_sides += 1
            if hs > 1e-9 or a_s > 1e-9:
                applied_games += 1

    print(
        f"Garbage-time applied (inferred): games={applied_games}/{total_games} "
        f"sides={applied_sides}/{2 * total_games}"
    )


def _print_shift_triggers(new_games_csv: Path, *, home_col: str, away_col: str, label: str) -> None:
    applied_games = 0
    applied_sides = 0
    total_games = 0
    total_shift = 0.0

    with new_games_csv.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_games += 1
            hs = float(row.get(home_col) or 0.0)
            a_s = float(row.get(away_col) or 0.0)
            total_shift += float(hs + a_s)
            if hs > 1e-9:
                applied_sides += 1
            if a_s > 1e-9:
                applied_sides += 1
            if hs > 1e-9 or a_s > 1e-9:
                applied_games += 1

    mean_shift_side = (total_shift / float(applied_sides)) if applied_sides else 0.0
    print(
        f"{label} applied (inferred): games={applied_games}/{total_games} "
        f"sides={applied_sides}/{2 * total_games} mean_shift_side={mean_shift_side:.3f}"
    )


def _print_bool_triggers(new_games_csv: Path, *, col: str, label: str) -> None:
    applied_games = 0
    total_games = 0
    with new_games_csv.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_games += 1
            v = row.get(col)
            try:
                vv = float(v) if v not in (None, "") else 0.0
            except Exception:
                vv = 0.0
            if vv > 0.0:
                applied_games += 1

    print(f"{label}: games={applied_games}/{total_games}")


def _percentile(vals: list[float], p: float) -> float | None:
    if not vals:
        return None
    if p <= 0.0:
        return float(min(vals))
    if p >= 100.0:
        return float(max(vals))
    s = sorted(vals)
    n = len(s)
    # Nearest-rank style percentile (good enough for small diagnostics).
    k = int(round((float(p) / 100.0) * float(n - 1)))
    k = max(0, min(n - 1, k))
    return float(s[k])


def _print_guardrails_summary(new_games_csv: Path) -> None:
    total_games = 0
    enabled_games = 0
    mode_counts: dict[str, int] = {}

    num_cols = [
        "guard_total_mu_shift",
        "guard_margin_mu_shift",
        "guard_home_scale_mean",
        "guard_away_scale_mean",
        "guard_home_scale_max_abs_dev",
        "guard_away_scale_max_abs_dev",
    ]
    vals: dict[str, list[float]] = {c: [] for c in num_cols}

    with new_games_csv.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_games += 1
            try:
                ge = float(row.get("guard_enabled") or 0.0)
            except Exception:
                ge = 0.0
            if ge > 0.0:
                enabled_games += 1

            m = str(row.get("guard_mode") or "").strip() or "(blank)"
            mode_counts[m] = int(mode_counts.get(m, 0) + 1)

            for c in num_cols:
                v = _to_float(row.get(c))
                if v is not None:
                    vals[c].append(float(v))

    print(f"Guardrails enabled (inferred): games={enabled_games}/{total_games}")
    if mode_counts:
        modes = ", ".join([f"{k}={mode_counts[k]}" for k in sorted(mode_counts, key=lambda x: (-mode_counts[x], x))])
        print(f"Guardrails mode counts: {modes}")

    for c in num_cols:
        xs = vals.get(c) or []
        if not xs:
            continue
        mean = float(sum(xs) / float(len(xs)))
        p95 = _percentile(xs, 95.0)
        mx = float(max(xs))
        if p95 is None:
            print(f"{c}: mean={mean:.6f} max={mx:.6f}")
        else:
            print(f"{c}: mean={mean:.6f} p95={p95:.6f} max={mx:.6f}")


def _load_games_csv(path: Path) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            gid = str(row.get("game_id") or "").strip()
            if not gid:
                continue
            out[gid] = row
    return out


def _to_float(x: Any) -> float | None:
    try:
        v = float(x)
        return v if v == v else None
    except Exception:
        return None


def _print_game_level_deltas(base_games_csv: Path, new_games_csv: Path) -> None:
    base = _load_games_csv(base_games_csv)
    new = _load_games_csv(new_games_csv)
    gids = sorted(set(base) & set(new))

    if not gids:
        print("Game-level deltas: no overlapping game_ids")
        return

    def _delta(col: str, gid: str) -> float | None:
        vb = _to_float(base[gid].get(col))
        vn = _to_float(new[gid].get(col))
        if vb is None or vn is None:
            return None
        return vn - vb

    def _summarize(label: str, col: str, higher_is_better: bool) -> None:
        eps = 1e-12
        pos = neg = zero = miss = 0
        regressors: list[str] = []
        for gid in gids:
            d = _delta(col, gid)
            if d is None:
                miss += 1
                continue
            if abs(d) <= eps:
                zero += 1
                continue
            improved = d > 0 if higher_is_better else d < 0
            if improved:
                pos += 1
            else:
                neg += 1
                regressors.append(gid)

        print(f"{label}: improve={pos} regress={neg} zero={zero} missing={miss}")
        if regressors:
            # keep output short: only show first few IDs
            head = ",".join(regressors[:12])
            tail = "" if len(regressors) <= 12 else f" (+{len(regressors) - 12} more)"
            print(f"  regressors (game_id): {head}{tail}")

    print("Game-level deltas (new vs base):")
    _summarize("home_min_mae_topk", "home_min_mae_topk", higher_is_better=False)
    _summarize("away_min_mae_topk", "away_min_mae_topk", higher_is_better=False)
    _summarize("home_pts_mae_topk", "home_pts_mae_topk", higher_is_better=False)
    _summarize("away_pts_mae_topk", "away_pts_mae_topk", higher_is_better=False)
    _summarize("home_min_corr_topk", "home_min_corr_topk", higher_is_better=True)
    _summarize("away_min_corr_topk", "away_min_corr_topk", higher_is_better=True)
    _summarize("sim_pathology_30min_zerostat", "sim_pathology_30min_zerostat", higher_is_better=False)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--base-json", type=Path, required=True)
    p.add_argument("--new-json", type=Path, required=True)
    p.add_argument("--base-games-csv", type=Path)
    p.add_argument("--new-games-csv", type=Path)
    args = p.parse_args()

    base = _load_json(args.base_json)
    new = _load_json(args.new_json)

    print(f"Base: {args.base_json}")
    print(f"New : {args.new_json}")
    print(f"Games base/new: {base.get('games')} / {new.get('games')}")
    print()

    _print_means_delta(base, new)

    if args.new_games_csv is not None:
        print()
        _print_garbage_triggers(args.new_games_csv)
        # Optional extra diagnostics.
        try:
            with args.new_games_csv.open("r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                fieldnames = set((reader.fieldnames or []) if reader is not None else [])
            if "home_foul_trouble_shift_minutes" in fieldnames and "away_foul_trouble_shift_minutes" in fieldnames:
                _print_shift_triggers(
                    args.new_games_csv,
                    home_col="home_foul_trouble_shift_minutes",
                    away_col="away_foul_trouble_shift_minutes",
                    label="Foul-trouble",
                )
            if "event_level_used" in fieldnames:
                _print_bool_triggers(args.new_games_csv, col="event_level_used", label="Event-level used (inferred)")

            # Guardrails telemetry (if present in the evaluator output).
            if "guard_enabled" in fieldnames:
                _print_guardrails_summary(args.new_games_csv)
        except Exception:
            pass

    if args.base_games_csv is not None and args.new_games_csv is not None:
        print()
        _print_game_level_deltas(args.base_games_csv, args.new_games_csv)


if __name__ == "__main__":
    main()
