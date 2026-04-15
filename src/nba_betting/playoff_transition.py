from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from .config import paths
from .features_enhanced import build_features_enhanced
from .player_logs import fetch_player_logs
from .props_features import build_props_features
from .props_train import train_props_models
from .schedule import fetch_schedule_2025_26
from .scrape_nba_api import current_season_end_year, fetch_games_nba_api
from .train_enhanced import train_models_enhanced


@dataclass(frozen=True)
class RegularSeasonWindow:
    season: str
    start: date
    end: date

    @property
    def days(self) -> int:
        return (self.end - self.start).days + 1


def _season_string(target_date: date) -> str:
    season_start = target_date.year if target_date.month >= 7 else target_date.year - 1
    return f"{season_start}-{(season_start + 1) % 100:02d}"


def _season_start_year(season: str) -> int:
    return int(str(season).split("-", 1)[0])


def _season_end_year(season: str) -> int:
    season_str = str(season).strip()
    if "-" not in season_str:
        return int(season_str)
    start_year, end_suffix = season_str.split("-", 1)
    century = int(start_year[:2]) * 100
    return century + int(end_suffix)


def _canon_game_id(value: Any) -> str:
    raw = str(value or "").strip()
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) == 8:
        return f"00{digits}"
    if len(digits) == 9:
        return f"0{digits}"
    return digits


def _is_regular_season_game_id(value: Any) -> bool:
    game_id = _canon_game_id(value)
    return len(game_id) >= 3 and game_id.startswith("002")


def _schedule_file_candidates(season: str) -> list[Path]:
    start_year, end_year = str(season).split("-", 1)
    end_year_full = f"20{end_year}"
    season_tag = f"{start_year}_{end_year}"
    return [
        paths.data_processed / f"schedule_{season_tag}.csv",
        paths.data_raw / f"schedule_{season_tag}.csv",
        paths.data_processed / f"schedule_{start_year}_{end_year_full}.csv",
        paths.data_raw / f"schedule_{start_year}_{end_year_full}.csv",
    ]


def _load_schedule(season: str) -> pd.DataFrame:
    for candidate in _schedule_file_candidates(season):
        if candidate.exists():
            return pd.read_csv(candidate)
    df = fetch_schedule_2025_26()
    out_path = _schedule_file_candidates(season)[0]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    return df


def resolve_regular_season_window(*, target_date: date, season: str | None = None) -> RegularSeasonWindow:
    season = str(season or _season_string(target_date)).strip()
    schedule = _load_schedule(season).copy()
    schedule["season_year"] = schedule.get("season_year", "").astype(str).str.strip()
    schedule["game_label"] = schedule.get("game_label", "").astype(str).str.strip()
    schedule["date_est"] = pd.to_datetime(schedule.get("date_est"), errors="coerce").dt.date
    schedule["is_regular_season"] = schedule.get("game_id", "").map(_is_regular_season_game_id)

    reg = schedule[
        (schedule["season_year"] == season)
        & schedule["is_regular_season"].astype(bool)
    ].copy()
    reg = reg[reg["date_est"].notna()].copy()
    if reg.empty:
        raise RuntimeError(f"No regular-season schedule rows found for {season}")

    start = reg["date_est"].min()
    end = min(reg["date_est"].max(), target_date - timedelta(days=1))
    if end < start:
        raise RuntimeError(f"Regular season has not completed before {target_date.isoformat()}")
    return RegularSeasonWindow(season=season, start=start, end=end)


def _load_games() -> pd.DataFrame:
    candidates = [
        paths.data_raw / "games_nba_api.csv",
        paths.data_processed / "games_nba_api.csv",
    ]
    for candidate in candidates:
        if candidate.exists():
            return pd.read_csv(candidate)
    raise FileNotFoundError("games_nba_api.csv not found")


def _refresh_games_for_window(window: RegularSeasonWindow) -> pd.DataFrame:
    target_end_year = _season_end_year(window.season)
    current_end = current_season_end_year(datetime.combine(window.end, datetime.min.time()))
    seasons_to_fetch = max(1, current_end - target_end_year + 1)
    return fetch_games_nba_api(
        last_n=seasons_to_fetch,
        with_periods=False,
        verbose=True,
        rate_delay=0.35,
        max_workers=4,
    )


def _games_cover_window(games: pd.DataFrame, window: RegularSeasonWindow) -> bool:
    if games.empty:
        return False
    dated = pd.to_datetime(games.get("date"), errors="coerce")
    if dated.isna().all():
        return False
    in_window = dated.dt.date.between(window.start, window.end)
    if not in_window.any():
        return False
    max_date = dated[in_window].max()
    return pd.notna(max_date) and max_date.date() >= window.end


def _regular_season_games(window: RegularSeasonWindow) -> pd.DataFrame:
    schedule = _load_schedule(window.season).copy()
    schedule["game_id_key"] = schedule.get("game_id", "").map(_canon_game_id)
    schedule["date_est"] = pd.to_datetime(schedule.get("date_est"), errors="coerce").dt.date
    schedule["game_label"] = schedule.get("game_label", "").astype(str).str.strip()
    schedule["season_year"] = schedule.get("season_year", "").astype(str).str.strip()
    schedule["is_regular_season"] = schedule.get("game_id", "").map(_is_regular_season_game_id)
    schedule = schedule[
        (schedule["season_year"] == window.season)
        & schedule["is_regular_season"].astype(bool)
        & (schedule["date_est"] >= window.start)
        & (schedule["date_est"] <= window.end)
    ].copy()
    if schedule.empty:
        raise RuntimeError(f"No regular-season schedule rows found inside {window.start}..{window.end}")

    games = _load_games().copy()
    if not _games_cover_window(games, window):
        games = _refresh_games_for_window(window).copy()
    games["game_id_key"] = games.get("game_id", "").map(_canon_game_id)
    games["date"] = pd.to_datetime(games.get("date"), errors="coerce")
    merged = games.merge(
        schedule[["game_id_key", "date_est"]].drop_duplicates(subset=["game_id_key"]),
        on="game_id_key",
        how="inner",
    )
    merged = merged[merged["date_est"].notna()].copy()
    merged = merged.sort_values("date")
    if merged.empty:
        raise RuntimeError(
            "No settled regular-season games available after joining games to schedule. "
            "Raw NBA API history may still be missing the target season."
        )
    return merged


def _run_python_script(script_path: Path, *args: str) -> dict[str, Any]:
    cmd = [sys.executable, str(script_path), *args]
    proc = subprocess.run(cmd, cwd=paths.root, capture_output=True, text=True, check=False)
    return {
        "command": cmd,
        "returncode": int(proc.returncode),
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def run_playoff_transition(
    *,
    target_date: date,
    season: str | None = None,
    refresh_player_logs: bool = False,
    retune_live_lens: bool = True,
) -> dict[str, Any]:
    window = resolve_regular_season_window(target_date=target_date, season=season)
    if refresh_player_logs or not (paths.data_processed / "player_logs.csv").exists():
        fetch_player_logs([window.season])

    games = _regular_season_games(window)
    features_df = build_features_enhanced(
        games,
        include_advanced_stats=True,
        include_injuries=True,
        season=_season_start_year(window.season),
    )
    game_metrics = train_models_enhanced(features_df, use_enhanced_features=True)

    props_features_df = build_props_features()
    props_models = train_props_models(alpha=1.0)

    tools_dir = paths.root / "tools"
    calibrate_games = _run_python_script(
        tools_dir / "calibrate_games_probability.py",
        "--days",
        str(window.days),
        "--end",
        window.end.isoformat(),
    )
    blend_games = _run_python_script(
        tools_dir / "games_blend.py",
        "--train-days",
        str(window.days),
        "--end",
        window.end.isoformat(),
    )
    calibrate_props = _run_python_script(
        tools_dir / "calibrate_props_probability_by_stat.py",
        "--days",
        str(window.days),
        "--end",
        window.end.isoformat(),
    )
    sweep_props_alpha = _run_python_script(
        tools_dir / "sweep_props_prob_calib_alpha.py",
        "--days",
        str(window.days),
        "--end",
        window.end.isoformat(),
    )

    live_lens = None
    if retune_live_lens:
        live_lens = _run_python_script(
            tools_dir / "daily_live_lens_tune.py",
            "--end",
            window.end.isoformat(),
            "--lookback-days",
            str(window.days),
            "--props-lookback-days",
            str(window.days),
            "--write-override",
        )

    summary = {
        "season": window.season,
        "regular_season_start": window.start.isoformat(),
        "regular_season_end": window.end.isoformat(),
        "regular_season_days": int(window.days),
        "games_trained": int(len(features_df)),
        "props_feature_rows": int(len(props_features_df)),
        "props_models": sorted(props_models.keys()),
        "game_metrics": game_metrics,
        "game_probability_calibration": calibrate_games,
        "game_market_blend": blend_games,
        "props_probability_calibration": calibrate_props,
        "props_calibration_alpha_sweep": sweep_props_alpha,
        "live_lens_retune": live_lens,
    }

    out_path = paths.data_processed / f"playoff_transition_{window.season}_{target_date.isoformat()}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["summary_path"] = str(out_path)
    return summary