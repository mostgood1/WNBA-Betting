from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import threading
import time
import traceback
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional

import pandas as pd

from .config import paths, reconcile_repo_data_to_active
from .player_names import normalize_player_name_key
from .teams import to_tricode


CRON_META_PATH = paths.data_processed / ".cron_meta.json"

_MODELED_ODDSAPI_PLAYER_PROP_MARKETS = frozenset(
    {
        "player_points",
        "player_rebounds",
        "player_assists",
        "player_points_rebounds_assists",
        "player_threes",
        "player_steals",
        "player_blocks",
        "player_turnovers",
        "player_points_rebounds",
        "player_points_assists",
        "player_rebounds_assists",
    }
)


def _utcnow_iso() -> str:
    return datetime.utcnow().isoformat()


def _append_log(log_file: Path, line: str) -> None:
    try:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        with log_file.open("a", encoding="utf-8", errors="ignore") as out:
            out.write(f"[{datetime.utcnow().isoformat(timespec='seconds')}] {line.rstrip()}\n")
    except Exception:
        pass


def _append_traceback(log_file: Path, exc: BaseException) -> None:
    try:
        tb = traceback.format_exc(limit=25)
        _append_log(log_file, f"Exception: {type(exc).__name__}: {exc}")
        if tb:
            log_file.parent.mkdir(parents=True, exist_ok=True)
            with log_file.open("a", encoding="utf-8", errors="ignore") as out:
                out.write(tb + "\n")
    except Exception:
        pass


def _count_csv_rows_quick(path: Optional[Path]) -> int:
    try:
        if not path or not path.exists() or not path.is_file():
            return 0
        newline_count = 0
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                newline_count += chunk.count(b"\n")
        return max(0, newline_count - 1)
    except Exception:
        return 0


def _read_csv_safe(path: Optional[Path]) -> pd.DataFrame:
    try:
        if not path or not path.exists() or not path.is_file() or path.stat().st_size <= 0:
            return pd.DataFrame()
        df = pd.read_csv(path)
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def _norm_player_key(value: Any) -> str:
    return normalize_player_name_key(value, case="lower")


def _safe_int(value: Any) -> int | None:
    try:
        parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        if pd.isna(parsed):
            return None
        return int(parsed)
    except Exception:
        return None


def _prediction_row_key(row: pd.Series, row_index: int) -> str:
    player_id = _safe_int(row.get("player_id"))
    if player_id is not None:
        return f"pid:{player_id}"
    team = to_tricode(str(row.get("team") or row.get("team_tri") or row.get("TEAM") or "")).strip().upper()
    player_key = _norm_player_key(row.get("player_name") or row.get("PLAYER_NAME") or row.get("player"))
    if team and player_key:
        return f"team:{team}:{player_key}"
    if player_key:
        return f"name:{player_key}"
    return f"row:{row_index}"


def _merge_props_prediction_frames(preferred_df: pd.DataFrame, fallback_df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    preferred = preferred_df.copy() if isinstance(preferred_df, pd.DataFrame) else pd.DataFrame()
    fallback = fallback_df.copy() if isinstance(fallback_df, pd.DataFrame) else pd.DataFrame()
    if preferred.empty and fallback.empty:
        return pd.DataFrame(), {
            "preferred_rows": 0,
            "fallback_rows": 0,
            "fallback_only_rows": 0,
            "merged_rows": 0,
        }

    preferred_cols = list(preferred.columns)
    fallback_cols = [col for col in fallback.columns if col not in preferred_cols]
    ordered_cols = preferred_cols + fallback_cols

    preferred = preferred.copy()
    fallback = fallback.copy()
    preferred["_merge_source"] = 0
    fallback["_merge_source"] = 1

    combined = pd.concat([preferred, fallback], ignore_index=True, sort=False)
    combined["_merge_row_key"] = [
        _prediction_row_key(combined.iloc[idx], idx) for idx in range(len(combined))
    ]

    preferred_keys = set(combined.loc[combined["_merge_source"] == 0, "_merge_row_key"].tolist())
    fallback_only_rows = int(
        len(
            combined[
                (combined["_merge_source"] == 1)
                & (~combined["_merge_row_key"].isin(preferred_keys))
            ]
        )
    )

    merged = combined.drop_duplicates(subset=["_merge_row_key"], keep="first").copy()
    merged = merged.drop(columns=["_merge_source", "_merge_row_key"], errors="ignore")
    if ordered_cols:
        merged = merged.reindex(columns=ordered_cols)

    return merged, {
        "preferred_rows": int(len(preferred_df)) if isinstance(preferred_df, pd.DataFrame) else 0,
        "fallback_rows": int(len(fallback_df)) if isinstance(fallback_df, pd.DataFrame) else 0,
        "fallback_only_rows": fallback_only_rows,
        "merged_rows": int(len(merged)),
    }


def _extract_modeled_snapshot_participants(snapshot_df: pd.DataFrame) -> pd.DataFrame:
    if snapshot_df is None or snapshot_df.empty:
        return pd.DataFrame(columns=["home_tri", "away_tri", "player_name", "player_key"])

    cols_lower = {str(col).lower(): col for col in snapshot_df.columns}
    name_col = next((cols_lower.get(name) for name in ("player_name", "player", "name")), None)
    home_col = next((cols_lower.get(name) for name in ("home_team", "home", "home_tri")), None)
    away_col = next((cols_lower.get(name) for name in ("away_team", "away", "away_tri")), None)
    market_col = next((cols_lower.get(name) for name in ("market", "market_key")), None)
    if not name_col or not home_col or not away_col:
        return pd.DataFrame(columns=["home_tri", "away_tri", "player_name", "player_key"])

    participants = snapshot_df.copy()
    if market_col:
        participants = participants[
            participants[market_col].astype(str).str.strip().str.lower().isin(_MODELED_ODDSAPI_PLAYER_PROP_MARKETS)
        ].copy()

    participants["player_name"] = participants[name_col].astype(str).str.strip()
    participants["player_key"] = participants["player_name"].map(_norm_player_key)
    participants["home_tri"] = participants[home_col].astype(str).map(lambda value: to_tricode(str(value or ""))).str.upper().str.strip()
    participants["away_tri"] = participants[away_col].astype(str).map(lambda value: to_tricode(str(value or ""))).str.upper().str.strip()
    participants = participants[
        (participants["player_key"] != "")
        & (participants["home_tri"] != "")
        & (participants["away_tri"] != "")
        & (participants["home_tri"] != participants["away_tri"])
    ].copy()
    participants = participants[["home_tri", "away_tri", "player_name", "player_key"]].drop_duplicates(
        subset=["home_tri", "away_tri", "player_key"],
        keep="first",
    )
    return participants.reset_index(drop=True)


def _prediction_player_keys_for_matchup(predictions_df: pd.DataFrame, home_tri: str, away_tri: str) -> set[str]:
    if predictions_df is None or predictions_df.empty or "player_name" not in predictions_df.columns:
        return set()
    team_col = "team" if "team" in predictions_df.columns else None
    if not team_col:
        return set()

    teams = predictions_df[team_col].astype(str).map(lambda value: to_tricode(str(value or ""))).str.upper().str.strip()
    mask = teams.isin({home_tri, away_tri})
    if "opponent" in predictions_df.columns:
        opponents = predictions_df["opponent"].astype(str).map(lambda value: to_tricode(str(value or ""))).str.upper().str.strip()
        mask &= opponents.isin({home_tri, away_tri})
    if not bool(mask.any()):
        return set()
    names = predictions_df.loc[mask, "player_name"].astype(str).map(_norm_player_key)
    return {name for name in names.tolist() if name}


def _load_smart_sim_player_keys_by_matchup(date_str: str) -> dict[tuple[str, str], set[str]]:
    coverage: dict[tuple[str, str], set[str]] = {}
    for path in sorted(paths.data_processed.glob(f"smart_sim_{date_str}_*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        stem_parts = path.stem.split("_")
        fallback_home = stem_parts[-2] if len(stem_parts) >= 5 else ""
        fallback_away = stem_parts[-1] if len(stem_parts) >= 5 else ""
        home_tri = to_tricode(str(payload.get("home") or fallback_home or "")).strip().upper()
        away_tri = to_tricode(str(payload.get("away") or fallback_away or "")).strip().upper()
        if not home_tri or not away_tri:
            continue
        players = payload.get("players") if isinstance(payload.get("players"), dict) else {}
        matchup_key = (home_tri, away_tri)
        names = coverage.setdefault(matchup_key, set())
        for side in ("home", "away"):
            rows = players.get(side) if isinstance(players, dict) else []
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                name_key = _norm_player_key(row.get("player_name") or row.get("name"))
                if name_key:
                    names.add(name_key)
    return coverage


def _collect_snapshot_coverage_gaps(
    snapshot_df: pd.DataFrame,
    predictions_df: pd.DataFrame,
    smart_sim_players_by_matchup: dict[tuple[str, str], set[str]],
) -> list[dict[str, Any]]:
    participants = _extract_modeled_snapshot_participants(snapshot_df)
    if participants.empty:
        return []

    prediction_cache: dict[tuple[str, str], set[str]] = {}
    gaps: list[dict[str, Any]] = []
    for row in participants.itertuples(index=False):
        matchup = (str(row.home_tri), str(row.away_tri))
        if matchup not in prediction_cache:
            prediction_cache[matchup] = _prediction_player_keys_for_matchup(predictions_df, matchup[0], matchup[1])
        pred_keys = prediction_cache.get(matchup, set())
        sim_keys = smart_sim_players_by_matchup.get(matchup, set())
        missing_prediction = row.player_key not in pred_keys
        missing_sim = row.player_key not in sim_keys
        if missing_prediction or missing_sim:
            gaps.append(
                {
                    "home_tri": matchup[0],
                    "away_tri": matchup[1],
                    "player_name": str(row.player_name),
                    "player_key": str(row.player_key),
                    "missing_prediction": bool(missing_prediction),
                    "missing_sim": bool(missing_sim),
                }
            )
    return gaps


def _smart_sim_output_path(date_str: str, home_tri: str, away_tri: str) -> Path:
    return paths.data_processed / f"smart_sim_{date_str}_{home_tri}_{away_tri}.json"


def _drop_smart_sim_outputs_for_matchups(date_str: str, matchups: set[tuple[str, str]], log_file: Path) -> int:
    removed = 0
    for home_tri, away_tri in sorted(matchups):
        target = _smart_sim_output_path(date_str, home_tri, away_tri)
        if not target.exists():
            continue
        try:
            target.unlink()
            removed += 1
            _append_log(log_file, f"Removed stale SmartSim artifact to force re-sim: {target.name}")
        except Exception as exc:
            _append_log(log_file, f"Failed removing SmartSim artifact {target.name}: {exc}")
    return removed


def _write_merged_props_predictions(
    pred_path: Path,
    preferred_df: pd.DataFrame,
    fallback_df: pd.DataFrame,
    *,
    fallback_label: str,
    log_file: Path,
) -> None:
    if fallback_df is None or fallback_df.empty or preferred_df is None or preferred_df.empty:
        return
    merged_df, merge_stats = _merge_props_prediction_frames(preferred_df, fallback_df)
    if merge_stats.get("fallback_only_rows", 0) <= 0 and int(len(merged_df)) == int(len(preferred_df)):
        return
    pred_path.parent.mkdir(parents=True, exist_ok=True)
    merged_df.to_csv(pred_path, index=False)
    _append_log(
        log_file,
        "Merged props predictions with "
        f"{fallback_label} baseline (fallback_only_rows={merge_stats.get('fallback_only_rows', 0)}, merged_rows={merge_stats.get('merged_rows', 0)})",
    )


def _cron_meta_update(kind: str, payload: dict[str, Any]) -> None:
    try:
        CRON_META_PATH.parent.mkdir(parents=True, exist_ok=True)
        base: dict[str, Any] = {}
        if CRON_META_PATH.exists():
            try:
                base = json.loads(CRON_META_PATH.read_text(encoding="utf-8", errors="ignore"))
                if not isinstance(base, dict):
                    base = {}
            except Exception:
                base = {}
        entry = dict(payload)
        entry["timestamp"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        base[f"last_{kind}"] = entry
        CRON_META_PATH.write_text(json.dumps(base, indent=2), encoding="utf-8")
    except Exception:
        pass


def _resolve_python() -> str:
    try:
        cand = os.environ.get("PYTHON")
        if cand and Path(cand).exists():
            return cand
        if sys.executable and Path(sys.executable).exists():
            return sys.executable
        venv = os.environ.get("VIRTUAL_ENV")
        if venv:
            py_win = Path(venv) / "Scripts" / "python.exe"
            py_posix = Path(venv) / "bin" / "python"
            if py_win.exists():
                return str(py_win)
            if py_posix.exists():
                return str(py_posix)
        py_win = paths.root / ".venv" / "Scripts" / "python.exe"
        py_posix = paths.root / ".venv" / "bin" / "python"
        if py_win.exists():
            return str(py_win)
        if py_posix.exists():
            return str(py_posix)
    except Exception:
        pass
    return "python"


def _worker_env() -> dict[str, str]:
    env = dict(os.environ)
    src_dir = str(paths.root / "src")
    existing = str(env.get("PYTHONPATH") or "").strip()
    env["PYTHONPATH"] = src_dir if not existing else f"{src_dir}{os.pathsep}{existing}"
    env.setdefault("PYTHONUNBUFFERED", "1")
    env.setdefault("OMP_NUM_THREADS", "1")
    env.setdefault("OMP_THREAD_LIMIT", "1")
    env.setdefault("OPENBLAS_NUM_THREADS", "1")
    env.setdefault("MKL_NUM_THREADS", "1")
    env.setdefault("NUMEXPR_NUM_THREADS", "1")
    return env


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(str(os.environ.get(name, str(default))).strip()))
    except Exception:
        return int(default)


def _env_timeout_s(name: str, default_s: float) -> float | None:
    try:
        raw = str(os.environ.get(name, str(default_s))).strip()
        value = float(raw)
    except Exception:
        value = float(default_s)
    return None if value <= 0 else float(value)


def _maybe_fetch_remote_processed(fname: str) -> Optional[Path]:
    try:
        allow = _env_bool("ALLOW_REMOTE_ARTIFACTS", False)
        if not allow:
            return None
        if not fname or "/" in fname or ".." in fname:
            return None
        out = paths.data_processed / fname
        if out.exists() and out.stat().st_size > 0:
            return out
        repo = os.environ.get("GITHUB_REPOSITORY") or "mostgood1/NBA-Betting"
        branch = os.environ.get("GIT_BRANCH") or os.environ.get("RENDER_GIT_BRANCH") or "main"
        url = f"https://raw.githubusercontent.com/{repo}/{branch}/data/processed/{fname}"
        try:
            from urllib.request import Request, urlopen
        except Exception:
            return None
        req = Request(
            url,
            headers={
                "User-Agent": "NBA-Betting/props-refresh-worker",
                "Accept": "text/csv,application/octet-stream,*/*",
            },
        )
        with urlopen(req, timeout=8) as resp:  # noqa: S310
            content = resp.read()
        if not content:
            return None
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("wb") as handle:
            handle.write(content)
        return out if out.exists() and out.stat().st_size > 0 else None
    except Exception:
        return None


def _run_with_heartbeat(
    work: Callable[[], Any],
    heartbeat_cb: Callable[[], None],
    *,
    heartbeat_every_s: float = 15.0,
) -> Any:
    stop_event = threading.Event()

    def _loop() -> None:
        while not stop_event.wait(max(1.0, float(heartbeat_every_s))):
            try:
                heartbeat_cb()
            except Exception:
                pass

    heartbeat_thread = threading.Thread(target=_loop, daemon=True)
    heartbeat_thread.start()
    try:
        return work()
    finally:
        stop_event.set()
        heartbeat_thread.join(timeout=1.0)
        try:
            heartbeat_cb()
        except Exception:
            pass


def _run_to_file(
    args: list[str],
    log_file: Path,
    *,
    cwd: Path,
    env: dict[str, str],
    timeout_s: float | None,
    heartbeat_cb: Callable[[], None] | None,
    heartbeat_every_s: float = 15.0,
) -> int:
    cmd_text = " ".join(shlex.quote(str(a)) for a in args)
    _append_log(log_file, f"$ {cmd_text}")
    start = time.time()
    last_heartbeat = start
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("a", encoding="utf-8", errors="ignore") as out:
        proc = subprocess.Popen(
            [str(a) for a in args],
            cwd=str(cwd),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=out,
            stderr=subprocess.STDOUT,
        )
        while True:
            try:
                return int(proc.wait(timeout=1.0))
            except subprocess.TimeoutExpired:
                now = time.time()
                if heartbeat_cb and (now - last_heartbeat) >= max(1.0, float(heartbeat_every_s)):
                    try:
                        heartbeat_cb()
                    except Exception:
                        pass
                    last_heartbeat = now
                if timeout_s is not None and (now - start) >= float(timeout_s):
                    try:
                        proc.kill()
                    except Exception:
                        pass
                    try:
                        proc.wait(timeout=5.0)
                    except Exception:
                        pass
                    _append_log(log_file, f"Command timed out after {int(timeout_s)}s")
                    return 124


def _active_player_logs_paths() -> list[Path]:
    return [
        paths.data_processed / "player_logs.parquet",
        paths.data_processed / "player_logs.csv",
    ]


def _file_is_fresh(path: Path, *, max_age_minutes: int) -> bool:
    try:
        if max_age_minutes <= 0:
            return path.exists() and path.stat().st_size > 0
        if not path.exists() or path.stat().st_size <= 0:
            return False
        age_s = max(0.0, time.time() - float(path.stat().st_mtime))
        return age_s <= (float(max_age_minutes) * 60.0)
    except Exception:
        return False


def _player_logs_ready(*, max_age_minutes: int) -> bool:
    for path in _active_player_logs_paths():
        if _file_is_fresh(path, max_age_minutes=max_age_minutes):
            return True
    return False


def _season_year_for_date(date_str: str) -> int:
    d = datetime.strptime(date_str, "%Y-%m-%d").date()
    return d.year if d.month >= 7 else (d.year - 1)


def _season_str_from_year(season_year: int) -> str:
    return f"{season_year}-{(season_year + 1) % 100:02d}"


def _ensure_player_logs_for_props_refresh(
    *,
    date_str: str,
    log_file: Path,
    heartbeat_cb: Callable[[], None],
) -> tuple[bool, str | None]:
    raw_max_age = (
        os.environ.get("REFRESH_PLAYER_LOGS_MAX_AGE_HOURS")
        or os.environ.get("DAILY_PLAYER_LOGS_MAX_AGE_HOURS")
        or "12"
    ).strip()
    try:
        max_age_minutes = int(max(0.0, float(raw_max_age) * 60.0))
    except Exception:
        max_age_minutes = 12 * 60

    if _player_logs_ready(max_age_minutes=max_age_minutes):
        return True, None

    if any(path.exists() and path.stat().st_size > 0 for path in _active_player_logs_paths()):
        return True, None

    allow_fetch_on_miss = (os.environ.get("REFRESH_PLAYER_LOGS_FETCH_ON_MISS") or "0").strip().lower() in {"1", "true", "yes"}
    if not allow_fetch_on_miss:
        return False, "player_logs not found; run fetch-player-logs"

    season_str = _season_str_from_year(_season_year_for_date(date_str))
    _append_log(log_file, f"Fetching player logs for {season_str}")
    py = _resolve_python()
    env = _worker_env()
    fetch_cmd = [str(py), "-m", "nba_betting.cli", "fetch-player-logs", "--seasons", season_str]
    rc = _run_to_file(
        fetch_cmd,
        log_file,
        cwd=paths.root,
        env=env,
        timeout_s=_env_timeout_s("REFRESH_PLAYER_LOGS_TIMEOUT_S", 20 * 60),
        heartbeat_cb=heartbeat_cb,
        heartbeat_every_s=5.0,
    )
    if int(rc) != 0:
        return False, f"fetch-player-logs failed with exit code {int(rc)}"
    if not any(path.exists() and path.stat().st_size > 0 for path in _active_player_logs_paths()):
        return False, "player_logs missing after fetch-player-logs"
    return True, None


def _predict_props_cli_args(*, date_str: str, out_path: Path, force_smart_sim: bool = False) -> list[str]:
    smart_sim_n_sims = max(1, _env_int("REFRESH_PREDICT_PROPS_SMART_SIM_N_SIMS", 150))
    smart_sim_workers = max(1, _env_int("REFRESH_PREDICT_PROPS_SMART_SIM_WORKERS", 1))
    calib_window = max(1, _env_int("REFRESH_PREDICT_PROPS_CALIB_WINDOW", 7))
    player_calib_window = max(1, _env_int("REFRESH_PREDICT_PROPS_PLAYER_CALIB_WINDOW", 30))
    player_min_pairs = max(1, _env_int("REFRESH_PREDICT_PROPS_PLAYER_MIN_PAIRS", 6))
    player_shrink_k = max(1, _env_int("REFRESH_PREDICT_PROPS_PLAYER_SHRINK_K", 8))
    args = [
        _resolve_python(),
        "-m",
        "nba_betting.cli",
        "predict-props",
        "--date",
        date_str,
        "--out",
        str(out_path),
        "--slate-only",
        "--calibrate",
        "--calib-window",
        str(calib_window),
        "--use-pure-onnx",
    ]
    if _env_bool("REFRESH_PREDICT_PROPS_CALIBRATE_PLAYER", True):
        args.extend(
            [
                "--calibrate-player",
                "--player-calib-window",
                str(player_calib_window),
                "--player-min-pairs",
                str(player_min_pairs),
                "--player-shrink-k",
                str(player_shrink_k),
            ]
        )
    else:
        args.append("--no-calibrate-player")
    if force_smart_sim or _env_bool("REFRESH_PREDICT_PROPS_USE_SMART_SIM", True):
        args.extend(
            [
                "--use-smart-sim",
                "--smart-sim-n-sims",
                str(smart_sim_n_sims),
                "--smart-sim-workers",
                str(smart_sim_workers),
            ]
        )
        args.append("--smart-sim-pbp" if _env_bool("REFRESH_PREDICT_PROPS_SMART_SIM_PBP", True) else "--no-smart-sim-pbp")
    else:
        args.append("--no-use-smart-sim")
    return args


def _generate_props_predictions_for_refresh(
    *,
    date_str: str,
    pred_path: Path,
    log_file: Path,
    heartbeat_cb: Callable[[], None],
    force_smart_sim: bool = False,
) -> tuple[Path | None, str | None]:
    player_logs_ok, player_logs_error = _ensure_player_logs_for_props_refresh(
        date_str=date_str,
        log_file=log_file,
        heartbeat_cb=heartbeat_cb,
    )
    if not player_logs_ok:
        return None, player_logs_error or f"player_logs missing before predict-props for {date_str}"

    env = _worker_env()
    predict_cmd = _predict_props_cli_args(date_str=date_str, out_path=pred_path, force_smart_sim=force_smart_sim)
    _append_log(
        log_file,
        f"Generating props predictions locally for {date_str}" + (" with forced SmartSim refresh" if force_smart_sim else ""),
    )
    rc = _run_to_file(
        predict_cmd,
        log_file,
        cwd=paths.root,
        env=env,
        timeout_s=_env_timeout_s("REFRESH_PREDICT_PROPS_TIMEOUT_S", 25 * 60),
        heartbeat_cb=heartbeat_cb,
        heartbeat_every_s=5.0,
    )
    rows = int(_count_csv_rows_quick(pred_path))
    if int(rc) != 0:
        return None, f"predict-props failed with exit code {int(rc)}"
    if rows <= 0:
        return None, f"predict-props completed without writing rows to {pred_path.name}"
    _append_log(log_file, f"Generated props predictions at {pred_path} (rows={rows})")
    return pred_path, None


def _ensure_props_predictions_for_refresh(
    date_str: str,
    log_file: Path,
    heartbeat_cb: Callable[[], None],
    snapshot_path: Path | None = None,
) -> tuple[Path | None, str | None]:
    pred_path = paths.data_processed / f"props_predictions_{date_str}.csv"
    snapshot_fp = snapshot_path or (paths.data_raw / f"odds_nba_player_props_{date_str}.csv")
    baseline_df = pd.DataFrame()
    baseline_label = ""

    try:
        if pred_path.exists() and pred_path.stat().st_size > 0 and _count_csv_rows_quick(pred_path) > 0:
            baseline_df = _read_csv_safe(pred_path)
            baseline_label = "active"
            _append_log(log_file, f"Using existing props predictions artifact: {pred_path}")
    except Exception:
        baseline_df = pd.DataFrame()
        baseline_label = ""

    if baseline_df.empty:
        try:
            repo_pred_path = paths.repo_data_processed / pred_path.name
            if repo_pred_path.exists() and repo_pred_path.stat().st_size > 0:
                pred_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(repo_pred_path, pred_path)
                if pred_path.exists() and pred_path.stat().st_size > 0 and _count_csv_rows_quick(pred_path) > 0:
                    baseline_df = _read_csv_safe(pred_path)
                    baseline_label = "repo"
                    _append_log(log_file, f"Copied props predictions artifact from repo data: {repo_pred_path}")
        except Exception:
            baseline_df = pd.DataFrame()
            baseline_label = baseline_label or ""

    if baseline_df.empty:
        fetched_path = _maybe_fetch_remote_processed(pred_path.name)
        try:
            if fetched_path and fetched_path.exists() and fetched_path.stat().st_size > 0 and _count_csv_rows_quick(fetched_path) > 0:
                baseline_df = _read_csv_safe(fetched_path)
                baseline_label = "remote"
                _append_log(log_file, f"Fetched props predictions artifact from GitHub raw: {fetched_path.name}")
        except Exception:
            baseline_df = pd.DataFrame()
            baseline_label = baseline_label or ""

    if not baseline_df.empty:
        try:
            snapshot_df = _read_csv_safe(snapshot_fp)
            smart_sim_players = _load_smart_sim_player_keys_by_matchup(date_str)
            coverage_gaps = _collect_snapshot_coverage_gaps(snapshot_df, baseline_df, smart_sim_players)
        except Exception as exc:
            coverage_gaps = []
            _append_log(log_file, f"Snapshot coverage-gap audit skipped: {exc}")

        if coverage_gaps and _env_bool("REFRESH_FORCE_RESIM_ON_NEW_PROPS", True):
            impacted_matchups = {
                (str(row.get("home_tri") or ""), str(row.get("away_tri") or ""))
                for row in coverage_gaps
                if row.get("home_tri") and row.get("away_tri")
            }
            preview_names = ", ".join(
                sorted({str(row.get("player_name") or "") for row in coverage_gaps if row.get("player_name")})[:6]
            )
            _append_log(
                log_file,
                "Detected line-bearing props lacking current predictions/sim coverage "
                f"(players={len(coverage_gaps)}, matchups={len(impacted_matchups)}, sample={preview_names or 'n/a'}); "
                "forcing targeted props rebuild.",
            )
            removed = _drop_smart_sim_outputs_for_matchups(date_str, impacted_matchups, log_file)
            if impacted_matchups and removed <= 0:
                _append_log(log_file, "No existing SmartSim files needed deletion; predict-props will rebuild missing matchup sims directly.")

            refreshed_path, refresh_error = _generate_props_predictions_for_refresh(
                date_str=date_str,
                pred_path=pred_path,
                log_file=log_file,
                heartbeat_cb=heartbeat_cb,
                force_smart_sim=True,
            )
            if refreshed_path is not None:
                refreshed_df = _read_csv_safe(refreshed_path)
                _write_merged_props_predictions(
                    pred_path,
                    refreshed_df,
                    baseline_df,
                    fallback_label=baseline_label or "daily-update",
                    log_file=log_file,
                )
                return pred_path, None
            _append_log(
                log_file,
                f"Targeted props rebuild failed; falling back to existing props predictions artifact. error={refresh_error}",
            )
        return pred_path, None

    generated_path, generated_error = _generate_props_predictions_for_refresh(
        date_str=date_str,
        pred_path=pred_path,
        log_file=log_file,
        heartbeat_cb=heartbeat_cb,
        force_smart_sim=False,
    )
    if generated_path is None:
        return None, generated_error
    return generated_path, None


def _compute_props_edges_direct(
    *,
    date_str: str,
    snapshot_path: Path,
    predictions_path: Path,
    bookmakers: str,
    log_file: Path,
    heartbeat_cb: Callable[[], None],
) -> tuple[int, int, str | None]:
    _append_log(log_file, f"Computing props edges via CLI for {date_str}")
    try:
        env = _worker_env()
        edges_cmd = [
            _resolve_python(),
            "-m",
            "nba_betting.cli",
            "props-edges",
            "--date",
            date_str,
            "--no-use-saved",
            "--mode",
            "current",
            "--source",
            "oddsapi",
            "--odds-path",
            str(snapshot_path),
            "--predictions-csv",
            str(predictions_path),
            "--file-only",
            "--calibrate-prob",
            "--no-slate-only",
            "--no-attach-opening-snapshot",
        ]
        if bookmakers:
            edges_cmd.extend(["--bookmakers", bookmakers])
        rc = _run_to_file(
            edges_cmd,
            log_file,
            cwd=paths.root,
            env=env,
            timeout_s=_env_timeout_s("REFRESH_PROPS_EDGES_TIMEOUT_S", 15 * 60),
            heartbeat_cb=heartbeat_cb,
            heartbeat_every_s=5.0,
        )
        rows = int(_count_csv_rows_quick(paths.data_processed / f"props_edges_{date_str}.csv"))
        if int(rc) != 0:
            return int(rc), rows, f"props-edges failed with exit code {int(rc)}"
        _append_log(log_file, f"Props edges CLI completed (rows={rows})")
        return 0, rows, None
    except Exception as exc:
        _append_traceback(log_file, exc)
        return 1, 0, f"props-edges failed: {exc}"


def _export_props_recommendations_cards(date_str: str, out_path: str | None, max_plus_odds: float = 125.0) -> tuple[int, Path]:
    import pandas as pd

    try:
        _ = pd.to_datetime(date_str).date()
    except Exception as exc:
        raise ValueError("Invalid date_str") from exc

    edges_path = paths.data_processed / f"props_edges_{date_str}.csv"
    preds_path = paths.data_processed / f"props_predictions_{date_str}.csv"

    edges_df = pd.read_csv(edges_path) if edges_path.exists() else pd.DataFrame()
    preds_df = pd.read_csv(preds_path) if preds_path.exists() else pd.DataFrame()

    cards: list[dict[str, Any]] = []
    if edges_df is None or edges_df.empty:
        if not preds_df.empty:
            for (player, team), group in preds_df.groupby(["player_name", "team"], dropna=False):
                model: dict[str, float] = {}
                for col, key in [
                    ("pred_pts", "pts"),
                    ("pred_reb", "reb"),
                    ("pred_ast", "ast"),
                    ("pred_threes", "threes"),
                    ("pred_stl", "stl"),
                    ("pred_blk", "blk"),
                    ("pred_tov", "tov"),
                    ("pred_pra", "pra"),
                ]:
                    if col in group.columns:
                        try:
                            vals = pd.to_numeric(group[col], errors="coerce").dropna()
                            if not vals.empty:
                                model[key] = float(vals.iloc[0])
                        except Exception:
                            pass
                cards.append({"player": player, "team": team, "plays": [], "ladders": [], "model": model})
    else:
        def _num(value: Any) -> float | None:
            try:
                return float(value)
            except Exception:
                return None

        def _is_regular_play(row: Any) -> bool:
            try:
                stat = str(row.get("stat") or row.get("market") or "").lower()
                side = str(row.get("side") or "").upper()
                if stat in {"dd", "td"}:
                    return False
                if side not in {"OVER", "UNDER"}:
                    return False
                price = pd.to_numeric(row.get("price"), errors="coerce")
                if not pd.notna(price):
                    return False
                mpo = float(max_plus_odds) if float(max_plus_odds) > 0 else 1e9
                if price < -150 or price > mpo:
                    return False
                if stat in {"pts", "pra"}:
                    edge_abs = pd.to_numeric(row.get("edge"), errors="coerce")
                    if not pd.notna(edge_abs) or abs(float(edge_abs)) < 0.15:
                        return False
                line = pd.to_numeric(row.get("line"), errors="coerce")
                return bool(pd.notna(line))
            except Exception:
                return False

        group_cols = [c for c in ["player_name", "team"] if c in edges_df.columns]
        for keys, group in edges_df.groupby(group_cols, dropna=False):
            if not isinstance(keys, tuple):
                keys = (keys,)
            player = keys[0] if len(keys) > 0 else None
            team = keys[1] if len(keys) > 1 else None
            filtered = group.copy()
            try:
                filtered = filtered[filtered.apply(_is_regular_play, axis=1)].copy()
            except Exception:
                pass
            filtered["ev_pct"] = pd.to_numeric(filtered.get("ev"), errors="coerce") * 100.0 if "ev" in filtered.columns else None
            plays: list[dict[str, Any]] = []
            for _, row in filtered.iterrows():
                ev_val = _num(row.get("ev"))
                plays.append(
                    {
                        "market": row.get("stat"),
                        "side": row.get("side"),
                        "line": _num(row.get("line")),
                        "price": _num(row.get("price")),
                        "edge": _num(row.get("edge")),
                        "ev": ev_val,
                        "ev_pct": (ev_val * 100.0) if ev_val is not None else None,
                        "book": row.get("bookmaker"),
                    }
                )
            cards.append({"player": player, "team": team, "plays": plays, "ladders": []})

    output = paths.data_processed / f"props_recommendations_{date_str}.csv" if not out_path else Path(out_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(cards).to_csv(output, index=False)
    return int(len(cards)), output


def _export_props_recommendations_direct(
    *,
    date_str: str,
    log_file: Path,
    heartbeat_cb: Callable[[], None],
) -> tuple[int, int, str | None]:
    _append_log(log_file, f"Exporting props recommendations in-process for {date_str}")
    try:
        rows = int(
            _run_with_heartbeat(
                lambda: _export_props_recommendations_cards(date_str, None)[0],
                heartbeat_cb,
                heartbeat_every_s=5.0,
            )
        )
        _append_log(log_file, f"Props recommendations export completed (rows={rows})")
        return 0, rows, None
    except Exception as exc:
        _append_traceback(log_file, exc)
        return 1, 0, f"export-props-recommendations failed: {exc}"


def _git_commit_and_push(msg: str) -> tuple[bool, str]:
    try:
        name = os.environ.get("GH_NAME") or os.environ.get("GIT_NAME")
        email = os.environ.get("GH_EMAIL") or os.environ.get("GIT_EMAIL") or "github-actions[bot]@users.noreply.github.com"
        if name:
            subprocess.run(["git", "config", "user.name", name], cwd=str(paths.root), check=False)
        if email:
            subprocess.run(["git", "config", "user.email", email], cwd=str(paths.root), check=False)

        try:
            branch = subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=str(paths.root), text=True).strip()
        except Exception:
            branch = "HEAD"
        if not branch or branch == "HEAD":
            branch = os.environ.get("GIT_BRANCH", "main")

        token = os.environ.get("GH_TOKEN") or os.environ.get("GIT_PAT") or os.environ.get("GITHUB_TOKEN") or os.environ.get("GH")
        origin = None
        push_url_set = False
        if token:
            try:
                origin = subprocess.check_output(["git", "remote", "get-url", "origin"], cwd=str(paths.root), text=True).strip()
            except Exception:
                origin = None
            if not origin:
                gh_repo = os.environ.get("GITHUB_REPOSITORY") or "mostgood1/NBA-Betting"
                origin = f"https://github.com/{gh_repo}.git"
                subprocess.run(["git", "remote", "add", "origin", origin], cwd=str(paths.root), check=False)
            url = str(origin or "")
            if url.startswith("git@github.com:"):
                url = f"https://github.com/{url.split(':', 1)[1]}"
            elif url.startswith("ssh://git@github.com/"):
                url = f"https://github.com/{url.split('github.com/', 1)[1]}"
            if url.startswith("https://"):
                without_scheme = url[len("https://"):]
                if "@" in without_scheme:
                    without_scheme = without_scheme.split("@", 1)[1]
                gh_user = os.environ.get("GH_USERNAME") or os.environ.get("GH_NAME") or os.environ.get("GIT_NAME") or "x-access-token"
                tokenized = f"https://{gh_user}:{token}@{without_scheme}"
                subprocess.run(["git", "remote", "set-url", "origin", tokenized], cwd=str(paths.root), check=False)
                subprocess.run(["git", "remote", "set-url", "--push", "origin", tokenized], cwd=str(paths.root), check=False)
                push_url_set = True
                origin = tokenized

        subprocess.run(["git", "add", "data/processed"], cwd=str(paths.root), check=False)
        subprocess.run(["git", "commit", "-m", msg, "--allow-empty"], cwd=str(paths.root), check=False)

        if not origin:
            return False, f"pushed to {branch}; remote=missing; push_url={'set' if push_url_set else 'default'}"

        rc = subprocess.run(["git", "push", "origin", f"HEAD:{branch}"], cwd=str(paths.root), check=False, capture_output=True, text=True)
        if rc.returncode == 0:
            return True, f"pushed to {branch}; push_url={'set' if push_url_set else 'default'}"
        stderr = (rc.stderr or "").strip().splitlines()
        err_snip = "\n".join(stderr[-3:])[:500]
        return False, f"git push failed rc={rc.returncode}; err='{err_snip}'"
    except Exception as exc:
        return False, f"git push error: {exc}"


def run_refresh_oddsapi_props_job(
    *,
    date_str: str,
    regions: str,
    bookmakers: str,
    markets: str,
    do_edges: bool,
    do_export: bool,
    do_push: bool,
    log_file: Path,
    started_at: str | None = None,
) -> None:
    started_iso = started_at or _utcnow_iso()
    raw_fp = paths.data_raw / f"odds_nba_player_props_{date_str}.csv"
    pred_fp = paths.data_processed / f"props_predictions_{date_str}.csv"
    edges_fp = paths.data_processed / f"props_edges_{date_str}.csv"
    rec_fp = paths.data_processed / f"props_recommendations_{date_str}.csv"
    state: dict[str, Any] = {
        "started_at": started_iso,
        "ended_at": None,
        "phase": "snapshot",
        "phase_started_at": started_iso,
        "heartbeat_at": _utcnow_iso(),
        "rc_snapshot": -1,
        "rc_edges": (-2 if do_edges else None),
        "rc_export": (-2 if do_export else None),
        "snapshot_rows": 0,
        "predictions_rows": 0,
        "edges_rows": 0,
        "recs_rows": 0,
        "snapshot_path": str(raw_fp),
        "predictions_path": str(pred_fp),
        "edges_path": str(edges_fp),
        "recs_path": str(rec_fp),
        "duration_s": None,
        "error": None,
    }
    started_ts = time.time()
    run_edges = bool(do_edges)
    run_export = bool(do_export)

    def _persist_progress(*, running: bool, ok: bool | None) -> None:
        payload = {
            "date": date_str,
            "regions": regions,
            "bookmakers": (bookmakers or None),
            "markets": markets,
            "run_edges": bool(run_edges),
            "run_export": bool(run_export),
            "run_push": bool(do_push),
            "running": bool(running),
            "ok": ok,
            "log_file": str(log_file),
            "started_at": state.get("started_at"),
            "ended_at": state.get("ended_at"),
            "phase": state.get("phase"),
            "phase_started_at": state.get("phase_started_at"),
            "heartbeat_at": state.get("heartbeat_at"),
            "rc_snapshot": state.get("rc_snapshot"),
            "rc_edges": state.get("rc_edges"),
            "rc_export": state.get("rc_export"),
            "snapshot_rows": state.get("snapshot_rows"),
            "predictions_rows": state.get("predictions_rows"),
            "edges_rows": state.get("edges_rows"),
            "recs_rows": state.get("recs_rows"),
            "snapshot": state.get("snapshot_path"),
            "snapshot_path": state.get("snapshot_path"),
            "predictions": state.get("predictions_path"),
            "predictions_path": state.get("predictions_path"),
            "edges": state.get("edges_path"),
            "edges_path": state.get("edges_path"),
            "recs": state.get("recs_path"),
            "recs_path": state.get("recs_path"),
            "duration_s": state.get("duration_s"),
        }
        if state.get("error"):
            payload["error"] = state.get("error")
        _cron_meta_update("refresh_oddsapi_props", payload)

    def _touch_progress() -> None:
        state["heartbeat_at"] = _utcnow_iso()
        _persist_progress(running=True, ok=None)

    try:
        try:
            reconcile = reconcile_repo_data_to_active()
            if isinstance(reconcile, dict) and not reconcile.get("skipped"):
                copied = reconcile.get("files_copied")
                if copied:
                    _append_log(log_file, f"Reconciled repo data to active data root (files_copied={copied})")
        except Exception:
            pass

        _append_log(log_file, f"Starting refresh_oddsapi_props worker for {date_str}")
        _persist_progress(running=True, ok=None)

        py = _resolve_python()
        env = _worker_env()

        snapshot_cmd = [str(py), "-m", "nba_betting.cli", "odds-snapshots-props", "--date", date_str, "--regions", regions]
        if bookmakers:
            snapshot_cmd += ["--bookmakers", bookmakers]
        if markets:
            snapshot_cmd += ["--markets", markets]
        rc_snapshot = _run_to_file(
            snapshot_cmd,
            log_file,
            cwd=paths.root,
            env=env,
            timeout_s=15 * 60,
            heartbeat_cb=_touch_progress,
        )
        state["heartbeat_at"] = _utcnow_iso()
        state["rc_snapshot"] = int(rc_snapshot)
        state["snapshot_rows"] = int(_count_csv_rows_quick(raw_fp))

        if int(state["snapshot_rows"] or 0) <= 0:
            run_edges = False
            run_export = False
            state["rc_edges"] = None if do_edges else state.get("rc_edges")
            state["rc_export"] = None if do_export else state.get("rc_export")
            state["phase"] = "finalizing"
            state["phase_started_at"] = _utcnow_iso()
        elif run_edges:
            state["phase"] = "edges"
            state["phase_started_at"] = _utcnow_iso()
            state["rc_edges"] = -1
        elif run_export:
            state["phase"] = "export"
            state["phase_started_at"] = _utcnow_iso()
            state["rc_export"] = -1
        else:
            state["phase"] = "finalizing"
            state["phase_started_at"] = _utcnow_iso()
        _persist_progress(running=True, ok=None)

        pred_path: Path | None = None
        if run_edges or run_export:
            state["phase"] = "predictions"
            state["phase_started_at"] = _utcnow_iso()
            _persist_progress(running=True, ok=None)

            pred_path, pred_error = _ensure_props_predictions_for_refresh(
                date_str=date_str,
                log_file=log_file,
                heartbeat_cb=_touch_progress,
                snapshot_path=raw_fp,
            )
            state["heartbeat_at"] = _utcnow_iso()
            state["predictions_rows"] = int(_count_csv_rows_quick(pred_fp))
            if pred_path is None:
                if run_edges:
                    state["rc_edges"] = 1
                    if do_export:
                        state["rc_export"] = None
                elif run_export:
                    state["rc_export"] = 1
                run_export = False
                run_edges = False
                state["error"] = pred_error
                state["phase"] = "finalizing"
                state["phase_started_at"] = _utcnow_iso()
                _persist_progress(running=True, ok=None)
            elif run_export and not run_edges:
                state["phase"] = "export"
                state["phase_started_at"] = _utcnow_iso()
                state["rc_export"] = -1
                _persist_progress(running=True, ok=None)

        rc_edges: int | None = None
        if run_edges and pred_path is not None:
            state["phase"] = "edges"
            state["phase_started_at"] = _utcnow_iso()
            state["rc_edges"] = -1
            _persist_progress(running=True, ok=None)
            rc_edges, edges_rows, edges_error = _compute_props_edges_direct(
                date_str=date_str,
                snapshot_path=raw_fp,
                predictions_path=pred_path,
                bookmakers=bookmakers,
                log_file=log_file,
                heartbeat_cb=_touch_progress,
            )
            state["heartbeat_at"] = _utcnow_iso()
            state["rc_edges"] = int(rc_edges)
            state["edges_rows"] = int(_count_csv_rows_quick(edges_fp)) if int(edges_rows) <= 0 else int(edges_rows)
            if int(rc_edges) != 0:
                run_export = False
                state["rc_export"] = None if do_export else state.get("rc_export")
                state["error"] = edges_error or f"props-edges failed with exit code {int(rc_edges)}"
            elif int(state["snapshot_rows"] or 0) > 0 and int(state["edges_rows"] or 0) <= 0:
                run_export = False
                state["rc_export"] = None if do_export else state.get("rc_export")
                state["error"] = "props-edges produced zero rows after a non-empty snapshot"
            if run_export:
                state["phase"] = "export"
                state["phase_started_at"] = _utcnow_iso()
                state["rc_export"] = -1
            else:
                state["phase"] = "finalizing"
                state["phase_started_at"] = _utcnow_iso()
            _persist_progress(running=True, ok=None)

        rc_export: int | None = None
        if run_export:
            rc_export, rec_rows, export_error = _export_props_recommendations_direct(
                date_str=date_str,
                log_file=log_file,
                heartbeat_cb=_touch_progress,
            )
            state["heartbeat_at"] = _utcnow_iso()
            state["rc_export"] = int(rc_export)
            state["recs_rows"] = int(_count_csv_rows_quick(rec_fp)) if int(rec_rows) <= 0 else int(rec_rows)
            if int(rc_export) != 0 and not state.get("error"):
                state["error"] = export_error or f"export-props-recommendations failed with exit code {int(rc_export)}"
            state["phase"] = "finalizing"
            state["phase_started_at"] = _utcnow_iso()
            _persist_progress(running=True, ok=None)

        state["snapshot_rows"] = int(_count_csv_rows_quick(raw_fp))
        state["predictions_rows"] = int(_count_csv_rows_quick(pred_fp))
        state["edges_rows"] = int(_count_csv_rows_quick(edges_fp))
        state["recs_rows"] = int(_count_csv_rows_quick(rec_fp))
        if run_edges and int(state["snapshot_rows"] or 0) > 0 and int(state["edges_rows"] or 0) <= 0 and not state.get("error"):
            state["error"] = "props-edges produced zero rows after a non-empty snapshot"

        ended_ts = time.time()
        state["phase"] = "done"
        state["phase_started_at"] = _utcnow_iso()
        state["heartbeat_at"] = _utcnow_iso()
        state["ended_at"] = _utcnow_iso()
        state["duration_s"] = float(max(0.0, ended_ts - started_ts))
        ok = (
            int(state.get("rc_snapshot") or 0) == 0
            and (rc_edges in (None, 0))
            and (rc_export in (None, 0))
            and not bool(state.get("error"))
        )
        _persist_progress(running=False, ok=bool(ok))

        if do_push:
            push_ok, push_detail = _git_commit_and_push(msg=f"refresh-oddsapi-props {date_str}")
            _append_log(log_file, f"git push ok={push_ok} detail={push_detail}")
    except Exception as exc:
        state["phase"] = "failed"
        state["phase_started_at"] = _utcnow_iso()
        state["heartbeat_at"] = _utcnow_iso()
        state["ended_at"] = _utcnow_iso()
        state["duration_s"] = float(max(0.0, time.time() - started_ts))
        state["error"] = f"{type(exc).__name__}: {exc}"
        _append_traceback(log_file, exc)
        _persist_progress(running=False, ok=False)


def main() -> int:
    payload_raw = (os.environ.get("NBA_BETTING_ODDSAPI_PROPS_JOB") or "").strip()
    if not payload_raw:
        print("missing NBA_BETTING_ODDSAPI_PROPS_JOB payload", file=sys.stderr)
        return 2
    try:
        payload = json.loads(payload_raw)
    except Exception as exc:
        print(f"invalid NBA_BETTING_ODDSAPI_PROPS_JOB payload: {exc}", file=sys.stderr)
        return 2
    log_file = Path(str(payload.get("log_file") or (paths.data_root / "logs" / f"refresh_oddsapi_props_{int(time.time())}.log")))
    run_refresh_oddsapi_props_job(
        date_str=str(payload.get("date_str") or ""),
        regions=str(payload.get("regions") or "us"),
        bookmakers=str(payload.get("bookmakers") or ""),
        markets=str(payload.get("markets") or ""),
        do_edges=bool(payload.get("do_edges")),
        do_export=bool(payload.get("do_export")),
        do_push=bool(payload.get("do_push")),
        log_file=log_file,
        started_at=str(payload.get("started_at") or "") or None,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())