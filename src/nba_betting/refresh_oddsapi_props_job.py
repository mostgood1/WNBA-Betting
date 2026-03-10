from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import threading
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional

from .config import paths, reconcile_repo_data_to_active


CRON_META_PATH = paths.data_processed / ".cron_meta.json"


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
    return env


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
    try:
        from .player_logs import fetch_player_logs

        def _fetch() -> Any:
            return fetch_player_logs([season_str])

        df = _run_with_heartbeat(_fetch, heartbeat_cb)
    except Exception as exc:
        _append_traceback(log_file, exc)
        return False, f"fetch-player-logs failed: {exc}"

    try:
        if df is None or getattr(df, "empty", True):
            return False, "player_logs missing after fetch-player-logs"
    except Exception:
        return False, "player_logs missing after fetch-player-logs"
    if not any(path.exists() and path.stat().st_size > 0 for path in _active_player_logs_paths()):
        return False, "player_logs missing after fetch-player-logs"
    return True, None


def _compute_props_edges_direct(
    *,
    date_str: str,
    bookmakers: str,
    log_file: Path,
    heartbeat_cb: Callable[[], None],
) -> tuple[int, int, str | None]:
    _append_log(log_file, f"Computing props edges directly for {date_str}")
    try:
        import pandas as pd
        from .odds_api import filter_player_prop_bookmakers_df
        from .props_edges import SigmaConfig, compute_props_edges

        def _work() -> int:
            edges = compute_props_edges(
                date=date_str,
                sigma=SigmaConfig(),
                use_saved=True,
                mode="current",
                api_key=(os.environ.get("ODDS_API_KEY") or None),
                source="oddsapi",
                predictions_path=None,
                from_file_only=False,
                calibrate_prob=True,
            )
            if edges is None:
                edges = pd.DataFrame()
            if not edges.empty:
                edges = filter_player_prop_bookmakers_df(edges, bookmakers)
                edges = edges[(edges["edge"] >= 0.0) & (edges["ev"] >= 0.0)].copy()
                if "ev" in edges.columns:
                    edges.sort_values(["stat", "ev"], ascending=[True, False], inplace=True)
                else:
                    edges.sort_values(["stat", "edge"], ascending=[True, False], inplace=True)
                top = 1000
                if top and len(edges) > top:
                    per_stat = max(1, top // max(1, edges["stat"].nunique()))
                    edges = edges.groupby("stat", group_keys=False).head(per_stat)
                out_path = paths.data_processed / f"props_edges_{date_str}.csv"
                out_path.parent.mkdir(parents=True, exist_ok=True)
                edges.to_csv(out_path, index=False)
                return int(len(edges))
            return 0

        rows = int(_run_with_heartbeat(_work, heartbeat_cb))
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
    _append_log(log_file, f"Exporting props recommendations directly for {date_str}")
    try:
        def _work() -> int:
            rows, output = _export_props_recommendations_cards(date_str, None)
            _append_log(log_file, f"Wrote props recommendations to {output}")
            return int(rows)

        rows = int(_run_with_heartbeat(_work, heartbeat_cb))
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
        "edges_rows": 0,
        "recs_rows": 0,
        "snapshot_path": str(raw_fp),
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
            "edges_rows": state.get("edges_rows"),
            "recs_rows": state.get("recs_rows"),
            "snapshot": state.get("snapshot_path"),
            "snapshot_path": state.get("snapshot_path"),
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

        rc_edges: int | None = None
        if run_edges:
            player_logs_ok, player_logs_error = _ensure_player_logs_for_props_refresh(
                date_str=date_str,
                log_file=log_file,
                heartbeat_cb=_touch_progress,
            )
            state["heartbeat_at"] = _utcnow_iso()
            if not player_logs_ok:
                rc_edges = 1
                run_export = False
                state["rc_edges"] = int(rc_edges)
                state["rc_export"] = None if do_export else state.get("rc_export")
                state["error"] = player_logs_error
                state["phase"] = "finalizing"
                state["phase_started_at"] = _utcnow_iso()
                _persist_progress(running=True, ok=None)
            else:
                rc_edges, edges_rows, edges_error = _compute_props_edges_direct(
                    date_str=date_str,
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