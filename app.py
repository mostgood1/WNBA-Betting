from __future__ import annotations

import base64
import os
from pathlib import Path
import sys

from flask import Flask, jsonify, redirect, request, send_from_directory
import threading
import subprocess
import shlex
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
import subprocess as _subp

import pandas as pd
import numpy as np

# Ensure local package in src/ is importable (for odds, schedule, CLI)
from pathlib import Path as _PathEarly
BASE_DIR = _PathEarly(__file__).resolve().parent
SRC_DIR = BASE_DIR / "src"
import sys as _sys_early
if str(SRC_DIR) not in _sys_early.path:
    _sys_early.path.insert(0, str(SRC_DIR))

try:
    # Optional (for scoreboard)
    from nba_api.stats.endpoints import scoreboardv2 as _scoreboardv2
    from nba_api.stats.library import http as _nba_http
except Exception:  # pragma: no cover
    _scoreboardv2 = None  # type: ignore
    _nba_http = None  # type: ignore

try:
    # local package for odds fetching (now importable due to early sys.path insert)
    from nba_betting.odds_bovada import (
        fetch_bovada_odds_current as _fetch_bovada_odds_current,
        fetch_bovada_player_props_current as _fetch_bovada_player_props_current,
        probe_bovada as _probe_bovada,
    )  # type: ignore
except Exception:  # pragma: no cover
    _fetch_bovada_odds_current = None  # type: ignore
    _fetch_bovada_player_props_current = None  # type: ignore
    _probe_bovada = None  # type: ignore

# Optional: load environment variables from a .env file if present
try:  # lightweight optional dependency
    from dotenv import load_dotenv  # type: ignore

    load_dotenv()
except Exception:
    pass

WEB_DIR = BASE_DIR / "web"
CRON_META_PATH = BASE_DIR / "data" / "processed" / ".cron_meta.json"
PLAYER_ID_CACHE_PATH = BASE_DIR / "data" / "processed" / "player_ids.csv"

# Serve the static frontend under /web, and serve the cards at '/'
app = Flask(__name__, static_folder=str(WEB_DIR), static_url_path="/web")


@app.route("/")
def root():
    # Serve the NBA slate homepage directly at '/'
    return send_from_directory(str(WEB_DIR), "index.html")


@app.route("/web/")
@app.route("/web/index.html")
def web_index():
    # Redirect legacy URL to root for a single canonical entrypoint
    return redirect("/")


@app.route("/web/<path:path>")
def web_static(path: str):
    # Serve any static asset in web/
    return send_from_directory(str(WEB_DIR), path)


@app.route("/data/<path:path>")
def data_static(path: str):
    # Serve data files (JSON/CSV) referenced by the frontend, e.g., /data/processed/*.json
    data_dir = BASE_DIR / "data"
    return send_from_directory(str(data_dir), path)

# Friendly routes to mirror NHL site paths (serve static HTML files)
@app.route("/recommendations")
def route_recommendations():
    return send_from_directory(str(WEB_DIR), "recommendations.html")

@app.route("/props")
def route_props():
    return send_from_directory(str(WEB_DIR), "props.html")

@app.route("/props/recommendations")
def route_props_recommendations():
    return send_from_directory(str(WEB_DIR), "props_recommendations.html")

@app.route("/props/reconciliation")
def route_props_reconciliation():
    return send_from_directory(str(WEB_DIR), "props_reconciliation.html")

@app.route("/reconciliation")
def route_reconciliation():
    return send_from_directory(str(WEB_DIR), "reconciliation.html")

@app.route("/odds-coverage")
def route_odds_coverage():
    return send_from_directory(str(WEB_DIR), "odds_coverage.html")


@app.route("/predictions_<date>.csv")
def serve_predictions_csv(date: str):
    """Serve predictions_YYYY-MM-DD.csv from data/processed (fallback root)."""
    processed = BASE_DIR / "data" / "processed" / f"predictions_{date}.csv"
    if processed.exists():
        return send_from_directory(str(processed.parent), processed.name)
    legacy = BASE_DIR / f"predictions_{date}.csv"
    if legacy.exists():
        return send_from_directory(str(BASE_DIR), legacy.name)
    from flask import abort
    abort(404)


@app.route("/props_predictions_<date>.csv")
def serve_props_predictions_csv(date: str):
    """Serve props_predictions_YYYY-MM-DD.csv from data/processed (fallback root)."""
    processed = BASE_DIR / "data" / "processed" / f"props_predictions_{date}.csv"
    if processed.exists():
        return send_from_directory(str(processed.parent), processed.name)
    legacy = BASE_DIR / f"props_predictions_{date}.csv"
    if legacy.exists():
        return send_from_directory(str(BASE_DIR), legacy.name)
    from flask import abort
    abort(404)


@app.route("/health")
def health():
    # Lightweight health/status
    try:
        exists = (WEB_DIR / "index.html").exists()
        return jsonify({"status": "ok", "have_index": bool(exists)}), 200
    except Exception as e:  # noqa: BLE001
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/api/version")
def api_version():
    """Return app version info (git SHA, branch) to verify deploy state."""
    try:
        sha = _subp.check_output(["git", "rev-parse", "HEAD"], cwd=str(BASE_DIR), text=True).strip()
    except Exception:
        sha = None
    try:
        branch = _subp.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=str(BASE_DIR), text=True).strip()
    except Exception:
        branch = None
    return jsonify(_to_jsonable({
        "sha": sha,
        "branch": branch,
        "time": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }))


@app.route("/favicon.ico")
def favicon():
    # 1x1 transparent PNG to avoid 404s
    png_b64 = (
        b"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Y6r/RwAAAAASUVORK5CYII="
    )
    png = base64.b64decode(png_b64)
    from flask import Response  # local import to keep module import light

    return Response(png, mimetype="image/png")


# ---------------- Shared helpers ---------------- #

# In-memory caches
_player_id_cache: dict[tuple[str, str | None], int] = {}
_rosters_df_cache: Optional[pd.DataFrame] = None
_team_name_to_abbr: Optional[dict[str, str]] = None
_team_abbr_to_id: Optional[dict[str, int]] = None

def _load_team_maps() -> dict[str, str]:
    global _team_name_to_abbr, _team_abbr_to_id
    if _team_name_to_abbr is not None:
        return _team_name_to_abbr
    mapping: dict[str, str] = {}
    abbr_to_id: dict[str, int] = {}
    try:
        from nba_api.stats.static import teams as _static_teams  # type: ignore
        team_list = _static_teams.get_teams()  # list of dicts
        for t in team_list:
            full = str(t.get("full_name") or "").strip()
            abbr = str(t.get("abbreviation") or "").strip().upper()
            tid = t.get("id")
            if full:
                mapping[full.lower()] = abbr
            if abbr:
                mapping[abbr.lower()] = abbr
                try:
                    if tid is not None:
                        abbr_to_id[abbr] = int(tid)
                except Exception:
                    pass
    except Exception:
        # Fallback: empty map; callers should handle by uppercasing input
        mapping = {}
        abbr_to_id = {}
    _team_name_to_abbr = mapping
    _team_abbr_to_id = abbr_to_id
    return mapping

def _get_tricode(team: str | None) -> str | None:
    if not team:
        return None
    m = _load_team_maps()
    abbr = m.get(str(team).strip().lower())
    return (abbr or str(team).strip().upper() or None)

def _get_team_id(team: str | None) -> Optional[int]:
    if not team:
        return None
    try:
        _ = _load_team_maps()  # ensures _team_abbr_to_id populated
    except Exception:
        pass
    try:
        tri = _get_tricode(team)
        if tri and _team_abbr_to_id and tri.upper() in _team_abbr_to_id:
            return int(_team_abbr_to_id[tri.upper()])
    except Exception:
        return None
    return None

def _ensure_rosters_loaded() -> pd.DataFrame:
    global _rosters_df_cache
    if _rosters_df_cache is not None:
        return _rosters_df_cache
    proc = BASE_DIR / "data" / "processed"
    frames: list[pd.DataFrame] = []
    try:
        for p in sorted(proc.glob("rosters_*.csv")):
            try:
                df = pd.read_csv(p)
                # Normalize expected columns
                cols = {c.upper(): c for c in df.columns}
                if ("PLAYER" in cols) and ("PLAYER_ID" in cols):
                    # Ensure TEAM_ABBREVIATION if possible
                    if "TEAM_ABBREVIATION" not in cols:
                        df["TEAM_ABBREVIATION"] = None
                    frames.append(df)
            except Exception:
                continue
    except Exception:
        frames = []
    if frames:
        _rosters_df_cache = pd.concat(frames, ignore_index=True)
    else:
        _rosters_df_cache = pd.DataFrame(columns=["PLAYER","PLAYER_ID","TEAM_ABBREVIATION"])  # empty
    return _rosters_df_cache

def _persist_player_id_cache_entry(name: str, team: str | None, pid: int) -> None:
    try:
        PLAYER_ID_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        # Append if unique
        rec = {"player_name": name, "team": (team or ""), "player_id": int(pid)}
        if PLAYER_ID_CACHE_PATH.exists():
            try:
                df = pd.read_csv(PLAYER_ID_CACHE_PATH)
            except Exception:
                df = pd.DataFrame()
            # If already present, skip append
            try:
                exists = False
                if not df.empty:
                    m = df[(df.get("player_name").astype(str) == str(name)) & (df.get("team").astype(str) == str(team or ""))]
                    exists = (len(m) > 0)
                if not exists:
                    df = pd.concat([df, pd.DataFrame([rec])], ignore_index=True)
                    df.to_csv(PLAYER_ID_CACHE_PATH, index=False)
            except Exception:
                # Fallback: append naive
                with PLAYER_ID_CACHE_PATH.open("a", encoding="utf-8") as f:
                    if PLAYER_ID_CACHE_PATH.stat().st_size == 0:
                        f.write("player_name,team,player_id\n")
                    f.write(f"{name},{team or ''},{pid}\n")
        else:
            pd.DataFrame([rec]).to_csv(PLAYER_ID_CACHE_PATH, index=False)
    except Exception:
        pass

def _resolve_player_id(name: str | None, team: str | None = None) -> Optional[int]:
    if not name:
        return None
    key = (str(name).strip(), (str(team).strip() if team else None))
    if key in _player_id_cache:
        return _player_id_cache[key]
    # Check on-disk cache first
    try:
        if PLAYER_ID_CACHE_PATH.exists():
            df = pd.read_csv(PLAYER_ID_CACHE_PATH)
            if not df.empty:
                m = df[(df.get("player_name").astype(str) == key[0]) & (df.get("team").astype(str) == (key[1] or ""))]
                if len(m) > 0:
                    pid = int(pd.to_numeric(m.iloc[0].get("player_id"), errors="coerce"))
                    _player_id_cache[key] = pid
                    return pid
    except Exception:
        pass
    # Search rosters
    roster = _ensure_rosters_loaded()
    pid: Optional[int] = None
    if not roster.empty and ("PLAYER" in roster.columns):
        try:
            cand = roster[roster["PLAYER"].astype(str).str.strip().str.lower() == key[0].lower()]
            if not cand.empty:
                if team and ("TEAM_ABBREVIATION" in cand.columns):
                    tri = _get_tricode(team) or (team.strip().upper())
                    cc = cand[cand["TEAM_ABBREVIATION"].astype(str).str.upper() == str(tri).upper()]
                    if not cc.empty:
                        pid = int(pd.to_numeric(cc.iloc[0].get("PLAYER_ID"), errors="coerce"))
                if pid is None:
                    pid = int(pd.to_numeric(cand.iloc[0].get("PLAYER_ID"), errors="coerce"))
        except Exception:
            pid = None
    # Fallback to nba_api static players lookup by name
    if pid is None:
        try:
            from nba_api.stats.static import players as _static_players  # type: ignore
            hits = _static_players.find_players_by_full_name(key[0]) or []
            if hits:
                # Prefer exact case-insensitive full name match
                exact = [h for h in hits if str(h.get("full_name","")).strip().lower() == key[0].lower()]
                pick = exact[0] if exact else hits[0]
                pid = int(pick.get("id")) if pick and pick.get("id") is not None else None
        except Exception:
            pid = None
    if isinstance(pid, int):
        _player_id_cache[key] = pid
        _persist_player_id_cache_entry(key[0], key[1], pid)
        return pid
    return None

def _parse_date_param(req, default_to_today: bool = True) -> str:
    val = (req.args.get("date") or req.args.get("d") or "").strip()
    if not val and default_to_today:
        try:
            return datetime.utcnow().date().isoformat()
        except Exception:
            return ""
    try:
        # normalize YYYY-MM-DD
        return pd.to_datetime(val).date().isoformat()
    except Exception:
        return val


def _read_csv_if_exists(path: Path) -> Optional[pd.DataFrame]:
    try:
        if path.exists():
            return pd.read_csv(path)
    except Exception:
        return None
    return None


def _number(x):
    try:
        if pd.isna(x):
            return None
        v = float(x)
        if not np.isfinite(v):
            return None
        return v
    except Exception:
        return None


def _json_primitive(x):
    """Convert numpy/pandas scalars and datetimes to native JSON-serializable types."""
    try:
        # None/str/bool/int/float already fine
        if x is None or isinstance(x, (str, bool, int, float)):
            return x
        # pandas NaN/NA
        try:
            if pd.isna(x):
                return None
        except Exception:
            pass
        # numpy scalar -> python scalar
        if isinstance(x, np.generic):
            try:
                return x.item()
            except Exception:
                pass
        # datetime-like -> ISO string
        if isinstance(x, (datetime,)):
            try:
                return x.isoformat()
            except Exception:
                return str(x)
        # pandas timestamp
        try:
            import pandas as _pd  # local alias
            if isinstance(x, _pd.Timestamp):
                return x.to_pydatetime().isoformat()
        except Exception:
            pass
        # numpy datetime64
        try:
            if isinstance(x, (np.datetime64,)):
                return str(x)
        except Exception:
            pass
        return x
    except Exception:
        return x


def _to_jsonable(obj):
    """Recursively convert container of dict/list/tuple/numpy/pandas scalars to JSONable primitives."""
    try:
        if isinstance(obj, dict):
            return {str(k): _to_jsonable(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple, set)):
            return [ _to_jsonable(v) for v in obj ]
        # numpy arrays -> lists
        try:
            if isinstance(obj, np.ndarray):
                return [ _to_jsonable(v) for v in obj.tolist() ]
        except Exception:
            pass
        return _json_primitive(obj)
    except Exception:
        return obj


def _normalize_team_str(x: str) -> str:
    # Best effort normalization: uppercase tricode if given, else strip
    try:
        return str(x).strip()
    except Exception:
        return str(x)


def _implied_prob_american(o: Any) -> Optional[float]:
    try:
        if o is None or (isinstance(o, float) and not np.isfinite(o)):
            return None
        o = float(o)
        if o == 0:
            return None
        if o > 0:
            return 100.0 / (o + 100.0)
        return (-o) / ((-o) + 100.0)
    except Exception:
        return None


def _american_to_b(o: Any) -> Optional[float]:
    try:
        o = float(o)
        return (o / 100.0) if o > 0 else (100.0 / abs(o))
    except Exception:
        return None


def _ev_from_prob_and_american(p: Optional[float], odds: Any) -> Optional[float]:
    if p is None:
        return None
    b = _american_to_b(odds)
    if b is None:
        return None
    try:
        return p * b - (1 - p)
    except Exception:
        return None


def _has_games_for_date(date_str: str, verbose: bool = False) -> bool:
    """Return True if there are NBA games on the given date.

    Preference order:
    1) nba_api ScoreboardV2 (if available)
    2) Bovada odds feed (if available)
    """
    # First try nba_api ScoreboardV2
    try:
        if _scoreboardv2 is not None:
            sb = _scoreboardv2.ScoreboardV2(game_date=date_str, day_offset=0, timeout=30)
            nd = sb.get_normalized_dict()
            gh = pd.DataFrame(nd.get("GameHeader", []))
            if not gh.empty and len(gh) > 0:
                return True
    except Exception as e:
        if verbose:
            print(f"[_has_games_for_date] scoreboard error: {e}")
    # Fallback to Bovada odds (pass date string for consistency)
    try:
        if _fetch_bovada_odds_current is not None:
            # Prefer passing the canonical YYYY-MM-DD string to avoid timezone/type pitfalls
            o = _fetch_bovada_odds_current(str(date_str), verbose=False)
            if isinstance(o, pd.DataFrame) and not o.empty:
                return True
    except Exception as e:
        if verbose:
            print(f"[_has_games_for_date] bovada error: {e}")
    # Last-resort: check processed schedule JSON (if present)
    try:
        sched = BASE_DIR / "data" / "processed" / "schedule_2025_26.json"
        if sched.exists():
            sdf = pd.read_json(sched)
            if not sdf.empty:
                if "date_utc" in sdf.columns:
                    sdf["date_utc"] = pd.to_datetime(sdf["date_utc"], errors="coerce").dt.date.astype(str)
                have = bool((sdf.get("date_utc") == str(date_str)).sum())
                if have:
                    return True
    except Exception:
        pass
    return False


# ---------------- Admin: daily update (mirrors NFL-Betting shape) ---------------- #
_job_state = {
    "running": False,
    "started_at": None,
    "ended_at": None,
    "ok": None,
    "logs": [],
    "log_file": None,
}


def _append_log(line: str) -> None:
    try:
        ts = datetime.utcnow().isoformat(timespec="seconds")
        msg = f"[{ts}] {line.rstrip()}"
        _job_state["logs"].append(msg)
        if len(_job_state["logs"]) > 1000:
            del _job_state["logs"][:-500]
        try:
            lf = _job_state.get("log_file")
            if lf:
                with open(lf, "a", encoding="utf-8", errors="ignore") as f:
                    f.write(msg + "\n")
        except Exception:
            pass
    except Exception:
        pass


def _ensure_logs_dir() -> Path:
    p = BASE_DIR / "logs"
    try:
        p.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    return p


def _count_csv_rows_quick(p: Optional[Path]) -> int:
    """Return number of data rows in a CSV quickly without loading via pandas.

    Counts newline characters and subtracts 1 for header if present. Returns 0 on error.
    """
    try:
        if not p or (not p.exists()) or (not p.is_file()):
            return 0
        # Fast newline count in chunks
        nl = 0
        with p.open('rb') as f:
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk:
                    break
                nl += chunk.count(b"\n")
        # If file has at least one line, subtract header
        return max(0, nl - 1)
    except Exception:
        return 0


def _find_fallback_odds_for_date(date_str: str) -> Optional[Path]:
    """Return best available non-Bovada odds file for the given date, if any.

    Preference order:
      1) closing_lines_{date}.csv
      2) odds_{date}.csv
      3) market_{date}.csv
    """
    candidates = [
        BASE_DIR / "data" / "processed" / f"closing_lines_{date_str}.csv",
        BASE_DIR / "data" / "processed" / f"odds_{date_str}.csv",
        BASE_DIR / "data" / "processed" / f"market_{date_str}.csv",
    ]
    for p in candidates:
        try:
            if p.exists() and p.is_file():
                return p
        except Exception:
            continue
    return None


def _git_commit_and_push(msg: str) -> tuple[bool, str]:
    """Commit and push changes to the current branch.

    Auth methods:
    - GH_TOKEN or GIT_PAT env: used to set a push URL for 'origin' without altering fetch URL.
    - GH_NAME/GH_EMAIL env: configure git user identity.
    Returns (ok, detail).
    """
    try:
        env = {**os.environ}
        # Configure identity if provided
        name = os.environ.get("GH_NAME") or os.environ.get("GIT_NAME")
        email = os.environ.get("GH_EMAIL") or os.environ.get("GIT_EMAIL") or "github-actions[bot]@users.noreply.github.com"
        if name:
            subprocess.run(["git", "config", "user.name", name], cwd=str(BASE_DIR), check=False)
        if email:
            subprocess.run(["git", "config", "user.email", email], cwd=str(BASE_DIR), check=False)
        # Determine current branch (detached HEAD -> use env/default)
        try:
            branch = subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=str(BASE_DIR), text=True).strip()
        except Exception:
            branch = "HEAD"
        if not branch or branch == "HEAD":
            branch = os.environ.get("GIT_BRANCH", "main")
        # Set push URL with token if present (support multiple env var names)
        token = (
            os.environ.get("GH_TOKEN")
            or os.environ.get("GIT_PAT")
            or os.environ.get("GITHUB_TOKEN")
            or os.environ.get("GH")
        )
        push_url_set = False
        origin = None
        if token:
            try:
                origin = subprocess.check_output(["git", "remote", "get-url", "origin"], cwd=str(BASE_DIR), text=True).strip()
            except Exception:
                origin = None
            try:
                # If origin missing, try to add it from environment
                if not origin:
                    # Prefer GIT_REMOTE_URL, else construct from GITHUB_REPOSITORY; finally project-specific default
                    env_url = os.environ.get("GIT_REMOTE_URL") or None
                    if not env_url:
                        gh_repo = os.environ.get("GITHUB_REPOSITORY")  # e.g., owner/repo
                        if gh_repo and "/" in gh_repo:
                            env_url = f"https://github.com/{gh_repo}.git"
                    # Project-specific fallback (safe for this deployment)
                    if not env_url:
                        env_url = "https://github.com/mostgood1/NBA-Betting.git"
                    if env_url:
                        subprocess.run(["git", "remote", "add", "origin", env_url], cwd=str(BASE_DIR), check=False)
                        origin = env_url
                url = origin or ""
                # Normalize SSH origin to HTTPS for token auth
                # e.g., git@github.com:owner/repo.git -> https://github.com/owner/repo.git
                if url.startswith("git@github.com:"):
                    path = url.split(":", 1)[1]
                    if not path.endswith(".git"):
                        path += ".git"
                    url = f"https://github.com/{path}"
                elif url.startswith("ssh://git@github.com/"):
                    path = url.split("github.com/", 1)[1]
                    if not path.endswith(".git"):
                        path += ".git"
                    url = f"https://github.com/{path}"
                elif url.startswith("https://"):
                    # already https
                    url = url
                # Embed token if https and not already credentialed
                if url.startswith("https://"):
                    # Strip any existing creds
                    without_scheme = url[len("https://"):]
                    if "@" in without_scheme:
                        without_scheme = without_scheme.split("@", 1)[1]
                    # Use a safe username for token auth
                    gh_user = (
                        os.environ.get("GH_USERNAME")
                        or os.environ.get("GH_NAME")
                        or os.environ.get("GIT_NAME")
                        or "x-access-token"
                    )
                    tokenized = f"https://{gh_user}:{token}@{without_scheme}"
                    try:
                        # Set both fetch and push URLs to ensure pulls work on private repos
                        subprocess.run(["git", "remote", "set-url", "origin", tokenized], cwd=str(BASE_DIR), check=False)
                        subprocess.run(["git", "remote", "set-url", "--push", "origin", tokenized], cwd=str(BASE_DIR), check=False)
                        push_url_set = True
                    except Exception:
                        push_url_set = False
            except Exception:
                push_url_set = False
        # Stage only data artifacts to avoid committing runtime files or secrets
        try:
            subprocess.run(["git", "add", "data/processed"], cwd=str(BASE_DIR), check=False)
            # Legacy root CSVs
            for pat in ("predictions_*.csv", "props_*.csv", "recon_*.csv"):
                subprocess.run(["bash", "-lc", f"git add -- {pat} 2>/dev/null || true"], cwd=str(BASE_DIR), check=False)
        except Exception:
            # Fallback to add -A only if selective add failed
            subprocess.run(["git", "add", "-A"], cwd=str(BASE_DIR), check=False)
        # Commit (allow empty to create a heartbeat commit if needed)
        subprocess.run(["git", "commit", "-m", msg, "--allow-empty"], cwd=str(BASE_DIR), check=False)
        # Optional pull --rebase only if on a real branch and remote exists
        try:
            if origin and branch and branch not in ("HEAD", ""):
                subprocess.run(["git", "pull", "--rebase", "origin", branch], cwd=str(BASE_DIR), check=False)
        except Exception:
            pass
        # Push if we have a remote
        ok = False
        if origin:
            rc_main = subprocess.run([
                "git", "push", "origin", f"HEAD:{branch}"
            ], cwd=str(BASE_DIR), check=False, capture_output=True, text=True)
            if rc_main.returncode == 0:
                ok = True
                detail = (
                    f"pushed to {branch}; remote={'ok' if bool(origin) else 'missing'}; "
                    f"push_url={'set' if push_url_set else 'default'}; rc_main={rc_main.returncode}"
                )
            else:
                # Fallback: push to an artifacts branch (for repos with protected main)
                alt_branch = os.environ.get("GIT_BRANCH_ALT", "data-artifacts")
                rc_alt = subprocess.run([
                    "git", "push", "origin", f"HEAD:{alt_branch}"
                ], cwd=str(BASE_DIR), check=False, capture_output=True, text=True)
                ok = (rc_alt.returncode == 0)
                # Heuristic error hint
                stderr_all = (rc_main.stderr or "") + "\n" + (rc_alt.stderr or "")
                hint = ""
                low = stderr_all.lower()
                if any(x in low for x in ["permission", "denied", "auth", "forbidden", "not allowed", "sso", "requires authentication"]):
                    hint = "; hint=auth/permission-denied-or-sso"
                if any(x in low for x in ["protected branch", "gh006", "pre-receive hook declined", "require signed commits", "status checks"]):
                    hint += "; hint=branch-protection"
                if any(x in low for x in ["non-fast-forward", "fetch first", "update rejected"]):
                    hint += "; hint=non-fast-forward (pull --rebase needed)"
                # Truncate stderr to last few lines for context
                lines = [ln for ln in (stderr_all.strip().splitlines() or [""]) if ln.strip()]
                err_snip = "\n".join(lines[-3:])[:500]
                detail = (
                    f"pushed to {branch if rc_main.returncode==0 else alt_branch} (fallback={'yes' if rc_main.returncode!=0 else 'no'}); "
                    f"remote={'ok' if bool(origin) else 'missing'}; push_url={'set' if push_url_set else 'default'}; "
                    f"rc_main={rc_main.returncode}; rc_alt={(rc_alt.returncode if 'rc_alt' in locals() else 'n/a')}{hint}; err='{err_snip}'"
                )
        else:
            detail = f"pushed to {branch}; remote=missing; push_url={'set' if push_url_set else 'default'}"
        return ok, detail
    except Exception as e:
        return False, f"git push error: {e}"


@app.route("/api/cron/git-diag", methods=["POST", "GET"])
def api_cron_git_diag():
    """Return git diagnostics to help debug push issues.

    Requires CRON_TOKEN or admin fallback. Does not expose secrets.
    """
    if not _cron_auth_ok(request):
        return jsonify({"error": "unauthorized"}), 401
    def _safe_run(cmd: list[str]) -> str:
        try:
            out = subprocess.check_output(cmd, cwd=str(BASE_DIR), text=True, stderr=subprocess.STDOUT)
            return out.strip()
        except Exception as e:
            return f"(error running {' '.join(cmd)}: {e})"
    data = {
        "have_gh_token": bool(os.environ.get("GH_TOKEN") or os.environ.get("GIT_PAT") or os.environ.get("GITHUB_TOKEN") or os.environ.get("GH")),
        "gh_name": os.environ.get("GH_NAME") or os.environ.get("GIT_NAME"),
        "gh_email": os.environ.get("GH_EMAIL") or os.environ.get("GIT_EMAIL"),
        "git_branch": _safe_run(["git", "rev-parse", "--abbrev-ref", "HEAD"]),
        "git_head": _safe_run(["git", "rev-parse", "HEAD"]),
        "remote_v": _safe_run(["git", "remote", "-v"]).splitlines()[-4:],
        "status": _safe_run(["git", "status", "-sb"]).splitlines()[:10],
        "branch_vv": _safe_run(["git", "branch", "-vv"]).splitlines()[-10:],
        "ls_remote_heads": _safe_run(["git", "ls-remote", "--heads", "origin"]).splitlines()[-10:],
    }
    return jsonify(data)


def _run_to_file(cmd: list[str] | str, log_fp: Path, cwd: Path | None = None, env: dict | None = None) -> int:
    if isinstance(cmd, list):
        popen_cmd = cmd
    else:
        popen_cmd = shlex.split(cmd)
    with log_fp.open("a", encoding="utf-8", errors="ignore") as out:
        out.write(f"[{datetime.utcnow().isoformat(timespec='seconds')}] Starting: {' '.join(popen_cmd)}\n")
        out.flush()
        proc = subprocess.Popen(
            popen_cmd,
            cwd=str(cwd) if cwd else None,
            env={**os.environ, **(env or {})},
            stdout=out,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
        )
        proc.wait()
        out.write(f"[{datetime.utcnow().isoformat(timespec='seconds')}] Exited with code {proc.returncode}\n")
        out.flush()
        return int(proc.returncode)


def _ensure_game_models(log_fp: Path | None = None) -> tuple[bool, dict]:
    """Ensure core game models exist; if missing, build features (if needed) and train.

    Returns (ok, info) where info includes rc_build/rc_train and paths.
    """
    try:
        models_dir = BASE_DIR / "models"
        need = [
            models_dir / "win_prob.joblib",
            models_dir / "spread_margin.joblib",
            models_dir / "totals.joblib",
            models_dir / "halves_models.joblib",
            models_dir / "quarters_models.joblib",
            models_dir / "feature_columns.joblib",
        ]
        have_all = all(p.exists() for p in need)
        if have_all:
            return True, {"skipped": True}
        # Choose python exe
        py = os.environ.get("PYTHON", (os.environ.get("VIRTUAL_ENV") or "") + "/bin/python")
        if not py or not Path(str(py)).exists():
            py_win = (Path(os.environ.get("VIRTUAL_ENV") or "") / "Scripts" / "python.exe")
            py = str(py_win) if py_win.exists() else "python"
        env = {"PYTHONPATH": str(SRC_DIR)}
        logs_dir = _ensure_logs_dir(); stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        lf = Path(log_fp) if log_fp else (logs_dir / f"cron_train_autofix_{stamp}.log")
        # Build features only if missing
        feats = BASE_DIR / "data" / "processed" / "features.parquet"
        rc_build = 0
        if not feats.exists():
            rc_build = _run_to_file([str(py), "-m", "nba_betting.cli", "build-features"], lf, cwd=BASE_DIR, env=env)
        rc_train = _run_to_file([str(py), "-m", "nba_betting.cli", "train"], lf, cwd=BASE_DIR, env=env)
        ok = (int(rc_build) == 0 and int(rc_train) == 0)
        return ok, {"rc_build": int(rc_build), "rc_train": int(rc_train), "log_file": str(lf)}
    except Exception as e:
        return False, {"error": str(e)}


def _ensure_props_models(log_fp: Path | None = None) -> tuple[bool, dict]:
    """Ensure props model artifacts exist; if missing, build features and train.

    Returns (ok, info) where info includes rc_build/rc_train and paths.
    """
    try:
        models_dir = BASE_DIR / "models"
        need = [
            models_dir / "props_models.joblib",
            models_dir / "props_feature_columns.joblib",
        ]
        have_all = all(p.exists() for p in need)
        if have_all:
            return True, {"skipped": True}
        # Choose python exe
        py = os.environ.get("PYTHON", (os.environ.get("VIRTUAL_ENV") or "") + "/bin/python")
        if not py or not Path(str(py)).exists():
            py_win = (Path(os.environ.get("VIRTUAL_ENV") or "") / "Scripts" / "python.exe")
            py = str(py_win) if py_win.exists() else "python"
        env = {"PYTHONPATH": str(SRC_DIR)}
        logs_dir = _ensure_logs_dir(); stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        lf = Path(log_fp) if log_fp else (logs_dir / f"cron_props_train_autofix_{stamp}.log")
        # Build props features first (uses player_logs)
        rc_build = _run_to_file([str(py), "-m", "nba_betting.cli", "build-props-features"], lf, cwd=BASE_DIR, env=env)
        rc_train = _run_to_file([str(py), "-m", "nba_betting.cli", "train-props"], lf, cwd=BASE_DIR, env=env)
        ok = (int(rc_build) == 0 and int(rc_train) == 0)
        return ok, {"rc_build": int(rc_build), "rc_train": int(rc_train), "log_file": str(lf)}
    except Exception as e:
        return False, {"error": str(e)}


def _admin_auth_ok(req) -> bool:
    key = os.environ.get("ADMIN_KEY")
    if not key:
        # If no key configured, allow only local (127.0.0.1) requests
        try:
            host = (req.remote_addr or "").strip()
            if host in {"127.0.0.1", "::1", "::ffff:127.0.0.1"}:
                return True
            # Also allow private LAN ranges for local development convenience
            if host.startswith("192.168.") or host.startswith("10."):
                return True
            if host.startswith("172."):
                try:
                    parts = host.split(".")
                    if len(parts) >= 2:
                        second = int(parts[1])
                        if 16 <= second <= 31:
                            return True
                except Exception:
                    pass
            return False
        except Exception:
            return False
    return (req.args.get("key") == key) or (req.headers.get("X-Admin-Key") == key)


def _cron_auth_ok(req) -> bool:
    """Cron auth using CRON_TOKEN. Accepts:
    - Authorization: Bearer <token>
    - X-Cron-Token: <token>
    - ?token=<token>
    If CRON_TOKEN is unset, fall back to admin auth policy for local/dev.
    """
    token = os.environ.get("CRON_TOKEN")
    if not token:
        # fall back to admin auth (local dev convenience)
        return _admin_auth_ok(req)
    auth = (req.headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer ") and auth.split(" ", 1)[1] == token:
        return True
    if (req.headers.get("X-Cron-Token") == token) or (req.args.get("token") == token):
        return True
    return False

@app.route("/api/cron/push-test", methods=["POST"])  # lightweight diagnostics
def api_cron_push_test():
    if not _cron_auth_ok(request):
        return jsonify({"error": "unauthorized"}), 401
    # Stage a no-op commit and attempt push (allow-empty)
    ok, detail = _git_commit_and_push(msg="push-test heartbeat")
    return jsonify({"pushed": bool(ok), "detail": detail})


def _daily_update_job(do_push: bool) -> None:
    _job_state["running"] = True
    _job_state["started_at"] = datetime.utcnow().isoformat()
    _job_state["ended_at"] = None
    _job_state["ok"] = None
    _job_state["logs"] = []
    logs_dir = _ensure_logs_dir()
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"web_daily_update_{stamp}.log"
    _job_state["log_file"] = str(log_file)
    try:
        _append_log("Starting daily update...")
        py = os.environ.get("PYTHON", (os.environ.get("VIRTUAL_ENV") or "") + "/bin/python")
        if not py or not Path(str(py)).exists():
            py = (Path(os.environ.get("VIRTUAL_ENV") or "") / "Scripts" / "python.exe")
            if not py.exists():
                py = "python"
        env = {"PYTHONPATH": str(SRC_DIR)}
        cmds: list[list[str]] = []
        # Light, safe steps for static UI refresh; customize as needed.
        # Example: rebuild closing lines snapshot if CLI supports it.
        # cmds.append([str(py), "-m", "nba_betting.cli", "export-closing-lines-csv", "--date", "2025-04-13"])  # sample
        # Example: re-run predictions for a sample matchups CSV (if present)
        sample_csv = BASE_DIR / "samples" / "matchups.csv"
        if sample_csv.exists():
            cmds.append([str(py), "-m", "nba_betting.cli", "predict", "--input", str(sample_csv)])
        rc_total = 0
        for c in cmds:
            _append_log(f"Running: {' '.join(c)}")
            rc = _run_to_file(c, log_file, cwd=BASE_DIR, env=env)
            rc_total += int(rc)
            _append_log(f"Exit code: {rc}")
            if rc != 0:
                break
        ok = (rc_total == 0)
        _append_log(f"Daily update finished. ok={ok}")
        # Optional: push updates back to Git if requested and configured
        if ok and do_push:
            try:
                _append_log("Committing and pushing daily update artifacts via _git_commit_and_push...")
                okp, detail = _git_commit_and_push(msg="daily-update")
                _append_log(f"Git push {'ok' if okp else 'failed'}: {detail}")
            except Exception as e:  # noqa: BLE001
                _append_log(f"Git push error: {e}")
        _job_state["ok"] = ok
    except Exception as e:  # noqa: BLE001
        _append_log(f"Daily update exception: {e}")
        _job_state["ok"] = False
    finally:
        _job_state["ended_at"] = datetime.utcnow().isoformat()
        _job_state["running"] = False


# ---------------- Data APIs (parity with NHL web) ---------------- #

def _find_predictions_for_date(date_str: str) -> Optional[Path]:
    # Prefer processed path
    p = BASE_DIR / "data" / "processed" / f"predictions_{date_str}.csv"
    if p.exists():
        return p
    # Legacy root fallback
    legacy = BASE_DIR / f"predictions_{date_str}.csv"
    if legacy.exists():
        return legacy
    return None


def _find_game_odds_for_date(date_str: str) -> Optional[Path]:
    # Processed standardized game odds
    p = BASE_DIR / "data" / "processed" / f"game_odds_{date_str}.csv"
    if p.exists():
        return p
    # Alternate names we search for
    for name in [
        f"closing_lines_{date_str}.csv",
        f"odds_{date_str}.csv",
        f"market_{date_str}.csv",
    ]:
        q = BASE_DIR / "data" / "processed" / name
        if q.exists():
            return q
    return None


@app.route("/api/status")
def api_status():
    try:
        dproc = BASE_DIR / "data" / "processed"
        files = [str(x.name) for x in dproc.glob("*.csv")] if dproc.exists() else []
        return jsonify({
            "status": "ok",
            "processed_files": files,
            "have_index": (WEB_DIR / "index.html").exists(),
        })
    except Exception as e:  # noqa: BLE001
        return jsonify({"status": "error", "error": str(e)}), 500


def _cron_meta_update(kind: str, payload: Dict[str, Any]) -> None:
    try:
        CRON_META_PATH.parent.mkdir(parents=True, exist_ok=True)
        base = {}
        if CRON_META_PATH.exists():
            try:
                import json as _json
                base = _json.loads(CRON_META_PATH.read_text(encoding="utf-8", errors="ignore"))
                if not isinstance(base, dict):
                    base = {}
            except Exception:
                base = {}
        now_iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        entry = dict(payload)
        entry["timestamp"] = now_iso
        base[f"last_{kind}"] = entry
        try:
            import json as _json
            CRON_META_PATH.write_text(_json.dumps(base, indent=2), encoding="utf-8")
        except Exception:
            pass
    except Exception:
        pass


@app.route("/api/cron/meta")
def api_cron_meta():
    """Return lightweight information about last cron runs (best-effort).

    Includes last_refresh_bovada if recorded, and the latest odds file mtime as fallback.
    """
    try:
        out: Dict[str, Any] = {}
        # Load recorded meta if present
        if CRON_META_PATH.exists():
            try:
                import json as _json
                meta = _json.loads(CRON_META_PATH.read_text(encoding="utf-8", errors="ignore"))
                if isinstance(meta, dict):
                    out.update(meta)
            except Exception:
                pass
        # Fallback: scan for the newest odds CSV mtime
        try:
            dproc = BASE_DIR / "data" / "processed"
            newest_ts = None
            newest_file = None
            if dproc.exists():
                for p in dproc.glob("*.csv"):
                    if not any(s in p.name for s in ("odds_", "game_odds_", "market_", "closing_lines_")):
                        continue
                    st = p.stat().st_mtime
                    if newest_ts is None or st > newest_ts:
                        newest_ts = st
                        newest_file = p
            if newest_ts is not None:
                out.setdefault("fallback_odds_latest", {})
                out["fallback_odds_latest"] = {
                    "path": str(newest_file),
                    "mtime": datetime.utcfromtimestamp(newest_ts).strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
        except Exception:
            pass
        return jsonify(_to_jsonable(out))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/last-updated")
def api_last_updated():
    d = _parse_date_param(request)
    pred = _find_predictions_for_date(d) if d else None
    odds = _find_game_odds_for_date(d) if d else None
    try:
        def mtime(p: Optional[Path]) -> Optional[str]:
            if p and p.exists():
                return datetime.utcfromtimestamp(p.stat().st_mtime).isoformat()
            return None
        return jsonify({
            "date": d,
            "predictions": str(pred) if pred else None,
            "predictions_mtime": mtime(pred),
            "odds": str(odds) if odds else None,
            "odds_mtime": mtime(odds),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/data-status")
def api_data_status():
    """Quick status for a date: counts for predictions, game_odds, props edges, and recon.

    Query: ?date=YYYY-MM-DD (defaults to today UTC)
    """
    d = _parse_date_param(request)
    if not d:
        return jsonify({"error": "missing date"}), 400
    out: Dict[str, Any] = {"date": d}
    try:
        # Predictions
        p = _find_predictions_for_date(d)
        out["predictions_path"] = str(p) if p else None
        out["predictions_rows"] = _count_csv_rows_quick(p) if p else 0
    except Exception:
        out["predictions_rows"] = 0
    try:
        # Game odds
        go = _find_game_odds_for_date(d)
        out["game_odds_path"] = str(go) if go else None
        out["game_odds_rows"] = _count_csv_rows_quick(go) if go else 0
    except Exception:
        out["game_odds_rows"] = 0
    try:
        # Props
        pe = BASE_DIR / "data" / "processed" / f"props_edges_{d}.csv"
        out["props_edges_path"] = str(pe) if pe.exists() else None
        out["props_edges_rows"] = _count_csv_rows_quick(pe) if pe.exists() else 0
    except Exception:
        out["props_edges_rows"] = 0
    try:
        # Recon
        rg = BASE_DIR / "data" / "processed" / f"recon_games_{d}.csv"
        out["recon_games_path"] = str(rg) if rg.exists() else None
        out["recon_games_rows"] = _count_csv_rows_quick(rg) if rg.exists() else 0
    except Exception:
        out["recon_games_rows"] = 0
    return jsonify(out)


@app.route("/api/predictions")
def api_predictions():
    d = _parse_date_param(request)
    if not d:
        return jsonify({"error": "missing date"}), 400
    p = _find_predictions_for_date(d)
    if not p:
        return jsonify(_to_jsonable({"date": d, "rows": []}))
    try:
        df = pd.read_csv(p)
        # Try to merge odds if available
        q = _find_game_odds_for_date(d)
        if q is not None:
            try:
                o = pd.read_csv(q)
                if "date" in o.columns:
                    o["date"] = pd.to_datetime(o["date"], errors="coerce").dt.date
                if "date" in df.columns:
                    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
                on = ["date", "home_team", "visitor_team"]
                if all(c in df.columns for c in on) and all(c in o.columns for c in on):
                    df = df.merge(o, on=on, how="left", suffixes=("", "_odds"))
                    # Compute implied and edges if possible
                    if "home_ml" in df.columns and "home_win_prob" in df.columns:
                        df["home_implied_prob"] = df["home_ml"].apply(_implied_prob_american)
                        df["edge_win"] = df["home_win_prob"].astype(float) - df["home_implied_prob"].astype(float)
                    if "home_spread" in df.columns and "pred_margin" in df.columns:
                        df["market_home_margin"] = -pd.to_numeric(df["home_spread"], errors="coerce")
                        df["edge_spread"] = pd.to_numeric(df["pred_margin"], errors="coerce") - df["market_home_margin"]
                    if "total" in df.columns and "pred_total" in df.columns:
                        df["edge_total"] = pd.to_numeric(df["pred_total"], errors="coerce") - pd.to_numeric(df["total"], errors="coerce")
            except Exception:
                pass
        # Return compact JSON
        rows = df.fillna("").to_dict(orient="records")
        return jsonify(_to_jsonable({"date": d, "rows": rows}))
    except Exception as e:  # noqa: BLE001
        return jsonify({"error": str(e)}), 500


@app.route("/api/recommendations")
def api_recommendations():
    """Derive simple recommendations from predictions and odds for the date.

    - Winner: pick side with higher EV if EV > 0
    - Spread: pick side of model margin if abs(edge_spread) >= threshold
    - Total: pick Over/Under if abs(edge_total) >= threshold
    """
    d = _parse_date_param(request)
    if not d:
        return jsonify({"error": "missing date"}), 400
    try:
        pred_path = _find_predictions_for_date(d)
        if not pred_path:
            return jsonify({"date": d, "rows": [], "summary": {}})
        df = pd.read_csv(pred_path)
        # Merge odds if present
        q = _find_game_odds_for_date(d)
        if q is not None:
            try:
                o = pd.read_csv(q)
                for col in ("date",):
                    if col in o.columns:
                        o[col] = pd.to_datetime(o[col], errors="coerce").dt.date
                if "date" in df.columns:
                    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
                on = ["date", "home_team", "visitor_team"]
                if all(c in df.columns for c in on) and all(c in o.columns for c in on):
                    df = df.merge(o, on=on, how="left", suffixes=("", "_odds"))
            except Exception:
                pass
        # Build recs
        recs: List[Dict[str, Any]] = []
        th_spread = float(request.args.get("spread_edge", 1.0))
        th_total = float(request.args.get("total_edge", 1.5))
        for _, r in df.iterrows():
            try:
                home = r.get("home_team"); away = r.get("visitor_team")
                # Winner EV
                p_home = _number(r.get("home_win_prob"))
                ev_h = _ev_from_prob_and_american(p_home, r.get("home_ml")) if p_home is not None else None
                ev_a = _ev_from_prob_and_american(None if p_home is None else (1 - p_home), r.get("away_ml"))
                if ev_h is not None or ev_a is not None:
                    side = home if (ev_h or -1) >= (ev_a or -1) else away
                    ev = ev_h if side == home else ev_a
                    if ev is not None and ev > 0:
                        recs.append({
                            "market": "ML", "side": side, "home": home, "away": away,
                            "ev": float(ev), "date": d,
                        })
                # Spread rec
                pred_m = _number(r.get("pred_margin"))
                home_spread = _number(r.get("home_spread"))
                if pred_m is not None and home_spread is not None:
                    market_home_margin = -home_spread
                    edge = pred_m - market_home_margin
                    if abs(edge) >= th_spread:
                        side = home if edge > 0 else away
                        recs.append({
                            "market": "ATS", "side": side, "home": home, "away": away,
                            "edge": float(edge), "date": d,
                        })
                # Total rec
                pred_t = _number(r.get("pred_total"))
                total = _number(r.get("total"))
                if pred_t is not None and total is not None:
                    edge_t = pred_t - total
                    if abs(edge_t) >= th_total:
                        side = "Over" if edge_t > 0 else "Under"
                        recs.append({
                            "market": "TOTAL", "side": side, "home": home, "away": away,
                            "edge": float(edge_t), "date": d,
                        })
            except Exception:
                continue
        # Simple summary
        summary = {
            "n": len(recs),
            "by_market": {
                k: int(sum(1 for x in recs if x["market"] == k)) for k in ("ML","ATS","TOTAL")
            },
        }
        return jsonify({"date": d, "rows": recs, "summary": summary})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/props")
def api_props():
    d = _parse_date_param(request)
    if not d:
        return jsonify({"error": "missing date"}), 400
    try:
        # Prefer edges if available, fall back to predictions (unless source=predictions requested)
        edges_p = BASE_DIR / "data" / "processed" / f"props_edges_{d}.csv"
        preds_p = BASE_DIR / "data" / "processed" / f"props_predictions_{d}.csv"
        requested_source = (request.args.get("source") or "").strip().lower() or None
        use_predictions_first = (requested_source == "predictions")
        df = None
        src = None
        if not use_predictions_first:
            df = _read_csv_if_exists(edges_p)
            src = "edges" if (df is not None and not df.empty) else None
        # Optionally auto-build edges for the date if not present
        if (not use_predictions_first) and (df is None or df.empty) and str(request.args.get("build", "0")).lower() in {"1","true","yes"}:
            try:
                # Run the same CLI used by cron endpoint
                py = os.environ.get("PYTHON", (os.environ.get("VIRTUAL_ENV") or "") + "/bin/python")
                if not py or not Path(str(py)).exists():
                    py_win = (Path(os.environ.get("VIRTUAL_ENV") or "") / "Scripts" / "python.exe")
                    py = str(py_win) if py_win.exists() else "python"
                env = {"PYTHONPATH": str(SRC_DIR)}
                logs_dir = _ensure_logs_dir(); stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                lf = logs_dir / f"props_edges_on_demand_{d}_{stamp}.log"
                _ = _run_to_file([str(py), "-m", "nba_betting.cli", "props-edges", "--date", d, "--source", "auto"], lf, cwd=BASE_DIR, env=env)
                if edges_p.exists():
                    df = _read_csv_if_exists(edges_p)
                    src = "edges"
            except Exception:
                pass
        if (df is None or df.empty) or use_predictions_first:
            # Load predictions and optionally auto-build
            pdf = _read_csv_if_exists(preds_p)
            if (pdf is None or pdf.empty) and str(request.args.get("build", "0")).lower() in {"1","true","yes"}:
                try:
                    py = os.environ.get("PYTHON", (os.environ.get("VIRTUAL_ENV") or "") + "/bin/python")
                    if not py or not Path(str(py)).exists():
                        py_win = (Path(os.environ.get("VIRTUAL_ENV") or "") / "Scripts" / "python.exe")
                        py = str(py_win) if py_win.exists() else "python"
                    env = {"PYTHONPATH": str(SRC_DIR)}
                    logs_dir = _ensure_logs_dir(); stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                    lf = logs_dir / f"props_predictions_on_demand_{d}_{stamp}.log"
                    _ = _run_to_file([str(py), "-m", "nba_betting.cli", "predict-props", "--date", d, "--slate-only"], lf, cwd=BASE_DIR, env=env)
                    pdf = _read_csv_if_exists(preds_p)
                except Exception:
                    pass
            # If predictions loaded, and predictions requested or edges absent, use them
            if isinstance(pdf, pd.DataFrame) and not pdf.empty and (use_predictions_first or df is None or df.empty):
                df = pdf
                src = "predictions"
        if df is None:
            return jsonify({"date": d, "rows": [], "source": None})
        # Optional filters
        min_edge = float(request.args.get("min_edge", "0"))
        min_ev = float(request.args.get("min_ev", "0"))
        if src == "edges" and "edge" in df.columns:
            df = df[pd.to_numeric(df["edge"], errors="coerce").fillna(0) >= min_edge]
        if src == "edges" and "ev" in df.columns:
            df = df[pd.to_numeric(df["ev"], errors="coerce").fillna(0) >= min_ev]
        # Filter by market/stat if requested
        market = (request.args.get("market") or "").strip().lower()
        if market and ("stat" in df.columns):
            df = df[df["stat"].astype(str).str.lower() == market]
        # Direct team filter (applies to both modes when a team column exists)
        team_q = (request.args.get("team") or "").strip()
        if team_q and ("team" in df.columns):
            tval = team_q.upper()
            df = df[df.get("team").astype(str).str.strip().str.upper() == tval]
        # Optional: narrow to a specific game by team names
        home_q = (request.args.get("home_team") or "").strip()
        away_q = (request.args.get("away_team") or "").strip()
        if (home_q or away_q) and ("team" in df.columns):
            keep = set([t for t in (home_q, away_q) if t])
            if keep:
                df = df[df.get("team").astype(str).isin(keep)]
        collapsed = False
        # If predictions mode, produce long format with one row per player/stat and predicted value
        if src == "predictions":
            try:
                tmp = df.copy()
                # Ensure player/team/opponent/home columns exist
                for c in ("player_id","player_name","team","opponent","home"):
                    if c not in tmp.columns:
                        tmp[c] = None
                # Identify prediction columns
                pred_cols = [c for c in tmp.columns if c.startswith("pred_")]
                rename_map = {"pred_pts":"pts","pred_reb":"reb","pred_ast":"ast","pred_threes":"threes","pred_pra":"pra"}
                use = {c: rename_map.get(c, c.replace("pred_","")) for c in pred_cols}
                long = tmp.melt(id_vars=["player_id","player_name","team","opponent","home"], value_vars=list(use.keys()), var_name="stat_col", value_name="pred")
                long["stat"] = long["stat_col"].map(use)
                long.drop(columns=["stat_col"], inplace=True)
                # Resolve missing player_id via rosters/lookup to enable photos
                try:
                    if "player_id" in long.columns:
                        mask = long["player_id"].isna() | (pd.to_numeric(long["player_id"], errors="coerce").isna())
                        if mask.any():
                            def _res_pid(row):
                                try:
                                    return _resolve_player_id(row.get("player_name"), row.get("team"))
                                except Exception:
                                    return None
                            long.loc[mask, "player_id"] = long.loc[mask].apply(_res_pid, axis=1)
                except Exception:
                    pass
                # Filter by market after melt if provided
                if market:
                    long = long[long["stat"].astype(str).str.lower() == market]
                # Team filter after melt
                if team_q:
                    tval = team_q.upper()
                    long = long[long["team"].astype(str).str.strip().str.upper() == tval]
                # Optional sorting by prediction value
                sort_by = (request.args.get("sortBy") or request.args.get("sort") or "pred_desc").strip().lower()
                try:
                    long["pred"] = pd.to_numeric(long["pred"], errors="coerce")
                    if sort_by == "pred_asc":
                        long = long.sort_values(["pred"], ascending=True, kind="stable")
                    elif sort_by == "pred_desc":
                        long = long.sort_values(["pred"], ascending=False, kind="stable")
                except Exception:
                    pass
                # Secondary stable sort for grouping when pred sort not requested explicitly
                if sort_by not in ("pred_asc","pred_desc"):
                    long = long.sort_values(["stat","team","player_name"], kind="stable")
                rows = long.fillna("").to_dict(orient="records")
                return jsonify({"date": d, "source": src, "rows": rows, "collapsed": False})
            except Exception:
                pass
        # Otherwise, edges mode (default) with optional collapse to best-of-book
        collapse_q = (request.args.get("collapse", "1") or "1").strip().lower()
        do_collapse = collapse_q not in ("0", "false", "no")
        if do_collapse and ("ev" in df.columns):
            try:
                keys = [k for k in ["player_id", "player_name", "team", "stat", "side", "line"] if k in df.columns]
                if len(keys) >= 4:
                    tmp = df.copy()
                    tmp["ev"] = pd.to_numeric(tmp["ev"], errors="coerce")
                    tmp["edge"] = pd.to_numeric(tmp.get("edge", 0), errors="coerce")
                    tmp = tmp.sort_values(["stat", "ev", "edge"], ascending=[True, False, False])
                    df = tmp.groupby(keys, as_index=False, sort=False).head(1)
                    collapsed = True
            except Exception:
                pass
        rows = df.fillna("").to_dict(orient="records")
        return jsonify({"date": d, "source": src, "rows": rows, "collapsed": collapsed})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/props/recommendations")
def api_props_recommendations():
    """Aggregate props edges into player cards for the given date, NFL-style.

    Query params:
      - date: YYYY-MM-DD
      - market: optional filter (e.g., 'pts','reb','ast','threes')
      - minEV: minimum EV percent (e.g., 1.5). We compute ev_pct = ev*100 when ev present.
      - onlyEV: 1 to hide plays without EV
      - home_team/away_team: optional filter to a specific game
        Response:
            { date, rows, games: [{home_team,away_team}], data: [{player,team,home_team,away_team,plays:[...] }]}.
        Sorting:
            - sortBy: ev_desc (default), ev_asc, edge_desc, edge_asc
    """
    d = _parse_date_param(request)
    if not d:
        return jsonify({"error": "missing date"}), 400
    try:
        edges_p = BASE_DIR / "data" / "processed" / f"props_edges_{d}.csv"
        preds_p = BASE_DIR / "data" / "processed" / f"predictions_{d}.csv"
        props_preds_p = BASE_DIR / "data" / "processed" / f"props_predictions_{d}.csv"
        df = _read_csv_if_exists(edges_p)
        # Load game predictions (for matchup context)
        games_df = _read_csv_if_exists(preds_p)
        if not isinstance(games_df, pd.DataFrame) or games_df is None:
            games_df = pd.DataFrame()
        # Load props predictions (model baselines) if present
        pp = _read_csv_if_exists(props_preds_p)
        if not isinstance(pp, pd.DataFrame) or pp is None:
            pp = pd.DataFrame()
        # If no edges, still return cards built from model predictions so the UI has content
        if (df is None) or (not isinstance(df, pd.DataFrame)) or df.empty:
            # Build minimal cards from model predictions
            cards: list[dict] = []
            if not pp.empty:
                # Ensure consistent columns
                for c in ("player_name","team"):
                    if c not in pp.columns:
                        pp[c] = None
                # Group by player/team
                for (player, team), grp in pp.groupby(["player_name","team"], dropna=False):
                    # Gather model stats
                    model: dict[str, float] = {}
                    for col, key in [("pred_pts","pts"),("pred_reb","reb"),("pred_ast","ast"),("pred_threes","threes"),("pred_pra","pra")]:
                        if col in grp.columns:
                            try:
                                v = pd.to_numeric(grp[col], errors="coerce").dropna()
                                if not v.empty:
                                    model[key] = float(v.iloc[0])
                            except Exception:
                                pass
                    # Try to infer matchup
                    away, home = (None, None)
                    if not games_df.empty and team is not None:
                        try:
                            for _, r in games_df.iterrows():
                                h = str(r.get("home_team") or "").strip(); a = str(r.get("visitor_team") or "").strip()
                                if str(team).strip() in {h, a}:
                                    away, home = (a, h)
                                    break
                        except Exception:
                            pass
                    # Photo and logo hints
                    # If player_id present in pp, use it; otherwise None
                    pid = None
                    if "player_id" in grp.columns:
                        try:
                            pid = int(pd.to_numeric(grp["player_id"], errors="coerce").dropna().iloc[0])
                        except Exception:
                            pid = None
                    photo = (f"https://cdn.nba.com/headshots/nba/latest/1040x760/{pid}.png" if pid else None)
                    team_id = _get_team_id(str(team)) if team is not None else None
                    if team_id:
                        team_logo = f"https://cdn.nba.com/logos/nba/{team_id}/primary/L/logo.svg"
                    else:
                        team_tri = (str(team).upper() if isinstance(team, str) else None)
                        team_logo = (f"/web/assets/logos/{(team_tri or '').upper()}.svg" if team_tri else None)
                    cards.append({
                        "player": player,
                        "team": team,
                        "home_team": home,
                        "away_team": away,
                        "plays": [],  # no edges available
                        "ladders": [],
                        "model": model,
                        "photo": photo,
                        "team_logo": team_logo,
                    })
            games: list[dict] = []
            if isinstance(games_df, pd.DataFrame) and (not games_df.empty):
                try:
                    g = games_df[["home_team","visitor_team"]].dropna()
                    for _, r in g.iterrows():
                        games.append({"home_team": r.get("home_team"), "away_team": r.get("visitor_team")})
                except Exception:
                    pass
            return jsonify({"date": d, "rows": len(cards), "data": cards, "games": games, "note": "no props edges for date; showing model predictions only"})
        # Normalize and compute ev_pct for convenience
        df = df.copy()
        if "ev" in df.columns:
            try:
                df["ev"] = pd.to_numeric(df["ev"], errors="coerce")
                df["ev_pct"] = df["ev"] * 100.0
            except Exception:
                df["ev_pct"] = None
        for c in ("edge","line","price"):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        # Filter by market if requested
        market = (request.args.get("market") or "").strip().lower()
        if market and ("stat" in df.columns):
            mk = market
            df = df[df["stat"].astype(str).str.lower() == mk]
        # Filter by EV threshold (percent)
        try:
            minEV = float(request.args.get("minEV", "0") or 0)
        except Exception:
            minEV = 0.0
        onlyEV = str(request.args.get("onlyEV", "0")).lower() in {"1","true","yes"}
        if df is not None and ("ev_pct" in df.columns):
            if onlyEV:
                df = df[pd.to_numeric(df["ev_pct"], errors="coerce").notna()]
            if minEV and minEV > 0:
                df = df[pd.to_numeric(df["ev_pct"], errors="coerce").fillna(-1e9) >= minEV]
        elif (onlyEV or (minEV and minEV > 0)):
            # If we don't have EVs, nothing qualifies
            df = df.iloc[0:0]
        # Optionally narrow to a game (home_team/away_team)
        home_q = (request.args.get("home_team") or "").strip()
        away_q = (request.args.get("away_team") or "").strip()
        # games_df already loaded above; build games list (optional)
        games: list[dict] = []
        if isinstance(games_df, pd.DataFrame) and (not games_df.empty):
            try:
                g = games_df[["home_team","visitor_team"]].dropna()
                for _, r in g.iterrows():
                    games.append({"home_team": r.get("home_team"), "away_team": r.get("visitor_team")})
                # Filter rows by game if requested: keep only players whose team appears in that matchup
                if home_q or away_q:
                    keep_teams = set()
                    for _, r in g.iterrows():
                        h = str(r.get("home_team") or "").strip(); a = str(r.get("visitor_team") or "").strip()
                        if (not home_q or h == home_q) and (not away_q or a == away_q):
                            keep_teams.update({h, a})
                    if keep_teams and ("team" in df.columns):
                        # Normalize both sides to tricodes to avoid full-name vs abbr mismatches
                        try:
                            keep_tris = { (_get_tricode(t) or str(t).strip().upper()) for t in keep_teams }
                            tmp = df.copy()
                            tmp["_team_tri"] = tmp["team"].astype(str).map(lambda x: (_get_tricode(x) or str(x).strip().upper()))
                            df = tmp[tmp["_team_tri"].isin(keep_tris)].drop(columns=["_team_tri"], errors="ignore")
                        except Exception:
                            # Fallback to original behavior if anything goes wrong
                            df = df[df["team"].astype(str).isin(keep_teams)]
            except Exception:
                pass
        # Build cards grouped by player/team regardless of games_df presence
        sort_by = (request.args.get("sortBy") or "ev_desc").strip().lower()
        player_col = next((c for c in ("player_name", "player") if c in df.columns), None)
        team_col = "team" if "team" in df.columns else None
        cards: list[dict] = []
        if not player_col:
            # No player column; return empty result set gracefully
            return jsonify({"date": d, "rows": 0, "games": games, "data": []})
        # Optional enrichment: derive matchup per card from predictions
        matchup_map: dict[str, tuple[str,str]] = {}
        if isinstance(games_df, pd.DataFrame) and (not games_df.empty):
            try:
                for _, r in games_df.iterrows():
                    h = str(r.get("home_team") or "").strip(); a = str(r.get("visitor_team") or "").strip()
                    matchup_map[h.upper()] = (a, h)
                    matchup_map[a.upper()] = (a, h)
            except Exception:
                pass
        # Group and assemble plays
        group_cols = [player_col] + ([team_col] if team_col else [])
        # Ensure columns exist for grouping
        group_cols = [c for c in group_cols if c]
        # Prepare props predictions lookup per player/team for model baselines
        pp_lookup: dict[tuple[str,str], dict[str,float]] = {}
        if not pp.empty and ("player_name" in pp.columns) and (team_col is not None and team_col in pp.columns):
            try:
                tmp = pp.copy(); tmp["player_name"] = tmp["player_name"].astype(str)
                tmp[team_col] = tmp[team_col].astype(str).str.upper()
                for (pname, tval), gpp in tmp.groupby(["player_name", team_col], dropna=False):
                    model: dict[str,float] = {}
                    for col, key in [("pred_pts","pts"),("pred_reb","reb"),("pred_ast","ast"),("pred_threes","threes"),("pred_pra","pra")]:
                        if col in gpp.columns:
                            try:
                                v = pd.to_numeric(gpp[col], errors="coerce").dropna()
                                if not v.empty:
                                    model[key] = float(v.iloc[0])
                            except Exception:
                                pass
                    pp_lookup[(str(pname), str(tval).upper())] = model
            except Exception:
                pass

        for keys, grp in df.groupby(group_cols, dropna=False):
                if not isinstance(keys, tuple):
                    keys = (keys,)
                player = keys[0] if len(keys) > 0 else None
                team = keys[1] if len(keys) > 1 else (grp[team_col].iloc[0] if team_col and (team_col in grp.columns) and (len(grp) > 0) else None)
                plays: list[dict] = []
                g2 = grp.copy()
                # Prefer descending by ev_pct then by absolute edge
                if "ev_pct" in g2.columns:
                    g2 = g2.sort_values(["ev_pct", "edge"], ascending=[False, False])
                for _, r in g2.iterrows():
                    plays.append({
                        "market": r.get("stat"),
                        "side": r.get("side"),
                        "line": r.get("line"),
                        "price": r.get("price"),
                        "edge": r.get("edge"),
                        "ev": r.get("ev"),
                        "ev_pct": r.get("ev_pct"),
                        "book": r.get("bookmaker"),
                    })
                # Build consolidated ladders per market/side with best offer per distinct line,
                # and compute a 'base' line per group (price closest to +100)
                ladders: list[dict] = []
                try:
                    if {"stat","side","line"}.issubset(set(g2.columns)):
                        g3 = g2.copy()
                        g3["ev_pct"] = pd.to_numeric(g3.get("ev_pct"), errors="coerce")
                        g3["edge"] = pd.to_numeric(g3.get("edge"), errors="coerce")
                        g3["price"] = pd.to_numeric(g3.get("price"), errors="coerce")
                        # Keep highest EV per (stat, side, line)
                        g3 = g3.sort_values(["stat","side","line","ev_pct","edge"], ascending=[True, True, True, False, False])
                        dedup = g3.drop_duplicates(subset=["stat","side","line"], keep="first")
                        for (mkt, side), sub in dedup.groupby(["stat","side"], dropna=False):
                            # Determine base row: price closest to +100; fallback to highest EV
                            sub = sub.copy()
                            base_row = None
                            try:
                                if "price" in sub.columns and sub["price"].notna().any():
                                    idx = (sub["price"].astype(float).sub(100).abs()).idxmin()
                                    base_row = sub.loc[idx]
                            except Exception:
                                base_row = None
                            if base_row is None:
                                # Fallback: choose the row with max ev_pct
                                try:
                                    idx = sub["ev_pct"].astype(float).idxmax()
                                    base_row = sub.loc[idx]
                                except Exception:
                                    base_row = None
                            entries = []
                            # Sort remaining entries by line asc, exclude base line
                            try:
                                sub_sorted = sub.sort_values(["line"], ascending=True)
                            except Exception:
                                sub_sorted = sub
                            for _, rr in sub_sorted.head(12).iterrows():
                                # Skip base line (match by exact line value)
                                if base_row is not None and (rr.get("line") == base_row.get("line")):
                                    continue
                                entries.append({
                                    "line": rr.get("line"),
                                    "price": rr.get("price"),
                                    "ev_pct": rr.get("ev_pct"),
                                    "book": rr.get("bookmaker"),
                                })
                            base_obj = None
                            if base_row is not None:
                                base_obj = {
                                    "line": base_row.get("line"),
                                    "price": base_row.get("price"),
                                    "ev_pct": base_row.get("ev_pct"),
                                    "book": base_row.get("bookmaker"),
                                }
                            ladders.append({"market": mkt, "side": side, "base": base_obj, "entries": entries})
                except Exception:
                    pass
                away, home = (None, None)
                try:
                    away, home = matchup_map.get(str(team or "").upper(), (None, None))
                except Exception:
                    away, home = (None, None)
                # Optional model predictions attached to card
                model: dict[str,float] = {}
                try:
                    key = (str(player), str(team).upper() if isinstance(team, str) else str(team))
                    model = pp_lookup.get(key, {})
                except Exception:
                    model = {}
                # Photo and logo hints
                pid = None
                # Resolve from column or lookup if missing
                if "player_id" in g2.columns:
                    try:
                        pid = int(pd.to_numeric(g2["player_id"], errors="coerce").dropna().iloc[0])
                    except Exception:
                        pid = None
                if pid is None and player:
                    try:
                        pid = _resolve_player_id(player, team)
                    except Exception:
                        pid = None
                photo = (f"https://cdn.nba.com/headshots/nba/latest/1040x760/{pid}.png" if pid else None)
                try:
                    team_id = _get_team_id(str(team)) if team is not None else None
                except Exception:
                    team_id = None
                if team_id:
                    team_logo = f"https://cdn.nba.com/logos/nba/{team_id}/primary/L/logo.svg"
                else:
                    try:
                        team_tri = _get_tricode(str(team)) if team is not None else None
                    except Exception:
                        team_tri = (str(team).upper() if isinstance(team, str) else None)
                    # Prefer uppercase SVG path; UI will attempt lowercase or PNG fallback if 404s
                    team_logo = (f"/web/assets/logos/{(team_tri or '').upper()}.svg" if team_tri else None)
                # Compute best metrics for sorting
                best_ev = None
                best_edge = None
                try:
                    evs = pd.to_numeric(g2.get("ev_pct"), errors="coerce").dropna()
                    if not evs.empty:
                        best_ev = float(evs.max())
                except Exception:
                    pass
                try:
                    edges = pd.to_numeric(g2.get("edge"), errors="coerce").dropna()
                    if not edges.empty:
                        best_edge = float(edges.abs().max())
                except Exception:
                    pass
                cards.append({
                    "player": player,
                    "team": team,
                    "home_team": home,
                    "away_team": away,
                    "plays": plays,
                    "ladders": ladders,
                    "model": model,
                    "photo": photo,
                    "team_logo": team_logo,
                    "_best_ev": best_ev,
                    "_best_edge": best_edge,
                })
        # Apply sorting of cards
        try:
            if cards:
                if sort_by == "ev_asc":
                    cards.sort(key=lambda c: (float('inf') if c.get('_best_ev') is None else c.get('_best_ev')))
                elif sort_by == "edge_desc":
                    cards.sort(key=lambda c: (c.get('_best_edge') or -1e9), reverse=True)
                elif sort_by == "edge_asc":
                    cards.sort(key=lambda c: (float('inf') if c.get('_best_edge') is None else c.get('_best_edge')))
                else:  # ev_desc default
                    cards.sort(key=lambda c: (c.get('_best_ev') or -1e9), reverse=True)
        except Exception:
            pass
        # Strip internal fields
        for c in cards:
            c.pop('_best_ev', None); c.pop('_best_edge', None)
        payload = {"date": d, "rows": len(cards), "games": games, "data": cards}
        return jsonify(_to_jsonable(payload))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/props/reconciliation")
@app.route("/api/player-props-reconciliation")
def api_props_reconciliation():
    """Return reconciled props rows for a date if available.

    Reads data/processed/recon_props_YYYY-MM-DD.csv and supports optional filtering:
      - team: limit to a specific team code/name
      - player: substring match on player name
    """
    d = _parse_date_param(request)
    if not d:
        return jsonify({"error": "missing date"}), 400
    try:
        p = BASE_DIR / "data" / "processed" / f"recon_props_{d}.csv"
        if not p.exists():
            return jsonify({"date": d, "rows": 0, "data": [], "note": "no recon props for date"})
        df = pd.read_csv(p)
        team_q = (request.args.get("team") or "").strip()
        player_q = (request.args.get("player") or "").strip().lower()
        if team_q and "team" in df.columns:
            df = df[df.get("team").astype(str).str.strip() == team_q]
        if player_q and "player" in df.columns:
            df = df[df.get("player").astype(str).str.lower().str.contains(player_q)]
        rows = df.fillna("").to_dict(orient="records")
        return jsonify({"date": d, "rows": len(rows), "data": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/reconciliation")
def api_reconciliation():
    d = _parse_date_param(request)
    if not d:
        return jsonify({"error": "missing date"}), 400
    try:
        # Use recon files if present
        gpath = BASE_DIR / "data" / "processed" / f"recon_games_{d}.csv"
        ppath = BASE_DIR / "data" / "processed" / f"recon_props_{d}.csv"
        gdf = _read_csv_if_exists(gpath)
        pdf = _read_csv_if_exists(ppath)
        out: Dict[str, Any] = {"date": d}
        if gdf is not None and not gdf.empty:
            # Compute simple errors summary
            for col in ("margin_error","total_error"):
                if col in gdf.columns:
                    s = pd.to_numeric(gdf[col], errors="coerce").dropna()
                    if not s.empty:
                        out[f"{col}_mae"] = float(s.abs().mean())
                        out[f"{col}_rmse"] = float(np.sqrt((s**2).mean()))
            out["games"] = int(len(gdf))
        else:
            out["games"] = 0
        out["props_rows"] = int(0 if pdf is None else len(pdf))
        return jsonify(_to_jsonable(out))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/odds-coverage")
def api_odds_coverage():
    d = _parse_date_param(request)
    if not d:
        return jsonify({"error": "missing date"}), 400
    try:
        # Import team normalizer (maps aliases/abbreviations to full names)
        try:
            from nba_betting.teams import normalize_team as _norm_team  # type: ignore
        except Exception:  # pragma: no cover
            def _norm_team(x: str) -> str:
                return str(x or "").strip()
        pred_p = _find_predictions_for_date(d)
        odds_p = _find_game_odds_for_date(d)
        df = pd.read_csv(pred_p) if pred_p else pd.DataFrame()
        o = pd.read_csv(odds_p) if odds_p else pd.DataFrame()
        rows = []
        if not df.empty:
            # Normalize keys
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
            if not o.empty and "date" in o.columns:
                o["date"] = pd.to_datetime(o["date"], errors="coerce").dt.date
            # Normalize team names to common canonical form to ensure merge matches
            for frame in (df, o):
                if not frame.empty:
                    if "home_team" in frame.columns:
                        frame["home_team_norm"] = frame["home_team"].map(lambda x: _norm_team(str(x)))
                    if "visitor_team" in frame.columns:
                        frame["visitor_team_norm"] = frame["visitor_team"].map(lambda x: _norm_team(str(x)))
            on_cols = ["date", "home_team_norm", "visitor_team_norm"] if ("home_team_norm" in df.columns and "home_team_norm" in o.columns) else ["date","home_team","visitor_team"]
            merged = df.merge(o, left_on=on_cols, right_on=on_cols, how="left", suffixes=("","_odds")) if not o.empty else df
            for _, r in merged.iterrows():
                rows.append({
                    "home_team": r.get("home_team"),
                    "visitor_team": r.get("visitor_team"),
                    "have_ml": bool(pd.notna(r.get("home_ml")) and pd.notna(r.get("away_ml"))),
                    "have_spread": bool(pd.notna(r.get("home_spread"))),
                    "have_total": bool(pd.notna(r.get("total"))),
                })
        return jsonify({"date": d, "rows": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/debug/models")
def api_debug_models():
    """Inspect presence and loadability of saved model artifacts on the server.

    Returns a JSON object with paths, existence flags, and load errors if any.
    """
    try:
        try:
            from nba_betting.config import paths as _paths  # type: ignore
        except Exception:
            class _P:
                class _Paths:
                    models = BASE_DIR / "models"
                paths = _Paths()
            _paths = _P.paths  # type: ignore
        model_names = [
            "win_prob.joblib",
            "spread_margin.joblib",
            "totals.joblib",
            "halves_models.joblib",
            "quarters_models.joblib",
            "feature_columns.joblib",
        ]
        out: dict[str, dict[str, object]] = {}
        for name in model_names:
            p = (_paths.models / name) if hasattr(_paths, "models") else (BASE_DIR / "models" / name)
            info: dict[str, object] = {"path": str(p), "exists": bool(p.exists())}
            if p.exists():
                try:
                    import joblib as _joblib  # type: ignore
                    _ = _joblib.load(p)
                    info["load_ok"] = True
                except Exception as e:
                    info["load_ok"] = False
                    info["error"] = str(e)
            out[name] = info
        return jsonify({"models": out})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/cron/train-games", methods=["POST", "GET"])
def api_cron_train_games():
    """Train game models on the server using existing features.parquet.

    Auth: CRON_TOKEN or ADMIN_KEY. Writes models into models/.
    """
    if not (_cron_auth_ok(request) or _admin_auth_ok(request)):
        return jsonify({"error": "unauthorized"}), 401
    # Choose python exe
    py = os.environ.get("PYTHON", (os.environ.get("VIRTUAL_ENV") or "") + "/bin/python")
    if not py or not Path(str(py)).exists():
        py_win = (Path(os.environ.get("VIRTUAL_ENV") or "") / "Scripts" / "python.exe")
        py = str(py_win) if py_win.exists() else "python"
    logs_dir = _ensure_logs_dir(); stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"cron_train_games_{stamp}.log"
    try:
        env = {"PYTHONPATH": str(SRC_DIR)}
        # Minimal sequence: build-features (if needed) then train
        feats = BASE_DIR / "data" / "processed" / "features.parquet"
        rc_build = 0
        if not feats.exists():
            rc_build = _run_to_file([str(py), "-m", "nba_betting.cli", "build-features"], log_file, cwd=BASE_DIR, env=env)
        rc_train = _run_to_file([str(py), "-m", "nba_betting.cli", "train"], log_file, cwd=BASE_DIR, env=env)
        ok = (int(rc_build) == 0 and int(rc_train) == 0)
        # Optional push
        pushed = None; push_detail = None
        if str(request.args.get("push", "0")).lower() in {"1","true","yes"}:
            okp, detail = _git_commit_and_push(msg="train games")
            pushed = bool(okp); push_detail = detail
        return jsonify({"rc_build": int(rc_build), "rc_train": int(rc_train), "ok": bool(ok), "log_file": str(log_file), "pushed": pushed, "push_detail": push_detail})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Simple in-memory cache for scoreboard (to limit API calls)
_scoreboard_cache: Dict[str, Tuple[float, Any]] = {}


@app.route("/api/scoreboard")
def api_scoreboard():
    d = _parse_date_param(request)
    if not d:
        return jsonify({"error": "missing date"}), 400
    # Serve from cache within 20 seconds
    now = time.time()
    ent = _scoreboard_cache.get(d)
    if ent and now - ent[0] < 20:
        return jsonify(ent[1])
    if _scoreboardv2 is None:
        return jsonify({"date": d, "error": "nba_api not installed"}), 500
    def _fallback_cdn(date_str: str) -> Optional[Dict[str, Any]]:
        """Fallback to public CDN scoreboard JSON for the given date (UTC).

        Tries, in order:
          1) https://data.nba.com/data/10s/prod/v1/YYYYMMDD/scoreboard.json
          2) https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json (today only)
        Returns payload in same shape as primary handler.
        """
        try:
            import requests as _rq
            ymd = date_str.replace("-", "")
            def _parse_prod_v1(j: dict) -> list[dict]:
                out: list[dict] = []
                for g in j.get('games', []) or []:
                    try:
                        home = (g.get('hTeam') or {}).get('triCode')
                        away = (g.get('vTeam') or {}).get('triCode')
                        sc_h = (g.get('hTeam') or {}).get('score')
                        sc_a = (g.get('vTeam') or {}).get('score')
                        try:
                            hp = int(sc_h) if sc_h not in (None, "") else None
                        except Exception:
                            hp = None
                        try:
                            ap = int(sc_a) if sc_a not in (None, "") else None
                        except Exception:
                            ap = None
                        status_num = int(g.get('statusNum') or 0)
                        clock = str(g.get('clock') or '')
                        period = (g.get('period') or {}).get('current')
                        is_ht = (g.get('period') or {}).get('isHalftime')
                        is_eop = (g.get('period') or {}).get('isEndOfPeriod')
                        if status_num == 3:
                            status_txt = 'Final'
                            is_final = True
                        elif status_num == 2:
                            if is_ht:
                                status_txt = 'Half'
                            elif is_eop and period:
                                status_txt = f'End Q{period}'
                            elif period and clock:
                                status_txt = f'Q{period} {clock}'
                            elif period:
                                status_txt = f'Q{period}'
                            else:
                                status_txt = 'LIVE'
                            is_final = False
                        else:
                            status_txt = g.get('startTimeUTC') or 'Scheduled'
                            is_final = False
                        out.append({
                            'home': home,
                            'away': away,
                            'status': status_txt,
                            'game_id': g.get('gameId'),
                            'home_pts': hp,
                            'away_pts': ap,
                            'final': bool(is_final),
                        })
                    except Exception:
                        continue
                return out

            def _parse_live_today(j: dict) -> list[dict]:
                out: list[dict] = []
                sb = (j.get('scoreboard') or {})
                games = sb.get('games') or []
                for g in games:
                    try:
                        homeTeam = g.get('homeTeam') or {}
                        awayTeam = g.get('awayTeam') or {}
                        home = str(homeTeam.get('teamTricode') or '').upper() or None
                        away = str(awayTeam.get('teamTricode') or '').upper() or None
                        sc_h = homeTeam.get('score')
                        sc_a = awayTeam.get('score')
                        try:
                            hp = int(sc_h) if sc_h not in (None, "", "-", 0) else None
                        except Exception:
                            hp = None
                        try:
                            ap = int(sc_a) if sc_a not in (None, "", "-", 0) else None
                        except Exception:
                            ap = None
                        txt = str(g.get('gameStatusText') or '').strip()
                        is_final = txt.upper().startswith('FINAL')
                        out.append({
                            'home': home,
                            'away': away,
                            'status': txt or 'Scheduled',
                            'game_id': g.get('gameId'),
                            'home_pts': hp,
                            'away_pts': ap,
                            'final': bool(is_final),
                        })
                    except Exception:
                        continue
                return out

            # Try prod/v1 first
            url1 = f"https://data.nba.com/data/10s/prod/v1/{ymd}/scoreboard.json"
            try:
                r1 = _rq.get(url1, timeout=6, headers={
                    'User-Agent': 'Mozilla/5.0',
                    'Accept': 'application/json, text/plain, */*',
                    'Origin': 'https://www.nba.com',
                    'Referer': 'https://www.nba.com/',
                })
                if r1.status_code == 200:
                    g1 = _parse_prod_v1(r1.json())
                    if g1:
                        return {'date': date_str, 'games': g1}
            except Exception:
                pass
            # Try liveData today's scoreboard; if its gameDate matches the requested date, use it
            url2 = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
            try:
                r2 = _rq.get(url2, timeout=6, headers={
                    'User-Agent': 'Mozilla/5.0',
                    'Accept': 'application/json, text/plain, */*',
                    'Origin': 'https://www.nba.com',
                    'Referer': 'https://www.nba.com/',
                })
                if r2.status_code == 200:
                    j2 = r2.json()
                    gd = None
                    try:
                        gd = str(((j2 or {}).get('scoreboard') or {}).get('gameDate') or '')[:10]
                    except Exception:
                        gd = None
                    g2 = _parse_live_today(j2)
                    if g2 and (gd == date_str or gd is None):
                        return {'date': date_str, 'games': g2}
            except Exception:
                pass
            return {'date': date_str, 'games': []}
        except Exception:
            return None

    try:
        # Harden headers
        try:
            if _nba_http is not None:
                _nba_http.STATS_HEADERS.update({
                    'Accept': 'application/json, text/plain, */*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Origin': 'https://www.nba.com',
                    'Referer': 'https://www.nba.com/stats/',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
                    'Connection': 'keep-alive',
                })
        except Exception:
            pass
        # Primary: nba_api with a short retry
        tries = 0
        last_err: Optional[Exception] = None
        gh = pd.DataFrame(); ls = pd.DataFrame()
        # Keep this snappy: one short attempt, then fallback to CDN
        while tries < 1:
            try:
                sb = _scoreboardv2.ScoreboardV2(game_date=d, day_offset=0, timeout=8)
                nd = sb.get_normalized_dict()
                gh = pd.DataFrame(nd.get("GameHeader", []))
                ls = pd.DataFrame(nd.get("LineScore", []))
                last_err = None
                break
            except Exception as e:
                last_err = e
                tries += 1
        games = []
        if not gh.empty and not ls.empty:
            cgh = {c.upper(): c for c in gh.columns}
            cls = {c.upper(): c for c in ls.columns}
            # Map TEAM_ID -> (ABBR, PTS)
            teams = {}
            for _, r in ls.iterrows():
                try:
                    tid = int(r[cls["TEAM_ID"]])
                    ab = str(r[cls["TEAM_ABBREVIATION"]]).upper()
                    pts = None
                    if "PTS" in cls:
                        try:
                            pts = int(r[cls["PTS"]])
                        except Exception:
                            pts = None
                    teams[tid] = {"abbr": ab, "pts": pts}
                except Exception:
                    continue
            for _, g in gh.iterrows():
                try:
                    hid = int(g[cgh["HOME_TEAM_ID"]]); vid = int(g[cgh["VISITOR_TEAM_ID"]])
                    home = teams.get(hid, {}); away = teams.get(vid, {})
                    stat_txt = g.get(cgh.get("GAME_STATUS_TEXT", "GAME_STATUS_TEXT"))
                    games.append({
                        "home": home.get("abbr"),
                        "away": away.get("abbr"),
                        "status": stat_txt,
                        "game_id": g.get(cgh.get("GAME_ID", "GAME_ID")),
                        "home_pts": home.get("pts"),
                        "away_pts": away.get("pts"),
                        "final": (str(stat_txt or "").strip().upper().startswith("FINAL")),
                    })
                except Exception:
                    continue
        # Fallback to CDN if primary is empty or failed
        if not games:
            alt = _fallback_cdn(d)
            if alt is not None:
                payload = alt
                _scoreboard_cache[d] = (now, payload)
                return jsonify(payload)
            # If CDN also fails, return a safe empty payload
            payload = {"date": d, "games": []}
        else:
            payload = {"date": d, "games": games}
        _scoreboard_cache[d] = (now, payload)
        return jsonify(payload)
    except Exception as e:
        # As a last resort, return an empty shape to avoid client errors
        return jsonify({"date": d, "games": [], "error": str(e)}), 200


@app.route("/api/schedule")
def api_schedule():
    """Serve the NBA schedule as JSON. If the processed JSON is missing, attempt to generate it.

    Query params:
      - season (optional): currently defaults to '2025-26'
      - date (optional): if provided, filter to that YYYY-MM-DD
    """
    season = (request.args.get("season") or "2025-26").strip()
    date_str = _parse_date_param(request, default_to_today=False)
    out_dir = BASE_DIR / "data" / "processed"
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "schedule_2025_26.json"  # schema tied to function name for now
    try:
        df = None
        if json_path.exists():
            try:
                df = pd.read_json(json_path)
            except Exception:
                df = None
        if df is None or df.empty:
            # Try to fetch via python module
            try:
                from nba_betting.schedule import fetch_schedule_2025_26  # type: ignore
                df = fetch_schedule_2025_26()
                # Save in compact list form for frontend
                df.to_json(json_path, orient="records", date_format="iso")
            except Exception as e:
                return jsonify({"error": f"Failed to load or build schedule: {e}"}), 500
        # Optional filter by date
        if date_str:
            try:
                df = df.copy()
                if "date_utc" in df.columns:
                    df["date_utc"] = pd.to_datetime(df["date_utc"], errors="coerce").dt.date
                mask = df["date_utc"].astype(str) == date_str
                df = df[mask]
            except Exception:
                pass
        return jsonify(df.to_dict(orient="records"))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/cron/refresh-bovada", methods=["POST", "GET"])
def api_cron_refresh_bovada():
    """Fetch current Bovada odds for a specific date and save a standardized CSV under data/processed.

    Query params:
      - date (required): YYYY-MM-DD
    Auth: CRON_TOKEN (preferred) or ADMIN_KEY (fallback if CRON_TOKEN unset).
    """
    if not (_cron_auth_ok(request) or _admin_auth_ok(request)):
        return jsonify({"error": "unauthorized"}), 401
    d = _parse_date_param(request, default_to_today=False)
    if not d:
        # Default to yesterday (UTC) for closing lines capture
        d = (datetime.utcnow().date() - timedelta(days=1)).isoformat()
    if _fetch_bovada_odds_current is None:
        return jsonify({"error": "bovada fetcher not available"}), 500
    # Pass date string directly; function treats as ET calendar date
    try:
        _ = pd.to_datetime(d)
    except Exception:
        return jsonify({"error": "invalid date"}), 400
    try:
        df = _fetch_bovada_odds_current(d)
        rows = 0 if df is None else len(df)
        out = BASE_DIR / "data" / "processed" / f"game_odds_{d}.csv"
        used_fallback = False
        fallback_path = None
        # For props: we'll attempt to fetch Bovada player props and compute edges if present
        props_raw_rows = 0
        props_raw_path = None
        props_edges_rows = 0
        props_edges_path = None
        if df is not None and not df.empty:
            out.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(out, index=False)
        else:
            # Try fallback to consensus/closing lines if Bovada has nothing
            fb = _find_fallback_odds_for_date(d)
            if fb is not None:
                try:
                    o = pd.read_csv(fb)
                    if not o.empty:
                        out.parent.mkdir(parents=True, exist_ok=True)
                        o.to_csv(out, index=False)
                        rows = int(len(o))
                        used_fallback = True
                        fallback_path = str(fb)
                except Exception:
                    pass
        # Attempt Bovada player props for this date and run props-edges if any lines found
        try:
            if _fetch_bovada_player_props_current is not None:  # type: ignore[name-defined]
                props_df = _fetch_bovada_player_props_current(d)
            else:
                try:
                    from nba_betting.odds_bovada import fetch_bovada_player_props_current as _fbppc  # type: ignore
                    props_df = _fbppc(d)
                except Exception:
                    props_df = None
            if props_df is not None and not props_df.empty:
                raw_dir = BASE_DIR / "data" / "raw"
                raw_dir.mkdir(parents=True, exist_ok=True)
                props_raw = raw_dir / f"odds_bovada_player_props_{d}.csv"
                try:
                    props_df.to_csv(props_raw, index=False)
                    props_raw_rows = int(len(props_df))
                    props_raw_path = str(props_raw)
                except Exception:
                    pass
                # Compute props edges from Bovada source for this date
                try:
                    # Choose python executable
                    py = os.environ.get("PYTHON", (os.environ.get("VIRTUAL_ENV") or "") + "/bin/python")
                    if not py or not Path(str(py)).exists():
                        py_win = (Path(os.environ.get("VIRTUAL_ENV") or "") / "Scripts" / "python.exe")
                        py = str(py_win) if py_win.exists() else "python"
                    env = dict(os.environ)
                    env["PYTHONPATH"] = str(SRC_DIR)
                    logs_dir = _ensure_logs_dir(); stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                    log_file = logs_dir / f"cron_props_edges_from_bovada_{d}_{stamp}.log"
                    # Ensure props models exist; if missing, build features and train
                    props_feat_cols = BASE_DIR / "models" / "props_feature_columns.joblib"
                    props_models_file = BASE_DIR / "models" / "props_models.joblib"
                    if (not props_feat_cols.exists()) or (not props_models_file.exists()):
                        _ = _run_to_file([str(py), "-m", "nba_betting.cli", "build-props-features"], log_file, cwd=BASE_DIR, env=env)
                        _ = _run_to_file([str(py), "-m", "nba_betting.cli", "train-props"], log_file, cwd=BASE_DIR, env=env)
                    _ = _run_to_file([str(py), "-m", "nba_betting.cli", "props-edges", "--date", d, "--source", "bovada", "--no-use-saved"], log_file, cwd=BASE_DIR, env=env)
                    pe = BASE_DIR / "data" / "processed" / f"props_edges_{d}.csv"
                    if pe.exists():
                        try:
                            props_edges_rows = int(len(pd.read_csv(pe)))
                            props_edges_path = str(pe)
                        except Exception:
                            props_edges_rows = 0
                except Exception:
                    pass
        except Exception:
            pass
        # Record cron meta best-effort
        try:
            meta = {
                "date": d,
                "rows": int(rows),
                "output": str(out),
            }
            if used_fallback:
                meta.update({"used_fallback": True, "fallback_path": fallback_path})
            if props_raw_path:
                meta.update({"props_raw_rows": int(props_raw_rows), "props_raw_path": props_raw_path})
            if props_edges_path:
                meta.update({"props_edges_rows": int(props_edges_rows), "props_edges_path": props_edges_path})
            _cron_meta_update("refresh_bovada", meta)
        except Exception:
            pass
        # Optional push
        pushed = None; push_detail = None
        if str(request.args.get("push", "0")).lower() in {"1","true","yes"}:
            ok, detail = _git_commit_and_push(msg=f"refresh-bovada {d}")
            pushed = bool(ok); push_detail = detail
        return jsonify({
            "date": d, "rows": int(rows), "output": str(out),
            "used_fallback": used_fallback, "fallback_path": fallback_path,
            "props_raw_rows": int(props_raw_rows), "props_raw_path": props_raw_path,
            "props_edges_rows": int(props_edges_rows), "props_edges_path": props_edges_path,
            "pushed": pushed, "push_detail": push_detail,
        })
    except Exception as e:
        return jsonify({"error": f"bovada fetch failed: {e}"}), 500


@app.route("/api/cron/probe-bovada")
def api_cron_probe_bovada():
    """Debug endpoint: probe Bovada endpoints and report event counts for a date.

    Auth: CRON_TOKEN or ADMIN_KEY. Query: date=YYYY-MM-DD
    """
    if not (_cron_auth_ok(request) or _admin_auth_ok(request)):
        return jsonify({"error": "unauthorized"}), 401
    d = _parse_date_param(request, default_to_today=True)
    if _probe_bovada is None:
        return jsonify({"error": "probe not available"}), 500
    try:
        dt = pd.to_datetime(d)
        out = _probe_bovada(dt, verbose=True)
        return jsonify(out)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/cron/capture-closing", methods=["POST", "GET"])
def api_cron_capture_closing():
    """Export consensus closing lines CSV for a given date under data/processed.

    Query params:
      - date (required): YYYY-MM-DD
    """
    if not _cron_auth_ok(request):
        return jsonify({"error": "unauthorized"}), 401
    d = _parse_date_param(request, default_to_today=False)
    if not d:
        return jsonify({"error": "missing date"}), 400
    # Choose python executable
    py = os.environ.get("PYTHON", (os.environ.get("VIRTUAL_ENV") or "") + "/bin/python")
    if not py or not Path(str(py)).exists():
        py_win = (Path(os.environ.get("VIRTUAL_ENV") or "") / "Scripts" / "python.exe")
        py = str(py_win) if py_win.exists() else "python"
    logs_dir = _ensure_logs_dir(); stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"cron_capture_closing_{d}_{stamp}.log"
    try:
        env = {"PYTHONPATH": str(SRC_DIR)}
        rc = _run_to_file([str(py), "-m", "nba_betting.cli", "export-closing-lines-csv", "--date", d], log_file, cwd=BASE_DIR, env=env)
        out = BASE_DIR / "data" / "processed" / f"closing_lines_{d}.csv"
        rows = 0
        if out.exists():
            try:
                rows = int(sum(1 for _ in out.open("r", encoding="utf-8", errors="ignore")) - 1)
            except Exception:
                try:
                    rows = len(pd.read_csv(out))
                except Exception:
                    rows = 0
        # Optional push
        pushed = None; push_detail = None
        if str(request.args.get("push", "0")).lower() in {"1","true","yes"}:
            ok, detail = _git_commit_and_push(msg=f"capture closing lines {d}")
            pushed = bool(ok); push_detail = detail
        return jsonify({"date": d, "rc": int(rc), "output": str(out), "rows": int(rows), "log_file": str(log_file), "pushed": pushed, "push_detail": push_detail})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/cron/predict-date", methods=["POST", "GET"])
def api_cron_predict_date():
    """Run predict-date for the given date to refresh predictions_YYYY-MM-DD.csv and odds CSV.

    Auth: CRON_TOKEN (preferred) or ADMIN_KEY (fallback/manual).
    """
    if not (_cron_auth_ok(request) or _admin_auth_ok(request)):
        return jsonify({"error": "unauthorized"}), 401
    d = _parse_date_param(request, default_to_today=True)
    do_push = (str(request.args.get("push", "0")).lower() in {"1","true","yes"})
    do_async = (str(request.args.get("async", "0")).lower() in {"1","true","yes"})
    skip_if_no_games = (str(request.args.get("skip_if_no_games", "1")).lower() in {"1","true","yes"})
    # If there are no games today and skipping is allowed, fast-exit.
    if skip_if_no_games and d:
        try:
            if not _has_games_for_date(d):
                return jsonify({
                    "status": "skipped",
                    "reason": "no games for date",
                    "date": d,
                    "push": do_push,
                })
        except Exception:
            # On check failure, proceed to run to be safe
            pass
    # Choose python executable
    py = os.environ.get("PYTHON", (os.environ.get("VIRTUAL_ENV") or "") + "/bin/python")
    if not py or not Path(str(py)).exists():
        py_win = (Path(os.environ.get("VIRTUAL_ENV") or "") / "Scripts" / "python.exe")
        py = str(py_win) if py_win.exists() else "python"
    logs_dir = _ensure_logs_dir(); stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"cron_predict_date_{d}_{stamp}.log"
    try:
        # Ensure models exist (fresh deploys may not have models volume)
        ok_models, info = _ensure_game_models(log_file)
        if not ok_models:
            # Proceed anyway; CLI will fail clearly if models missing
            pass
        env = {"PYTHONPATH": str(SRC_DIR)}
        if do_async:
            # Background job to avoid Render timeouts
            def _job():
                try:
                    _run_to_file([str(py), "-m", "nba_betting.cli", "predict-date", "--date", d], log_file, cwd=BASE_DIR, env=env)
                    if do_push:
                        _git_commit_and_push(msg=f"predict-date {d}")
                except Exception:
                    pass
            t = threading.Thread(target=_job, daemon=True)
            t.start()
            return jsonify({
                "status": "started",
                "date": d,
                "log_file": str(log_file),
                "push": do_push,
                "models": info,
            }), 202
        # Synchronous mode (original behavior)
        rc = _run_to_file([str(py), "-m", "nba_betting.cli", "predict-date", "--date", d], log_file, cwd=BASE_DIR, env=env)
        # Locate predictions from either processed/ or legacy root
        pred_path = _find_predictions_for_date(d)
        pred = pred_path if pred_path is not None else (BASE_DIR / f"predictions_{d}.csv")
        odds = BASE_DIR / "data" / "processed" / f"game_odds_{d}.csv"
        n_pred = int(len(pd.read_csv(pred))) if (hasattr(pred, 'exists') and pred.exists()) else (int(len(pd.read_csv(pred))) if pred_path is not None else 0)
        n_odds = int(len(pd.read_csv(odds))) if odds.exists() else 0
        pushed = None; push_detail = None
        if do_push:
            ok, detail = _git_commit_and_push(msg=f"predict-date {d}")
            pushed = bool(ok); push_detail = detail
        return jsonify({
            "date": d,
            "rc": int(rc),
            "predictions": (str(pred) if (hasattr(pred, 'exists') and pred.exists()) else (str(pred) if pred_path is not None else None)),
            "pred_rows": n_pred,
            "odds": str(odds) if odds.exists() else None,
            "odds_rows": n_odds,
            "log_file": str(log_file),
            "pushed": pushed,
            "push_detail": push_detail,
            "models": info,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/cron/reconcile-games", methods=["POST", "GET"])
def api_cron_reconcile_games():
    """Build reconciliation CSV for a date by joining predictions with final scores.

    - Input date defaults to yesterday (UTC) if omitted.
    - Output: data/processed/recon_games_YYYY-MM-DD.csv
    - Columns: date, home_team, visitor_team, home_pts, visitor_pts, pred_margin, pred_total, actual_margin, total_actual, margin_error, total_error
    """
    if not _cron_auth_ok(request):
        return jsonify({"error": "unauthorized"}), 401
    # Date defaults to yesterday
    d = _parse_date_param(request, default_to_today=False)
    if not d:
        d = (datetime.utcnow().date() - timedelta(days=1)).isoformat()
    # Load predictions for that date (prefer processed/ fallback root)
    pred_path = _find_predictions_for_date(d)
    if pred_path is None:
        # Gracefully return rows=0 so cron runs don't fail on off days or missed predictions
        try:
            _cron_meta_update("reconcile_games", {"date": d, "rows": 0, "output": None, "reason": "predictions missing"})
        except Exception:
            pass
        return jsonify({"date": d, "rows": 0, "output": None, "reason": "predictions missing"})
    try:
        preds = pd.read_csv(pred_path)
    except Exception as e:
        return jsonify({"error": f"failed to read predictions: {e}"}), 500

    # Normalize prediction team names to tricodes
    try:
        from nba_api.stats.static import teams as _static_teams  # type: ignore
        team_list = _static_teams.get_teams()
        full_to_abbr = {str(t.get('full_name')).upper(): str(t.get('abbreviation')).upper() for t in team_list}
        alt = {
            "LOS ANGELES CLIPPERS": "LAC",
            "LA CLIPPERS": "LAC",
            "PHOENIX SUNS": "PHX",
            "GOLDEN STATE WARRIORS": "GSW",
            "SAN ANTONIO SPURS": "SAS",
            "NEW YORK KNICKS": "NYK",
            "BROOKLYN NETS": "BKN",
            "UTAH JAZZ": "UTA",
        }
        def to_tri(name: str) -> str:
            if name is None:
                return ""
            s = str(name).strip().upper()
            if s in full_to_abbr:
                return full_to_abbr[s]
            if s in alt:
                return alt[s]
            return s if len(s) <= 4 else s
        preds = preds.copy()
        preds["home_tri"] = preds.get("home_team").apply(to_tri)
        preds["away_tri"] = preds.get("visitor_team").apply(to_tri)
    except Exception:
        preds = preds.copy()
        preds["home_tri"] = preds.get("home_team").astype(str).str.upper()
        preds["away_tri"] = preds.get("visitor_team").astype(str).str.upper()

    # Helper: finals from NBA CDN (with optional ±1 day) limited to prediction pairs
    def _finals_from_cdn(date_str_local: str, pred_pairs: set[tuple[str, str]], include_adjacent: bool = False) -> pd.DataFrame:
        try:
            import requests as _rq  # type: ignore
        except Exception:
            return pd.DataFrame()

        def _rows_for(ds: str) -> list[dict[str, object]]:
            try:
                ymd = ds.replace('-', '')
                url = f"https://data.nba.com/data/10s/prod/v1/{ymd}/scoreboard.json"
                r = _rq.get(url, timeout=4)
                out: list[dict[str, object]] = []
                if r.status_code == 200:
                    jd = r.json()
                    games = jd.get('games', []) if isinstance(jd, dict) else []
                    for g in games:
                        try:
                            htri = str((g.get('hTeam') or {}).get('triCode') or '').upper()
                            vtri = str((g.get('vTeam') or {}).get('triCode') or '').upper()
                            if (htri, vtri) not in pred_pairs:
                                continue
                            hs = (g.get('hTeam') or {}).get('score'); vs = (g.get('vTeam') or {}).get('score')
                            hpts = int(hs) if (hs not in (None, '')) else None
                            vpts = int(vs) if (vs not in (None, '')) else None
                            out.append({"home_tri": htri, "away_tri": vtri, "home_pts": hpts, "visitor_pts": vpts})
                        except Exception:
                            continue
                return out
            except Exception:
                return []

        rows = _rows_for(date_str_local)
        if include_adjacent and not rows:
            try:
                from datetime import datetime as _dt, timedelta as _td
                base = _dt.strptime(date_str_local, "%Y-%m-%d").date()
                for off in (-1, 1):
                    rows += _rows_for((base + _td(days=off)).isoformat())
            except Exception:
                pass
        return pd.DataFrame(rows)

    # Fetch finals from ScoreboardV2 if available, else use CDN fallback
    pred_pairs: set[tuple[str, str]] = set(zip(
        preds.get("home_tri").astype(str).str.upper(),
        preds.get("away_tri").astype(str).str.upper()
    ))
    try:
        if _scoreboardv2 is None:
            finals = _finals_from_cdn(d, pred_pairs, include_adjacent=False)
        else:
            # Try stats API once
            try:
                if _nba_http is not None:
                    _nba_http.STATS_HEADERS.update({
                        'Accept': 'application/json, text/plain, */*',
                        'Accept-Language': 'en-US,en;q=0.9',
                        'Origin': 'https://www.nba.com',
                        'Referer': 'https://www.nba.com/stats/',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
                        'Connection': 'keep-alive',
                    })
            except Exception:
                pass
            last_err: Optional[Exception] = None
            gh = pd.DataFrame(); ls = pd.DataFrame()
            try:
                sb = _scoreboardv2.ScoreboardV2(game_date=d, day_offset=0, timeout=10)
                nd = sb.get_normalized_dict()
                gh = pd.DataFrame(nd.get("GameHeader", []))
                ls = pd.DataFrame(nd.get("LineScore", []))
            except Exception as e:
                last_err = e
            out_rows: list[dict[str, object]] = []
            finals = pd.DataFrame()
            if not (gh.empty or ls.empty):
                cgh = {c.upper(): c for c in gh.columns}
                cls = {c.upper(): c for c in ls.columns}
                team_rows: dict[int, dict[str, object]] = {}
                for _, r in ls.iterrows():
                    try:
                        tid = int(r[cls["TEAM_ID"]])
                        tri = str(r[cls["TEAM_ABBREVIATION"]]).upper()
                        pts = None
                        if "PTS" in cls:
                            try:
                                pts = int(r[cls["PTS"]])
                            except Exception:
                                pts = None
                        team_rows[tid] = {"tri": tri, "pts": pts}
                    except Exception:
                        continue
                for _, g in gh.iterrows():
                    try:
                        hid = int(g[cgh["HOME_TEAM_ID"]]); vid = int(g[cgh["VISITOR_TEAM_ID"]])
                        h = team_rows.get(hid, {}); v = team_rows.get(vid, {})
                        htri = str(h.get("tri") or "").upper(); vtri = str(v.get("tri") or "").upper()
                        hpts = h.get("pts"); vpts = v.get("pts")
                        if (htri, vtri) not in pred_pairs:
                            continue
                        out_rows.append({"home_tri": htri, "away_tri": vtri, "home_pts": hpts, "visitor_pts": vpts})
                    except Exception:
                        continue
                finals = pd.DataFrame(out_rows)
            if finals.empty and last_err is not None:
                finals = _finals_from_cdn(d, pred_pairs, include_adjacent=False)

        # Join and compute errors
        merged = preds.merge(finals, on=["home_tri","away_tri"], how="left")
        for col in ("pred_margin", "pred_total", "home_pts", "visitor_pts"):
            if col not in merged.columns:
                merged[col] = pd.NA
            try:
                merged[col] = pd.to_numeric(merged[col], errors="coerce")
            except Exception:
                merged[col] = pd.NA
        merged["actual_margin"] = merged["home_pts"] - merged["visitor_pts"]
        merged["total_actual"] = merged[["home_pts","visitor_pts"]].sum(axis=1)
        merged["margin_error"] = merged["pred_margin"] - merged["actual_margin"]
        merged["total_error"] = merged["pred_total"] - merged["total_actual"]
    except Exception:
        # Fallback to minimal recon if any step fails
        merged = preds.copy()
        for col in ("home_pts","visitor_pts","actual_margin","total_actual","margin_error","total_error"):
            merged[col] = pd.NA

    keep = [
        "date","home_team","visitor_team","home_tri","away_tri",
        "home_pts","visitor_pts","pred_margin","pred_total",
        "actual_margin","total_actual","margin_error","total_error"
    ]
    if "date" not in merged.columns:
        merged["date"] = d
    out_df = merged[[c for c in keep if c in merged.columns]]

    out = BASE_DIR / "data" / "processed" / f"recon_games_{d}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out, index=False)

    pushed = None; push_detail = None
    if str(request.args.get("push", "0")).lower() in {"1","true","yes"}:
        ok, detail = _git_commit_and_push(msg=f"reconcile games {d}")
        pushed = bool(ok); push_detail = detail
    try:
        _cron_meta_update("reconcile_games", {"date": d, "rows": int(len(out_df)), "output": str(out), "pushed": pushed})
    except Exception:
        pass
    return jsonify({"date": d, "rows": int(len(out_df)), "output": str(out), "pushed": pushed, "push_detail": push_detail})

@app.route("/api/cron/daily-update", methods=["POST", "GET"])
def api_cron_daily_update():
    """Trigger the daily update job via cron token. Git push is disabled by default for cron."""
    if not _cron_auth_ok(request):
        return jsonify({"error": "unauthorized"}), 401
    if _job_state["running"]:
        return jsonify({"status": "already-running", "started_at": _job_state["started_at"]}), 409
    do_push = (str(request.args.get("push", "0")).lower() in {"1", "true", "yes"})
    t = threading.Thread(target=_daily_update_job, args=(do_push,), daemon=True)
    t.start()
    return jsonify({"status": "started", "push": do_push, "started_at": datetime.utcnow().isoformat()}), 202


@app.route("/api/cron/run-all", methods=["POST", "GET"])
def api_cron_run_all():
    """Composite daily cron mirroring NHL behavior: predict, fetch odds, reconcile, props actuals/edges.

    Steps (best-effort, continue on failures):
      1) Predict for today's slate
      2) Refresh Bovada odds (or fallback to any available odds file)
      3) Reconcile yesterday's games (to post finals)
      4) Props actuals upsert for yesterday
      5) Compute props edges for today (if supported)
    """
    if not _cron_auth_ok(request):
        return jsonify({"error": "unauthorized"}), 401
    today = datetime.utcnow().date()
    yesterday = today - timedelta(days=1)
    d_today = request.args.get("date") or today.isoformat()
    d_yest = request.args.get("yesterday") or yesterday.isoformat()
    push = (str(request.args.get("push", "0")).lower() in {"1","true","yes"})
    # Choose python exe
    py = os.environ.get("PYTHON", (os.environ.get("VIRTUAL_ENV") or "") + "/bin/python")
    if not py or not Path(str(py)).exists():
        py_win = (Path(os.environ.get("VIRTUAL_ENV") or "") / "Scripts" / "python.exe")
        py = str(py_win) if py_win.exists() else "python"
    env = {"PYTHONPATH": str(SRC_DIR)}
    logdir = _ensure_logs_dir(); stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_file = logdir / f"web_daily_update_{stamp}.log"
    results: Dict[str, Any] = {"date": d_today, "yesterday": d_yest, "log_file": str(log_file)}
    try:
        # Resolve base URL for internal HTTP calls (Render-safe)
        base_url = os.environ.get("RENDER_EXTERNAL_URL") or os.environ.get("BASE_URL")
        if not base_url:
            port_env = os.environ.get("PORT", "5000")
            base_url = f"http://127.0.0.1:{port_env}"
        # Ensure models exist before predictions
        ok_models, info = _ensure_game_models(log_file)
        results["models"] = info
        # 1) predict-date today
        rc1 = _run_to_file([str(py), "-m", "nba_betting.cli", "predict-date", "--date", d_today], log_file, cwd=BASE_DIR, env=env)
        results["predict_date"] = int(rc1)
        # 2) refresh-bovada for today (HTTP to our own endpoint, to centralize logic/meta)
        try:
            import requests as _rq
            token = os.environ.get("CRON_TOKEN")
            headers = {"Authorization": f"Bearer {token}"} if token else {}
            r = _rq.get(f"{base_url}/api/cron/refresh-bovada?date={d_today}", headers=headers, timeout=60)
            results["refresh_bovada"] = (r.status_code, r.text[:200])
        except Exception as e:
            results["refresh_bovada_error"] = str(e)
        # 3) reconcile-games for yesterday
        try:
            import requests as _rq
            token = os.environ.get("CRON_TOKEN")
            headers = {"Authorization": f"Bearer {token}"} if token else {}
            r = _rq.get(f"{base_url}/api/cron/reconcile-games?date={d_yest}", headers=headers, timeout=60)
            results["reconcile_games"] = (r.status_code, r.text[:200])
        except Exception as e:
            results["reconcile_games_error"] = str(e)
        # 4) props actuals upsert for yesterday via CLI
        try:
            # Updated CLI subcommand name: fetch-prop-actuals
            rc4 = _run_to_file([str(py), "-m", "nba_betting.cli", "fetch-prop-actuals", "--date", d_yest], log_file, cwd=BASE_DIR, env=env)
            results["props_actuals"] = int(rc4)
        except Exception as e:
            results["props_actuals_error"] = str(e)
        # 5) ensure props models exist and precompute props predictions for today (calibrated)
        try:
            ok_props_models, info_props = _ensure_props_models(log_file)
            results["props_models"] = info_props
            rc5a = _run_to_file([str(py), "-m", "nba_betting.cli", "predict-props", "--date", d_today, "--slate-only", "--calibrate", "--calib-window", "7"], log_file, cwd=BASE_DIR, env=env)
            results["props_predictions"] = int(rc5a)
        except Exception as e:
            results["props_predictions_error"] = str(e)
        # 6) compute props edges for today via CLI if available
        try:
            rc6 = _run_to_file([str(py), "-m", "nba_betting.cli", "props-edges", "--date", d_today, "--source", "auto"], log_file, cwd=BASE_DIR, env=env)
            results["props_edges"] = int(rc6)
        except Exception as e:
            results["props_edges_error"] = str(e)
        if push:
            ok, detail = _git_commit_and_push(msg=f"daily-run-all {d_today}")
            results["pushed"] = bool(ok)
            results["push_detail"] = detail
        _cron_meta_update("run_all", {"date": d_today, "yesterday": d_yest, "log_file": str(log_file)})
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e), **results}), 500


@app.route("/api/cron/config")
def api_cron_config():
    """Introspection for cron/admin configuration (safe to expose booleans only)."""
    try:
        return jsonify({
            "have_cron_token": bool(os.environ.get("CRON_TOKEN")),
            "have_admin_key": bool(os.environ.get("ADMIN_KEY")),
            "endpoints": [
                "/api/cron/refresh-bovada",
                "/api/cron/predict-date",
                "/api/cron/capture-closing",
                "/api/cron/reconcile-games",
                "/api/cron/daily-update",
                "/api/cron/props-edges",
                "/api/cron/props-predictions",
                "/api/cron/fetch-rosters",
            ],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/daily-update", methods=["POST", "GET"])
def api_admin_daily_update():
    if not _admin_auth_ok(request):
        return jsonify({"error": "unauthorized"}), 401
    if _job_state["running"]:
        return jsonify({"status": "already-running", "started_at": _job_state["started_at"]}), 409
    do_push = (str(request.args.get("push", "1")).lower() in {"1", "true", "yes"})
    t = threading.Thread(target=_daily_update_job, args=(do_push,), daemon=True)
    t.start()
    return jsonify({"status": "started", "push": do_push, "started_at": datetime.utcnow().isoformat()}), 202


@app.route("/api/admin/daily-update/status")
def api_admin_daily_update_status():
    if not _admin_auth_ok(request):
        return jsonify({"error": "unauthorized"}), 401
    try:
        tail = int(request.args.get("tail", "200"))
    except Exception:
        tail = 200
    logs = _job_state.get("logs", [])
    if tail > 0:
        logs = logs[-tail:]
    return jsonify({
        "running": _job_state["running"],
        "started_at": _job_state["started_at"],
        "ended_at": _job_state["ended_at"],
        "ok": _job_state["ok"],
        "log_file": _job_state.get("log_file"),
        "logs": logs,
    })


@app.route("/api/cron/props-edges", methods=["POST", "GET"])
def api_cron_props_edges():
    """Compute player props edges CSV for the given date via CLI.

    Query params:
      - date (required): YYYY-MM-DD
      - source (optional): odds source; default 'auto'
      - push (optional): '1' to git commit/push artifacts

    Auth: CRON_TOKEN (preferred) or ADMIN_KEY (fallback/manual).
    Writes data/processed/props_edges_YYYY-MM-DD.csv
    """
    if not (_cron_auth_ok(request) or _admin_auth_ok(request)):
        return jsonify({"error": "unauthorized"}), 401
    d = _parse_date_param(request, default_to_today=True)
    source = (request.args.get("source") or "auto").strip()
    mode = (request.args.get("mode") or "auto").strip()
    use_saved_q = (request.args.get("use_saved") or request.args.get("use-saved") or "1").strip().lower()
    use_saved = (use_saved_q in {"1","true","yes"})
    do_push = (str(request.args.get("push", "0")).lower() in {"1", "true", "yes"})
    do_async = (str(request.args.get("async", "0")).lower() in {"1","true","yes"})
    # Choose python executable
    py = os.environ.get("PYTHON", (os.environ.get("VIRTUAL_ENV") or "") + "/bin/python")
    if not py or not Path(str(py)).exists():
        py_win = (Path(os.environ.get("VIRTUAL_ENV") or "") / "Scripts" / "python.exe")
        py = str(py_win) if py_win.exists() else "python"
    logs_dir = _ensure_logs_dir(); stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"cron_props_edges_{d}_{stamp}.log"
    try:
        # Ensure props models exist; if not, build and train automatically
        _okp, _info = _ensure_props_models(log_file)
        env = dict(os.environ)
        env["PYTHONPATH"] = str(SRC_DIR)
        cmd = [str(py), "-m", "nba_betting.cli", "props-edges", "--date", d, "--source", source]
        if mode:
            cmd += ["--mode", mode]
        if not use_saved:
            cmd += ["--no-use-saved"]
        if do_async:
            def _job():
                try:
                    _run_to_file(cmd, log_file, cwd=BASE_DIR, env=env)
                    if do_push:
                        _git_commit_and_push(msg=f"props-edges {d}")
                except Exception:
                    pass
            t = threading.Thread(target=_job, daemon=True)
            t.start()
            return jsonify({
                "status": "started",
                "date": d,
                "log_file": str(log_file),
            }), 202
        rc = _run_to_file(cmd, log_file, cwd=BASE_DIR, env=env)
        out = BASE_DIR / "data" / "processed" / f"props_edges_{d}.csv"
        rows = 0
        if out.exists():
            try:
                rows = int(len(pd.read_csv(out)))
            except Exception:
                rows = 0
        pushed = None; push_detail = None
        if do_push:
            ok, detail = _git_commit_and_push(msg=f"props-edges {d}")
            pushed = bool(ok); push_detail = detail
        try:
            _cron_meta_update("props_edges", {"date": d, "rows": int(rows), "output": str(out)})
        except Exception:
            pass
        return jsonify({
            "date": d,
            "rc": int(rc),
            "output": str(out),
            "rows": int(rows),
            "log_file": str(log_file),
            "pushed": pushed,
            "push_detail": push_detail,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/cron/fetch-rosters", methods=["POST", "GET"])
def api_cron_fetch_rosters():
    """Fetch team rosters for a season and persist under data/processed.

    Query params:
      - season: NBA season string (e.g., 2025-26)
      - push: 1 to commit/push artifacts
    """
    if not (_cron_auth_ok(request) or _admin_auth_ok(request)):
        return jsonify({"error": "unauthorized"}), 401
    season = (request.args.get("season") or "2025-26").strip()
    # Choose python executable
    py = os.environ.get("PYTHON", (os.environ.get("VIRTUAL_ENV") or "") + "/bin/python")
    if not py or not Path(str(py)).exists():
        py_win = (Path(os.environ.get("VIRTUAL_ENV") or "") / "Scripts" / "python.exe")
        py = str(py_win) if py_win.exists() else "python"
    logs_dir = _ensure_logs_dir(); stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"cron_fetch_rosters_{season}_{stamp}.log"
    try:
        env = dict(os.environ); env["PYTHONPATH"] = str(SRC_DIR)
        cmd = [str(py), "-m", "nba_betting.cli", "fetch-rosters-cmd", "--season", season]
        rc = _run_to_file(cmd, log_file, cwd=BASE_DIR, env=env)
        out_csv = BASE_DIR / "data" / "processed" / f"rosters_{season.replace('/', '-')}.csv"
        rows = 0
        if out_csv.exists():
            try:
                rows = int(len(pd.read_csv(out_csv)))
            except Exception:
                rows = 0
        pushed = None; push_detail = None
        if str(request.args.get("push", "0")).lower() in {"1","true","yes"}:
            ok, detail = _git_commit_and_push(msg=f"fetch-rosters {season}")
            pushed = bool(ok); push_detail = detail
        return jsonify({"season": season, "rc": int(rc), "rows": int(rows), "output": str(out_csv), "log_file": str(log_file), "pushed": pushed, "push_detail": push_detail})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/cron/props-predictions", methods=["POST", "GET"])
def api_cron_props_predictions():
    """Precompute player props predictions CSV for a given date via CLI.

    Query params:
      - date (optional): YYYY-MM-DD, defaults to today (UTC)
      - slate_only (optional): '1' to limit to today's slate (default 1)
      - push (optional): '1' to git commit/push artifacts

    Auth: CRON_TOKEN (preferred) or ADMIN_KEY (fallback/manual).
    Writes data/processed/props_predictions_YYYY-MM-DD.csv
    """
    if not (_cron_auth_ok(request) or _admin_auth_ok(request)):
        return jsonify({"error": "unauthorized"}), 401
    d = _parse_date_param(request, default_to_today=True)
    slate_only_q = (request.args.get("slate_only") or request.args.get("slate-only") or "1").strip().lower()
    slate_only = (slate_only_q in {"1", "true", "yes"})
    calib_q = (request.args.get("calibrate") or "1").strip().lower()
    do_calib = (calib_q in {"1","true","yes"})
    calib_window = request.args.get("calib_window") or request.args.get("calib-window") or "7"
    do_push = (str(request.args.get("push", "0")).lower() in {"1", "true", "yes"})
    do_async = (str(request.args.get("async", "0")).lower() in {"1","true","yes"})

    # Choose python executable
    py = os.environ.get("PYTHON", (os.environ.get("VIRTUAL_ENV") or "") + "/bin/python")
    if not py or not Path(str(py)).exists():
        py_win = (Path(os.environ.get("VIRTUAL_ENV") or "") / "Scripts" / "python.exe")
        py = str(py_win) if py_win.exists() else "python"

    logs_dir = _ensure_logs_dir(); stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"cron_props_predictions_{d}_{stamp}.log"
    try:
        # Ensure props models exist; if not, build and train automatically
        _okp, _info = _ensure_props_models(log_file)
        env = dict(os.environ)
        env["PYTHONPATH"] = str(SRC_DIR)
        cmd = [str(py), "-m", "nba_betting.cli", "predict-props", "--date", d]
        if slate_only:
            cmd += ["--slate-only"]
        if do_calib:
            cmd += ["--calibrate", "--calib-window", str(calib_window)]
        else:
            cmd += ["--no-calibrate"]
        if do_async:
            def _job():
                try:
                    _run_to_file(cmd, log_file, cwd=BASE_DIR, env=env)
                    if do_push:
                        _git_commit_and_push(msg=f"props-predictions {d}")
                except Exception:
                    pass
            t = threading.Thread(target=_job, daemon=True)
            t.start()
            return jsonify({
                "status": "started",
                "date": d,
                "log_file": str(log_file),
            }), 202
        rc = _run_to_file(cmd, log_file, cwd=BASE_DIR, env=env)
        out = BASE_DIR / "data" / "processed" / f"props_predictions_{d}.csv"
        rows = 0
        if out.exists():
            try:
                rows = int(len(pd.read_csv(out)))
            except Exception:
                rows = 0
        pushed = None; push_detail = None
        if do_push:
            ok, detail = _git_commit_and_push(msg=f"props-predictions {d}")
            pushed = bool(ok); push_detail = detail
        try:
            _cron_meta_update("props_predictions", {"date": d, "rows": int(rows), "output": str(out)})
        except Exception:
            pass
        return jsonify({
            "date": d,
            "rc": int(rc),
            "output": str(out),
            "rows": int(rows),
            "log_file": str(log_file),
            "pushed": pushed,
            "push_detail": push_detail,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5050"))
    app.run(host="0.0.0.0", port=port, debug=False)
