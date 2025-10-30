from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Dict

import numpy as np
import pandas as pd
import joblib

from .config import paths


SECONDS_Q = 12 * 60


def _to_sec_left(s: str) -> Optional[int]:
    """Parse various clock formats to seconds left in period.

    Supports:
    - MM:SS (e.g., '11:45')
    - ISO8601-like NBA strings: 'PT11M45.00S'
    - Plain integers or floats representing seconds
    Returns None on failure.
    """
    try:
        if s is None:
            return None
        if isinstance(s, (int, float, np.integer, np.floating)):
            v = float(s)
            if np.isnan(v):
                return None
            return int(v)
        if not isinstance(s, str):
            s = str(s)
        s = s.strip()
        # ISO8601-ish format 'PT11M45.00S'
        m_iso = re.match(r"^PT(?:(\d+)M)?(?:(\d+)(?:\.\d+)?)S$", s)
        if m_iso:
            mm = int(m_iso.group(1) or 0)
            ss = int(m_iso.group(2) or 0)
            return mm * 60 + ss
        # Classic mm:ss
        parts = s.split(":")
        if len(parts) == 2:
            m = int(parts[0]); sec = int(parts[1])
            return m * 60 + sec
    except Exception:
        return None
    return None


def _time_elapsed_q1(row: pd.Series) -> Optional[int]:
    per = row.get("PERIOD") or row.get("period")
    if pd.isna(per) or int(per) != 1:
        return None
    t = row.get("PCTIMESTRING") or row.get("clock") or row.get("time")
    left = _to_sec_left(t)
    if left is None:
        return None
    return SECONDS_Q - left


def _desc_cols(df: pd.DataFrame) -> List[str]:
    for cols in (["HOMEDESCRIPTION","VISITORDESCRIPTION","NEUTRALDESCRIPTION"], ["home_desc","visitor_desc","neutral_desc"], ["description"]):
        if all(c in df.columns for c in cols if c is not None):
            return [c for c in cols if c in df.columns]
    return [c for c in df.columns if isinstance(c, str) and c.lower().endswith("description")]


def _first_fg_event(df: pd.DataFrame) -> Optional[dict]:
    if df is None or df.empty:
        return None
    # Prefer structured columns if present (newer per-game CSVs from NBA liveData)
    # Logic: in Q1, first row with isFieldGoal==1 and shotResult=='Made' and actionType!='Free Throw'
    cols = set(df.columns)
    tmp = df.copy()
    c_per = "PERIOD" if "PERIOD" in cols else ("period" if "period" in cols else None)
    if c_per:
        try:
            tmp = tmp[tmp[c_per] == 1]
        except Exception:
            pass
    # Order by actionNumber if available, else try clock descending (time left), else as-is
    if "actionNumber" in cols:
        try:
            tmp = tmp.sort_values("actionNumber", ascending=True)
        except Exception:
            pass
    else:
        c_time = "PCTIMESTRING" if "PCTIMESTRING" in cols else ("clock" if "clock" in cols else None)
        if c_time:
            try:
                # Convert to elapsed to sort ascending
                tmp["__elapsed"] = tmp.apply(lambda r: (SECONDS_Q - (_to_sec_left(r.get(c_time)) or SECONDS_Q+1)), axis=1)
                tmp = tmp.sort_values("__elapsed", ascending=True)
            except Exception:
                pass
    if {"isFieldGoal","shotResult"}.issubset(cols):
        for _, r in tmp.iterrows():
            try:
                is_fg = int(r.get("isFieldGoal") or 0) == 1
            except Exception:
                is_fg = bool(r.get("isFieldGoal"))
            shot_res = str(r.get("shotResult") or "").lower()
            a_type = str(r.get("actionType") or "").lower()
            if is_fg and shot_res == "made" and ("free throw" not in a_type):
                pid = r.get("personId") or r.get("PLAYER1_ID") or r.get("player1_id")
                pname = r.get("playerName") or r.get("PLAYER1_NAME") or r.get("player1_name")
                team = r.get("teamTricode") or r.get("PLAYER1_TEAM_ABBREVIATION") or r.get("team_abbr")
                return {"player_id": pid, "player_name": pname, "team": team}
    # Fallback to text-based detection for legacy schemas
    desc_cols = _desc_cols(df)
    for _, r in tmp.iterrows():
        text = " ".join([str(r.get(c, "")) for c in desc_cols]).lower()
        if not text:
            continue
        # Accept either explicit verbs or NBA phrasing like "Bridges 3PT Jump Shot (3 PTS)"
        if ("makes" in text or "made" in text) or ("jump shot" in text and "free throw" not in text):
            pid = r.get("PLAYER1_ID") or r.get("player1_id") or r.get("personId")
            pname = r.get("PLAYER1_NAME") or r.get("player1_name") or r.get("playerName")
            team = r.get("PLAYER1_TEAM_ABBREVIATION") or r.get("teamTricode") or r.get("team_abbr")
            return {"player_id": pid, "player_name": pname, "team": team}
    return None


def _jump_ball_event(df: pd.DataFrame) -> Optional[dict]:
    if df is None or df.empty:
        return None
    cols = set(df.columns)
    tmp = df.copy()
    c_per = "PERIOD" if "PERIOD" in cols else ("period" if "period" in cols else None)
    if c_per:
        try:
            tmp = tmp[tmp[c_per] == 1]
        except Exception:
            pass
    if "actionNumber" in cols:
        try:
            tmp = tmp.sort_values("actionNumber", ascending=True)
        except Exception:
            pass
    desc_cols = _desc_cols(df)
    jb_pat = re.compile(r"jump ball", re.IGNORECASE)
    for _, r in tmp.iterrows():
        a_type = str(r.get("actionType") or "").lower()
        text = " ".join([str(r.get(c, "")) for c in desc_cols])
        if not text and not a_type:
            continue
        if ("jump ball" in a_type) or jb_pat.search(text or ""):
            tlow = (text or "").lower()
            winner: Optional[str] = None
            # Handle both classic '- X gains possession' and liveData 'Tip to X'
            m1 = re.search(r"-\s*([\w\.'\-\s]+)\s+gains\s+possession", tlow)
            m2 = re.search(r"tip\s+to\s+([\w\.'\-\s]+)", tlow)
            if m1:
                winner = m1.group(1).strip()
            elif m2:
                winner = m2.group(1).strip()
            return {"raw": text, "winner_text": winner}
    return None


@dataclass
class TipModelArtifacts:
    model_path: Path
    onnx_path: Optional[Path]


@dataclass
class FirstBasketModelArtifacts:
    model_path: Path
    onnx_path: Optional[Path]


@dataclass
class EarlyThreesModelArtifacts:
    model_path: Path
    onnx_path: Optional[Path]


def _load_rosters_latest() -> pd.DataFrame:
    files = sorted((paths.data_processed).glob("rosters_*.csv"))
    if not files:
        return pd.DataFrame()
    # Prefer season-format files like rosters_2025-26.csv over plain rosters_2025.csv
    season_files = [f for f in files if '-' in f.stem]
    target = season_files[-1] if season_files else files[-1]
    try:
        return pd.read_csv(target)
    except Exception:
        return pd.DataFrame()


def _load_player_minutes_avg() -> Optional[pd.Series]:
    """Return Series mapping PLAYER_ID -> avg minutes from data/processed/player_logs.csv.

    Minutes are parsed from 'MIN' (mm:ss) if present; otherwise try 'MINUTES' numeric.
    Returns None if file missing or parse fails.
    """
    try:
        p = paths.data_processed / "player_logs.csv"
        if not p.exists():
            return None
        df = pd.read_csv(p)
        if df is None or df.empty:
            return None
        col_pid = None
        for c in ("PLAYER_ID","personId","PERSON_ID"):
            if c in df.columns:
                col_pid = c; break
        if not col_pid:
            return None
        if "MIN" in df.columns:
            def _to_minutes(v):
                if pd.isna(v):
                    return 0.0
                s = str(v)
                if ':' in s:
                    return float(_mmss_to_seconds(s) / 60.0)
                try:
                    return float(s)
                except Exception:
                    return 0.0
            mins = df["MIN"].map(_to_minutes)
        elif "MINUTES" in df.columns:
            mins = pd.to_numeric(df["MINUTES"], errors="coerce").fillna(0.0)
        else:
            return None
        tmp = pd.DataFrame({"PLAYER_ID": df[col_pid], "MIN": mins})
        s = tmp.groupby("PLAYER_ID")["MIN"].mean()
        return s
    except Exception:
        return None


def _height_in_inches(h: str | float | int) -> Optional[float]:
    if isinstance(h, (int, float)):
        return float(h)
    if not h:
        return None
    s = str(h)
    m = re.match(r"(\d+)[-\s:](\d+)", s)
    try:
        if m:
            return int(m.group(1)) * 12 + int(m.group(2))
        if s.isdigit():
            return float(int(s))
    except Exception:
        return None
    return None

def build_tip_dataset(pbp_frames: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Minimal stub dataset builder for tip model training.

    Returns an empty DataFrame with required columns to avoid import-time/lint errors
    on environments without training dependencies. Proper training should be run on
    a dev box and artifacts committed.
    """
    return pd.DataFrame(columns=["height_diff","home_won_tip"])

def train_tip_model(pbp_frames: Dict[str, pd.DataFrame]) -> TipModelArtifacts:
    # Import locally to avoid hard dependency when training is done on x86_64 only
    try:
        from sklearn.linear_model import LogisticRegression
    except Exception as e:
        raise RuntimeError("scikit-learn not available for training: " + str(e))
    df = build_tip_dataset(pbp_frames)
    df = df.dropna(subset=["home_won_tip"])
    if df.empty:
        raise RuntimeError("No tip dataset rows")
    X = df[["height_diff"]].fillna(0.0).values
    y = df["home_won_tip"].astype(int).values
    clf = LogisticRegression(max_iter=1000)
    clf.fit(X, y)
    out_dir = paths.models / "pbp"
    out_dir.mkdir(parents=True, exist_ok=True)
    model_path = out_dir / "tip_winner_lr.joblib"
    joblib.dump(clf, model_path)
    # ONNX export
    onnx_path = None
    try:
        from skl2onnx import convert_sklearn
        from skl2onnx.common.data_types import FloatTensorType
        onx = convert_sklearn(clf, initial_types=[("float_input", FloatTensorType([None, 1]))], target_opset=13)
        onnx_path = out_dir / "tip_winner_lr.onnx"
        with open(onnx_path, "wb") as f:
            f.write(onx.SerializeToString())
    except Exception:
        onnx_path = None
    return TipModelArtifacts(model_path, onnx_path)


def build_first_basket_dataset(pbp_frames: Dict[str, pd.DataFrame], boxscores_dir: Path) -> pd.DataFrame:
    """Return per-player rows across games with label first_basket=1 if scored the first FG in that game.
    Features: minutes (proxy for starter), team indicator.
    """
    rows = []
    # Build a map game_id -> first scorer (player_id or name)
    first_by_game: Dict[str, dict] = {}
    for gid, df in pbp_frames.items():
        ev = _first_fg_event(df)
        if ev:
            first_by_game[str(gid)] = ev
    # Iterate boxscores to get candidate players
    for f in sorted(boxscores_dir.glob("boxscore_*.csv")):
        try:
            b = pd.read_csv(f)
        except Exception:
            continue
        if b is None or b.empty:
            continue
        gid = str(b.get("game_id").iloc[0] if "game_id" in b.columns else b.get("gameId").iloc[0])
        first = first_by_game.get(gid)
        if not first:
            continue
        pid_first = first.get("player_id")
        pname_first = (first.get("player_name") or "").strip()
        # Select top 5 minutes per team as starters proxy
        try:
            b["min_sec"] = b["minutes"].fillna("0:00").apply(lambda s: _mmss_to_seconds(s))
        except Exception:
            b["min_sec"] = 0
        for team in b["teamTricode"].dropna().unique():
            bt = b[b["teamTricode"] == team].copy()
            bt = bt.sort_values("min_sec", ascending=False).head(5)
            for _, r in bt.iterrows():
                pid = r.get("personId")
                pname = (str(r.get("firstName","")) + " " + str(r.get("familyName",""))).strip()
                minutes = float(r.get("min_sec", 0)) / 60.0
                label = 0
                if pid_first and pid and int(pid) == int(pid_first):
                    label = 1
                elif not pid_first and pname_first and pname and pname_first.lower() in pname.lower():
                    label = 1
                rows.append({
                    "game_id": gid,
                    "team": team,
                    "player_id": pid,
                    "player_name": pname,
                    "minutes": minutes,
                    "first_basket": label,
                })
    return pd.DataFrame(rows)


def train_first_basket_model(pbp_frames: Dict[str, pd.DataFrame]) -> FirstBasketModelArtifacts:
    try:
        from sklearn.linear_model import LogisticRegression
    except Exception as e:
        raise RuntimeError("scikit-learn not available for training: " + str(e))
    box_dir = paths.data_processed / "boxscores"
    df = build_first_basket_dataset(pbp_frames, box_dir)
    if df is None or df.empty:
        raise RuntimeError("No first-basket dataset rows")
    # Simple baseline: use minutes as proxy (starters tend to score first more often)
    X = df[["minutes"]].fillna(0.0).values
    y = df["first_basket"].astype(int).values
    clf = LogisticRegression(max_iter=1000)
    clf.fit(X, y)
    out_dir = paths.models / "pbp"
    out_dir.mkdir(parents=True, exist_ok=True)
    model_path = out_dir / "first_basket_lr.joblib"
    joblib.dump(clf, model_path)
    onnx_path = None
    try:
        from skl2onnx import convert_sklearn
        from skl2onnx.common.data_types import FloatTensorType
        onx = convert_sklearn(clf, initial_types=[("float_input", FloatTensorType([None, 1]))], target_opset=13)
        onnx_path = out_dir / "first_basket_lr.onnx"
        with open(onnx_path, "wb") as f:
            f.write(onx.SerializeToString())
    except Exception:
        onnx_path = None
    return FirstBasketModelArtifacts(model_path, onnx_path)


def build_early_threes_dataset(pbp_frames: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for gid, df in pbp_frames.items():
        if df is None or df.empty:
            continue
        # Count made threes in first 3 minutes of Q1
        cnt = 0
        for _, r in df.iterrows():
            t = _time_elapsed_q1(r)
            if t is None or t > 180:
                continue
            text = " ".join([str(r.get(c, "")) for c in _desc_cols(df)]).lower()
            if ("3pt" in text) and ("makes" in text or "made" in text):
                cnt += 1
        rows.append({"game_id": gid, "threes_0_3": int(cnt)})
    return pd.DataFrame(rows)


def train_early_threes_model(pbp_frames: Dict[str, pd.DataFrame]) -> EarlyThreesModelArtifacts:
    try:
        from sklearn.ensemble import GradientBoostingRegressor
    except Exception as e:
        raise RuntimeError("scikit-learn not available for training: " + str(e))
    df = build_early_threes_dataset(pbp_frames)
    if df is None or df.empty:
        raise RuntimeError("No early threes dataset rows")
    # Very simple baseline regressor: predict mean using a constant-like model via GBR with tiny depth
    # Future: add features (team early 3PA rates, pace)
    X = np.zeros((len(df), 1))
    y = df["threes_0_3"].astype(float).values
    reg = GradientBoostingRegressor(max_depth=1, n_estimators=50, learning_rate=0.1)
    reg.fit(X, y)
    out_dir = paths.models / "pbp"
    out_dir.mkdir(parents=True, exist_ok=True)
    model_path = out_dir / "early_threes_gbr.joblib"
    joblib.dump(reg, model_path)
    onnx_path = None
    try:
        from skl2onnx import convert_sklearn
        from skl2onnx.common.data_types import FloatTensorType
        onx = convert_sklearn(reg, initial_types=[("float_input", FloatTensorType([None, 1]))], target_opset=13)
        onnx_path = out_dir / "early_threes_gbr.onnx"
        with open(onnx_path, "wb") as f:
            f.write(onx.SerializeToString())
    except Exception:
        onnx_path = None
    return EarlyThreesModelArtifacts(model_path, onnx_path)


def _load_pbp_frames(source: str | None = None) -> Dict[str, pd.DataFrame]:
    """Load PBP from processed folder. Returns map game_id -> DataFrame.
    If source is a date 'YYYY-MM-DD', load that date file and split by game_id.
    """
    out: Dict[str, pd.DataFrame] = {}
    if source and re.match(r"\d{4}-\d{2}-\d{2}", str(source)):
        p = paths.data_processed / f"pbp_{source}.csv"
        if p.exists():
            df = pd.read_csv(p)
            if "game_id" in df.columns:
                for gid, grp in df.groupby("game_id"):
                    # Skip malformed IDs such as NA/None/blank or non-numeric placeholders
                    key_raw = str(gid).strip()
                    if not key_raw or key_raw.lower() in ("na", "nan", "none"):
                        continue
                    if not key_raw.isdigit():
                        continue
                    key = key_raw.zfill(10)
                    out[key] = grp.copy()
            else:
                out["unknown"] = df
            return out
    # Else, read per-game files if present
    d = paths.data_processed / "pbp"
    if d.exists():
        for f in d.glob("pbp_*.csv"):
            try:
                df = pd.read_csv(f)
                # Derive a valid numeric gameId from filename; skip files like 'pbp_        NA.csv'
                m = re.findall(r"(\d{9,12})", f.name)
                key = m[0] if m else f.stem.replace("pbp_", "").strip()
                if not key or (not key.isdigit()):
                    continue
                out[str(key.zfill(10))] = df
            except Exception:
                continue
    return out


def train_all_pbp_markets() -> dict:
    frames = _load_pbp_frames()
    if not frames:
        raise RuntimeError("No PBP frames found under data/processed")
    tip = train_tip_model(frames)
    fb = train_first_basket_model(frames)
    th = train_early_threes_model(frames)
    return {
        "tip": tip,
        "first_basket": fb,
        "early_threes": th,
    }


def _make_onnx_session(onnx_path: Path):
    """Create an ONNX Runtime session with QNN if available, else CPU.

    Returns (session, input_name) or (None, None) on failure.
    """
    try:
        import onnxruntime as ort  # type: ignore
    except Exception:
        return None, None
    try:
        providers: List[str] = []
        # Prefer QNN on ARM64 if available
        try:
            providers = [p for p in ("QNNExecutionProvider", "CPUExecutionProvider") if p in ort.get_available_providers()]
            if not providers:
                providers = ["CPUExecutionProvider"]
        except Exception:
            providers = ["CPUExecutionProvider"]
        sess = ort.InferenceSession(str(onnx_path), providers=providers)
        input_name = sess.get_inputs()[0].name
        return sess, input_name
    except Exception:
        return None, None


def _onnx_predict_proba_binary(sess, input_name: str, X: np.ndarray) -> Optional[np.ndarray]:
    """Run ONNX session and return P(y=1) for a binary classifier if possible."""
    if sess is None or not input_name:
        return None
    try:
        x = X.astype(np.float32)
        outs = sess.run(None, {input_name: x})
        # Try to find a probability-like output [N,2] and take class 1
        for out in outs:
            arr = np.array(out)
            if arr.ndim == 2 and arr.shape[1] == 2:
                return arr[:, 1].astype(float)
            if arr.ndim == 2 and arr.shape[1] == 1:
                # Could be decision function scaled, clip to [0,1]
                v = arr[:, 0]
                v = 1.0 / (1.0 + np.exp(-v))
                return v.astype(float)
        # If label + prob map format (skl2onnx sometimes uses maps), try to coerce
        return None
    except Exception:
        return None


def _onnx_predict_regression(sess, input_name: str, X: np.ndarray) -> Optional[np.ndarray]:
    if sess is None or not input_name:
        return None
    try:
        x = X.astype(np.float32)
        outs = sess.run(None, {input_name: x})
        for out in outs:
            arr = np.array(out)
            if arr.ndim == 2 and arr.shape[1] == 1:
                return arr[:, 0].astype(float)
            if arr.ndim == 1:
                return arr.astype(float)
        return None
    except Exception:
        return None


def _mmss_to_seconds(val) -> int:
    """Parse a minutes string to total seconds.

    Accepts formats like 'MM:SS', 'M:SS', 'MM-SS', numeric seconds, or floats.
    Returns 0 on failure.
    """
    try:
        if val is None:
            return 0
        # If already numeric, interpret as seconds
        if isinstance(val, (int, float, np.integer, np.floating)):
            v = float(val)
            if np.isnan(v):
                return 0
            return int(v)
        s = str(val).strip()
        if not s:
            return 0
        # Replace dash with colon, then split
        s = s.replace('-', ':')
        parts = s.split(':')
        if len(parts) == 1:
            # Might be plain seconds in string form
            return int(float(parts[0]))
        # Take last two as mm:ss
        mm = int(parts[-2]) if parts[-2] else 0
        ss = int(parts[-1]) if parts[-1] else 0
        if ss < 0 or ss >= 60:
            # Clamp weird seconds
            ss = max(0, min(59, ss))
        return int(mm * 60 + ss)
    except Exception:
        return 0


def _game_ids_for_date(date_str: str) -> List[str]:
    """Determine game IDs for a given date using local processed files.

    Preference:
    1) data/processed/pbp_<date>.csv (column game_id)
    2) data/processed/boxscores_<date>.csv (column gameId or game_id)
    3) NBA scoreboard (pregame fallback) via NBA CDN
    Returns a list of string IDs; empty if none found.
    """
    # 1) pbp_<date>.csv
    p1 = paths.data_processed / f"pbp_{date_str}.csv"
    try:
        if p1.exists():
            df = pd.read_csv(p1)
            col = "game_id" if "game_id" in df.columns else None
            if col:
                gids = [str(x).strip() for x in df[col].dropna().unique().tolist()]
                # Keep only numeric IDs and zero-pad to 10
                gids = [g.zfill(10) for g in gids if g.isdigit()]
                if gids:
                    return gids
    except Exception:
        pass
    # 2) boxscores_<date>.csv
    p2 = paths.data_processed / f"boxscores_{date_str}.csv"
    try:
        if p2.exists():
            df = pd.read_csv(p2)
            col = "gameId" if "gameId" in df.columns else ("game_id" if "game_id" in df.columns else None)
            if col:
                gids = [str(x).strip() for x in df[col].dropna().unique().tolist()]
                gids = [g.zfill(10) for g in gids if g.isdigit()]
                if gids:
                    return gids
    except Exception:
        pass
    # 2.5) Odds-driven mapping via local season schedule helper (maps HOME/AWAY tricodes -> gameId)
    try:
        odds_path = paths.data_processed / f"game_odds_{date_str}.csv"
        if odds_path.exists():
            go = pd.read_csv(odds_path)
            if go is not None and not go.empty and {"home_team","visitor_team"}.issubset(go.columns):
                try:
                    from .teams import to_tricode as _to_tri
                except Exception:
                    _to_tri = lambda x: str(x)  # fallback no-op
                try:
                    from .schedule import fetch_schedule_2025_26 as _fetch_sched
                    sched = _fetch_sched()
                except Exception:
                    sched = None
                if sched is not None and not sched.empty:
                    ds = pd.to_datetime(date_str).date()
                    day = sched[pd.to_datetime(sched.get("date_est"), errors="coerce").dt.date == ds].copy()
                    gids = set()
                    for _, r in go.iterrows():
                        h = _to_tri(str(r.get("home_team")))
                        a = _to_tri(str(r.get("visitor_team")))
                        cand = day[((day.get("home_tricode").astype(str).str.upper()==h.upper()) & (day.get("away_tricode").astype(str).str.upper()==a.upper()))
                                   | ((day.get("home_tricode").astype(str).str.upper()==a.upper()) & (day.get("away_tricode").astype(str).str.upper()==h.upper()))]
                        if not cand.empty:
                            gid = str(cand.iloc[0].get("game_id") or "").strip()
                            if gid and gid.isdigit():
                                gids.add(gid.zfill(10))
                    if gids:
                        return sorted(gids)
    except Exception:
        pass

    # 3) NBA CDN fallbacks (no auth, browser-accessible)
    #    a) If target date is today, use liveData/todaysScoreboard_00.json
    #    b) Otherwise, use static season schedule and filter by ET calendar day
    try:
        import requests  # type: ignore
        from datetime import datetime as _dt, date as _date
        target = _dt.strptime(date_str, "%Y-%m-%d").date()
        today = _date.today()
        # a) Today's scoreboard (live)
        if target == today:
            u = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
            r = requests.get(u, headers={"Accept":"application/json","User-Agent":"nba-betting/1.0"}, timeout=10)
            if r.ok:
                j = r.json() or {}
                games = (j.get("scoreboard") or {}).get("games") or []
                out = []
                for g in games:
                    gid = str(g.get("gameId") or "").strip()
                    if gid:
                        out.append(gid)
                if out:
                    return out
        # b) Full-season schedule (works for any date)
        u = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_1.json"
        r = requests.get(u, headers={"Accept":"application/json","User-Agent":"nba-betting/1.0"}, timeout=15)
        if r.ok:
            j = r.json() or {}
            league = j.get("leagueSchedule") or {}
            game_dates = league.get("gameDates") or []
            out: List[str] = []
            for gd in game_dates:
                for g in (gd.get("games") or []):
                    gid = str(g.get("gameId") or "").strip()
                    if not gid:
                        continue
                    # Prefer startTimeUTC to determine ET calendar day; fall back to gd.gameDate on failure
                    ds_ok = False
                    st = g.get("gameDateTimeUTC") or g.get("startTimeUTC") or g.get("startTimeUTCFormatted")
                    if st:
                        try:
                            dt = pd.to_datetime(st, utc=True)
                            ds_et = dt.tz_convert("US/Eastern").strftime("%Y-%m-%d")
                            ds_ok = (ds_et == date_str)
                        except Exception:
                            ds_ok = False
                    if not ds_ok:
                        try:
                            gd_str = str(gd.get("gameDate") or "").split("T")[0]
                            ds_ok = (gd_str == date_str)
                        except Exception:
                            ds_ok = False
                    if ds_ok:
                        out.append(gid)
            if out:
                return out
    except Exception:
        pass
    return []


def _gid_team_map_for_date(date_str: str) -> Dict[str, tuple[str, str]]:
    """Return mapping game_id -> (home_tri, away_tri) for a date using NBA public CDN.

    Uses today's live scoreboard for the current date, else filters the static season schedule.
    Returns an empty dict on failure.
    """
    try:
        import requests  # type: ignore
        from datetime import datetime as _dt, date as _date
        target = _dt.strptime(date_str, "%Y-%m-%d").date()
        today = _date.today()
        # Today's live scoreboard (augment; do not return early)
        out: Dict[str, tuple[str, str]] = {}
        if target == today:
            u = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
            r = requests.get(u, headers={"Accept":"application/json","User-Agent":"nba-betting/1.0"}, timeout=10)
            if r.ok:
                j = r.json() or {}
                games = (j.get("scoreboard") or {}).get("games") or []
                for g in games:
                    gid = str(g.get("gameId") or "").strip()
                    home = str(((g.get("homeTeam") or {}).get("teamTricode")) or "").upper()
                    away = str(((g.get("awayTeam") or {}).get("teamTricode")) or "").upper()
                    if gid and home and away:
                        out[gid] = (home, away)
        # Static season schedule (filter by ET calendar day)
        u = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_1.json"
        r = requests.get(u, headers={"Accept":"application/json","User-Agent":"nba-betting/1.0"}, timeout=15)
        if r.ok:
            j = r.json() or {}
            league = j.get("leagueSchedule") or {}
            game_dates = league.get("gameDates") or []
            out: Dict[str, tuple[str, str]] = {}
            for gd in game_dates:
                for g in (gd.get("games") or []):
                    gid = str(g.get("gameId") or "").strip()
                    if not gid:
                        continue
                    # Determine ET day for the game using startTimeUTC if available
                    ds_ok = False
                    st = g.get("gameDateTimeUTC") or g.get("startTimeUTC") or g.get("startTimeUTCFormatted")
                    if st:
                        try:
                            dt = pd.to_datetime(st, utc=True)
                            ds_et = dt.tz_convert("US/Eastern").strftime("%Y-%m-%d")
                            ds_ok = (ds_et == date_str)
                        except Exception:
                            ds_ok = False
                    if not ds_ok:
                        try:
                            gd_str = str(gd.get("gameDate") or "").split("T")[0]
                            ds_ok = (gd_str == date_str)
                        except Exception:
                            ds_ok = False
                    if not ds_ok:
                        continue
                    home = str(((g.get("homeTeam") or {}).get("teamTricode")) or "").upper()
                    away = str(((g.get("awayTeam") or {}).get("teamTricode")) or "").upper()
                    if gid and home and away:
                        out[gid] = (home, away)
            return out
    except Exception:
        return {}
    return {}


def _select_pregame_starters(roster_df: pd.DataFrame, team_tri: str, minutes_avg: Optional[pd.Series] = None) -> list[dict]:
    """Heuristic pick of 5 likely starters for a team when boxscores aren't available.

    Strategy:
    - Parse positions (G/F/C). Prefer 2 guards, 2 forwards, 1 center when available.
    - Within each group, prefer taller players using HEIGHT as a proxy.
    - Fallback: fill remaining spots by tallest available players.
    Returns list of dicts with keys: personId, firstName, familyName, teamTricode, minutes_proxy.
    """
    # First, try deriving starters purely from player logs for this team (more reliable team labels)
    try:
        logs_p = paths.data_processed / "player_logs.csv"
        if logs_p.exists():
            logs = pd.read_csv(logs_p)
            if {"TEAM_ABBREVIATION","PLAYER_ID"}.issubset(set(logs.columns)):
                sub = logs[logs["TEAM_ABBREVIATION"].astype(str).str.upper() == str(team_tri).upper()].copy()
                if not sub.empty:
                    # Parse minutes
                    if "MIN" in sub.columns:
                        def _to_min(v):
                            s = str(v)
                            if ':' in s:
                                return float(_mmss_to_seconds(s) / 60.0)
                            try:
                                return float(s)
                            except Exception:
                                return 0.0
                        sub["MINUTES"] = sub["MIN"].map(_to_min)
                    elif "MINUTES" in sub.columns:
                        sub["MINUTES"] = pd.to_numeric(sub["MINUTES"], errors="coerce").fillna(0.0)
                    else:
                        sub["MINUTES"] = 0.0
                    mins = sub.groupby("PLAYER_ID")["MINUTES"].mean().sort_values(ascending=False)
                    picked = []
                    for pid, m in mins.head(5).items():
                        # Find a representative name row
                        row = sub[sub["PLAYER_ID"] == pid].iloc[0]
                        name = str(row.get("PLAYER_NAME") or row.get("PLAYER") or "").strip()
                        parts = name.split()
                        fn = " ".join(parts[:-1]) if len(parts) >= 2 else name
                        ln = parts[-1] if len(parts) >= 2 else ""
                        picked.append({
                            "personId": str(int(pid)) if str(pid).isdigit() else str(pid),
                            "firstName": fn,
                            "familyName": ln,
                            "teamTricode": str(team_tri).upper(),
                            "isG": 0, "isF": 0, "isC": 0,  # unknown from logs
                            "height_in": 0.0,
                            "avg_min": float(m),
                            "minutes_proxy": float(m),
                        })
                    if len(picked) >= 5:
                        return picked[:5]
    except Exception:
        pass
    if roster_df is None or roster_df.empty:
        return []
    # Normalize expected columns from nba_api CommonTeamRoster
    df = roster_df.copy()
    # Filter this team
    def _find_tri_col(dfx: pd.DataFrame) -> Optional[str]:
        for cand in ("TEAM_ABBREVIATION","teamTricode","TEAM_TRI","team_tri"):
            if cand in dfx.columns:
                return cand
        # Heuristic: find any col with many 3-letter uppercase tokens
        for c in dfx.columns:
            try:
                frac = dfx[c].astype(str).str.fullmatch(r"[A-Z]{3}").mean()
                if pd.notna(frac) and frac > 0.5:
                    return c
            except Exception:
                continue
        return None
    tri_col = _find_tri_col(df)
    if tri_col is not None:
        df = df[df[tri_col].astype(str).str.upper() == str(team_tri).upper()].copy()
    if df.empty:
        return []
    # Build helper columns
    def _pos_flags(p: str) -> tuple[int, int, int]:
        p = str(p or "").upper()
        return (1 if "G" in p else 0, 1 if "F" in p else 0, 1 if "C" in p else 0)
    def _split_name(row) -> tuple[str, str]:
        # Try FIRST_NAME/LAST_NAME else split PLAYER
        fn = str(row.get("FIRST_NAME") or row.get("firstName") or "").strip()
        ln = str(row.get("LAST_NAME") or row.get("lastName") or "").strip()
        if not fn and not ln:
            nm = str(row.get("PLAYER") or row.get("player_name") or "").strip()
            parts = nm.split()
            if len(parts) >= 2:
                fn = " ".join(parts[:-1]); ln = parts[-1]
            else:
                fn = nm; ln = ""
        return fn, ln
    def _pid(row) -> str | None:
        for c in ("PLAYER_ID","personId","PERSON_ID"):
            if c in row and pd.notna(row[c]):
                return str(int(row[c])) if str(row[c]).isdigit() else str(row[c])
        return None
    # Compute height in inches to rank
    def _hin(row) -> float:
        for c in ("HEIGHT","height","Height"):
            if c in row and pd.notna(row[c]):
                return float(_height_in_inches(row[c]) or 0.0)
        return 0.0
    # Annotate
    tmp = []
    for _, r in df.iterrows():
        g, f, c = _pos_flags(r.get("POSITION"))
        h = _hin(r)
        pid = _pid(r)
        fn, ln = _split_name(r)
        if not pid:
            continue
        avg_min = None
        if minutes_avg is not None:
            try:
                # minutes_avg indexed by PLAYER_ID; pid may be str-int convertible
                key = int(pid) if str(pid).isdigit() else pid
                if key in minutes_avg.index:
                    avg_min = float(minutes_avg.loc[key])
            except Exception:
                avg_min = None
        tmp.append({
            "personId": pid,
            "firstName": fn,
            "familyName": ln,
            "teamTricode": str(team_tri).upper(),
            "isG": g, "isF": f, "isC": c,
            "height_in": h,
            "avg_min": avg_min,
        })
    if not tmp:
        return []
    # Prefer top 5 by avg_min if available for enough players
    with_min = [x for x in tmp if x.get("avg_min") is not None]
    picked: list[dict] = []
    if len(with_min) >= 5:
        picked = sorted(with_min, key=lambda x: x["avg_min"], reverse=True)[:5]
    else:
        # Fallback to pos/height heuristic
        g_list = sorted([x for x in tmp if x["isG"]], key=lambda x: x["height_in"], reverse=True)
        f_list = sorted([x for x in tmp if x["isF"]], key=lambda x: x["height_in"], reverse=True)
        c_list = sorted([x for x in tmp if x["isC"]], key=lambda x: x["height_in"], reverse=True)
        # 1 C, 2 G, 2 F
        if c_list:
            picked.append(c_list[0])
        picked.extend(g_list[:2])
        picked.extend(f_list[:2])
        # Fill remaining if less than 5 by tallest remaining
        if len(picked) < 5:
            remaining = [x for x in tmp if x not in picked]
            remaining = sorted(remaining, key=lambda x: x["height_in"], reverse=True)
            for x in remaining:
                if len(picked) >= 5:
                    break
                picked.append(x)
    # Assign minute proxy for starters (simple pos-based differentiation)
    for x in picked:
        if x.get("avg_min") is not None:
            x["minutes_proxy"] = float(x.get("avg_min") or 0.0)
        else:
            if x.get("isG") and not x.get("isF") and not x.get("isC"):
                x["minutes_proxy"] = 34.0
            elif x.get("isC") and not x.get("isG"):
                x["minutes_proxy"] = 30.0
            else:
                x["minutes_proxy"] = 32.0
    return picked[:5]


def predict_tip_for_date(date_str: str) -> pd.DataFrame:
    # Score with ONNX if available (QNN/CPU), else joblib if sklearn present, else baseline 0.5.
    gids = _game_ids_for_date(date_str)
    onnx_path = paths.models / "pbp" / "tip_winner_lr.onnx"
    sess = None; input_name = None
    if onnx_path.exists():
        sess, input_name = _make_onnx_session(onnx_path)
    clf = None
    if sess is None:
        model_path = paths.models / "pbp" / "tip_winner_lr.joblib"
        try:
            clf = joblib.load(model_path) if model_path.exists() else None
        except Exception:
            clf = None
    # Pregame: estimate height_diff using roster-based likely jump ball participants (centers)
    team_map = _gid_team_map_for_date(date_str)
    roster = _load_rosters_latest()
    def _center_height_for_team(team_tri: str) -> float | None:
        # Try starters from our heuristic first
        starters = _select_pregame_starters(roster, team_tri)
        c_heights = [s.get("height_in", 0.0) for s in starters if s.get("isC")]
        if c_heights:
            return float(max(c_heights))
        # Fallback: tallest roster player
        if roster is not None and not roster.empty:
            tri_col = "TEAM_ABBREVIATION" if "TEAM_ABBREVIATION" in roster.columns else ("teamTricode" if "teamTricode" in roster.columns else None)
            if tri_col:
                sub = roster[roster[tri_col].astype(str).str.upper() == str(team_tri).upper()].copy()
                if not sub.empty:
                    hs = [float(_height_in_inches(h) or 0.0) for h in sub.get("HEIGHT", [])]
                    if hs:
                        return float(max(hs))
        return None
    # Optional calibration: tip logit intercept bias from pbp_calibration.csv (last <= date)
    tip_logit_bias = 0.0
    try:
        cal_path = paths.data_processed / "pbp_calibration.csv"
        if cal_path.exists():
            cdf = pd.read_csv(cal_path)
            if not cdf.empty and "tip_logit_bias" in cdf.columns:
                cdf["date"] = pd.to_datetime(cdf["date"], errors="coerce").dt.date
                target = pd.to_datetime(date_str).date()
                cdf = cdf[cdf["date"] <= target]
                if not cdf.empty:
                    tip_logit_bias = float(pd.to_numeric(cdf["tip_logit_bias"], errors="coerce").iloc[-1])
    except Exception:
        tip_logit_bias = 0.0

    rows = []
    for gid in gids:
        # Estimate height diff if we know teams; else 0 baseline
        hdiff = 0.0
        pair = team_map.get(gid) or team_map.get(str(gid).zfill(10))
        if pair:
            home, away = pair
            hh = _center_height_for_team(home)
            ah = _center_height_for_team(away)
            if hh is not None and ah is not None:
                hdiff = float(hh - ah)
        x = np.array([[hdiff]], dtype=np.float32)
        ph: float = 0.5
        # Prefer ONNX
        if sess is not None and input_name:
            proba = _onnx_predict_proba_binary(sess, input_name, x)
            if proba is not None and len(proba) > 0:
                ph = float(proba[0])
        elif clf is not None:
            try:
                ph = float(clf.predict_proba(x)[0, 1])
            except Exception:
                ph = 0.5
        else:
            # Heuristic fallback when no model available: logistic on height diff (inches)
            try:
                k = 0.06  # sensitivity per inch
                ph = float(1.0 / (1.0 + np.exp(-k * hdiff)))
                # mild clamp
                ph = max(0.35, min(0.65, ph))
            except Exception:
                ph = 0.5
        # Apply calibration in logit space if configured
        try:
            if tip_logit_bias != 0.0:
                p0 = float(min(max(ph, 1e-6), 1-1e-6))
                logit = np.log(p0/(1-p0))
                p_adj = 1.0/(1.0 + np.exp(-(logit + tip_logit_bias)))
                ph = float(min(max(p_adj, 0.0), 1.0))
        except Exception:
            pass
        rows.append({"game_id": gid, "prob_home_tip": ph})
    out = pd.DataFrame(rows)
    out_path = paths.data_processed / f"tip_winner_probs_{date_str}.csv"
    out.to_csv(out_path, index=False)
    return out


def predict_first_basket_for_date(date_str: str) -> pd.DataFrame:
    # Build candidate list from boxscores and minutes feature; score with ONNX if available, else LR joblib; normalize across game.
    onnx_path = paths.models / "pbp" / "first_basket_lr.onnx"
    sess = None; input_name = None
    if onnx_path.exists():
        sess, input_name = _make_onnx_session(onnx_path)
    clf = None
    if sess is None:
        model_path = paths.models / "pbp" / "first_basket_lr.joblib"
        try:
            clf = joblib.load(model_path) if model_path.exists() else None
        except Exception:
            clf = None
    box_dir = paths.data_processed / "boxscores"
    # Determine games on date
    gids = _game_ids_for_date(date_str)
    # Map game -> (home, away) to enable optional coupling with tip probability
    team_map = _gid_team_map_for_date(date_str)
    # Optional calibration: global temperature for candidate score normalization (fb_temp)
    fb_temp = 1.0
    try:
        cal_path = paths.data_processed / "pbp_calibration.csv"
        if cal_path.exists():
            cdf = pd.read_csv(cal_path)
            if not cdf.empty and "fb_temp" in cdf.columns:
                cdf["date"] = pd.to_datetime(cdf["date"], errors="coerce").dt.date
                target = pd.to_datetime(date_str).date()
                cdf = cdf[cdf["date"] <= target]
                if not cdf.empty:
                    fb_temp = float(pd.to_numeric(cdf["fb_temp"], errors="coerce").iloc[-1])
    except Exception:
        fb_temp = 1.0

    # Optional coupling: weight team-level chances by tip winner probability.
    # Controlled by calibration parameter fb_tip_alpha in [0,1]. If absent, default small effect.
    fb_tip_alpha = 0.0
    try:
        cal_path = paths.data_processed / "pbp_calibration.csv"
        if cal_path.exists():
            cdf = pd.read_csv(cal_path)
            if not cdf.empty and "fb_tip_alpha" in cdf.columns:
                cdf["date"] = pd.to_datetime(cdf["date"], errors="coerce").dt.date
                target = pd.to_datetime(date_str).date()
                cdf = cdf[cdf["date"] <= target]
                if not cdf.empty:
                    fb_tip_alpha = float(pd.to_numeric(cdf["fb_tip_alpha"], errors="coerce").iloc[-1])
    except Exception:
        fb_tip_alpha = 0.0

    # Load tip probabilities if coupling is enabled; generate if missing and possible
    tip_probs: dict[str, float] = {}
    if fb_tip_alpha and fb_tip_alpha > 0:
        tp_path = paths.data_processed / f"tip_winner_probs_{date_str}.csv"
        tp_df = None
        try:
            if tp_path.exists():
                tp_df = pd.read_csv(tp_path)
            else:
                # Try to compute on the fly (best-effort)
                try:
                    tp_df = predict_tip_for_date(date_str)
                except Exception:
                    tp_df = None
        except Exception:
            tp_df = None
        if tp_df is not None and not tp_df.empty and {"game_id","prob_home_tip"}.issubset(tp_df.columns):
            for _, r in tp_df.iterrows():
                gid_key = str(r.get("game_id")).zfill(10)
                try:
                    tip_probs[gid_key] = float(r.get("prob_home_tip", 0.5))
                except Exception:
                    tip_probs[gid_key] = 0.5

    rows = []
    audit_rows = []
    for gid in gids:
        # Use that game's boxscore to pick candidates
        bs_path = box_dir / f"boxscore_{gid}.csv"
        if not bs_path.exists():
            # Try zero-padded 10-digit NBA gameId format
            gid_padded = str(gid).zfill(10)
            bs_path = box_dir / f"boxscore_{gid_padded}.csv"
        candidates = []
        if bs_path.exists():
            b = pd.read_csv(bs_path)
            b["min_sec"] = b["minutes"].fillna("0:00").apply(lambda s: _mmss_to_seconds(s))
            for team in b["teamTricode"].dropna().unique():
                bt = b[b["teamTricode"] == team].copy().sort_values("min_sec", ascending=False).head(5)
                for _, r in bt.iterrows():
                    minutes = float(r.get("min_sec", 0)) / 60.0
                    x = np.array([[minutes]], dtype=np.float32)
                    raw_p = minutes  # proportional to minutes; normalized later
                    if sess is not None and input_name:
                        proba = _onnx_predict_proba_binary(sess, input_name, x)
                        if proba is not None and len(proba) > 0:
                            raw_p = float(proba[0])
                    elif clf is not None:
                        try:
                            raw_p = float(clf.predict_proba(x)[0, 1])
                        except Exception:
                            raw_p = minutes
                    pname = (str(r.get("firstName","")) + " " + str(r.get("familyName",""))).strip()
                    candidates.append((r.get("personId"), pname, r.get("teamTricode"), raw_p, "boxscore", minutes))
        else:
            # Pregame fallback: derive teams for this gid and pick likely starters from latest rosters
            if gid in team_map:
                home, away = team_map[gid]
            else:
                # Try zero-padded key
                home = away = None
                gzp = str(gid).zfill(10)
                if gzp in team_map:
                    home, away = team_map[gzp]
            if home and away:
                roster = _load_rosters_latest()
                minutes_avg = _load_player_minutes_avg()
                for t in (home, away):
                    starters = _select_pregame_starters(roster, t, minutes_avg=minutes_avg)
                    for s in starters:
                        minutes = float(s.get("minutes_proxy", 32.0))
                        x = np.array([[minutes]], dtype=np.float32)
                        raw_p = minutes  # proportional to minutes; normalized later
                        if sess is not None and input_name:
                            proba = _onnx_predict_proba_binary(sess, input_name, x)
                            if proba is not None and len(proba) > 0:
                                raw_p = float(proba[0])
                        elif clf is not None:
                            try:
                                raw_p = float(clf.predict_proba(x)[0, 1])
                            except Exception:
                                raw_p = minutes
                        pname = (str(s.get("firstName","")) + " " + str(s.get("familyName",""))).strip()
                        candidates.append((s.get("personId"), pname, t, raw_p, "roster", minutes))
        if candidates:
            # Optional team weighting from tip probabilities
            team_weights: dict[str, float] = {}
            gid_key = str(gid).zfill(10)
            if fb_tip_alpha and fb_tip_alpha > 0 and gid_key in tip_probs and (gid in team_map or gid_key in team_map):
                pair = team_map.get(gid) or team_map.get(gid_key)
                if pair:
                    home_tri, away_tri = pair
                    ph = float(tip_probs.get(gid_key, 0.5))
                    # Team weight: interpolate between 0.5 and tip probability with alpha
                    w_home = 0.5 + float(fb_tip_alpha) * (ph - 0.5)
                    w_away = 1.0 - w_home
                    team_weights[home_tri] = float(max(0.0, min(1.0, w_home)))
                    team_weights[away_tri] = float(max(0.0, min(1.0, w_away)))
            # Build scores and apply team weights if any
            scores = []
            for c in candidates:
                base = max(0.0, float(c[3]))
                ttri = str(c[2]) if c[2] is not None else ""
                w = team_weights.get(ttri, 0.5 if fb_tip_alpha and fb_tip_alpha > 0 else 1.0)
                # If coupling enabled but team not in map, default to neutral 0.5 multiplier; else 1.0
                if fb_tip_alpha and fb_tip_alpha > 0:
                    scores.append(base * w)
                else:
                    scores.append(base)
            scores = np.array(scores, dtype=float)
            try:
                if fb_temp is not None and fb_temp > 0 and abs(fb_temp - 1.0) > 1e-6:
                    # Temperature scaling on scores: higher fb_temp flattens, lower sharpens
                    # Use exponent 1/fb_temp to match standard temperature behavior
                    scores = np.power(scores + 1e-9, 1.0/float(fb_temp))
            except Exception:
                pass
            denom = scores.sum()
            if denom <= 0:
                probs = np.full_like(scores, 1.0 / len(scores))
            else:
                probs = scores / denom
            for (pid, pname, t, raw_p, src, minutes_used), pr in zip(candidates, probs):
                rows.append({"game_id": gid, "team": t, "player_id": pid, "player_name": pname, "prob_first_basket": float(pr)})
                audit_rows.append({
                    "date": date_str,
                    "game_id": gid,
                    "team": t,
                    "player_id": pid,
                    "player_name": pname,
                    "minutes_used": float(minutes_used),
                    "source": src,
                    "raw_score": float(raw_p),
                    "probability": float(pr),
                    "tip_prob_home": float(tip_probs.get(str(gid).zfill(10), np.nan)) if (fb_tip_alpha and fb_tip_alpha > 0) else np.nan,
                    "team_weight": float(team_weights.get(str(t), np.nan)) if (fb_tip_alpha and fb_tip_alpha > 0) else np.nan,
                })
    out = pd.DataFrame(rows)
    out_path = paths.data_processed / f"first_basket_probs_{date_str}.csv"
    out.to_csv(out_path, index=False)
    # Write audit if any
    if audit_rows:
        audit_df = pd.DataFrame(audit_rows)
        audit_path = paths.data_processed / f"first_basket_candidates_{date_str}.csv"
        audit_df.to_csv(audit_path, index=False)
    return out


def predict_early_threes_for_date(date_str: str) -> pd.DataFrame:
    # Predict with ONNX if available; else joblib; else baseline constant 1.2
    onnx_path = paths.models / "pbp" / "early_threes_gbr.onnx"
    sess = None; input_name = None
    if onnx_path.exists():
        sess, input_name = _make_onnx_session(onnx_path)
    reg = None
    if sess is None:
        model_path = paths.models / "pbp" / "early_threes_gbr.joblib"
        try:
            reg = joblib.load(model_path) if model_path.exists() else None
        except Exception:
            reg = None
    gids = _game_ids_for_date(date_str)
    team_map = _gid_team_map_for_date(date_str)
    # Optional heuristic: scale yhat by average team 3PA factor from recent logs if available
    logs_csv = paths.data_processed / "player_logs.csv"
    team_3pa_avg = None
    league_3pa_avg = None
    if logs_csv.exists():
        try:
            logs = pd.read_csv(logs_csv)
            # Normalize columns (nba_api leaguegamelog)
            # Use FG3A (3PA) per team-game: sum(player FG3A) grouped by TEAM_ABBREVIATION+GAME_ID
            if {"TEAM_ABBREVIATION","GAME_ID","FG3A"}.issubset(set(logs.columns)):
                tg = logs.groupby(["TEAM_ABBREVIATION","GAME_ID"])['FG3A'].sum().reset_index()
                team_3pa_avg = tg.groupby("TEAM_ABBREVIATION")['FG3A'].mean()
                league_3pa_avg = float(team_3pa_avg.mean()) if len(team_3pa_avg) > 0 else None
        except Exception:
            team_3pa_avg = None
            league_3pa_avg = None
    # Optional calibration: apply intercept bias learned from recent reconciliation
    cal_bias = 0.0
    try:
        cal_path = paths.data_processed / "pbp_calibration.csv"
        if cal_path.exists():
            cdf = pd.read_csv(cal_path)
            if not cdf.empty:
                cdf["date"] = pd.to_datetime(cdf["date"], errors="coerce").dt.date
                target = pd.to_datetime(date_str).date()
                cdf = cdf[cdf["date"] <= target]
                if not cdf.empty and "thr_bias" in cdf.columns:
                    cal_bias = float(pd.to_numeric(cdf["thr_bias"], errors="coerce").iloc[-1])
    except Exception:
        cal_bias = 0.0

    rows = []
    for gid in gids:
        X = np.zeros((1,1), dtype=np.float32)
        yhat = 1.2
        if sess is not None and input_name:
            yh = _onnx_predict_regression(sess, input_name, X)
            if yh is not None and len(yh) > 0:
                yhat = float(yh[0])
        elif reg is not None:
            try:
                yhat = float(reg.predict(X)[0])
            except Exception:
                yhat = 1.2
        # Apply heuristic scaling by team 3PA if available
        pair = team_map.get(gid) or team_map.get(str(gid).zfill(10))
        if pair and team_3pa_avg is not None and league_3pa_avg and league_3pa_avg > 0:
            home, away = pair
            h_rate = float(team_3pa_avg.get(home, league_3pa_avg))
            a_rate = float(team_3pa_avg.get(away, league_3pa_avg))
            factor = max(0.6, min(1.4, 0.5 * (h_rate + a_rate) / league_3pa_avg))
            yhat = float(yhat * factor)
        # Apply calibration bias (additive on expected threes) and clamp
        try:
            yhat = max(0.0, float(yhat + cal_bias))
        except Exception:
            yhat = max(0.0, yhat)
        rows.append({"game_id": gid, "expected_threes_0_3": yhat, "prob_ge_1": 1.0 - float(np.exp(-max(0.0, yhat)))})
    out = pd.DataFrame(rows)
    out_path = paths.data_processed / f"early_threes_{date_str}.csv"
    out.to_csv(out_path, index=False)
    return out
