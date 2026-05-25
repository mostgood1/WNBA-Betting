from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo
import pandas as pd
import requests

from .league import LEAGUE, season_label_from_date
from .teams import to_tricode


EASTERN_TZ = ZoneInfo("America/New_York")


def team_last_game_dates(games: pd.DataFrame) -> dict[str, datetime]:
    df = games.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df = df.sort_values("date")
    last_dates: dict[str, datetime] = {}
    for _, row in df.iterrows():
        d = row["date"]
        last_dates[row["home_team"]] = d
        last_dates[row["visitor_team"]] = d
    return last_dates


def compute_rest_for_matchups(matchups: pd.DataFrame, history_games: pd.DataFrame) -> pd.DataFrame:
    """Adds rest_days and b2b flags for home/visitor using last game dates from history.

    Expects matchups columns: date, home_team, visitor_team
    """
    out = matchups.copy()
    out["date"] = pd.to_datetime(out["date"]).dt.normalize()
    last_dates = team_last_game_dates(history_games)
    home_rest = []
    away_rest = []
    home_b2b = []
    away_b2b = []
    for _, r in out.iterrows():
        d = r["date"]
        h = r["home_team"]
        v = r["visitor_team"]
        ld_h = last_dates.get(h)
        ld_v = last_dates.get(v)
        rd_h = (d - ld_h).days if ld_h is not None else None
        rd_v = (d - ld_v).days if ld_v is not None else None
        home_rest.append(rd_h)
        away_rest.append(rd_v)
        home_b2b.append(1 if rd_h == 1 else 0 if rd_h is not None else None)
        away_b2b.append(1 if rd_v == 1 else 0 if rd_v is not None else None)
    out["home_rest_days"] = home_rest
    out["visitor_rest_days"] = away_rest
    out["home_b2b"] = home_b2b
    out["visitor_b2b"] = away_b2b
    return out


def _request_schedule_payload() -> dict[str, Any]:
    """Fetch a representative ESPN scoreboard payload for the active league.

    Returns the parsed JSON dict. Raises on failure.
    """
    ymd = datetime.utcnow().strftime("%Y%m%d")
    url = f"https://site.web.api.espn.com/apis/site/v2/{LEAGUE.espn_sport_path}/scoreboard?dates={ymd}"
    r = requests.get(url, headers={"Accept": "application/json", "User-Agent": LEAGUE.user_agent_product}, timeout=30)
    r.raise_for_status()
    payload = r.json()
    if not isinstance(payload, dict):
        raise RuntimeError("Failed to fetch scoreboard payload")
    return payload


def _scoreboard_for_date(date_ymd: str) -> dict[str, Any]:
    url = f"https://site.web.api.espn.com/apis/site/v2/{LEAGUE.espn_sport_path}/scoreboard?dates={date_ymd}"
    r = requests.get(url, headers={"Accept": "application/json", "User-Agent": LEAGUE.user_agent_product}, timeout=30)
    r.raise_for_status()
    payload = r.json()
    return payload if isinstance(payload, dict) else {}


def _season_year_from_token(season: str | None) -> int | None:
    token = str(season or "").strip()
    if not token:
        return None
    head = token.split("-", 1)[0].strip()
    try:
        return int(head)
    except Exception:
        return None


def _season_scan_dates(start_year: int) -> list[str]:
    start = date(start_year, max(1, int(LEAGUE.season_start_month)), 1)
    end = date(start_year, 12, 31)
    dates: list[str] = []
    cur = start
    while cur <= end:
        dates.append(cur.strftime("%Y%m%d"))
        cur += timedelta(days=1)
    return dates


def _phase_label_from_event(event: dict[str, Any], season_meta: dict[str, Any]) -> tuple[str | None, str | None]:
    event_season = event.get("season") or {}
    slug = str(event_season.get("slug") or "").strip().lower()
    season_type = event_season.get("type")

    if slug == "preseason" or season_type == 1:
        return "Preseason", "preseason"
    if slug == "regular-season" or season_type == 2:
        return "Regular Season", "regular-season"
    if slug == "post-season" or season_type == 3:
        return "Playoffs", "post-season"

    fallback = None
    if isinstance(season_meta.get("type"), dict):
        fallback = str((season_meta.get("type") or {}).get("name") or "").strip() or None
    if fallback:
        return fallback, slug or None
    return None, slug or None


def _schedule_rows_from_payloads(game_dates: list[str], season_meta: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    seen_game_ids: set[str] = set()
    for ymd in game_dates:
        day_payload = _scoreboard_for_date(ymd)
        for event in day_payload.get("events") or []:
            comp = (((event or {}).get("competitions") or [None])[0]) or {}
            status = comp.get("status") or {}
            status_type = status.get("type") or {}
            competitors = comp.get("competitors") or []
            home = next((team for team in competitors if str((team or {}).get("homeAway") or "").strip().lower() == "home"), None)
            away = next((team for team in competitors if str((team or {}).get("homeAway") or "").strip().lower() == "away"), None)
            if not home or not away:
                continue

            game_id = str((event or {}).get("id") or "").strip()
            if not game_id or game_id in seen_game_ids:
                continue
            seen_game_ids.add(game_id)

            home_team = (home.get("team") or {})
            away_team = (away.get("team") or {})
            venue = comp.get("venue") or {}
            geo = venue.get("address") or {}
            game_label, season_slug = _phase_label_from_event(event, season_meta)

            broadcasts = []
            for source in (comp.get("broadcasts") or []):
                names = source.get("names") or []
                broadcasts.extend(str(name) for name in names if str(name).strip())

            dt_utc = pd.to_datetime(comp.get("date"), utc=True, errors="coerce") if comp.get("date") else pd.NaT
            date_est = dt_utc.tz_convert(EASTERN_TZ) if pd.notna(dt_utc) else pd.NaT
            rows.append(
                {
                    "game_id": game_id,
                    "season_year": season_label_from_date(comp.get("date")) if comp.get("date") else str((event.get("season") or {}).get("year") or season_meta.get("year") or ""),
                    "game_label": game_label,
                    "game_subtype": (comp.get("type") or {}).get("abbreviation") if isinstance(comp.get("type"), dict) else None,
                    "season_type_slug": season_slug,
                    "game_status": status_type.get("id"),
                    "game_status_text": status_type.get("description") or status.get("displayClock"),
                    "date_utc": dt_utc.date() if pd.notna(dt_utc) else None,
                    "time_utc": dt_utc.strftime("%H:%M") if pd.notna(dt_utc) else None,
                    "datetime_utc": dt_utc,
                    "date_est": date_est.date() if pd.notna(date_est) else None,
                    "time_est": date_est.strftime("%H:%M") if pd.notna(date_est) else None,
                    "datetime_est": date_est,
                    "home_team_id": home_team.get("id"),
                    "home_tricode": to_tricode(str(home_team.get("abbreviation") or home_team.get("displayName") or "")),
                    "home_city": home_team.get("location"),
                    "home_name": home_team.get("name") or home_team.get("displayName"),
                    "away_team_id": away_team.get("id"),
                    "away_tricode": to_tricode(str(away_team.get("abbreviation") or away_team.get("displayName") or "")),
                    "away_city": away_team.get("location"),
                    "away_name": away_team.get("name") or away_team.get("displayName"),
                    "arena_name": venue.get("fullName"),
                    "arena_city": geo.get("city"),
                    "arena_state": geo.get("state"),
                    "broadcasters_national": " | ".join(dict.fromkeys(broadcasts)) if broadcasts else None,
                }
            )

    df = pd.DataFrame(rows)
    for col in ("datetime_utc", "datetime_est"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    for col in ("date_utc", "date_est"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    return df


def fetch_schedule_2025_26(season: str | None = None) -> pd.DataFrame:
    """Fetch and normalize the active WNBA schedule from ESPN scoreboard feeds.

    Returns a DataFrame with a stable schema suitable for frontend consumption:
    - game_id (str)
    - season_year (str like '2025-26')
    - game_label (e.g., 'Regular Season', 'Preseason', 'Playoffs')
    - game_subtype (e.g., 'PlayIn', 'InSeasonTournament' when applicable)
    - game_status (int) and game_status_text (str)
    - date_utc (YYYY-MM-DD) and time_utc (HH:MM) and datetime_utc (ISO8601)
    - date_est, time_est, datetime_est (local Eastern)
    - home_team_id, home_tricode, home_city, home_name
    - away_team_id, away_tricode, away_city, away_name
    - arena_name, arena_city, arena_state
    - broadcasters_national (pipe-delimited)
    """
    payload = _request_schedule_payload()
    leagues = payload.get("leagues") or []
    league = (leagues[0] or {}) if leagues else {}
    season_meta = league.get("season") or {}
    requested_year = _season_year_from_token(season)
    active_year = None
    try:
        active_year = int(season_meta.get("year")) if season_meta.get("year") is not None else None
    except Exception:
        active_year = None

    if requested_year is not None and active_year is not None and requested_year != active_year:
        game_dates = _season_scan_dates(requested_year)
    else:
        calendar = league.get("calendar") or []
        game_dates = []
        for value in calendar:
            try:
                game_dates.append(pd.to_datetime(value, utc=True).strftime("%Y%m%d"))
            except Exception:
                continue

    return _schedule_rows_from_payloads(game_dates, season_meta)
