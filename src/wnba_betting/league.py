from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True)
class LeagueConfig:
    code: str
    name: str
    data_root_env: str
    legacy_data_root_env: str
    live_lens_env: str
    legacy_live_lens_env: str
    odds_api_sport_key: str
    espn_sport_path: str
    user_agent_product: str
    season_start_month: int
    season_games: int
    regulation_period_seconds: int
    overtime_period_seconds: int
    regulation_team_minutes: float
    baseline_pace: float
    baseline_off_rating: float
    baseline_def_rating: float
    min_team_points: float
    spread_winprob_sigma: float
    min_event_possessions: float


LEAGUE = LeagueConfig(
    code="wnba",
    name="WNBA",
    data_root_env="WNBA_BETTING_DATA_ROOT",
    legacy_data_root_env="NBA_BETTING_DATA_ROOT",
    live_lens_env="WNBA_LIVE_LENS_DIR",
    legacy_live_lens_env="NBA_LIVE_LENS_DIR",
    odds_api_sport_key="basketball_wnba",
    espn_sport_path="sports/basketball/wnba",
    user_agent_product="wnba-betting/1.0",
    season_start_month=5,
    season_games=44,
    regulation_period_seconds=10 * 60,
    overtime_period_seconds=5 * 60,
    regulation_team_minutes=200.0,
    baseline_pace=79.5,
    baseline_off_rating=101.5,
    baseline_def_rating=101.5,
    min_team_points=55.0,
    spread_winprob_sigma=9.75,
    min_event_possessions=67.5,
)


def _coerce_date(value: date | datetime | str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value).strip()).date()


def season_start_year_from_date(value: date | datetime | str) -> int:
    dt = _coerce_date(value)
    return int(dt.year) if int(dt.month) >= int(LEAGUE.season_start_month) else int(dt.year) - 1


def season_label_from_date(value: date | datetime | str) -> str:
    return str(season_start_year_from_date(value))


def season_year_from_date(value: date | datetime | str) -> int:
    return season_start_year_from_date(value)


def season_label_from_year(season_year: int) -> str:
    return str(int(season_year))