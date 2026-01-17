from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd


@dataclass
class PlayerMinute:
    player: str
    team: str
    minutes: float
    usage: Optional[float] = None  # percent of team possessions


class MinutesForecaster:
    """Heuristic minutes/usage forecaster.

    Inputs: recent box scores (if available), injury flags, role (starter/bench).
    Outputs: per-player minutes and optional usage rates for props simulation.
    """

    def __init__(self, date: str):
        self.date = date

    def forecast(self, roster: List[Dict], injuries_map: Dict[str, int] | None = None) -> List[PlayerMinute]:
        out: List[PlayerMinute] = []
        injuries_map = injuries_map or {}
        for p in roster:
            name = str(p.get("player") or p.get("player_name") or "").strip()
            team = str(p.get("team") or p.get("team_abbr") or "").strip().upper()
            role = str(p.get("role") or "").lower()  # starter/bench if provided
            # Baseline minutes
            base = 30.0 if role == "starter" else 20.0
            # Injury impact: if injuries_map says player flagged (1), reduce minutes
            inj_flag = int(injuries_map.get(name.lower(), 0))
            if inj_flag:
                base = max(10.0, base - 8.0)
            # Gentle cap for back-to-backs or return-from-injury could be added here
            out.append(PlayerMinute(player=name, team=team, minutes=base, usage=None))
        return out

    @staticmethod
    def from_processed(date: str, processed_dir: str) -> MinutesForecaster:
        # Future: load recent box scores or props predictions to infer roles
        return MinutesForecaster(date)
