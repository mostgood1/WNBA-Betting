from __future__ import annotations

import json

import pandas as pd

import app as app_module


def _write_games_fixture(processed, date_str: str) -> None:
    pd.DataFrame(
        [
            {
                "market": "ATS",
                "side": "Sample Home",
                "home": "Sample Home",
                "away": "Sample Away",
                "date": date_str,
                "ev": 0.16,
                "price": -110.0,
                "implied_prob": 0.5238,
                "edge": 3.4,
                "line": -4.5,
                "pred_margin": 7.6,
                "market_home_margin": 4.5,
                "pred_total": 226.0,
                "tier": "High",
            },
            {
                "market": "TOTAL",
                "side": "Under",
                "home": "Totals Home",
                "away": "Totals Away",
                "date": date_str,
                "ev": 0.12,
                "price": -105.0,
                "implied_prob": 0.5122,
                "edge": 4.8,
                "line": 224.5,
                "pred_margin": 1.2,
                "market_home_margin": 1.5,
                "pred_total": 218.8,
                "tier": "High",
            },
        ]
    ).to_csv(processed / f"recommendations_{date_str}.csv", index=False)

    pd.DataFrame(
        [
            {"gameId": "G1", "teamTricode": "Sample Home", "points": 118, "reboundsTotal": 45, "assists": 30, "threePointersMade": 16},
            {"gameId": "G1", "teamTricode": "Sample Away", "points": 102, "reboundsTotal": 41, "assists": 22, "threePointersMade": 11},
            {"gameId": "G2", "teamTricode": "Totals Home", "points": 105, "reboundsTotal": 43, "assists": 24, "threePointersMade": 13},
            {"gameId": "G2", "teamTricode": "Totals Away", "points": 99, "reboundsTotal": 39, "assists": 20, "threePointersMade": 9},
        ]
    ).to_csv(processed / f"boxscores_{date_str}.csv", index=False)

    pd.DataFrame(
        [
            {"team": "Sample Home", "pace": 101.5, "off_rtg": 120.2, "def_rtg": 108.6},
            {"team": "Sample Away", "pace": 98.4, "off_rtg": 109.1, "def_rtg": 116.4},
            {"team": "Totals Home", "pace": 95.8, "off_rtg": 109.8, "def_rtg": 107.9},
            {"team": "Totals Away", "pace": 94.9, "off_rtg": 105.2, "def_rtg": 110.3},
        ]
    ).to_csv(processed / f"team_advanced_stats_2026_asof_{date_str}.csv", index=False)

    pd.DataFrame(
        [
            {
                "date": date_str,
                "home_team": "Sample Home",
                "visitor_team": "Sample Away",
                "home_win_prob": 0.73,
                "spread_margin": 7.6,
                "totals": 226.0,
                "commence_time": f"{date_str}T23:00:00Z",
                "home_ml": -175.0,
                "away_ml": 145.0,
                "home_spread": -4.5,
                "away_spread": 4.5,
                "total": 224.5,
            },
            {
                "date": date_str,
                "home_team": "Totals Home",
                "visitor_team": "Totals Away",
                "home_win_prob": 0.58,
                "spread_margin": 1.2,
                "totals": 218.8,
                "commence_time": f"{date_str}T23:30:00Z",
                "home_ml": -130.0,
                "away_ml": 110.0,
                "home_spread": -1.5,
                "away_spread": 1.5,
                "total": 224.5,
            },
        ]
    ).to_csv(processed / f"predictions_{date_str}.csv", index=False)

    (processed / f"injuries_counts_{date_str}.json").write_text(
        json.dumps(
            {
                "date": date_str,
                "team_counts": {"SAMPLE AWAY": 2, "TOTALS HOME": 1, "TOTALS AWAY": 2},
                "players": [
                    {"player": "Missing Wing", "team": "SAMPLE AWAY", "status": "OUT"},
                    {"player": "Backup Guard", "team": "SAMPLE AWAY", "status": "OUT"},
                    {"player": "Reserve Big", "team": "TOTALS AWAY", "status": "OUT"},
                ],
            }
        ),
        encoding="utf-8",
    )


def _write_props_fixture(processed, date_str: str) -> None:
    pd.DataFrame(
        [
            {
                "player": "Star One",
                "team": "BOS",
                "plays": str([
                    {
                        "market": "pts",
                        "side": "OVER",
                        "line": 24.5,
                        "price": -110.0,
                        "edge": 0.08,
                        "ev": 0.12,
                        "ev_pct": 12.0,
                        "book": "draftkings",
                    }
                ]),
                "ladders": "[]",
                "_plays_list": str([
                    {
                        "market": "pts",
                        "side": "OVER",
                        "line": 24.5,
                        "price": -110.0,
                        "edge": 0.08,
                        "ev": 0.12,
                        "ev_pct": 12.0,
                        "book": "draftkings",
                    }
                ]),
                "top_play": str(
                    {
                        "market": "pts",
                        "side": "OVER",
                        "line": 24.5,
                        "price": -110.0,
                        "ev": 0.12,
                        "ev_pct": 12.0,
                        "book": "draftkings",
                    }
                ),
                "top_play_explain": "model 27.4 vs line 24.5 (+2.9)",
                "top_play_baseline": 27.4,
                "top_play_reasons": str(["EV 12.0%", "Consensus: 3 books aligned", "Best line available"]),
                "top_play_consensus": 0.75,
                "top_play_line_adv": 1.0,
            },
            {
                "player": "Glass Cleaner",
                "team": "LAL",
                "plays": str([
                    {
                        "market": "reb",
                        "side": "UNDER",
                        "line": 8.5,
                        "price": 102.0,
                        "edge": 0.06,
                        "ev": 0.11,
                        "ev_pct": 11.0,
                        "book": "betmgm",
                    }
                ]),
                "ladders": "[]",
                "_plays_list": str([
                    {
                        "market": "reb",
                        "side": "UNDER",
                        "line": 8.5,
                        "price": 102.0,
                        "edge": 0.06,
                        "ev": 0.11,
                        "ev_pct": 11.0,
                        "book": "betmgm",
                    }
                ]),
                "top_play": str(
                    {
                        "market": "reb",
                        "side": "UNDER",
                        "line": 8.5,
                        "price": 102.0,
                        "ev": 0.11,
                        "ev_pct": 11.0,
                        "book": "betmgm",
                    }
                ),
                "top_play_explain": "model 7.1 vs line 8.5 (-1.4)",
                "top_play_baseline": 7.1,
                "top_play_reasons": str(["EV 11.0%", "Best line available"]),
                "top_play_consensus": 0.25,
                "top_play_line_adv": 1.0,
            },
        ]
    ).to_csv(processed / f"props_recommendations_{date_str}.csv", index=False)

    pd.DataFrame(
        [
            {
                "player_name": "Star One",
                "team": "BOS",
                "b2b": 0.0,
                "lag1_pts": 31.0,
                "roll3_pts": 28.5,
                "roll5_pts": 27.9,
                "pred_min": 35.2,
                "roll5_min": 34.6,
                "pred_pts": 27.4,
                "sd_pts": 5.4,
                "opponent": "MIA",
            },
            {
                "player_name": "Glass Cleaner",
                "team": "LAL",
                "b2b": 1.0,
                "lag1_reb": 6.0,
                "roll3_reb": 6.7,
                "roll5_reb": 6.8,
                "pred_min": 30.4,
                "roll5_min": 29.7,
                "pred_reb": 7.1,
                "sd_reb": 2.6,
                "opponent": "PHX",
            },
        ]
    ).to_csv(processed / f"props_predictions_{date_str}.csv", index=False)

    pd.DataFrame(
        [
            {
                "date": date_str,
                "home_team": "BOS",
                "visitor_team": "MIA",
                "home_win_prob": 0.63,
                "spread_margin": 5.8,
                "totals": 227.0,
                "commence_time": f"{date_str}T23:00:00Z",
                "home_ml": -160.0,
                "away_ml": 135.0,
                "home_spread": -5.5,
                "away_spread": 5.5,
                "total": 224.5,
            },
            {
                "date": date_str,
                "home_team": "LAL",
                "visitor_team": "PHX",
                "home_win_prob": 0.54,
                "spread_margin": 2.1,
                "totals": 219.0,
                "commence_time": f"{date_str}T23:45:00Z",
                "home_ml": -125.0,
                "away_ml": 105.0,
                "home_spread": -2.0,
                "away_spread": 2.0,
                "total": 222.5,
            },
        ]
    ).to_csv(processed / f"predictions_{date_str}.csv", index=False)

    (processed / f"injuries_counts_{date_str}.json").write_text(
        json.dumps(
            {
                "date": date_str,
                "team_counts": {"BOS": 2, "LAL": 0},
                "players": [
                    {"player": "Second Unit Wing", "team": "BOS", "status": "OUT"},
                    {"player": "Reserve Guard", "team": "BOS", "status": "OUT"},
                ],
            }
        ),
        encoding="utf-8",
    )


def _write_props_recommendations_route_fixture(processed, date_str: str) -> None:
    _write_props_fixture(processed, date_str)

    pd.DataFrame(
        [
            {
                "player_name": "Star One",
                "team": "BOS",
                "stat": "pts",
                "side": "OVER",
                "line": 24.5,
                "price": -110.0,
                "edge": 0.08,
                "ev": 0.12,
                "bookmaker": "draftkings",
                "model_prob": 0.61,
            },
            {
                "player_name": "Glass Cleaner",
                "team": "LAL",
                "stat": "reb",
                "side": "UNDER",
                "line": 8.5,
                "price": 102.0,
                "edge": 0.06,
                "ev": 0.11,
                "bookmaker": "betmgm",
                "model_prob": 0.58,
            }
        ]
    ).to_csv(processed / f"props_edges_{date_str}.csv", index=False)

    pd.DataFrame(
        [
            {"team": "BOS", "pace": 101.0, "off_rtg": 118.0, "def_rtg": 109.0},
            {"team": "MIA", "pace": 100.0, "off_rtg": 111.0, "def_rtg": 113.0},
            {"team": "LAL", "pace": 98.0, "off_rtg": 108.0, "def_rtg": 112.0},
            {"team": "PHX", "pace": 97.0, "off_rtg": 113.0, "def_rtg": 112.0},
        ]
    ).to_csv(processed / f"team_advanced_stats_2026_asof_{date_str}.csv", index=False)


def test_api_best_bets_parlays_orders_basketball_reasons_first(tmp_path, monkeypatch):
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)
    date_str = "2026-03-19"
    _write_games_fixture(processed, date_str)

    monkeypatch.setattr(app_module, "DATA_PROCESSED_DIR", processed)
    monkeypatch.setattr(app_module, "_maybe_fetch_remote_processed", lambda _name: None)

    app_module.app.testing = True
    with app_module.app.test_client() as client:
        resp = client.get(f"/api/best-bets-parlays?date={date_str}&best_bets=2&candidate_pool=2&parlay_size=2&max_parlays=1")

    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["status"] == "ok"
    assert payload["best_bet"]["basketball_summary"]
    assert payload["best_bet"]["reasons"][0] == payload["best_bet"]["basketball_reasons"][0]
    first_reason = payload["best_bet"]["basketball_reasons"][0].lower()
    assert any(token in first_reason for token in ("offense", "offensive", "defense", "points per game"))
    assert "rotation outs" not in first_reason
    assert payload["best_bets"][0]["recommendation_priority_score"] >= payload["best_bets"][1]["recommendation_priority_score"]
    assert len(payload["parlays"]) == 1
    legs = payload["parlays"][0]["legs"]
    assert len({leg["game_key"] for leg in legs}) == len(legs)


def test_api_props_best_bets_parlays_builds_basketball_first_payload(tmp_path, monkeypatch):
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)
    date_str = "2026-03-19"
    _write_props_fixture(processed, date_str)

    monkeypatch.setattr(app_module, "DATA_PROCESSED_DIR", processed)
    monkeypatch.setattr(app_module, "_maybe_fetch_remote_processed", lambda _name: None)

    app_module.app.testing = True
    with app_module.app.test_client() as client:
        resp = client.get(f"/api/props/best-bets-parlays?date={date_str}&best_bets=2&candidate_pool=2&parlay_size=2&max_parlays=1")

    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["status"] == "ok"
    assert payload["best_bet"]["basketball_summary"]
    assert payload["best_bet"]["model_baseline"] is not None
    assert payload["best_bet"]["reasons"][0] == payload["best_bet"]["basketball_reasons"][0]
    assert len(payload["best_bets"]) == 2
    assert len(payload["parlays"]) == 1
    legs = payload["parlays"][0]["legs"]
    assert len({leg["player_key"] for leg in legs}) == len(legs)


def test_api_props_recommendations_builds_basketball_first_reason_buckets(tmp_path, monkeypatch):
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)
    date_str = "2026-03-19"
    _write_props_recommendations_route_fixture(processed, date_str)

    monkeypatch.setattr(app_module, "DATA_PROCESSED_DIR", processed)
    monkeypatch.setattr(app_module, "_maybe_fetch_remote_processed", lambda _name: None)
    monkeypatch.setattr(app_module, "_resolve_player_id", lambda player, _team=None: {"STAR ONE": 1628369, "GLASS CLEANER": 203999}.get(str(player or "").upper()))
    monkeypatch.setattr(app_module, "_compute_team_allowed_stats", lambda _date: ({}, {"MIA": {"pts": 25}}))
    monkeypatch.setattr(app_module, "_compute_team_offense_stats", lambda _date: ({}, {"BOS": 22}))
    monkeypatch.setattr(app_module, "_team_injury_counts", lambda _date: {"BOS": 2})
    monkeypatch.setattr(
        app_module,
        "_team_injury_identity",
        lambda _date: ({"BOS": ["Second Unit Wing", "Reserve Guard"]}, {"BOS": 1.5}),
    )
    monkeypatch.setattr(
        app_module,
        "_roster_players_for_date",
        lambda _date: {
            "BOS": {
                app_module._norm_player_name("Second Unit Wing"),
                app_module._norm_player_name("Reserve Guard"),
            }
        },
    )

    app_module.app.testing = True
    with app_module.app.test_client() as client:
        resp = client.get(f"/api/props-recommendations?date={date_str}&market=pts&minEV=0")

    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["rows"] >= 1
    assert payload["data"]

    card = payload["data"][0]
    assert card["player_id"] == 1628369
    assert card["photo"] == "https://cdn.nba.com/headshots/nba/latest/1040x760/1628369.png"
    assert card["basketball_summary"]
    assert card["basketball_reasons"]
    assert card["model_reasons"]
    assert card["market_reasons"]
    assert card["top_play_reasons"][0] == card["basketball_reasons"][0]
    assert any("Pace spot:" in reason or "Off/def matchup:" in reason for reason in card["basketball_reasons"])
    assert any("Model baseline:" in reason for reason in card["model_reasons"])
    assert any(reason.startswith("EV ") for reason in card["market_reasons"])


def test_api_props_recommendations_builds_basketball_first_reason_buckets_for_unders(tmp_path, monkeypatch):
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)
    date_str = "2026-03-19"
    _write_props_recommendations_route_fixture(processed, date_str)

    monkeypatch.setattr(app_module, "DATA_PROCESSED_DIR", processed)
    monkeypatch.setattr(app_module, "_maybe_fetch_remote_processed", lambda _name: None)
    monkeypatch.setattr(app_module, "_compute_team_allowed_stats", lambda _date: ({}, {"PHX": {"reb": 8}}))
    monkeypatch.setattr(app_module, "_compute_team_offense_stats", lambda _date: ({}, {"LAL": 8}))
    monkeypatch.setattr(app_module, "_team_injury_counts", lambda _date: {"LAL": 0})
    monkeypatch.setattr(app_module, "_team_injury_identity", lambda _date: ({}, {}))
    monkeypatch.setattr(app_module, "_roster_players_for_date", lambda _date: {"LAL": set()})

    app_module.app.testing = True
    with app_module.app.test_client() as client:
        resp = client.get(f"/api/props-recommendations?date={date_str}&market=reb&minEV=0")

    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["rows"] >= 1
    assert payload["data"]

    card = next(item for item in payload["data"] if str(item["player"]).upper() == "GLASS CLEANER")
    assert card["top_play"]["side"] == "UNDER"
    assert card["basketball_summary"]
    assert card["basketball_reasons"]
    assert card["model_reasons"]
    assert card["market_reasons"]
    assert card["top_play_reasons"][0] == card["basketball_reasons"][0]
    assert any(
        "Pace spot:" in reason or "Off/def matchup:" in reason or "Opponent matchup:" in reason
        for reason in card["basketball_reasons"]
    )
    assert any("Model baseline:" in reason for reason in card["model_reasons"])
    assert any(reason.startswith("EV ") for reason in card["market_reasons"])


def test_recommendations_all_games_builds_basketball_first_reason_buckets(tmp_path, monkeypatch):
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)
    date_str = "2026-03-19"
    _write_games_fixture(processed, date_str)

    monkeypatch.setattr(app_module, "DATA_PROCESSED_DIR", processed)
    monkeypatch.setattr(app_module, "_maybe_fetch_remote_processed", lambda _name: None)
    monkeypatch.setattr(
        app_module,
        "_load_best_bets_game_context",
        lambda _date: {
            "slate_total_median": 221.0,
            "by_pair": {
                ("SAMPLE HOME", "SAMPLE AWAY"): {
                    "pred_margin": 7.6,
                    "pred_total": 226.0,
                    "home_win_prob": 0.73,
                    "commence_time": f"{date_str}T23:00:00Z",
                },
                ("TOTALS HOME", "TOTALS AWAY"): {
                    "pred_margin": 1.2,
                    "pred_total": 218.8,
                    "home_win_prob": 0.58,
                    "commence_time": f"{date_str}T23:30:00Z",
                },
            },
        },
    )
    monkeypatch.setattr(
        app_module,
        "_best_bets_load_injury_snapshot",
        lambda _date: {
            "counts": {"SAMPLE HOME": 0, "SAMPLE AWAY": 2, "TOTALS HOME": 1, "TOTALS AWAY": 2},
            "key_outs": {
                "SAMPLE AWAY": ["Missing Wing", "Backup Guard"],
                "TOTALS AWAY": ["Reserve Big"],
            },
        },
    )
    monkeypatch.setattr(app_module, "_compute_team_allowed_stats", lambda _date: ({}, {}))
    monkeypatch.setattr(app_module, "_compute_team_offense_stats", lambda _date, days_back=None: ({}, {}))
    monkeypatch.setattr(app_module, "_team_injury_counts", lambda _date: {})
    monkeypatch.setattr(app_module, "_team_injury_identity", lambda _date: ({}, {}))
    monkeypatch.setattr(app_module, "_get_slate_team_tricodes", lambda _date: set())
    monkeypatch.setattr(app_module, "_roster_players_for_date", lambda _date: {})

    app_module.app.testing = True
    with app_module.app.test_client() as client:
        resp = client.get(f"/recommendations?format=json&view=all&date={date_str}&categories=games")

    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["games"]

    ats = next(item for item in payload["games"] if item["market"] == "ATS")
    assert ats["basketball_summary"]
    assert ats["basketball_reasons"]
    assert ats["model_reasons"]
    assert ats["market_reasons"]
    assert ats["why_reasons"][0] == ats["basketball_reasons"][0]
    assert ats["why_explain"]
    assert any("depleted" in reason.lower() or "health profile" in reason.lower() for reason in ats["basketball_reasons"])
    assert any("model" in reason.lower() or "sim" in reason.lower() for reason in ats["model_reasons"])
    assert any("value" in reason.lower() or "price" in reason.lower() for reason in ats["market_reasons"])


def test_recommendations_all_props_preserves_bucketed_why_reasons(tmp_path, monkeypatch):
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)
    date_str = "2026-03-19"
    _write_props_recommendations_route_fixture(processed, date_str)

    monkeypatch.setattr(app_module, "DATA_PROCESSED_DIR", processed)
    monkeypatch.setattr(app_module, "_maybe_fetch_remote_processed", lambda _name: None)
    monkeypatch.setattr(
        app_module,
        "_load_best_bets_game_context",
        lambda _date: {
            "slate_total_median": 223.0,
            "by_team": {
                "BOS": {
                    "home_team": "BOS",
                    "away_team": "MIA",
                    "home_tri": "BOS",
                    "away_tri": "MIA",
                    "pred_total": 227.0,
                    "commence_time": f"{date_str}T23:00:00Z",
                },
                "LAL": {
                    "home_team": "LAL",
                    "away_team": "PHX",
                    "home_tri": "LAL",
                    "away_tri": "PHX",
                    "pred_total": 219.0,
                    "commence_time": f"{date_str}T23:45:00Z",
                },
            },
        },
    )
    monkeypatch.setattr(
        app_module,
        "_load_best_bets_props_prediction_lookup",
        lambda _date: {
            (app_module._norm_player_name("Star One"), "BOS"): {
                "player_name": "Star One",
                "team": "BOS",
                "b2b": 0.0,
                "lag1_pts": 31.0,
                "roll3_pts": 28.5,
                "roll5_pts": 27.9,
                "pred_min": 35.2,
                "roll5_min": 34.6,
                "pred_pts": 27.4,
                "sd_pts": 5.4,
                "opponent": "MIA",
            },
            (app_module._norm_player_name("Glass Cleaner"), "LAL"): {
                "player_name": "Glass Cleaner",
                "team": "LAL",
                "b2b": 1.0,
                "lag1_reb": 6.0,
                "roll3_reb": 6.7,
                "roll5_reb": 6.8,
                "pred_min": 30.4,
                "roll5_min": 29.7,
                "pred_reb": 7.1,
                "sd_reb": 2.6,
                "opponent": "PHX",
            },
        },
    )
    monkeypatch.setattr(
        app_module,
        "_best_bets_load_injury_snapshot",
        lambda _date: {
            "counts": {"BOS": 3, "LAL": 0},
            "key_outs": {"BOS": ["Second Unit Wing", "Reserve Guard", "Bench Big"]},
        },
    )
    monkeypatch.setattr(app_module, "_team_injury_counts", lambda _date: {"BOS": 3, "LAL": 0})
    monkeypatch.setattr(
        app_module,
        "_team_injury_identity",
        lambda _date: ({"BOS": ["Second Unit Wing", "Reserve Guard", "Bench Big"]}, {"BOS": 60.0, "LAL": 0.0}),
    )

    app_module.app.testing = True
    with app_module.app.test_client() as client:
        resp = client.get(f"/recommendations?format=json&view=all&date={date_str}&categories=props")

    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["props"]

    star = next(item for item in payload["props"] if str(item.get("player") or "").upper() == "STAR ONE")
    assert star["basketball_summary"]
    assert star["basketball_reasons"]
    assert star["model_reasons"]
    assert star["market_reasons"]
    assert star["top_play_reasons"][0] == star["basketball_reasons"][0]
    assert star["why_reasons"][0] == star["basketball_reasons"][0]
    assert star["why_explain"]
    assert star["score_reasons"]
    assert star["why_reasons"] != star["score_reasons"]
    assert any("bench usage" in reason.lower() or "usage uptick" in reason.lower() for reason in star["basketball_reasons"])
    assert any("model baseline" in reason.lower() or "win probability" in reason.lower() for reason in star["model_reasons"])
    assert any("value" in reason.lower() or "price" in reason.lower() for reason in star["market_reasons"])


def test_best_bets_page_routes_render():
    app_module.app.testing = True
    with app_module.app.test_client() as client:
        games_page = client.get("/best-bets-parlays?date=2026-03-19")
        props_page = client.get("/props/best-bets-parlays?date=2026-03-19")
        props_recs = client.get("/props/recommendations?date=2026-03-19")

    assert games_page.status_code == 200
    assert props_page.status_code == 200
    assert props_recs.status_code == 200
    assert "Best Bets & Parlays" in games_page.get_data(as_text=True)
    assert "Props - Best Bets & Parlays" in props_page.get_data(as_text=True)
    assert "Props Recs" in props_recs.get_data(as_text=True)