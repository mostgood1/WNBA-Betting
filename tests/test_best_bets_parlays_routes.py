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


def test_load_best_bets_game_context_blends_props_and_matchup(tmp_path, monkeypatch):
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)
    date_str = "2026-03-30"

    pd.DataFrame(
        [
            {
                "date": date_str,
                "home_team": "Boston Celtics",
                "visitor_team": "Miami Heat",
                "home_win_prob": 0.61,
                "spread_margin": 3.0,
                "totals": 219.0,
                "total": 222.5,
                "commence_time": f"{date_str}T23:00:00Z",
            }
        ]
    ).to_csv(processed / f"predictions_{date_str}.csv", index=False)

    pd.DataFrame(
        [
            {"team": "BOS", "player_name": "Jayson Tatum", "pts_mean": 28.0, "min_mean": 36.0},
            {"team": "BOS", "player_name": "Jaylen Brown", "pts_mean": 24.0, "min_mean": 35.0},
            {"team": "BOS", "player_name": "Kristaps Porzingis", "pts_mean": 20.0, "min_mean": 31.0},
            {"team": "BOS", "player_name": "Derrick White", "pts_mean": 16.0, "min_mean": 33.0},
            {"team": "BOS", "player_name": "Jrue Holiday", "pts_mean": 14.0, "min_mean": 32.0},
            {"team": "BOS", "player_name": "Bench One", "pts_mean": 9.0, "min_mean": 24.0},
            {"team": "BOS", "player_name": "Bench Two", "pts_mean": 7.0, "min_mean": 18.0},
            {"team": "BOS", "player_name": "Bench Three", "pts_mean": 6.0, "min_mean": 16.0},
            {"team": "MIA", "player_name": "Jimmy Butler", "pts_mean": 24.0, "min_mean": 35.0},
            {"team": "MIA", "player_name": "Bam Adebayo", "pts_mean": 21.0, "min_mean": 35.0},
            {"team": "MIA", "player_name": "Tyler Herro", "pts_mean": 22.0, "min_mean": 34.0},
            {"team": "MIA", "player_name": "Terry Rozier", "pts_mean": 15.0, "min_mean": 31.0},
            {"team": "MIA", "player_name": "Duncan Robinson", "pts_mean": 12.0, "min_mean": 29.0},
            {"team": "MIA", "player_name": "Bench Four", "pts_mean": 8.0, "min_mean": 22.0},
            {"team": "MIA", "player_name": "Bench Five", "pts_mean": 7.0, "min_mean": 17.0},
            {"team": "MIA", "player_name": "Bench Six", "pts_mean": 6.0, "min_mean": 16.0},
        ]
    ).to_csv(processed / f"props_predictions_{date_str}.csv", index=False)

    monkeypatch.setattr(app_module, "DATA_PROCESSED_DIR", processed)
    monkeypatch.setattr(app_module, "_maybe_fetch_remote_processed", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(app_module, "_compute_team_offense_stats", lambda _date: ({"BOS": 119.5, "MIA": 114.0}, {}))
    monkeypatch.setattr(
        app_module,
        "_compute_team_allowed_stats",
        lambda _date: ({"BOS": {"pts": 109.0}, "MIA": {"pts": 116.0}}, {}),
    )
    monkeypatch.setattr(
        app_module,
        "_load_team_advanced_stats_frame",
        lambda _date: pd.DataFrame(
            [
                {"team": "BOS", "pace": 100.6, "off_rtg": 121.8, "def_rtg": 109.2},
                {"team": "MIA", "pace": 98.8, "off_rtg": 116.2, "def_rtg": 114.8},
                {"team": "ATL", "pace": 101.0, "off_rtg": 116.0, "def_rtg": 118.0},
                {"team": "ORL", "pace": 97.1, "off_rtg": 110.4, "def_rtg": 108.7},
            ]
        ),
    )

    app_module._load_best_bets_props_team_scoring_context.cache_clear()
    ctx = app_module._load_best_bets_game_context(date_str)
    pair_ctx = (ctx.get("by_pair") or {}).get(("BOS", "MIA"))

    assert isinstance(pair_ctx, dict)
    assert pair_ctx["pred_margin_raw"] == 3.0
    assert pair_ctx["pred_margin_adjusted"] is not None
    assert pair_ctx["pred_margin_adjusted"] > pair_ctx["pred_margin_raw"]
    assert pair_ctx["pred_total_raw"] == 219.0
    assert pair_ctx["pred_total_adjusted"] is not None
    assert pair_ctx["pred_total_adjusted"] > pair_ctx["pred_total_raw"]
    assert pair_ctx["home_win_prob_adjusted"] is not None
    assert pair_ctx["home_win_prob_adjusted"] > 0.61
    assert pair_ctx["home_prop_minutes_coverage"] > 0.8
    assert pair_ctx["away_prop_minutes_coverage"] > 0.8
    assert pair_ctx["home_prop_blend_weight"] > 0.0
    assert pair_ctx["away_prop_blend_weight"] > 0.0


def test_decorate_game_best_bet_candidate_prefers_adjusted_total_context():
    payload = app_module._decorate_game_best_bet_candidate(
        {
            "home": "Boston Celtics",
            "away": "Miami Heat",
            "market": "TOTAL",
            "side": "Under",
            "line": 224.5,
            "pred_total": 218.0,
            "date": "2026-03-30",
        },
        date_str="2026-03-30",
        ctx={
            "pred_total_raw": 218.0,
            "pred_total_adjusted": 229.5,
            "pred_total": 229.5,
            "commence_time": "2026-03-30T23:00:00Z",
        },
        injury_snapshot={"counts": {}, "key_outs": {}},
        slate_total_median=222.0,
    )

    assert payload is not None
    assert payload["pred_total"] == 229.5
    assert any("229.5" in reason for reason in (payload.get("sim_reasons") or []))
    assert any(
        "Prop scoring and opponent matchup context push the projection" in reason
        for reason in (payload.get("sim_reasons") or [])
    )


def test_decorate_game_best_bet_candidate_prefers_adjusted_side_context():
    ats_payload = app_module._decorate_game_best_bet_candidate(
        {
            "home": "Boston Celtics",
            "away": "Miami Heat",
            "market": "ATS",
            "side": "Boston Celtics",
            "line": -3.5,
            "market_home_margin": 3.5,
            "pred_margin": 2.0,
            "price": -110.0,
            "ev": 0.08,
            "date": "2026-03-30",
        },
        date_str="2026-03-30",
        ctx={
            "pred_margin_raw": 2.0,
            "pred_margin_adjusted": 6.4,
            "pred_margin": 6.4,
            "commence_time": "2026-03-30T23:00:00Z",
        },
        injury_snapshot={"counts": {}, "key_outs": {}},
        slate_total_median=222.0,
    )

    assert ats_payload is not None
    assert ats_payload["pred_margin"] == 6.4
    assert any("+2.9" in reason for reason in (ats_payload.get("sim_reasons") or []))
    assert any(
        "Prop scoring and opponent matchup context shift the spread projection" in reason
        for reason in (ats_payload.get("sim_reasons") or [])
    )

    ml_payload = app_module._decorate_game_best_bet_candidate(
        {
            "home": "Boston Celtics",
            "away": "Miami Heat",
            "market": "ML",
            "side": "Boston Celtics",
            "price": -120.0,
            "date": "2026-03-30",
        },
        date_str="2026-03-30",
        ctx={
            "home_win_prob_raw": 0.58,
            "home_win_prob_adjusted": 0.67,
            "home_win_prob": 0.67,
            "commence_time": "2026-03-30T23:00:00Z",
        },
        injury_snapshot={"counts": {}, "key_outs": {}},
        slate_total_median=222.0,
    )

    assert ml_payload is not None
    assert ml_payload["p_win"] == 0.67
    assert any("67.0%" in reason for reason in (ml_payload.get("sim_reasons") or []))
    assert any(
        "Prop scoring and opponent matchup context move win probability" in reason
        for reason in (ml_payload.get("sim_reasons") or [])
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
                    "pred_margin_raw": 7.6,
                    "pred_margin_adjusted": 9.1,
                    "pred_margin": 9.1,
                    "pred_total": 226.0,
                    "home_win_prob_raw": 0.73,
                    "home_win_prob_adjusted": 0.78,
                    "home_win_prob": 0.73,
                    "commence_time": f"{date_str}T23:00:00Z",
                },
                ("TOTALS HOME", "TOTALS AWAY"): {
                    "pred_margin": 1.2,
                    "pred_total_raw": 218.8,
                    "pred_total_adjusted": 214.4,
                    "pred_total": 214.4,
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
    total = next(item for item in payload["games"] if item["market"] == "TOTAL")
    assert ats["basketball_summary"]
    assert ats["basketball_reasons"]
    assert ats["model_reasons"]
    assert ats["market_reasons"]
    assert ats["why_reasons"][0] == ats["basketball_reasons"][0]
    assert ats["why_explain"]
    assert any("depleted" in reason.lower() or "health profile" in reason.lower() for reason in ats["basketball_reasons"])
    assert any("model" in reason.lower() or "sim" in reason.lower() for reason in ats["model_reasons"])
    assert any("value" in reason.lower() or "price" in reason.lower() for reason in ats["market_reasons"])
    assert ats["pred_margin"] == 9.1
    assert any("+4.6" in reason for reason in (ats.get("sim_reasons") or []))
    assert any(
        "Prop scoring and opponent matchup context shift the spread projection" in reason
        for reason in (ats.get("sim_reasons") or [])
    )
    assert total["pred_total"] == 214.4
    assert any("214.4" in reason for reason in (total.get("sim_reasons") or []))
    assert any(
        "Prop scoring and opponent matchup context push the projection" in reason
        for reason in (total.get("sim_reasons") or [])
    )


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
        betting_card = client.get("/betting-card?date=2026-03-19")
        pregame_page = client.get("/pregame?date=2026-03-19")
        live_page = client.get("/live?date=2026-03-19")
        betting_recap = client.get("/betting-recap?since=2026-03-12&until=2026-03-19")
        games_page = client.get("/best-bets-parlays?date=2026-03-19")
        props_page = client.get("/props/best-bets-parlays?date=2026-03-19")
        props_recs = client.get("/props/recommendations?date=2026-03-19")
        recommendations_page = client.get("/recommendations?date=2026-03-19")
        reconciliation_page = client.get("/reconciliation?date=2026-03-19")
        season_betting_card_page = client.get("/season/2026/betting-card?date=2026-03-19&profile=retuned")

    assert betting_card.status_code == 200
    assert "NBA Betting - Daily Betting Card" in betting_card.get_data(as_text=True)

    assert pregame_page.status_code == 302
    assert "/betting-card?date=2026-03-19" in pregame_page.headers["Location"]

    assert live_page.status_code == 302
    assert "/betting-card?date=2026-03-19" in live_page.headers["Location"]

    assert betting_recap.status_code == 302
    assert "/betting-card" in betting_recap.headers["Location"]

    assert games_page.status_code == 302
    assert "/betting-card?date=2026-03-19&section=games" in games_page.headers["Location"]

    assert props_page.status_code == 302
    assert "/betting-card?date=2026-03-19&section=props" in props_page.headers["Location"]

    assert props_recs.status_code == 302
    assert "/betting-card?date=2026-03-19&section=props" in props_recs.headers["Location"]

    assert recommendations_page.status_code == 302
    assert "/betting-card?date=2026-03-19" in recommendations_page.headers["Location"]

    assert reconciliation_page.status_code == 302
    assert "/betting-card?date=2026-03-19" in reconciliation_page.headers["Location"]

    assert season_betting_card_page.status_code == 200
    assert "NBA Betting Card" in season_betting_card_page.get_data(as_text=True)


def test_api_betting_card_flattens_game_and_prop_plays(monkeypatch):
    date_str = "2026-03-19"

    def _fake_cards():
        return app_module.jsonify(
            {
                "date": date_str,
                "requested_date": date_str,
                "lookahead_applied": False,
                "games": [
                    {
                        "home_tri": "BOS",
                        "away_tri": "MIA",
                        "odds": {"commence_time": f"{date_str}T23:00:00Z"},
                        "game_market_recommendations": [
                            {"market": "ML", "recommendation_priority_score": 1.4, "edge": 0.08},
                            {"market": "ATS", "recommendation_priority_score": 0.9, "edge": 0.05},
                        ],
                        "prop_recommendations": {
                            "home": [
                                {"player": "Star One", "recommendation_priority_score": 1.2, "ev": 0.11},
                            ],
                            "away": [
                                {"player": "Star Two", "recommendation_priority_score": 0.7, "ev": 0.06},
                            ],
                        },
                    }
                ],
            }
        )

    monkeypatch.setattr(app_module, "api_cards", _fake_cards)

    app_module.app.testing = True
    with app_module.app.test_client() as client:
        resp = client.get(f"/api/betting-card?date={date_str}")
        props_only = client.get(f"/api/betting-card?date={date_str}&section=props")

    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["generated_from"] == "/api/cards"
    assert payload["counts"]["games"] == 1
    assert payload["counts"]["game_plays"] == 2
    assert payload["counts"]["prop_plays"] == 2
    assert payload["game_plays"][0]["market"] == "ML"
    assert payload["game_plays"][0]["matchup"] == "MIA @ BOS"
    assert payload["prop_plays"][0]["player"] == "Star One"
    assert payload["prop_plays"][0]["team_side"] == "home"

    assert props_only.status_code == 200
    props_payload = props_only.get_json()
    assert props_payload["counts"]["game_plays"] == 0
    assert props_payload["counts"]["prop_plays"] == 2
    assert props_payload["game_plays"] == []

def test_api_cards_v2_matches_mlb_betting_v2_shape(monkeypatch):
    date_str = "2026-03-19"

    def _fake_cards():
        return app_module.jsonify(
            {
                "date": date_str,
                "requested_date": date_str,
                "lookahead_applied": False,
                "games": [
                    {
                        "home_tri": "BOS",
                        "away_tri": "MIA",
                        "home_name": "Boston Celtics",
                        "away_name": "Miami Heat",
                        "odds": {
                            "commence_time": f"{date_str}T23:00:00Z",
                            "home_ml": -160,
                            "away_ml": 140,
                            "home_spread": -4.5,
                            "away_spread": 4.5,
                            "total": 221.5,
                        },
                        "sim": {
                            "game_id": 123456,
                            "n_sims": 5000,
                            "score": {
                                "home_mean": 114.2,
                                "away_mean": 108.7,
                                "p_home_win": 0.633,
                                "p_away_win": 0.367,
                                "total_q": {"p50": 222.0},
                                "margin_q": {"p50": 5.0},
                            },
                        },
                        "game_market_recommendations": [
                            {"market": "ML", "recommendation_priority_score": 1.4, "edge": 0.08, "display_pick": "Boston Celtics ML"},
                            {"market": "ATS", "recommendation_priority_score": 0.9, "edge": 0.05, "display_pick": "Miami Heat +4.5"},
                            {"market": "OU", "recommendation_priority_score": 0.8, "edge": 0.04, "display_pick": "Over 221.5"},
                        ],
                        "prop_recommendations": {
                            "home": [
                                {"player": "Star One", "recommendation_priority_score": 1.2, "ev": 0.11},
                            ],
                            "away": [
                                {"player": "Star Two", "recommendation_priority_score": 0.7, "ev": 0.06},
                            ],
                        },
                    }
                ],
            }
        )

    monkeypatch.setattr(app_module, "api_cards", _fake_cards)

    app_module.app.testing = True
    with app_module.app.test_client() as client:
        resp = client.get(f"/api/cards-v2?date={date_str}")

    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["date"] == date_str
    assert payload["view"]["mode"] == "legacy_daily"
    assert isinstance(payload["cards"], list)
    assert len(payload["cards"]) == 1

    card = payload["cards"][0]
    assert card["gamePk"] == 123456
    assert card["away"]["abbr"] == "MIA"
    assert card["home"]["abbr"] == "BOS"
    assert card["predictions"]["full"]["home_win_prob"] == 0.633
    assert card["markets"]["ml"]["market"] == "ML"
    assert card["markets"]["spreads"]["market"] == "ATS"
    assert card["markets"]["totals"]["market"] == "OU"
    assert len(card["markets"]["playerProps"]) == 2
    assert card["flags"]["hasAnyRecommendations"] is True
    assert card["flags"]["hasPlayerProps"] is True


def test_api_season_betting_card_manifest_and_day(monkeypatch):
    date_str = "2026-03-19"

    def _fake_cards():
        return app_module.jsonify(
            {
                "date": date_str,
                "requested_date": date_str,
                "games": [
                    {
                        "home_tri": "BOS",
                        "away_tri": "MIA",
                        "home_name": "Boston Celtics",
                        "away_name": "Miami Heat",
                        "odds": {
                            "commence_time": f"{date_str}T23:00:00Z",
                        },
                        "sim": {
                            "game_id": 123456,
                        },
                        "game_market_recommendations": [
                            {
                                "market": "ML",
                                "selection": "home",
                                "display_pick": "BOS ML",
                                "price": -115,
                                "edge": 0.04,
                                "result": "win",
                                "actual": 1,
                            },
                            {
                                "market": "ATS",
                                "selection": "away",
                                "display_pick": "MIA +4.5",
                                "line": 4.5,
                                "price": -110,
                                "edge": 0.03,
                                "result": "loss",
                            },
                        ],
                        "prop_recommendations": {
                            "home": [
                                {
                                    "market": "pts",
                                    "player": "Star One",
                                    "selection": "over",
                                    "display_pick": "Star One OVER 24.5 Points",
                                    "line": 24.5,
                                    "price": -105,
                                    "edge": 0.05,
                                    "result": "push",
                                    "actual": 24.5,
                                },
                            ],
                            "away": [],
                        },
                    }
                ],
            }
        )

    monkeypatch.setattr(app_module, "api_cards", _fake_cards)
    monkeypatch.setattr(app_module, "_season_betting_card_candidate_dates", lambda season, requested_date=None: [date_str])

    app_module.app.testing = True
    with app_module.app.test_client() as client:
        manifest_resp = client.get(f"/api/season/2026/betting-card?date={date_str}&profile=retuned")
        day_resp = client.get(f"/api/season/2026/betting-card/day/{date_str}?profile=retuned")

    assert manifest_resp.status_code == 200
    manifest = manifest_resp.get_json()
    assert manifest["season"] == 2026
    assert manifest["profile"] == "retuned"
    assert manifest["summary"]["cards"] == 1
    assert manifest["summary"]["selected_counts"]["combined"] == 3
    assert manifest["days"][0]["date"] == date_str

    assert day_resp.status_code == 200
    day = day_resp.get_json()
    assert day["date"] == date_str
    assert day["selected_counts"]["combined"] == 3
    assert len(day["games"]) == 1
    assert len(day["games"][0]["betting"]["officialRows"]) == 3
    assert day["games"][0]["betting"]["officialRows"][0]["display_pick"] == "BOS ML"


def test_duplicate_best_bets_aliases_redirect_to_betting_card():
    app_module.app.testing = True
    with app_module.app.test_client() as client:
        games_alias = client.get("/api/games/best-bets-parlays?date=2026-03-19")
        props_alias = client.get("/api/props-best-bets-parlays?date=2026-03-19")

    assert games_alias.status_code == 302
    assert "/api/betting-card?date=2026-03-19&section=games" in games_alias.headers["Location"]

    assert props_alias.status_code == 302
    assert "/api/betting-card?date=2026-03-19&section=props" in props_alias.headers["Location"]


def test_api_cards_omits_players_by_default_but_supports_matchup_detail(monkeypatch):
    date_str = "2026-03-30"
    payload = {
        "home": "OKC",
        "away": "DET",
        "game_id": "DET@OKC",
        "players": {
            "home": [{"player_name": "Shai Gilgeous-Alexander", "pts_mean": 31.2}],
            "away": [{"player_name": "Cade Cunningham", "pts_mean": 24.8}],
        },
        "score": {"home_mean": 118.5, "away_mean": 111.3, "total_mean": 229.8},
        "market": {},
        "periods": {},
        "context": {},
    }

    monkeypatch.setattr(app_module, "_load_smart_sim_files_for_authoritative_slate", lambda _date: [])
    monkeypatch.setattr(app_module, "_find_next_available_smart_sim_date", lambda *_args, **_kwargs: (None, []))
    monkeypatch.setattr(
        app_module,
        "_load_game_odds_map",
        lambda _date: {("OKC", "DET"): {"home_team": "Oklahoma City Thunder", "visitor_team": "Detroit Pistons", "commence_time": f"{date_str}T23:00:00Z"}},
    )
    monkeypatch.setattr(app_module, "_load_predictions_rows_map", lambda _date: {("OKC", "DET"): {}})
    monkeypatch.setattr(app_module, "_load_props_predictions_map", lambda _date: {})
    monkeypatch.setattr(app_module, "_compute_player_minutes_priors", lambda _date, days_back=21: {})
    monkeypatch.setattr(app_module, "_live_load_props_edges_index", lambda _date: {})
    monkeypatch.setattr(app_module, "_load_props_recommendations_by_team", lambda _date: {})
    monkeypatch.setattr(app_module, "_load_injury_context_map", lambda _date: {})
    monkeypatch.setattr(app_module, "_roster_players_for_date", lambda _date: {})
    monkeypatch.setattr(app_module, "_load_best_bets_game_context", lambda _date: {})
    monkeypatch.setattr(app_module, "_load_best_bets_props_prediction_lookup", lambda _date: {})
    monkeypatch.setattr(app_module, "_best_bets_load_injury_snapshot", lambda _date: {})
    monkeypatch.setattr(app_module, "_load_cards_game_recommendations_index", lambda _date: {})
    monkeypatch.setattr(app_module, "_load_cards_prop_snapshot_index", lambda _date: {})
    monkeypatch.setattr(app_module, "_load_cards_prop_recommendations_index", lambda _date: {})
    monkeypatch.setattr(app_module, "_load_recon_props_lookup", lambda _date: ({}, {}))
    monkeypatch.setattr(app_module, "_matchup_writeup", lambda _game: "")
    monkeypatch.setattr(app_module, "_build_fallback_smart_sim_object", lambda *_args, **_kwargs: dict(payload))
    monkeypatch.setattr(app_module, "_sim_vs_line_prop_recommendations", lambda *args, **kwargs: {"home": [], "away": []})
    monkeypatch.setattr(app_module, "_build_cards_game_market_recommendations", lambda *args, **kwargs: [])

    app_module.app.testing = True
    with app_module.app.test_client() as client:
        base_resp = client.get(f"/api/cards?date={date_str}")
        detail_resp = client.get(f"/api/cards?date={date_str}&away=DET&home=OKC&include_players=1")

    assert base_resp.status_code == 200
    assert detail_resp.status_code == 200

    base_payload = base_resp.get_json()
    detail_payload = detail_resp.get_json()
    base_game = base_payload["games"][0]
    detail_game = detail_payload["games"][0]

    assert base_payload["players_included"] is False
    assert base_game["sim"]["players_loaded"] is False
    assert "players" not in base_game["sim"]
    assert detail_payload["players_included"] is True
    assert detail_game["sim"]["players_loaded"] is True
    assert detail_game["sim"]["players"]["home"][0]["player_name"] == "Shai Gilgeous-Alexander"


def test_api_cards_sim_detail_reads_snapshot(monkeypatch):
    monkeypatch.setattr(
        app_module,
        "_load_cards_sim_detail_index",
        lambda _date: {
            ("OKC", "DET"): {
                "players_summary": {"home": 1, "away": 1},
                "players": {
                    "home": [{"player_name": "Shai Gilgeous-Alexander"}],
                    "away": [{"player_name": "Cade Cunningham"}],
                },
                "missing_prop_players": {"home": [], "away": []},
                "injuries": {"home": [], "away": []},
            }
        },
    )

    app_module.app.testing = True
    with app_module.app.test_client() as client:
        resp = client.get("/api/cards/sim-detail?date=2026-03-30&away=DET&home=OKC")

    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["players_included"] is True
    assert payload["games"][0]["sim"]["players_loaded"] is True
    assert payload["games"][0]["sim"]["players"]["home"][0]["player_name"] == "Shai Gilgeous-Alexander"


def test_api_cards_uses_sim_detail_snapshot_for_summary_when_raw_players_missing(monkeypatch):
    date_str = "2026-03-31"
    payload = {
        "home": "ORL",
        "away": "PHX",
        "game_id": "PHX@ORL",
        "players": {"home": [], "away": []},
        "score": {"home_mean": 111.0, "away_mean": 109.0, "total_mean": 220.0},
        "market": {},
        "periods": {},
        "context": {"fallback_reason": "missing_smart_sim"},
        "error": "missing_smart_sim",
    }

    monkeypatch.setattr(app_module, "_load_smart_sim_files_for_authoritative_slate", lambda _date: [])
    monkeypatch.setattr(app_module, "_find_next_available_smart_sim_date", lambda *_args, **_kwargs: (None, []))
    monkeypatch.setattr(
        app_module,
        "_load_game_odds_map",
        lambda _date: {("ORL", "PHX"): {"home_team": "Orlando Magic", "visitor_team": "Phoenix Suns", "commence_time": f"{date_str}T23:00:00Z"}},
    )
    monkeypatch.setattr(app_module, "_load_predictions_rows_map", lambda _date: {("ORL", "PHX"): {}})
    monkeypatch.setattr(app_module, "_load_props_predictions_map", lambda _date: {})
    monkeypatch.setattr(app_module, "_compute_player_minutes_priors", lambda _date, days_back=21: {})
    monkeypatch.setattr(app_module, "_live_load_props_edges_index", lambda _date: {})
    monkeypatch.setattr(app_module, "_load_props_recommendations_by_team", lambda _date: {})
    monkeypatch.setattr(app_module, "_load_injury_context_map", lambda _date: {})
    monkeypatch.setattr(app_module, "_roster_players_for_date", lambda _date: {})
    monkeypatch.setattr(app_module, "_load_best_bets_game_context", lambda _date: {})
    monkeypatch.setattr(app_module, "_load_best_bets_props_prediction_lookup", lambda _date: {})
    monkeypatch.setattr(app_module, "_best_bets_load_injury_snapshot", lambda _date: {})
    monkeypatch.setattr(app_module, "_load_cards_game_recommendations_index", lambda _date: {})
    monkeypatch.setattr(app_module, "_load_cards_prop_snapshot_index", lambda _date: {})
    monkeypatch.setattr(app_module, "_load_cards_prop_recommendations_index", lambda _date: {})
    monkeypatch.setattr(
        app_module,
        "_load_cards_sim_detail_index",
        lambda _date: {
            ("ORL", "PHX"): {
                "players_summary": {"home": 9, "away": 6, "missing_home": 2, "missing_away": 0, "injured_home": 0, "injured_away": 0},
                "players": {"home": [{"player_name": "Paolo Banchero"}], "away": [{"player_name": "Devin Booker"}]},
                "missing_prop_players": {"home": [{"player_name": "Jalen Suggs"}], "away": []},
                "injuries": {"home": [], "away": []},
            }
        },
    )
    monkeypatch.setattr(app_module, "_load_recon_props_lookup", lambda _date: ({}, {}))
    monkeypatch.setattr(app_module, "_matchup_writeup", lambda _game: "")
    monkeypatch.setattr(app_module, "_build_fallback_smart_sim_object", lambda *_args, **_kwargs: dict(payload))
    monkeypatch.setattr(app_module, "_sim_vs_line_prop_recommendations", lambda *args, **kwargs: {"home": [], "away": []})
    monkeypatch.setattr(app_module, "_build_cards_game_market_recommendations", lambda *args, **kwargs: [])

    app_module.app.testing = True
    with app_module.app.test_client() as client:
        base_resp = client.get(f"/api/cards?date={date_str}")
        detail_resp = client.get(f"/api/cards?date={date_str}&home=ORL&away=PHX&include_players=1")

    base_game = base_resp.get_json()["games"][0]
    detail_game = detail_resp.get_json()["games"][0]

    assert base_game["sim"]["players_summary"]["home"] == 9
    assert base_game["sim"]["players_summary"]["away"] == 6
    assert detail_game["sim"]["players"]["home"][0]["player_name"] == "Paolo Banchero"
    assert detail_game["sim"]["players"]["away"][0]["player_name"] == "Devin Booker"