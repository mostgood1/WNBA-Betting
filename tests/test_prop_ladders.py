from __future__ import annotations

import json

from nba_betting.prop_ladders import build_card_sim_ladders, build_exact_ladder_payload, load_smart_sim_prop_ladder_lookup


def test_build_exact_ladder_payload_includes_distribution_counts() -> None:
    payload = build_exact_ladder_payload([10, 10.2, 11, 11.4, 11.4, 13])

    assert payload is not None
    assert payload["distribution"] == {"10": 2, "11": 3, "13": 1}
    assert payload["simCount"] == 6
    assert payload["ladderShape"] == "exact"


def test_load_smart_sim_prop_ladder_lookup_reads_exact_prop_distributions(tmp_path) -> None:
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)
    sim_path = processed / "smart_sim_2026-03-25_POR_MIL.json"
    sim_path.write_text(
        json.dumps(
            {
                "home": "POR",
                "away": "MIL",
                "date": "2026-03-25",
                "n_sims": 5,
                "players": {
                    "home": [
                        {
                            "player_name": "Scoot Henderson",
                            "player_id": 1,
                            "prop_distributions": {
                                "pts": {
                                    "simCount": 5,
                                    "mean": 12.8,
                                    "distribution": {"10": 1, "12": 2, "15": 2},
                                }
                            },
                            "pts_mean": 12.8,
                            "pts_sd": 1.9,
                            "pts_q": {"p50": 12.0, "p90": 15.0},
                        }
                    ],
                    "away": [],
                },
            }
        ),
        encoding="utf-8",
    )

    lookup = load_smart_sim_prop_ladder_lookup(processed, "2026-03-25")

    entry = lookup[("scoot henderson", "POR")]
    pts_market = entry["markets"]["pts"]
    assert pts_market["ladderShape"] == "exact"
    assert pts_market["simCount"] == 5
    assert [row["total"] for row in pts_market["ladder"]] == [10, 12, 15]
    assert pts_market["ladder"][-1]["hitCount"] == 2
    assert pts_market["ladder"][-1]["exactCount"] == 2


def test_build_card_sim_ladders_merges_market_ladder_lines() -> None:
    lookup = {
        ("jalen wilson", "BKN"): {
            "player": "Jalen Wilson",
            "player_id": 4431714,
            "team": "BKN",
            "opponent": "OKC",
            "game_id": "game-1",
            "sourceFile": "smart_sim_2026-03-25_BKN_OKC.json",
            "markets": {
                "pts": {
                    "market": "pts",
                    "label": "Points",
                    "simCount": 100,
                    "mean": 8.7,
                    "mode": 8,
                    "modeProb": 0.12,
                    "minTotal": 2,
                    "maxTotal": 18,
                    "ladderShape": "exact",
                    "ladder": [
                        {"total": 8, "hitCount": 61, "hitProb": 0.61, "exactCount": 14, "exactProb": 0.14},
                        {"total": 9, "hitCount": 44, "hitProb": 0.44, "exactCount": 10, "exactProb": 0.10},
                    ],
                }
            },
        }
    }

    rows = build_card_sim_ladders(
        "Jalen Wilson",
        "BKN",
        [],
        [
            {
                "market": "pts",
                "side": "UNDER",
                "base": {"line": 8.5, "price": 150, "ev_pct": 4.1},
                "entries": [{"line": 7.5, "price": 215, "ev_pct": 2.8}],
            }
        ],
        lookup,
    )

    assert len(rows) == 1
    pts_row = rows[0]
    assert any(entry["line"] == 8.5 and entry["underOdds"] == 150 for entry in pts_row["marketLinesByStat"])
    assert any(entry["line"] == 7.5 and entry["underOdds"] == 215 for entry in pts_row["marketLinesByStat"])