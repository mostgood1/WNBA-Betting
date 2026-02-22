from __future__ import annotations

import pytest


def test_props_recommendations_api_smoke() -> None:
    requests = pytest.importorskip("requests")

    url = "http://127.0.0.1:5051/api/props-recommendations?date=2025-10-17"
    try:
        resp = requests.get(url, timeout=3)
    except requests.exceptions.RequestException as exc:
        pytest.skip(f"Local API not reachable at {url}: {exc}")

    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, dict)
    # shape is stable even if no plays are present
    assert "date" in data
    assert "data" in data
