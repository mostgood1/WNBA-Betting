from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest


def test_props_edges_file_smoke() -> None:
    base_dir = Path(__file__).parent
    d = "2025-10-17"
    edges_p = base_dir / "data" / "processed" / f"props_edges_{d}.csv"

    if not edges_p.exists():
        pytest.skip(f"Missing processed edges file: {edges_p}")

    df = pd.read_csv(edges_p)
    assert not df.empty
    # Basic schema expectations
    assert "player_name" in df.columns
    assert "stat" in df.columns
