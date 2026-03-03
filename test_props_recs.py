from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pytest


def test_props_edges_file_smoke() -> None:
    base_dir = Path(__file__).resolve().parent
    d = "2025-10-17"
    data_root_env = (os.environ.get("NBA_BETTING_DATA_ROOT") or "").strip()
    data_root = Path(data_root_env).expanduser().resolve() if data_root_env else (base_dir / "data")
    edges_p = data_root / "processed" / f"props_edges_{d}.csv"

    if not edges_p.exists():
        pytest.skip(f"Missing processed edges file: {edges_p}")

    df = pd.read_csv(edges_p)
    assert not df.empty
    # Basic schema expectations
    assert "player_name" in df.columns
    assert "stat" in df.columns
