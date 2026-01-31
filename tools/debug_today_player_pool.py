from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def _norm_bool(x) -> str:
    try:
        s = str(x).strip().lower()
    except Exception:
        return ""
    if s in {"true", "1", "yes", "y"}:
        return "true"
    if s in {"false", "0", "no", "n"}:
        return "false"
    return s


def _jsonable(x):
    # Convert numpy/pandas scalars to plain Python types for json.dumps
    try:
        import numpy as np

        if isinstance(x, (np.integer,)):
            return int(x)
        if isinstance(x, (np.floating,)):
            return float(x)
        if isinstance(x, (np.bool_,)):
            return bool(x)
    except Exception:
        pass
    return x


def main() -> None:
    date_str = "2026-01-29"
    team = "PHI"
    player = "Tyrese Maxey"

    pp = Path(f"data/processed/props_predictions_{date_str}.csv")
    if not pp.exists():
        raise SystemExit(f"missing {pp}")

    df = pd.read_csv(pp)

    out: dict[str, object] = {
        "props_predictions_path": str(pp),
        "rows": int(len(df)),
        "columns": list(df.columns),
    }

    # Maxey
    m = df[df.get("player_name").astype(str).str.strip().eq(player)] if "player_name" in df.columns else df.iloc[0:0]
    out["maxey_rows"] = int(len(m))
    if len(m):
        r = m.iloc[0]
        cols = [
            c
            for c in [
                "player_id",
                "player_name",
                "team",
                "opponent",
                "home",
                "injury_status",
                "playing_today",
                "team_on_slate",
            ]
            if c in df.columns
        ]
        out["maxey_fields"] = {c: (r.get(c) if c in cols else None) for c in cols}
        if "playing_today" in df.columns:
            out["maxey_playing_today_norm"] = _norm_bool(r.get("playing_today"))

        out["maxey_fields"] = {k: _jsonable(v) for k, v in (out.get("maxey_fields") or {}).items()}

    # PHI team summary
    if "team" in df.columns:
        phi = df[df["team"].astype(str).str.upper().str.strip().eq(team)]
        out["phi_rows"] = int(len(phi))
        if "playing_today" in phi.columns:
            pt = phi["playing_today"].map(_norm_bool)
            bad = phi[pt.eq("false")].copy()
            out["phi_playing_today_false_rows"] = int(len(bad))
            if len(bad):
                keep = [c for c in ["player_name", "playing_today", "injury_status"] if c in bad.columns]
                rows = bad[keep].head(25).to_dict(orient="records")
                out["phi_playing_today_false_sample"] = [
                    {k: _jsonable(v) for k, v in (row or {}).items()} for row in (rows or [])
                ]

    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
