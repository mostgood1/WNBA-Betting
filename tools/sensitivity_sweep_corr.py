import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Dict, List, Tuple

import sys

try:
    import requests  # type: ignore
except Exception:
    requests = None
    import urllib.request
    import urllib.parse


def fetch_portfolio(base_url: str, date: str, scale: float) -> List[Dict]:
    params = {
        "date": date,
        "compact": "true",
        "portfolio_only": "true",
        "corr_penalty_scale": str(scale),
    }
    url = base_url + "/api/props/recommendations"
    if requests:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
    else:
        q = urllib.parse.urlencode(params)
        with urllib.request.urlopen(url + "?" + q) as resp:
            import json as _json
            data = _json.loads(resp.read().decode("utf-8"))
    # Expect data in { rows: [...] } or list
    rows = data.get("rows") if isinstance(data, dict) else data
    if not isinstance(rows, list):
        return []
    return rows


def key_of_row(row: Dict) -> Tuple:
    return (
        row.get("player"), row.get("market"), row.get("stat"), row.get("side"), row.get("book")
    )


def main():
    parser = argparse.ArgumentParser(description="Sweep corr_penalty_scale and summarize portfolio changes")
    parser.add_argument("--base-url", type=str, default="http://127.0.0.1:5051")
    parser.add_argument("--date", type=str, default=dt.date.today().strftime("%Y-%m-%d"))
    parser.add_argument("--scales", type=str, default="0.0,0.5,1.0,1.5,2.0")
    parser.add_argument("--outdir", type=str, default=str(Path("data/processed/metrics")))
    args = parser.parse_args()

    scales = [float(s) for s in args.scales.split(",")]
    portfolios: Dict[float, List[Dict]] = {}

    for s in scales:
        try:
            rows = fetch_portfolio(args.base_url, args.date, s)
        except Exception as ex:
            print(f"Failed to fetch portfolio for scale {s}: {ex}", file=sys.stderr)
            rows = []
        portfolios[s] = rows

    # Compare baseline (first scale) to others
    base_scale = scales[0]
    base_keys = set(key_of_row(r) for r in portfolios.get(base_scale, []))
    change_summary: List[Dict] = []
    for s in scales[1:]:
        keys = set(key_of_row(r) for r in portfolios.get(s, []))
        added = sorted(list(keys - base_keys))
        removed = sorted(list(base_keys - keys))
        change_summary.append({
            "scale": s,
            "added_count": len(added),
            "removed_count": len(removed),
            "added": added[:20],
            "removed": removed[:20],
        })

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    out_json = outdir / f"sensitivity_corr_{args.date}.json"
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump({
            "date": args.date,
            "scales": scales,
            "changes": change_summary,
        }, f, indent=2)

    print(f"Wrote sensitivity summary to {out_json}")


if __name__ == "__main__":
    main()
