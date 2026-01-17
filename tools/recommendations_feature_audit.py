import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Dict, List

try:
    import requests  # type: ignore
except Exception:
    requests = None
    import urllib.request
    import urllib.parse

FEATURES = [
    "team_injury_impact",
    "minutes_stability",
    "opp_allowed_pts_rank",
    "opp_allowed_threes_rank",
    "home_tricode",
    "away_tricode",
    "team_is_away",
    "altitude_game",
    "tz_diff_hours",
    "travel_km",
    "team_b2b",
    "team_3in4",
]


def fetch_recs(base_url: str, date: str) -> List[Dict]:
    params = {
        "date": date,
        "compact": "true",
        "portfolio_only": "false",
    }
    url = base_url + "/api/props/recommendations"
    if requests:
        r = requests.get(url, params=params, timeout=25)
        r.raise_for_status()
        data = r.json()
    else:
        q = urllib.parse.urlencode(params)
        with urllib.request.urlopen(url + "?" + q) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    rows = data.get("rows") if isinstance(data, dict) else data
    if not isinstance(rows, list):
        return []
    return rows


def audit(rows: List[Dict]) -> Dict:
    total = len(rows)
    out: Dict = {"total_rows": total, "features": {}}
    for f in FEATURES:
        present = 0
        non_null = 0
        for r in rows:
            if f in r:
                present += 1
                v = r.get(f)
                if v is not None and v != "" and not (isinstance(v, float) and (v != v)):
                    non_null += 1
        out["features"][f] = {
            "present_count": present,
            "non_null_count": non_null,
            "present_pct": (present / total) if total else 0.0,
            "non_null_pct": (non_null / total) if total else 0.0,
        }
    return out


def main():
    ap = argparse.ArgumentParser(description="Audit recommendations feature coverage")
    ap.add_argument("--base-url", default="http://127.0.0.1:5051")
    ap.add_argument("--date", default=dt.date.today().strftime("%Y-%m-%d"))
    ap.add_argument("--out", default=str(Path("data/processed/metrics/recs_feature_audit.json")))
    args = ap.parse_args()

    rows = fetch_recs(args.base_url, args.date)
    rep = audit(rows)
    rep["date"] = args.date

    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)
    with open(outp, "w", encoding="utf-8") as f:
        json.dump(rep, f, indent=2)
    print(f"Wrote feature audit to {outp} (rows={rep['total_rows']})")


if __name__ == "__main__":
    main()
