import os
import sys
import json
import datetime as dt
from urllib.parse import urlencode

try:
    import requests
except Exception as e:
    print(f"error: requests not installed: {e}")
    sys.exit(1)

BASE_URL = os.environ.get("NBA_API_BASE_URL", "http://127.0.0.1:5051").rstrip("/")

def _get(url, params=None):
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json() if r.headers.get("content-type","" ).startswith("application/json") else r.text

def _post(url, json_body=None):
    r = requests.post(url, json=json_body, timeout=120)
    r.raise_for_status()
    return r.json() if r.headers.get("content-type","" ).startswith("application/json") else r.text


def main():
    d = (os.environ.get("NBA_DATE") or dt.date.today().isoformat())
    base = BASE_URL
    results = {"date": d, "base_url": base, "steps": []}

    # Warm endpoints that are required by the minimal UI allowlist.

    # 1) Warm schedule
    try:
        u = f"{base}/api/schedule"
        r = _get(u, params={"date": d})
        rows = None
        if isinstance(r, dict):
            rr = r.get("rows")
            if isinstance(rr, list):
                rows = len(rr)
        elif isinstance(r, list):
            rows = len(r)
        results["steps"].append({"step": "schedule", "ok": True, "rows": rows})
    except Exception as e:
        results["steps"].append({"step": "schedule", "ok": False, "error": str(e)})

    # 2) Warm predictions
    try:
        u = f"{base}/api/predictions"
        r = _get(u, params={"date": d})
        rows = None
        if isinstance(r, dict):
            rr = r.get("rows")
            if isinstance(rr, list):
                rows = len(rr)
        elif isinstance(r, list):
            rows = len(r)
        results["steps"].append({"step": "predictions", "ok": True, "rows": rows})
    except Exception as e:
        results["steps"].append({"step": "predictions", "ok": False, "error": str(e)})

    # 3) Warm game cards (this is the critical one for roster correctness)
    try:
        u = f"{base}/api/cards"
        r = _get(u, params={"date": d})
        games = (r.get("games") if isinstance(r, dict) else None)
        results["steps"].append({"step": "cards", "ok": True, "games": (len(games) if isinstance(games, list) else None)})
    except Exception as e:
        results["steps"].append({"step": "cards", "ok": False, "error": str(e)})

    # 4) Warm all recommendations bundle for homepage widgets
    try:
        u = f"{base}/recommendations?format=json&view=all"
        q = {"date": d, "compact": "1"}
        r = _get(u, params=q)
        results["steps"].append({"step": "recommendations all", "ok": True})
    except Exception as e:
        results["steps"].append({"step": "recommendations all", "ok": False, "error": str(e)})

    out = json.dumps(results, indent=2)
    print(out)

if __name__ == "__main__":
    main()
