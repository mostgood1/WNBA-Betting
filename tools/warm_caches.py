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

    # 1) Rebuild props correlation cache (numeric/co-occurrence fallback inside server)
    try:
        u = f"{base}/api/admin/props/corr-cache"
        r = _post(u, json_body={"days": 60})
        results["steps"].append({"step": "corr-cache rebuild", "ok": True, "result": r})
    except Exception as e:
        results["steps"].append({"step": "corr-cache rebuild", "ok": False, "error": str(e)})

    # 2) Warm props recommendations (compact, portfolio-only)
    try:
        u = f"{base}/api/props/recommendations"
        q = {"date": d, "compact": "1", "portfolio_only": "1", "optimize": "1", "limit": "20"}
        r = _get(u, params=q)
        rows = (r.get("rows") if isinstance(r, dict) else None)
        results["steps"].append({"step": "props portfolio", "ok": True, "rows": rows})
    except Exception as e:
        results["steps"].append({"step": "props portfolio", "ok": False, "error": str(e)})

    # 3) Warm full compact props list for UI
    try:
        u = f"{base}/api/props/recommendations"
        q = {"date": d, "compact": "1"}
        r = _get(u, params=q)
        rows = (r.get("rows") if isinstance(r, dict) else None)
        results["steps"].append({"step": "props compact", "ok": True, "rows": rows})
    except Exception as e:
        results["steps"].append({"step": "props compact", "ok": False, "error": str(e)})

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
