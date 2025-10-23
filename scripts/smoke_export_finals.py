import sys, pathlib
ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from app import app

if __name__ == "__main__":
    with app.test_client() as c:
        r = c.get('/api/finals/export?date=2025-10-21')
        print("Status:", r.status_code)
        try:
            js = r.get_json()
        except Exception:
            js = None
        print("JSON:", js)
