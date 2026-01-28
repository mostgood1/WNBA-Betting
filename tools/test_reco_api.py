import sys, os
import importlib.util

app = None
try:
    here = os.path.abspath(os.getcwd())
    fp = os.path.join(here, 'app.py')
    spec = importlib.util.spec_from_file_location('app', fp)
    m = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(m)
    app = getattr(m, 'app', None)
except Exception as e:
    print('ERROR: failed to import app.py:', e)
if app is None:
    print('ERROR: Flask app not found in app.py')
    sys.exit(2)

date_arg = (sys.argv[1] if len(sys.argv) > 1 else '2026-01-14')
with app.test_client() as c:
    resp = c.get(f'/recommendations?format=json&view=all&date={date_arg}&compact=1')
    print('status', resp.status_code)
    data = resp.get_json()
    if not isinstance(data, dict):
        print('ERROR: Non-JSON response')
        sys.exit(3)
    counts = {k: len((data.get(k) or [])) for k in ['games','props','first_basket','early_threes']}
    print('counts', counts)
    meta_dates = ((data.get('meta') or {}).get('data_dates') or {})
    print('meta_dates', meta_dates)
