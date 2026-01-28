import os, sys
base = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, base)
sys.path.insert(0, os.path.join(base, 'src'))
from app import app

if __name__ == "__main__":
    with app.test_client() as c:
        r = c.get('/recommendations?format=json&view=all&date=2025-10-23&compact=1')
        print('status', r.status_code)
        j = r.get_json()
        print('count', len(j.get('rows', [])))
        for row in j.get('rows', []):
            if row.get('home')=='Indiana Pacers':
                print(row)
