import requests
import json

url = "http://127.0.0.1:5051/api/props-recommendations?date=2025-10-17"

print(f"Testing: {url}\n")

try:
    resp = requests.get(url, timeout=5)
    print(f"Status Code: {resp.status_code}")
    
    if resp.status_code == 200:
        data = resp.json()
        print(f"\nResponse Keys: {list(data.keys())}")
        print(f"Date: {data.get('date')}")
        print(f"Total Rows: {data.get('rows')}")
        print(f"Games: {len(data.get('games', []))}")
        print(f"Player Cards: {len(data.get('data', []))}")
        
        if data.get('data'):
            print(f"\nFirst Player Card:")
            first_card = data['data'][0]
            print(json.dumps(first_card, indent=2))
        else:
            print(f"\n❌ NO PLAYER CARDS!")
            print(f"\nFull response:")
            print(json.dumps(data, indent=2))
    else:
        print(f"\n❌ Error Response:")
        print(resp.text)
        
except Exception as e:
    print(f"\n❌ Exception: {e}")
