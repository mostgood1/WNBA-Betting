import requests
import json

try:
    resp = requests.get('http://127.0.0.1:5051/api/props-recommendations?date=2025-10-17', timeout=3)
    data = resp.json()
    
    if data.get('data'):
        first_card = data['data'][0]
        print("First player card structure:")
        print(json.dumps(first_card, indent=2))
        
        print("\n\nKey observations:")
        print(f"Player: {first_card.get('player')}")
        print(f"Team: {first_card.get('team')}")
        print(f"Home Team: {first_card.get('home_team')}")
        print(f"Away Team: {first_card.get('away_team')}")
        print(f"Opponent field: {first_card.get('opponent', 'NOT FOUND')}")
        print(f"\nModel stats: {list(first_card.get('model', {}).keys())}")
        
except Exception as e:
    print(f"Error: {e}")
