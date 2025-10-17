import requests
import json

try:
    resp = requests.get('http://127.0.0.1:5051/api/props-recommendations?date=2025-10-17', timeout=3)
    data = resp.json()
    
    print(f"Total players: {len(data['data'])}\n")
    
    # Check a few players
    for player_card in data['data'][:5]:
        print(f"Player: {player_card['player']}")
        print(f"  Team: {player_card['team']}")
        print(f"  Opponent: {player_card.get('opponent', 'N/A')}")
        print(f"  Home: {player_card.get('home_team', 'N/A')}")
        print(f"  Away: {player_card.get('away_team', 'N/A')}")
        print(f"  Model stats: {list(player_card.get('model', {}).keys())}")
        print(f"  Number of props: {len(player_card.get('plays', []))}")
        print()
    
    # Check LaMelo Ball specifically
    lamelo = next((c for c in data['data'] if 'LaMelo' in c.get('player', '')), None)
    if lamelo:
        print("LaMelo Ball details:")
        print(f"  Model: {lamelo['model']}")
        print(f"  Plays: {len(lamelo['plays'])} props")
        
except Exception as e:
    print(f"Error: {e}")
