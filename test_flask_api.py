from app import app
import json

with app.test_client() as client:
    # Test predictions API
    print("Testing /api/predictions?date=2025-10-17")
    r = client.get('/api/predictions?date=2025-10-17')
    print(f"Status: {r.status_code}")
    
    if r.status_code == 200:
        data = r.get_json()
        games = data.get('rows', [])  # Changed from 'games' to 'rows'
        print(f"Games returned: {len(games)}")
        
        if games:
            game = games[0]
            print(f"\nFirst game sample:")
            print(f"  Away: {game.get('away_team')} @ Home: {game.get('home_team')}")
            print(f"  Win Prob (Home): {game.get('home_win_prob')}")
            print(f"  Pred Margin: {game.get('pred_margin')}")
            print(f"  Pred Total: {game.get('pred_total')}")
            print(f"  Home ML: {game.get('home_ml')}")
            print(f"  Away ML: {game.get('away_ml')}")
            print(f"  Spread (Home): {game.get('home_spread')}")
            print(f"  Total: {game.get('total')}")
            print(f"  Edge Win: {game.get('edge_win')}")
            print(f"  Edge Spread: {game.get('edge_spread')}")
            print(f"  Edge Total: {game.get('edge_total')}")
            print(f"  Bookmaker: {game.get('bookmaker')}")
    else:
        print(f"Error: {r.data.decode('utf-8')[:200]}")
    
    # Test recommendations API
    print("\n\nTesting /api/recommendations?date=2025-10-17")
    r2 = client.get('/api/recommendations?date=2025-10-17&spread_edge=1.0&total_edge=1.5')
    print(f"Status: {r2.status_code}")
    
    if r2.status_code == 200:
        data2 = r2.get_json()
        rows = data2.get('rows', [])
        print(f"Recommendations returned: {len(rows)}")
        
        if rows:
            rec = rows[0]
            print(f"\nFirst recommendation sample:")
            print(f"  Game: {rec.get('away_team')} @ {rec.get('home_team')}")
            print(f"  Market: {rec.get('market')}")
            print(f"  Bet: {rec.get('bet')}")
            print(f"  Edge: {rec.get('edge')}")
            print(f"  EV: {rec.get('ev')}")
    
    print("\n✅ Flask API test complete!")
