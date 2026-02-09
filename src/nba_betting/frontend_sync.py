"""
🏀 Frontend Data Sync Module for NBA-Betting
Ensures all prediction data is properly formatted and available for frontend endpoints
"""

from __future__ import annotations

import pandas as pd
import json
from pathlib import Path
from datetime import datetime, date
from typing import Dict, List, Optional, Any
import warnings
warnings.filterwarnings('ignore')

from .config import paths


def validate_and_sync_frontend_data(target_date: str) -> Dict[str, Any]:
    """Validate and sync all data files for frontend consumption"""
    
    results = {
        "date": target_date,
        "validation_results": {},
        "files_created": [],
        "errors": []
    }
    
    try:
        # 1. Validate game predictions
        game_files = [
            paths.data_processed / f"predictions_{target_date}.csv",
            paths.data_processed / f"games_predictions_npu_{target_date}.csv"
        ]
        
        game_pred_file = None
        for file_path in game_files:
            if file_path.exists():
                game_pred_file = file_path
                break
        
        if game_pred_file:
            games_df = pd.read_csv(game_pred_file)
            results["validation_results"]["games"] = {
                "rows": len(games_df),
                "columns": list(games_df.columns),
                "file_used": str(game_pred_file)
            }
            
            # Create frontend-friendly JSON
            games_json = _format_games_for_frontend(games_df, target_date)
            json_path = paths.data_processed / f"games_predictions_{target_date}.json"
            with open(json_path, 'w') as f:
                json.dump(games_json, f, indent=2, default=str)
            results["files_created"].append(str(json_path))
            
        else:
            results["errors"].append("No game predictions file found")
        
        # 2. Validate prop predictions  
        prop_files = [
            paths.data_processed / f"props_predictions_npu_{target_date}.csv",
            paths.data_processed / f"props_predictions_{target_date}.csv"
        ]
        
        prop_pred_file = None
        for file_path in prop_files:
            if file_path.exists():
                prop_pred_file = file_path
                break
        
        if prop_pred_file:
            props_df = pd.read_csv(prop_pred_file)
            results["validation_results"]["props"] = {
                "rows": len(props_df),
                "columns": list(props_df.columns),
                "file_used": str(prop_pred_file)
            }
            
            # Create frontend-friendly JSON
            props_json = _format_props_for_frontend(props_df, target_date)
            json_path = paths.data_processed / f"props_predictions_{target_date}.json"
            with open(json_path, 'w') as f:
                json.dump(props_json, f, indent=2, default=str)
            results["files_created"].append(str(json_path))
            
        else:
            results["errors"].append("No prop predictions file found")
        
        # 3. Validate prop edges
        edges_file = paths.data_processed / f"props_edges_{target_date}.csv"
        if edges_file.exists():
            edges_df = pd.read_csv(edges_file)
            results["validation_results"]["edges"] = {
                "rows": len(edges_df),
                "columns": list(edges_df.columns),
                "file_used": str(edges_file)
            }
            
            # Create frontend-friendly JSON
            edges_json = _format_edges_for_frontend(edges_df, target_date)
            json_path = paths.data_processed / f"props_edges_{target_date}.json"
            with open(json_path, 'w') as f:
                json.dump(edges_json, f, indent=2, default=str)
            results["files_created"].append(str(json_path))
        else:
            results["errors"].append("No prop edges file found")
        
        # 4. Validate recommendations
        rec_files = [
            paths.data_processed / f"recommendations_{target_date}.csv",
            paths.data_processed / f"props_recommendations_{target_date}.csv"
        ]
        
        for rec_file in rec_files:
            if rec_file.exists():
                rec_df = pd.read_csv(rec_file)
                file_type = "game_recommendations" if "props" not in rec_file.name else "prop_recommendations"
                results["validation_results"][file_type] = {
                    "rows": len(rec_df),
                    "columns": list(rec_df.columns),
                    "file_used": str(rec_file)
                }
        
        # 5. Create master data file for frontend
        master_data = _create_master_data_file(target_date, results)
        master_path = paths.data_processed / f"master_data_{target_date}.json"
        with open(master_path, 'w') as f:
            json.dump(master_data, f, indent=2, default=str)
        results["files_created"].append(str(master_path))
        
        # 6. Update latest symlinks/copies for frontend
        _update_latest_files(target_date, results)
        
        return results
        
    except Exception as e:
        results["errors"].append(f"Frontend sync error: {str(e)}")
        return results


def _format_games_for_frontend(games_df: pd.DataFrame, target_date: str) -> Dict[str, Any]:
    """Format games data for frontend consumption"""
    
    games_data = {
        "date": target_date,
        "games": [],
        "last_updated": datetime.now().isoformat()
    }
    
    for _, row in games_df.iterrows():
        game = {
            "id": row.get("game_id", ""),
            "home_team": row.get("home_team", ""),
            "visitor_team": row.get("visitor_team", ""), 
            "date": target_date,
            "predictions": {
                "win_prob": float(row.get("win_prob", 0.5)),
                "spread": float(row.get("spread_margin", 0)),
                "total": float(row.get("totals", 200))
            }
        }
        
        # Add period predictions if available
        if "halves_h1_win" in row:
            game["predictions"]["periods"] = {
                "halves": {
                    "h1": {
                        "win": float(row.get("halves_h1_win", 0.5)),
                        "margin": float(row.get("halves_h1_margin", 0)),
                        "total": float(row.get("halves_h1_total", 100))
                    },
                    "h2": {
                        "win": float(row.get("halves_h2_win", 0.5)),
                        "margin": float(row.get("halves_h2_margin", 0)),
                        "total": float(row.get("halves_h2_total", 100))
                    }
                }
            }
        
        games_data["games"].append(game)
    
    return games_data


def _format_props_for_frontend(props_df: pd.DataFrame, target_date: str) -> Dict[str, Any]:
    """Format props data for frontend consumption"""
    
    props_data = {
        "date": target_date,
        "players": [],
        "last_updated": datetime.now().isoformat()
    }
    
    for _, row in props_df.iterrows():
        player = {
            "id": row.get("player_id", ""),
            "name": row.get("player_name", ""),
            "team": row.get("team", ""),
            "predictions": {}
        }
        
        # Add prop predictions.
        # Prefer mean_* (SmartSim-enhanced) when present; otherwise fall back to pred_*.
        for col in ["t_pts", "t_reb", "t_ast", "t_threes", "t_pra"]:
            if col in row and pd.notna(row[col]):
                stat_name = col.replace("t_", "")
                player["predictions"][stat_name] = float(row[col])

        for stat in ["pts", "reb", "ast", "threes", "pra"]:
            v = None
            mean_col = f"mean_{stat}"
            pred_col = f"pred_{stat}"
            if mean_col in row and pd.notna(row[mean_col]):
                v = row[mean_col]
            elif pred_col in row and pd.notna(row[pred_col]):
                v = row[pred_col]
            if v is not None:
                player["predictions"][stat] = float(v)
        
        props_data["players"].append(player)
    
    return props_data


def _format_edges_for_frontend(edges_df: pd.DataFrame, target_date: str) -> Dict[str, Any]:
    """Format edges data for frontend consumption"""
    
    edges_data = {
        "date": target_date,
        "edges": [],
        "last_updated": datetime.now().isoformat()
    }
    
    for _, row in edges_df.iterrows():
        edge = {
            "player": row.get("player_name", ""),
            "team": row.get("team", ""),
            "stat": row.get("stat", ""),
            "side": row.get("side", ""),
            "line": float(row.get("line", 0)) if pd.notna(row.get("line")) else 0,
            "price": float(row.get("price", 0)) if pd.notna(row.get("price")) else 0,
            "edge": float(row.get("edge", 0)) if pd.notna(row.get("edge")) else 0,
            "ev": float(row.get("ev", 0)) if pd.notna(row.get("ev")) else 0,
            "bookmaker": row.get("bookmaker", ""),
            "model_pred": float(row.get("model_pred", 0)) if pd.notna(row.get("model_pred")) else 0
        }
        
        edges_data["edges"].append(edge)
    
    return edges_data


def _create_master_data_file(target_date: str, validation_results: Dict) -> Dict[str, Any]:
    """Create a master data file with all information for the date"""
    
    master_data = {
        "date": target_date,
        "generated_at": datetime.now().isoformat(),
        "data_summary": {
            "games_available": "games" in validation_results["validation_results"],
            "props_available": "props" in validation_results["validation_results"],
            "edges_available": "edges" in validation_results["validation_results"],
            "total_games": validation_results["validation_results"].get("games", {}).get("rows", 0),
            "total_players": validation_results["validation_results"].get("props", {}).get("rows", 0),
            "total_edges": validation_results["validation_results"].get("edges", {}).get("rows", 0)
        },
        "files_available": {
            "games_predictions": f"games_predictions_{target_date}.json",
            "props_predictions": f"props_predictions_{target_date}.json", 
            "props_edges": f"props_edges_{target_date}.json"
        },
        "api_endpoints": {
            "games": f"/api/games/{target_date}",
            "props": f"/api/props/{target_date}",
            "edges": f"/api/edges/{target_date}",
            "recommendations": f"/recommendations?format=json&view=all&date={target_date}"
        },
        "validation_results": validation_results["validation_results"],
        "errors": validation_results["errors"]
    }
    
    return master_data


def _update_latest_files(target_date: str, results: Dict) -> None:
    """Update 'latest' files for frontend to always get current data"""
    
    # List of files to create 'latest' versions of
    file_mappings = [
        (f"games_predictions_{target_date}.json", "games_predictions_latest.json"),
        (f"props_predictions_{target_date}.json", "props_predictions_latest.json"),
        (f"props_edges_{target_date}.json", "props_edges_latest.json"),
        (f"master_data_{target_date}.json", "master_data_latest.json")
    ]
    
    for source_file, latest_file in file_mappings:
        source_path = paths.data_processed / source_file
        latest_path = paths.data_processed / latest_file
        
        if source_path.exists():
            # Copy content to latest file
            content = source_path.read_text()
            latest_path.write_text(content)
            results["files_created"].append(str(latest_path))


def get_frontend_data_status() -> Dict[str, Any]:
    """Get status of all frontend data files"""
    
    status = {
        "timestamp": datetime.now().isoformat(),
        "latest_files": {},
        "dated_files": {},
        "missing_files": []
    }
    
    # Check latest files
    latest_files = [
        "games_predictions_latest.json",
        "props_predictions_latest.json", 
        "props_edges_latest.json",
        "master_data_latest.json"
    ]
    
    for file_name in latest_files:
        file_path = paths.data_processed / file_name
        if file_path.exists():
            stat = file_path.stat()
            status["latest_files"][file_name] = {
                "exists": True,
                "size_bytes": stat.st_size,
                "last_modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
            }
        else:
            status["latest_files"][file_name] = {"exists": False}
            status["missing_files"].append(file_name)
    
    # Check recent dated files (last 7 days)
    from datetime import timedelta
    today = date.today()
    for days_back in range(7):
        check_date = today - timedelta(days=days_back)
        date_str = check_date.strftime("%Y-%m-%d")
        
        dated_files = [
            f"games_predictions_{date_str}.json",
            f"props_predictions_{date_str}.json",
            f"props_edges_{date_str}.json"
        ]
        
        status["dated_files"][date_str] = {}
        for file_name in dated_files:
            file_path = paths.data_processed / file_name
            status["dated_files"][date_str][file_name] = file_path.exists()
    
    return status


def cleanup_old_files(keep_days: int = 30) -> Dict[str, int]:
    """Clean up old prediction files to save space"""
    
    from datetime import timedelta
    import os
    
    cleanup_stats = {
        "files_removed": 0,
        "bytes_freed": 0
    }
    
    cutoff_date = date.today() - timedelta(days=keep_days)
    
    # Patterns for files to clean up
    patterns = [
        "games_predictions_*.json",
        "props_predictions_*.json", 
        "props_edges_*.json",
        "master_data_*.json",
        "predictions_*.csv",
        "props_predictions_*.csv",
        "props_edges_*.csv"
    ]
    
    for pattern in patterns:
        for file_path in paths.data_processed.glob(pattern):
            # Extract date from filename
            try:
                date_part = file_path.stem.split('_')[-1]
                if len(date_part) == 10 and date_part.count('-') == 2:  # YYYY-MM-DD format
                    file_date = datetime.strptime(date_part, "%Y-%m-%d").date()
                    if file_date < cutoff_date:
                        file_size = file_path.stat().st_size
                        file_path.unlink()
                        cleanup_stats["files_removed"] += 1
                        cleanup_stats["bytes_freed"] += file_size
            except (ValueError, IndexError):
                continue  # Skip files that don't match expected pattern
    
    return cleanup_stats