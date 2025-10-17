"""
New CLI command additions for pure ONNX predictions (no sklearn)
Add these to cli.py
"""

# Add this import at the top of cli.py with other imports:
# from .props_onnx_pure import PureONNXPredictor
# from .props_features_pure import build_features_for_date_pure

# Add this command to cli.py:

@cli.command("predict-props-pure-onnx")
@click.option("--date", "date_str", type=str, required=True, help="Prediction date YYYY-MM-DD")
@click.option("--out", "out_path", type=click.Path(dir_okay=False), required=False, 
              help="Output CSV path (default props_predictions_YYYY-MM-DD.csv)")
@click.option("--slate-only", is_flag=True, default=False, 
              help="Filter to only players in today's slate")
def predict_props_pure_onnx_cmd(date_str: str, out_path: str | None, slate_only: bool):
    """
    Predict player props using PURE ONNX models (NO sklearn dependencies).
    
    This command uses:
    - Pure numpy/pandas feature engineering (no sklearn)
    - Pure ONNX runtime inference (no sklearn)
    - Qualcomm NPU acceleration when available
    
    Perfect for ARM64 Windows where sklearn won't compile.
    """
    console.rule(f"🚀 Predict Props (Pure ONNX - No sklearn) - {date_str}")
    
    try:
        # Import pure modules (no sklearn dependencies)
        from .props_features_pure import build_features_for_date_pure
        from .props_onnx_pure import PureONNXPredictor
        
        # Build features without sklearn
        console.print("📊 Building features (pure numpy/pandas)...")
        feats = build_features_for_date_pure(date_str)
        
        if feats.empty:
            console.print(f"⚠️  No games found for {date_str}", style="yellow")
            return
        
        console.print(f"✅ Built features for {len(feats)} players")
        
        # Optional: Filter to today's slate
        if slate_only:
            console.print("🎯 Filtering to today's slate...")
            try:
                from nba_api.stats.endpoints import scoreboardv2
                sb = scoreboardv2.ScoreboardV2(game_date=date_str, day_offset=0, timeout=30)
                nd = sb.get_normalized_dict()
                gh = pd.DataFrame(nd.get("GameHeader", []))
                ls = pd.DataFrame(nd.get("LineScore", []))
                
                if not gh.empty and not ls.empty:
                    ls_cols = {c.upper(): c for c in ls.columns}
                    if {"TEAM_ID","TEAM_ABBREVIATION"}.issubset(ls_cols.keys()):
                        team_map = {}
                        for _, r in ls.iterrows():
                            try:
                                team_map[int(r[ls_cols["TEAM_ID"]])] = str(r[ls_cols["TEAM_ABBREVIATION"]]).upper()
                            except Exception:
                                continue
                        
                        gh_cols = {c.upper(): c for c in gh.columns}
                        if {"HOME_TEAM_ID","VISITOR_TEAM_ID"}.issubset(gh_cols.keys()):
                            games = []
                            for _, g in gh.iterrows():
                                try:
                                    hid = int(g[gh_cols["HOME_TEAM_ID"]])
                                    vid = int(g[gh_cols["VISITOR_TEAM_ID"]])
                                    h = team_map.get(hid)
                                    v = team_map.get(vid)
                                    if h and v:
                                        games.append({"team": h, "opponent": v, "home": True})
                                        games.append({"team": v, "opponent": h, "home": False})
                                except Exception:
                                    continue
                            
                            slate = pd.DataFrame(games)
                            if not slate.empty and "team" in feats.columns:
                                feats["team"] = feats["team"].astype(str).str.upper()
                                before_count = len(feats)
                                feats = feats.merge(slate, on="team", how="inner")
                                console.print(f"   Filtered {before_count} → {len(feats)} players")
            except Exception as e:
                console.print(f"⚠️  Slate filtering failed: {e}", style="yellow")
        
        # Run pure ONNX inference
        console.print("🚀 Running ONNX inference (NPU accelerated)...")
        predictor = PureONNXPredictor()
        preds = predictor.predict(feats)
        
        # Save results
        if not out_path:
            out_path = str(paths.data_processed / f"props_predictions_{date_str}.csv")
        
        preds.to_csv(out_path, index=False)
        
        # Summary
        pred_cols = [c for c in preds.columns if c.startswith('pred_')]
        console.print(f"\n✅ Saved predictions to: {out_path}")
        console.print(f"   Players: {len(preds)}")
        console.print(f"   Props: {pred_cols}")
        console.print(f"   Pure ONNX: ✅ (No sklearn)")
        console.print(f"   NPU Accelerated: {'✅' if predictor.has_qnn else '❌'}")
        
    except ImportError as e:
        console.print(f"❌ Import error: {e}", style="red")
        console.print("Ensure onnxruntime-qnn is installed: pip install onnxruntime-qnn", style="yellow")
    except FileNotFoundError as e:
        console.print(f"❌ File not found: {e}", style="red")
        console.print("Run 'fetch-player-logs' first to populate player_logs", style="yellow")
    except Exception as e:
        console.print(f"❌ Prediction failed: {e}", style="red")
        import traceback
        traceback.print_exc()
