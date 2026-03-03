"""
Performance tracking dashboard for NBA betting models.
Tracks accuracy, ROI, calibration, and profit/loss over time.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from pathlib import Path

from nba_betting.config import paths


def _resolve_against_data_root(p: str) -> Path:
    path = Path(p)
    if path.is_absolute():
        return path
    parts = path.parts
    if len(parts) >= 1 and parts[0].lower() == "data":
        return paths.data_root / Path(*parts[1:])
    return path


class PerformanceTracker:
    """Tracks model performance against actual results."""
    
    def __init__(self, predictions_dir: str = "data/processed", 
                 results_dir: str = "data/raw"):
        self.predictions_dir = _resolve_against_data_root(predictions_dir)
        self.results_dir = _resolve_against_data_root(results_dir)
    
    def load_predictions_and_results(self, start_date: str = None, 
                                     end_date: str = None) -> pd.DataFrame:
        """
        Load predictions and match with actual results.
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
        
        Returns:
            DataFrame with predictions and actual results
        """
        # Load all prediction files
        pred_files = sorted(self.predictions_dir.glob("predictions_*.csv"))
        
        if not pred_files:
            print("No prediction files found")
            return pd.DataFrame()
        
        all_preds = []
        for file in pred_files:
            df = pd.read_csv(file)
            # Extract date from filename
            date_str = file.stem.replace('predictions_', '')
            df['prediction_date'] = date_str
            all_preds.append(df)
        
        predictions = pd.concat(all_preds, ignore_index=True)
        
        # Load actual results
        results_file = self.results_dir / "games_with_odds.csv"
        if not results_file.exists():
            print("No results file found")
            return predictions
        
        results = pd.read_csv(results_file)
        
        # Merge predictions with results
        # Match on date, home_team, visitor_team
        merged = predictions.merge(
            results,
            left_on=['prediction_date', 'home', 'visitor'],
            right_on=['date_est', 'home_team', 'visitor_team'],
            how='left',
            suffixes=('_pred', '_actual')
        )
        
        return merged
    
    def calculate_accuracy(self, df: pd.DataFrame) -> Dict[str, float]:
        """
        Calculate prediction accuracy metrics.
        
        Args:
            df: DataFrame with predictions and actuals
        
        Returns:
            Dict with accuracy metrics
        """
        if df.empty or 'home_score' not in df.columns:
            return {}
        
        # Filter to games with actual results
        completed = df[df['home_score'].notna()].copy()
        
        if len(completed) == 0:
            return {'total_games': 0}
        
        # Calculate actual margins and totals
        completed['actual_margin'] = completed['home_score'] - completed['visitor_score']
        completed['actual_total'] = completed['home_score'] + completed['visitor_score']
        
        # Win prediction accuracy
        completed['pred_winner'] = completed['win_prob'] > 0.5
        completed['actual_winner'] = completed['actual_margin'] > 0
        win_accuracy = (completed['pred_winner'] == completed['actual_winner']).mean()
        
        # Spread RMSE
        if 'spread_margin' in completed.columns:
            spread_rmse = np.sqrt(((completed['spread_margin'] - completed['actual_margin']) ** 2).mean())
        else:
            spread_rmse = np.nan
        
        # Total RMSE
        if 'totals' in completed.columns:
            total_rmse = np.sqrt(((completed['totals'] - completed['actual_total']) ** 2).mean())
        else:
            total_rmse = np.nan
        
        # Confidence-stratified accuracy
        high_conf = completed[completed['win_prob'].apply(lambda x: x > 0.6 or x < 0.4)]
        high_conf_acc = (high_conf['pred_winner'] == high_conf['actual_winner']).mean() if len(high_conf) > 0 else np.nan
        
        return {
            'total_games': len(completed),
            'win_accuracy': float(win_accuracy),
            'spread_rmse': float(spread_rmse),
            'total_rmse': float(total_rmse),
            'high_confidence_accuracy': float(high_conf_acc),
            'high_confidence_games': len(high_conf),
        }
    
    def calculate_roi(self, df: pd.DataFrame, 
                      bet_type: str = 'moneyline',
                      confidence_threshold: float = 0.55) -> Dict[str, float]:
        """
        Calculate Return on Investment (ROI) for betting strategy.
        
        Args:
            df: DataFrame with predictions and actuals
            bet_type: 'moneyline', 'spread', or 'total'
            confidence_threshold: Minimum confidence to place bet
        
        Returns:
            Dict with ROI metrics
        """
        completed = df[df['home_score'].notna()].copy()
        
        if len(completed) == 0:
            return {'total_bets': 0, 'roi': 0.0}
        
        # Calculate actual results
        completed['actual_margin'] = completed['home_score'] - completed['visitor_score']
        completed['actual_total'] = completed['home_score'] + completed['visitor_score']
        
        total_wagered = 0
        total_profit = 0
        bets_placed = 0
        wins = 0
        
        for _, row in completed.iterrows():
            # Determine if we should bet
            if bet_type == 'moneyline':
                if row.get('win_prob', 0.5) > confidence_threshold:
                    # Bet on home team
                    odds = row.get('home_ml', -110)
                    won = row['actual_margin'] > 0
                elif row.get('win_prob', 0.5) < (1 - confidence_threshold):
                    # Bet on away team
                    odds = row.get('visitor_ml', -110)
                    won = row['actual_margin'] < 0
                else:
                    continue  # No bet
                
                # Calculate payout
                if won:
                    if odds > 0:
                        profit = odds / 100
                    else:
                        profit = 100 / abs(odds)
                    total_profit += profit
                    wins += 1
                else:
                    total_profit -= 1
                
                total_wagered += 1
                bets_placed += 1
        
        roi = (total_profit / total_wagered * 100) if total_wagered > 0 else 0.0
        win_rate = (wins / bets_placed * 100) if bets_placed > 0 else 0.0
        
        return {
            'total_bets': bets_placed,
            'wins': wins,
            'losses': bets_placed - wins,
            'win_rate': float(win_rate),
            'total_wagered': float(total_wagered),
            'total_profit': float(total_profit),
            'roi': float(roi),
        }
    
    def calculate_calibration(self, df: pd.DataFrame, n_bins: int = 10) -> pd.DataFrame:
        """
        Calculate prediction calibration (predicted vs actual win rates).
        
        Args:
            df: DataFrame with predictions and actuals
            n_bins: Number of probability bins
        
        Returns:
            DataFrame with calibration data
        """
        completed = df[df['home_score'].notna()].copy()
        
        if len(completed) == 0:
            return pd.DataFrame()
        
        completed['actual_margin'] = completed['home_score'] - completed['visitor_score']
        completed['actual_win'] = (completed['actual_margin'] > 0).astype(float)
        
        # Bin predictions
        bins = np.linspace(0, 1, n_bins + 1)
        completed['prob_bin'] = pd.cut(completed['win_prob'], bins=bins, 
                                       labels=False, include_lowest=True)
        
        # Calculate actual win rate per bin
        calibration = completed.groupby('prob_bin').agg({
            'win_prob': 'mean',
            'actual_win': 'mean',
            'home': 'count',
        }).rename(columns={'home': 'count'})
        
        calibration['bin_center'] = bins[:-1] + (bins[1] - bins[0]) / 2
        
        return calibration
    
    def generate_performance_report(self, days_back: int = 30) -> Dict:
        """
        Generate comprehensive performance report.
        
        Args:
            days_back: Number of days to include in report
        
        Returns:
            Dict with all performance metrics
        """
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        df = self.load_predictions_and_results(start_date, end_date)
        
        if df.empty:
            return {'error': 'No data available'}
        
        accuracy = self.calculate_accuracy(df)
        roi = self.calculate_roi(df, bet_type='moneyline', confidence_threshold=0.55)
        calibration = self.calculate_calibration(df)
        
        return {
            'date_range': {'start': start_date, 'end': end_date},
            'accuracy_metrics': accuracy,
            'roi_metrics': roi,
            'calibration': calibration.to_dict() if not calibration.empty else {},
        }
    
    def print_performance_summary(self, report: Dict):
        """Pretty print performance report."""
        
        print("\n" + "="*60)
        print("NBA BETTING MODEL - PERFORMANCE REPORT")
        print("="*60)
        
        if 'date_range' in report:
            print(f"\nDate Range: {report['date_range']['start']} to {report['date_range']['end']}")
        
        if 'accuracy_metrics' in report:
            acc = report['accuracy_metrics']
            print(f"\n📊 ACCURACY METRICS:")
            print(f"   Total Games: {acc.get('total_games', 0)}")
            print(f"   Win Accuracy: {acc.get('win_accuracy', 0):.1%}")
            print(f"   Spread RMSE: {acc.get('spread_rmse', 0):.2f} points")
            print(f"   Total RMSE: {acc.get('total_rmse', 0):.2f} points")
            print(f"   High Confidence Accuracy: {acc.get('high_confidence_accuracy', 0):.1%}")
            print(f"   High Confidence Games: {acc.get('high_confidence_games', 0)}")
        
        if 'roi_metrics' in report:
            roi = report['roi_metrics']
            print(f"\n💰 ROI METRICS:")
            print(f"   Total Bets: {roi.get('total_bets', 0)}")
            print(f"   Win Rate: {roi.get('win_rate', 0):.1f}%")
            print(f"   Total Profit: ${roi.get('total_profit', 0):.2f}")
            print(f"   ROI: {roi.get('roi', 0):.1f}%")
        
        print("\n" + "="*60 + "\n")


# Example usage
if __name__ == "__main__":
    tracker = PerformanceTracker()
    
    print("Generating performance report for last 30 days...")
    report = tracker.generate_performance_report(days_back=30)
    tracker.print_performance_summary(report)
    
    print("\nCalculating seasonal performance...")
    report_season = tracker.generate_performance_report(days_back=365)
    tracker.print_performance_summary(report_season)
