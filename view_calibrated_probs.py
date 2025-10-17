import pandas as pd

df = pd.read_csv('data/processed/predictions_2025-10-17.csv')

print('=' * 80)
print('NN-CALIBRATED WIN PROBABILITIES')
print('(80% Spread→Sigmoid + 20% Direct Model)')
print('=' * 80)

cols = ['home_team', 'visitor_team']
if 'home_win_prob_raw' in df.columns:
    cols.extend(['home_win_prob_raw', 'home_win_prob_from_spread', 'home_win_prob', 'pred_margin'])
    print(df[cols].to_string(index=False))
else:
    cols.extend(['home_win_prob', 'pred_margin'])
    print(df[cols].to_string(index=False))
    print("\n⚠️  Calibration columns not found - may need to update code")

print('\n' + '=' * 80)
print('Win Probability Statistics:')
print(df['home_win_prob'].describe())

extreme = ((df['home_win_prob'] < 0.10) | (df['home_win_prob'] > 0.90)).sum()
print(f'\nExtreme probabilities (<10% or >90%): {extreme} / {len(df)} games ({extreme/len(df)*100:.1f}%)')
