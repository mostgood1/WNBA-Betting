import pandas as pd

df = pd.read_csv('data/processed/predictions_2025-10-17.csv')
print('First game:')
print(f"Home: {df.iloc[0]['home_team']} | Away: {df.iloc[0]['visitor_team']}")
print(f"Pred Margin: {df.iloc[0]['pred_margin']:.2f}")
print(f"Pred Total: {df.iloc[0]['pred_total']:.2f}")
print(f"\nQuarters:")
print(f"Q1 Total: {df.iloc[0]['quarters_q1_total']:.1f}")
print(f"Q2 Total: {df.iloc[0]['quarters_q2_total']:.1f}")
print(f"Q3 Total: {df.iloc[0]['quarters_q3_total']:.1f}")
print(f"Q4 Total: {df.iloc[0]['quarters_q4_total']:.1f}")
print(f"Sum of Quarters: {df.iloc[0]['quarters_q1_total'] + df.iloc[0]['quarters_q2_total'] + df.iloc[0]['quarters_q3_total'] + df.iloc[0]['quarters_q4_total']:.1f}")

# Calculate team scores per quarter
pred_margin = df.iloc[0]['pred_margin']
pred_total = df.iloc[0]['pred_total']
home_score = (pred_total + pred_margin) / 2
away_score = (pred_total - pred_margin) / 2

print(f"\nPredicted scores:")
print(f"Home: {home_score:.2f}")
print(f"Away: {away_score:.2f}")
