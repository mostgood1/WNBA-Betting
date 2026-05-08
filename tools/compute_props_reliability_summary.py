import pandas as pd
import numpy as np
from pathlib import Path
import json

root = Path('c:/Users/mostg/OneDrive/Coding/WNBA-Betting')
proc = root / 'data' / 'processed'
csv = proc / 'reliability_props.csv'

if not csv.exists():
    print({'status':'missing','path':str(csv)})
    raise SystemExit(0)

df = pd.read_csv(csv)
# Basic sanity
required = ['n','hit_rate','avg_model_prob','roi']
missing = [c for c in required if c not in df.columns]
if missing:
    print({'status':'schema-missing', 'missing': missing})
    raise SystemExit(0)

# Compute calibration gaps per bin
out = df.copy()
out['calibration_gap'] = out['hit_rate'] - out['avg_model_prob']

# Overall metrics
try:
    total_bets = int(out['n'].sum())
    weighted_hit = float(np.average(out['hit_rate'], weights=out['n']))
    weighted_model_prob = float(np.average(out['avg_model_prob'], weights=out['n']))
    weighted_roi = float(np.average(out['roi'], weights=out['n']))
    rmse_calibration = float(np.sqrt(np.average((out['calibration_gap']**2), weights=out['n'])))
except Exception:
    # Fallback without weights if any issue
    total_bets = int(out['n'].sum())
    weighted_hit = float(out['hit_rate'].mean())
    weighted_model_prob = float(out['avg_model_prob'].mean())
    weighted_roi = float(out['roi'].mean())
    rmse_calibration = float(np.sqrt(((out['calibration_gap']**2)).mean()))

# Top bins
cols_subset = ['bin_low','bin_high','n','roi','hit_rate','avg_model_prob','calibration_gap']
cols_subset = [c for c in cols_subset if c in out.columns]

top_roi_bins = out.sort_values('roi', ascending=False).head(3)[cols_subset]
largest_gap_bins = out.reindex(out['calibration_gap'].abs().sort_values(ascending=False).index).head(3)[cols_subset]

summary = {
    'total_bets': total_bets,
    'weighted': {
        'hit_rate': weighted_hit,
        'model_prob': weighted_model_prob,
        'roi': weighted_roi,
        'rmse_calibration': rmse_calibration,
    },
    'top_roi_bins': top_roi_bins.to_dict(orient='records'),
    'largest_calibration_gap_bins': largest_gap_bins.to_dict(orient='records'),
}

# Write JSON and Markdown
json_path = proc / 'reliability_props_summary.json'
with open(json_path, 'w') as f:
    json.dump(summary, f, indent=2)

md_lines = []
md_lines.append('# Props Reliability Summary\n')
md_lines.append(f"Total bets: {total_bets}\n")
md_lines.append(f"Weighted hit rate: {weighted_hit:.3f}\n")
md_lines.append(f"Weighted model prob: {weighted_model_prob:.3f}\n")
md_lines.append(f"Weighted ROI: {weighted_roi:.3f}\n")
md_lines.append(f"RMSE calibration: {rmse_calibration:.3f}\n")
md_lines.append('\n## Top ROI Bins\n')
for r in summary['top_roi_bins']:
    try:
        md_lines.append(f"- Bin [{r.get('bin_low', float('nan')):.3f}, {r.get('bin_high', float('nan')):.3f}]: n={int(r.get('n', 0))}, roi={float(r.get('roi', float('nan'))):.3f}, hit={float(r.get('hit_rate', float('nan'))):.3f}, model={float(r.get('avg_model_prob', float('nan'))):.3f}, gap={float(r.get('calibration_gap', float('nan'))):.3f}\n")
    except Exception:
        md_lines.append("- Bin: (formatting error)\n")
md_lines.append('\n## Largest Calibration Gaps\n')
for r in summary['largest_calibration_gap_bins']:
    try:
        md_lines.append(f"- Bin [{r.get('bin_low', float('nan')):.3f}, {r.get('bin_high', float('nan')):.3f}]: n={int(r.get('n', 0))}, roi={float(r.get('roi', float('nan'))):.3f}, hit={float(r.get('hit_rate', float('nan'))):.3f}, model={float(r.get('avg_model_prob', float('nan'))):.3f}, gap={float(r.get('calibration_gap', float('nan'))):.3f}\n")
    except Exception:
        md_lines.append("- Bin: (formatting error)\n")

md_path = proc / 'reliability_props_summary.md'
with open(md_path, 'w', encoding='utf-8') as f:
    f.writelines(md_lines)

print({'json': str(json_path), 'md': str(md_path), 'bins': int(len(out))})
