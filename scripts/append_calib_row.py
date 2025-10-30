import pandas as pd
from pathlib import Path
import sys
if __name__ == '__main__':
    if len(sys.argv) < 7:
        print('Usage: append_calib_row.py DATE thr_bias tip_logit_bias fb_temp fb_tip_alpha window_days', file=sys.stderr)
        sys.exit(2)
    date, thr_bias, tip_logit_bias, fb_temp, fb_tip_alpha, window_days = sys.argv[1:7]
    p = Path(__file__).resolve().parent.parent / 'data' / 'processed' / 'pbp_calibration.csv'
    df = pd.read_csv(p) if p.exists() else pd.DataFrame(columns=['date','window_days','thr_bias','tip_logit_bias','fb_temp','fb_tip_alpha'])
    row = {
        'date': date,
        'window_days': int(window_days),
        'thr_bias': float(thr_bias) if thr_bias.lower()!='nan' else float('nan'),
        'tip_logit_bias': float(tip_logit_bias) if tip_logit_bias.lower()!='nan' else float('nan'),
        'fb_temp': float(fb_temp) if fb_temp.lower()!='nan' else float('nan'),
        'fb_tip_alpha': float(fb_tip_alpha) if fb_tip_alpha.lower()!='nan' else float('nan'),
    }
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    df.to_csv(p, index=False)
    print('appended row:', row)
