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
    def _to_float(s):
        try:
            if s is None:
                return float('nan')
            t = str(s).strip()
            if t.lower() == 'nan' or t == '':
                return float('nan')
            return float(t)
        except Exception:
            return float('nan')
    # Parse values
    _thr = _to_float(thr_bias)
    _tip = _to_float(tip_logit_bias)
    _tmp = _to_float(fb_temp)
    _alp = _to_float(fb_tip_alpha)
    try:
        _win = int(window_days)
    except Exception:
        _win = 7
    # Clamp to safe ranges to avoid runaway effects downstream
    # early-threes bias in expected count units
    if pd.notna(_thr):
        _thr = max(-1.0, min(1.0, _thr))
    # tip probability logit intercept bias
    if pd.notna(_tip):
        _tip = max(-1.5, min(1.5, _tip))
    # first-basket temperature
    if pd.notna(_tmp):
        _tmp = max(0.5, min(2.0, _tmp))
    # tip influence on first-basket candidates
    if pd.notna(_alp):
        _alp = max(0.0, min(1.0, _alp))
    # window days
    _win = max(1, min(30, _win))
    row = {
        'date': date,
        'window_days': _win,
        'thr_bias': _thr,
        'tip_logit_bias': _tip,
        'fb_temp': _tmp,
        'fb_tip_alpha': _alp,
    }
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    df.to_csv(p, index=False)
    print('appended row:', row)
