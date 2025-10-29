from __future__ import annotations

import argparse
import math
from datetime import date as _date
import numpy as np
import pandas as pd
from pathlib import Path

from nba_betting.config import paths
from nba_betting.pbp_markets import (
    _game_ids_for_date as _pbp_game_ids_for_date,
    _first_fg_event as _pbp_first_fg_event,
    _jump_ball_event as _pbp_jump_ball_event,
    _desc_cols as _pbp_desc_cols,
)
from nba_betting.cli import predict_tip_for_date, predict_first_basket_for_date, predict_early_threes_for_date
from nba_betting.pbp import fetch_pbp_for_date


def _cdn_map_for_date(ds: str) -> dict[str, tuple[str,str]]:
    from nba_betting.pbp_markets import _gid_team_map_for_date as _map
    try:
        m = _map(ds) or {}
        return {str(k): (v[0], v[1]) for k, v in m.items()}
    except Exception:
        return {}


def _brier(p, y):
    return float(np.mean([(pi-yi)**2 for pi, yi in zip(p, y)])) if p and y else float('nan')

def _logloss(p, y, eps=1e-9):
    if not p or not y:
        return float('nan')
    return float(np.mean([-(yi*math.log(max(eps, pi)) + (1-yi)*math.log(max(eps, 1-pi))) for pi, yi in zip(p, y)]))


def run_backtest(start_date: str, end_date: str | None, ensure_preds: bool, ensure_pbp: bool):
    s = pd.to_datetime(start_date).date()
    e = pd.to_datetime(end_date).date() if end_date else _date.today()
    dates = list(pd.date_range(s, e, freq='D').date)

    # Accumulators
    tip_probs = []; tip_outcomes = []
    fb_top1 = 0; fb_top5 = 0; fb_total = 0; fb_probs_actual = []
    thr_errs = []; thr_ge1_probs = []; thr_ge1_outcomes = []

    for d in dates:
        ds = str(d)
        # Optional PBP fetch (uses nba_api first, falls back to CDN automatically)
        if ensure_pbp:
            try:
                fetch_pbp_for_date(ds, only_final=True)
            except Exception:
                pass
        # Ensure predictions
        if ensure_preds:
            try:
                predict_tip_for_date(ds)
                predict_first_basket_for_date(ds)
                predict_early_threes_for_date(ds)
            except Exception:
                pass
        tip_path = paths.data_processed / f"tip_winner_probs_{ds}.csv"
        fb_path = paths.data_processed / f"first_basket_probs_{ds}.csv"
        thr_path = paths.data_processed / f"early_threes_{ds}.csv"
        def _safe_read(p: Path) -> pd.DataFrame:
            if not p.exists():
                return pd.DataFrame()
            try:
                df = pd.read_csv(p)
                return df if df is not None and not df.empty else pd.DataFrame()
            except Exception:
                return pd.DataFrame()
        tip = _safe_read(tip_path)
        fb = _safe_read(fb_path)
        thr = _safe_read(thr_path)
        if tip.empty and fb.empty and thr.empty:
            continue
        # PBP map
        pbp_comb = paths.data_processed / f"pbp_{ds}.csv"
        pbp_map: dict[str, pd.DataFrame] = {}
        if pbp_comb.exists():
            df = pd.read_csv(pbp_comb)
            if 'game_id' in df.columns:
                for gid, grp in df.groupby('game_id'):
                    pbp_map[str(gid)] = grp.copy()
        else:
            dpg = paths.data_processed / 'pbp'
            if dpg.exists():
                for f in dpg.glob('pbp_*.csv'):
                    try:
                        gid = f.stem.replace('pbp_', '')
                        gidd = str(int(gid)) if gid.isdigit() else gid
                        pbp_map[gidd] = pd.read_csv(f)
                    except Exception:
                        continue
        # Early threes
        if not thr.empty and pbp_map:
            actuals = {}
            for gid, gdf in pbp_map.items():
                cnt = 0
                desc_cols = _pbp_desc_cols(gdf)
                c_time = 'PCTIMESTRING' if 'PCTIMESTRING' in gdf.columns else ('clock' if 'clock' in gdf.columns else None)
                c_per = 'PERIOD' if 'PERIOD' in gdf.columns else ('period' if 'period' in gdf.columns else None)
                tmp = gdf.copy()
                if c_per: tmp = tmp[tmp[c_per] == 1]
                if c_time: tmp = tmp.sort_values(c_time, ascending=False)
                for _, r in tmp.iterrows():
                    t = r.get('PCTIMESTRING') or r.get('clock') or r.get('time')
                    sec_left = None
                    if isinstance(t, str) and ':' in t:
                        try:
                            m, s2 = t.split(':'); sec_left = int(m)*60+int(s2)
                        except Exception:
                            sec_left = None
                    if sec_left is None: continue
                    elapsed = 12*60 - sec_left
                    if elapsed is None or elapsed > 180:
                        continue
                    text = ' '.join([str(r.get(c, '')) for c in desc_cols]).lower()
                    if ('3pt' in text) and ('makes' in text or 'made' in text):
                        cnt += 1
                actuals[str(gid)] = int(cnt)
            for _, row in thr.iterrows():
                gid = str(row.get('game_id'))
                if not gid: continue
                yhat = float(row.get('expected_threes_0_3', row.get('threes_0_3_pred', 0.0)) or 0.0)
                a = actuals.get(gid)
                if a is None: continue
                thr_errs.append(float(a - yhat))
                p_ge1 = float(row.get('prob_ge_1', 1.0 - float(np.exp(-max(0.0, yhat)))))
                thr_ge1_probs.append(p_ge1)
                thr_ge1_outcomes.append(1.0 if a >= 1 else 0.0)
        # First basket
        if not fb.empty and pbp_map:
            actual_first: dict[str, dict] = {}
            for gid, gdf in pbp_map.items():
                ev = _pbp_first_fg_event(gdf)
                if ev:
                    actual_first[str(gid)] = ev
            for gid, grp in fb.groupby('game_id'):
                gid = str(gid)
                ev = actual_first.get(gid)
                if not ev: continue
                pid_first = ev.get('player_id')
                pname_first = (ev.get('player_name') or '').strip().lower()
                sub = grp.copy().sort_values('prob_first_basket', ascending=False)
                fb_total += 1
                top = sub.iloc[0]
                top_hit = False
                if pd.notna(pid_first) and pd.notna(top.get('player_id')):
                    try:
                        top_hit = int(top.get('player_id')) == int(pid_first)
                    except Exception:
                        top_hit = False
                if not top_hit and pname_first:
                    top_hit = pname_first in str(top.get('player_name','')).lower()
                if top_hit:
                    fb_top1 += 1
                hit5 = False; prob_actual = None
                for _, r in sub.head(5).iterrows():
                    if pd.notna(pid_first) and pd.notna(r.get('player_id')):
                        try:
                            if int(r.get('player_id')) == int(pid_first):
                                hit5 = True; prob_actual = float(r.get('prob_first_basket', np.nan)); break
                        except Exception:
                            pass
                    if (not hit5) and pname_first:
                        if pname_first in str(r.get('player_name','')).lower():
                            hit5 = True; prob_actual = float(r.get('prob_first_basket', np.nan)); break
                if hit5:
                    fb_top5 += 1
                if prob_actual is not None and not np.isnan(prob_actual):
                    fb_probs_actual.append(float(prob_actual))
        # Tip (best effort; may skip if jump ball parsing fails)
        if not tip.empty and pbp_map:
            gid2ha = _cdn_map_for_date(ds)
            # roster for name->team lookup
            from nba_betting.pbp_markets import _load_rosters_latest as _load_rost
            rost = _load_rost()
            tri_col = 'TEAM_ABBREVIATION' if 'TEAM_ABBREVIATION' in rost.columns else ('teamTricode' if 'teamTricode' in rost.columns else None)
            for _, r in tip.iterrows():
                gid = str(r.get('game_id'))
                if gid not in pbp_map: continue
                ev = _pbp_jump_ball_event(pbp_map[gid])
                if not ev: continue
                winner_text = (ev.get('winner_text') or '').strip().lower()
                if not winner_text: continue
                home, away = gid2ha.get(gid) or gid2ha.get(gid.zfill(10)) or (None, None)
                if not (home and away): continue
                outcome = None
                try:
                    if tri_col and not rost.empty:
                        subh = rost[rost[tri_col].astype(str).str.upper() == str(home).upper()].copy()
                        suba = rost[rost[tri_col].astype(str).str.upper() == str(away).upper()].copy()
                        names_h = (subh.get('PLAYER') or subh.get('PLAYER_NAME') or pd.Series(dtype=str)).astype(str).str.lower()
                        names_a = (suba.get('PLAYER') or suba.get('PLAYER_NAME') or pd.Series(dtype=str)).astype(str).str.lower()
                        if names_h.str.contains(winner_text).any(): outcome = 1.0
                        elif names_a.str.contains(winner_text).any(): outcome = 0.0
                except Exception:
                    outcome = None
                if outcome is not None:
                    tip_probs.append(float(r.get('prob_home_tip', 0.5)))
                    tip_outcomes.append(outcome)

    # Summaries
    tip_brier = _brier(tip_probs, tip_outcomes)
    tip_logloss = _logloss(tip_probs, tip_outcomes)
    tip_acc = float(np.mean([int((pi>=0.5)==(yi==1.0)) for pi, yi in zip(tip_probs, tip_outcomes)])) if tip_probs else float('nan')
    fb_top1_acc = (fb_top1 / fb_total) if fb_total else float('nan')
    fb_top5_cov = (fb_top5 / fb_total) if fb_total else float('nan')
    fb_mean_prob_actual = float(np.mean(fb_probs_actual)) if fb_probs_actual else float('nan')
    thr_mae = float(np.mean([abs(e) for e in thr_errs])) if thr_errs else float('nan')
    thr_rmse = float(np.sqrt(np.mean([e*e for e in thr_errs]))) if thr_errs else float('nan')
    thr_brier = _brier(thr_ge1_probs, thr_ge1_outcomes)

    out = {
        'range': f'{start_date}..{str(e)}',
        'tip': {'n': len(tip_outcomes), 'brier': tip_brier, 'logloss': tip_logloss, 'acc@0.5': tip_acc},
        'first_basket': {'n': fb_total, 'top1_acc': fb_top1_acc, 'top5_cov': fb_top5_cov, 'mean_prob_actual': fb_mean_prob_actual},
        'early_threes': {'n': len(thr_errs), 'mae': thr_mae, 'rmse': thr_rmse, 'brier_ge1': thr_brier},
    }
    print(out)


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--start', required=True)
    ap.add_argument('--end', required=False)
    ap.add_argument('--ensure-preds', action='store_true')
    ap.add_argument('--ensure-pbp', action='store_true')
    args = ap.parse_args()
    run_backtest(args.start, args.end, args.ensure_preds, args.ensure_pbp)
