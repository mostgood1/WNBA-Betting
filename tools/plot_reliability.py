import argparse
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / 'data' / 'processed'

HTML_TMPL = """<!DOCTYPE html><html><head><meta charset='utf-8'><title>Reliability Curve</title>
<style>body{font-family:Arial;margin:24px;background:#111;color:#eee} table{border-collapse:collapse;margin-top:16px} td,th{border:1px solid #444;padding:4px 8px;font-size:12px} .wrap{max-width:900px;margin:auto} .chart{width:100%;height:500px} a{color:#6bf}</style>
</head><body><div class='wrap'><h1>Games Reliability Curve</h1><p>Date range: __START__ → __END__</p>
<canvas id='c' class='chart'></canvas>
<table><thead><tr><th>Bin</th><th>p_mean</th><th>y_rate</th><th>count</th><th>brier_mean</th></tr></thead><tbody>__ROWS__</tbody></table>
<script>
const data = __DATA__;
const ctx = document.getElementById('c').getContext('2d');
function draw(){
    const W = ctx.canvas.width = ctx.canvas.clientWidth; const H = ctx.canvas.height = ctx.canvas.clientHeight;
    ctx.clearRect(0,0,W,H);
    const margin = 40; const innerW = W - margin*2; const innerH = H - margin*2;
    ctx.strokeStyle='#888'; ctx.lineWidth=1; ctx.strokeRect(margin,margin,innerW,innerH);
    ctx.fillStyle='#eee'; ctx.font='12px Arial'; ctx.fillText('Predicted Probability', W/2-60, H-10); ctx.save(); ctx.translate(10,H/2+40); ctx.rotate(-Math.PI/2); ctx.fillText('Empirical Win Rate',0,0); ctx.restore();
    const ps = data.map(d=>d.p_mean); const ys = data.map(d=>d.y_rate);
    const minP = Math.min(...ps,0), maxP = Math.max(...ps,1), minY=0, maxY=1;
    function x(v){return margin + (v-minP)/(maxP-minP)*innerW;} function y(v){return margin + (1-(v-minY)/(maxY-minY))*innerH;}
    ctx.beginPath(); ctx.strokeStyle='#444'; ctx.setLineDash([5,5]); ctx.moveTo(x(0), y(0)); ctx.lineTo(x(1), y(1)); ctx.stroke(); ctx.setLineDash([]);
    ctx.beginPath(); ctx.strokeStyle='#6bf'; ctx.lineWidth=2; data.forEach((d,i)=>{ if(i===0) ctx.moveTo(x(d.p_mean), y(d.y_rate)); else ctx.lineTo(x(d.p_mean), y(d.y_rate)); }); ctx.stroke();
    data.forEach(d=>{ ctx.beginPath(); ctx.arc(x(d.p_mean), y(d.y_rate), Math.max(3, Math.min(12, Math.sqrt(d.count))), 0, 2*Math.PI); ctx.fillStyle='#f90'; ctx.fill(); });
}
window.addEventListener('resize', draw); draw();
</script></div></body></html>"""

def main():
    ap = argparse.ArgumentParser(description='Render reliability curve HTML from reliability_games.csv')
    ap.add_argument('--out', type=str, default='reliability_games.html')
    args = ap.parse_args()
    path = PROCESSED / 'reliability_games.csv'
    if not path.exists():
        print('Missing reliability_games.csv'); return 1
    df = pd.read_csv(path)
    if df.empty:
        print('Empty reliability file'); return 1
    start = df.get('start').dropna().iloc[0] if 'start' in df.columns else ''
    end = df.get('end').dropna().iloc[0] if 'end' in df.columns else ''
    rows = '\n'.join(f"<tr><td>{int(r['bin'])}</td><td>{r['p_mean']:.3f}</td><td>{r['y_rate']:.3f}</td><td>{int(r['count'])}</td><td>{r['brier_mean']:.4f}</td></tr>" for _, r in df.iterrows())
    data_json = df[['bin','p_mean','y_rate','count','brier_mean']].to_json(orient='records')
    html = HTML_TMPL.replace('__ROWS__', rows).replace('__DATA__', data_json).replace('__START__', start).replace('__END__', end)
    out = PROCESSED / args.out
    out.write_text(html, encoding='utf-8')
    print(f'WROTE:{out}')
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
