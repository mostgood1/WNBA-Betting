from __future__ import annotations

"""Render drift report CSV to a simple HTML dashboard.

Reads data/processed/drift_games_<date>.csv and writes
data/processed/drift_games_<date>.html with sortable table and quick flags.
"""

from pathlib import Path
import os
import argparse
from datetime import datetime
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
_DATA_ROOT = os.environ.get("NBA_BETTING_DATA_ROOT")
DATA_ROOT = Path(_DATA_ROOT).expanduser() if _DATA_ROOT else (BASE_DIR / "data")
PROCESSED = DATA_ROOT / "processed"

TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Drift Report {date}</title>
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Arial; margin: 20px; }
    table { border-collapse: collapse; width: 100%; }
    th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
    th { background: #f4f4f4; cursor: pointer; }
    tr:hover { background: #fafafa; }
    .flag { font-weight: 600; }
    .sev { color: #b00020; }
    .mod { color: #d17a00; }
  </style>
  <script>
    function sortTable(n){
      const table=document.getElementById("driftTable");
      let switching=true, dir="desc";
      while(switching){
        switching=false; const rows=table.rows;
        for(let i=1;i<rows.length-1;i++){
          let x=rows[i].getElementsByTagName("TD")[n];
          let y=rows[i+1].getElementsByTagName("TD")[n];
          let xx=parseFloat(x.innerText); let yy=parseFloat(y.innerText);
          if(isNaN(xx)||isNaN(yy)){ xx=x.innerText.toLowerCase(); yy=y.innerText.toLowerCase(); }
          let shouldSwitch=(dir==="asc"? xx>yy : xx<yy);
          if(shouldSwitch){ rows[i].parentNode.insertBefore(rows[i+1], rows[i]); switching=true; break; }
        }
        if(!switching && dir==="desc"){ dir="asc"; switching=true; }
      }
    }
  </script>
</head>
<body>
  <h1>Drift Report {date}</h1>
  <p>Flags: <span class="flag sev">Severe PSI &gt; 0.3</span>, <span class="flag mod">Moderate PSI &gt; 0.2</span></p>
  <table id="driftTable">
    <thead>
      <tr>
        <th onclick="sortTable(0)">feature</th>
        <th onclick="sortTable(1)">ref_count</th>
        <th onclick="sortTable(2)">cur_count</th>
        <th onclick="sortTable(3)">ref_mean</th>
        <th onclick="sortTable(4)">cur_mean</th>
        <th onclick="sortTable(5)">ref_std</th>
        <th onclick="sortTable(6)">cur_std</th>
        <th onclick="sortTable(7)">psi</th>
        <th onclick="sortTable(8)">ks</th>
        <th>flags</th>
      </tr>
    </thead>
    <tbody>
      {rows}
    </tbody>
  </table>
</body>
</html>
"""

def main():
    ap = argparse.ArgumentParser(description="Render drift CSV to HTML")
    ap.add_argument("--date", required=True, help="Date YYYY-MM-DD")
    args = ap.parse_args()
    d = datetime.strptime(args.date, "%Y-%m-%d")
    p = PROCESSED / f"drift_games_{d:%Y-%m-%d}.csv"
    if not p.exists():
        print(f"Drift CSV not found: {p}")
        return
    df = pd.read_csv(p)
    def flag_row(r):
        flags = []
        if r.get("psi_severe", False): flags.append("<span class='flag sev'>PSI&gt;0.3</span>")
        elif r.get("psi_flag", False): flags.append("<span class='flag mod'>PSI&gt;0.2</span>")
        if r.get("ks_flag", False): flags.append("KS flag")
        return ", ".join(flags)
    rows = []
    for _, r in df.iterrows():
        rows.append(
            f"<tr><td>{r['feature']}</td><td>{r['ref_count']}</td><td>{r['cur_count']}</td>"
            f"<td>{r['ref_mean']:.3f}</td><td>{r['cur_mean']:.3f}</td><td>{r['ref_std']:.3f}</td><td>{r['cur_std']:.3f}</td>"
            f"<td>{r['psi']:.3f}</td><td>{r['ks']:.3f}</td><td>{flag_row(r)}</td></tr>"
        )
    html = TEMPLATE.replace("{date}", f"{d:%Y-%m-%d}").replace("{rows}", "\n".join(rows))
    out = PROCESSED / f"drift_games_{d:%Y-%m-%d}.html"
    out.write_text(html, encoding="utf-8")
    print(f"Drift HTML written: {out}")

if __name__ == "__main__":
    main()
