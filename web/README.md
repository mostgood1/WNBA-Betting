# WNBA Frontend (Local Preview)

This is a lightweight static UI that renders the processed WNBA schedule by date and shows model recommendations if a predictions CSV is present.

## Data sources
- Schedule JSON: `../data/processed/schedule_YYYY.json`
- Predictions CSV (optional): `../predictions_YYYY-MM-DD.csv` (generated via `python -m nba_betting.cli predict-date --date YYYY-MM-DD [--merge-odds odds.csv]`)

## Run locally
Run the Flask app which serves both the UI and APIs:

```powershell
python app.py  # http://127.0.0.1:5050
```

The date picker defaults to today if available in the schedule; otherwise the first schedule date. If a `predictions_YYYY-MM-DD.csv` exists for the selected date, recommendation badges are shown.

## Team assets (logos)
The frontend uses official WNBA logo assets via a same-origin backend proxy. The backend fetches the real files from the official WNBA CDN using verified team IDs:

```
https://cdn.wnba.com/logos/wnba/<TEAM_ID>/primary/L/logo.svg
```

- The browser requests `/api/team-logo/<TRICODE>.svg`, and the Flask app proxies the official CDN asset.
- If a team ID is unavailable or the remote logo fails to load, the UI falls back to a colored badge with the team’s tricode.
- If you need to self-host approved logo files later, prefer wiring them behind the backend logo helper rather than hardcoding file paths in the frontend.

## Predictions CSV format (minimal)
Columns expected (case-sensitive) for basic badges:
- `date` (YYYY-MM-DD)
- `home_team` (Full name, e.g., "Los Angeles Lakers")
- `visitor_team` (Full name)
- `home_win_prob` (0–1)
- `pred_margin` (home margin, points)
- `pred_total` (total points)

Optional edge columns (if merging odds):
- `edge_spread` (positive favors HOME ATS, negative favors AWAY ATS)
- `edge_total` (positive favors OVER, negative favors UNDER)

## Accessibility
- Inputs have accessible labels; team logos include alt text; team names are rendered visibly next to logos.

## Customization
- Styles: `web/styles.css`
- Team colors/names: `web/assets/teams_wnba.json`
- Card rendering logic: `web/app.js`
