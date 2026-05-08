# WNBA Daily Update Runbook

**Updated**: 2026-05-08  
**Primary script**: `scripts/daily_update.ps1`

## Purpose

The daily update workflow is the single operational entrypoint for the WNBA repo. It is designed to handle three jobs in one run:

1. Reconcile the prior day.
2. Build the current day slate.
3. Generate a short look-ahead slate for the next day.

## Current State

The workflow already exists and is active in three places:

- `scripts/daily_update.ps1` is the main local/operator script.
- `daily_update.cmd` is the root-level launcher.
- `.github/workflows/nbabetting-daily-update.yml` is the scheduled GitHub Actions runner.

The script now uses WNBA wording and resolves schedule gating from the year-based artifacts in `data/processed/schedule_YYYY.json` instead of the old NBA-style `schedule_2025_26.json` path.

## Daily Operating Sequence

### 1. Prior-day reconciliation

For the previous slate date, the script performs best-effort reconciliation and historical maintenance, including:

- `reconcile-date`
- `fetch-prop-actuals`
- `finals-export`
- `fetch-pbp --finals-only`
- `fetch-boxscores --finals-only`
- `update-boxscores-history --finals-only`
- `update-pbp-espn-history --finals-only`
- `update-rotations-espn-history`
- `reconcile-pbp-markets`
- `calibrate-pbp-markets`
- recon player and live player lens tuning builds
- `reconcile-quarters`

On no-slate days, the script anchors that reconciliation work to the most recent available slate date.

### 2. Current-day build

For the requested date, the script builds the active slate and downstream artifacts, including:

- injuries / rosters / player logs refreshes where applicable
- `build-league-status`
- `check-dressed`
- `predict-date`
- `odds-snapshots`
- `simulate-games`
- `fetch-advanced-stats`
- `smart-sim-date`
- `predict-props`
- shared OddsAPI props refresh worker
- `export-recommendations`

The script validates key props artifacts and will mark the run failed when props predictions exist but required processed outputs are missing.

### 3. Day look-ahead

The script supports short future-slate generation with `-LookAheadDays`.

- Default behavior resolves from `DAILY_LOOKAHEAD_DAYS`, defaulting to `1`.
- The script caps look-ahead generation at `3` days.
- Look-ahead runs use the backend `_daily_update_job(..., mode="lookahead")` path for future-slate artifacts.

This is the mechanism that keeps tomorrow's Render/UI slate ready even before the next full daily run.

## Operator Commands

### Standard local run

```powershell
powershell -ExecutionPolicy Bypass -File scripts\daily_update.ps1 -GitSyncFirst -GitPush
```

### Run for a specific slate date

```powershell
powershell -ExecutionPolicy Bypass -File scripts\daily_update.ps1 -Date "2026-05-08" -GitSyncFirst -GitPush
```

### Run without pushing git changes

```powershell
powershell -ExecutionPolicy Bypass -File scripts\daily_update.ps1 -Date "2026-05-08"
```

### Run with explicit 1-day look-ahead

```powershell
powershell -ExecutionPolicy Bypass -File scripts\daily_update.ps1 -Date "2026-05-08" -LookAheadDays 1 -GitSyncFirst -GitPush
```

### Root-level launcher

```powershell
.\daily_update -Date 2026-05-08
```

## Recommended Daily Usage

For normal in-season operation, use one command:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\daily_update.ps1 -Date "2026-05-08" -LookAheadDays 1 -GitSyncFirst -GitPush
```

That single run covers:

- prior-day reconciliation
- current-day build
- next-day look-ahead

## Scheduled Automation

GitHub Actions runs the daily updater from:

- `.github/workflows/nbabetting-daily-update.yml`

It is scheduled for 7:00 AM Central and creates a local venv, installs dependencies, runs `scripts/daily_update.ps1`, and pushes resulting artifacts.

## Logs

Local logs are written to:

```text
logs/local_daily_update_YYYYMMDD_HHMMSS.log
```

Latest log command:

```powershell
Get-Content (Get-ChildItem logs -Filter 'local_daily_update_*.log' | Sort LastWriteTime -Desc | Select-Object -First 1).FullName
```

## Notes

- The daily update flow is WNBA-operational, but some legacy filenames and workflow filenames may still use older naming conventions.
- The workflow name and operator-facing wording have been updated to WNBA.
- The main script should be treated as the authoritative daily operations entrypoint.
