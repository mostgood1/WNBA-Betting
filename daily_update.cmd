@echo off
REM Root-level launcher to run the daily update end-to-end and push to git
REM Usage examples:
REM   .\daily_update            -> runs for today with -GitPush
REM   .\daily_update -Date 2026-05-08  -> override date

setlocal
set "SCRIPT_DIR=%~dp0"

REM Always push by default; sync from origin/main first and forward user args (e.g., -Date)
powershell -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%scripts\daily_update.ps1" -GitPush -GitSyncFirst %*

set EXITCODE=%ERRORLEVEL%
exit /b %EXITCODE%
