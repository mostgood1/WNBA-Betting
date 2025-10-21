@echo off
REM Root-level launcher to run the daily update end-to-end and push to git
REM Usage examples:
REM   .\daily_update            -> runs for today with -GitPush
REM   .\daily_update -Date 2025-10-21  -> override date

setlocal
set "SCRIPT_DIR=%~dp0"

REM Always push by default; forward any user-provided args (e.g., -Date)
powershell -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%scripts\daily_update.ps1" -GitPush %*

set EXITCODE=%ERRORLEVEL%
exit /b %EXITCODE%
