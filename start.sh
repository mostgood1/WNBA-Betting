#!/bin/bash
set -euo pipefail

echo "🚀 Starting NBA Betting (static frontend) on Render..."

# Ensure directories
mkdir -p logs

python --version || true

export PYTHONUNBUFFERED=1

echo "Using PORT=${PORT:-5000} WEB_CONCURRENCY=${WEB_CONCURRENCY:-1} WEB_THREADS=${WEB_THREADS:-4}"

# On deploy/start: refresh today's odds and compute odds-related edges only (no retraining)
# This keeps Render behavior minimal and aligned with local.
if [ -n "${ODDS_API_KEY:-}" ]; then
  REFRESH_DATE=$(date -u +%F)
  echo "[prestart] Refreshing odds and recomputing props edges for ${REFRESH_DATE}..."
  # Ignore failures so the web still comes up if the refresh times out
  python -m nba_betting.cli odds-refresh --date "${REFRESH_DATE}" || echo "[prestart] odds-refresh failed (non-fatal)"
  # Also export game recommendations based on predictions + updated game odds
  python -m nba_betting.cli export-recommendations --date "${REFRESH_DATE}" || echo "[prestart] export-recommendations failed (non-fatal)"
  # Also export props recommendation cards from computed edges
  python -m nba_betting.cli export-props-recommendations --date "${REFRESH_DATE}" || echo "[prestart] export-props-recommendations failed (non-fatal)"
  # Optionally commit and push refreshed artifacts back to Git for cross-env consistency
  if [ "${RENDER_PUSH_ON_DEPLOY:-0}" = "1" ]; then
    TOKEN="${GH_TOKEN:-${GIT_PAT:-}}"
    if [ -n "$TOKEN" ]; then
      echo "[prestart] Pushing refreshed odds/edges/recs to Git (date=${REFRESH_DATE})"
      # Configure git identity if provided
      git config user.name "${GH_NAME:-render-bot}" || true
      git config user.email "${GH_EMAIL:-render-bot@users.noreply.github.com}" || true
      # Stage and commit if there are changes
      git add -A || true
      if ! git diff --cached --quiet; then
        git commit -m "render: odds/edges/recs refresh ${REFRESH_DATE}" || true
      else
        echo "[prestart] No changes to commit"
      fi
      # Push using a token-auth URL without altering the saved remote
      ORIGIN_URL=$(git remote get-url origin 2>/dev/null || echo "")
      if echo "$ORIGIN_URL" | grep -q "github.com"; then
        AUTH_URL="$ORIGIN_URL"
        AUTH_URL=${AUTH_URL/git@github.com:/https://github.com/}
        AUTH_URL=${AUTH_URL/https:\/\/github.com\//https:\/\/${TOKEN}@github.com/}
        git push "$AUTH_URL" HEAD:main || echo "[prestart] git push failed (non-fatal)"
      else
        git push origin HEAD:main || echo "[prestart] git push failed (non-fatal)"
      fi
    else
      echo "[prestart] RENDER_PUSH_ON_DEPLOY=1 but no GH_TOKEN/GIT_PAT provided; skipping push"
    fi
  fi
else
  echo "[prestart] ODDS_API_KEY not set; skipping odds refresh"
fi

exec gunicorn app:app \
  --bind 0.0.0.0:${PORT:-5000} \
  --workers ${WEB_CONCURRENCY:-1} \
  --worker-class gthread \
  --threads ${WEB_THREADS:-4} \
  --timeout 120 \
  --log-level info
