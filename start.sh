#!/bin/bash
set -euo pipefail

echo "🚀 Starting NBA Betting (static frontend) on Render..."

# Ensure directories
mkdir -p logs

python --version || true

export PYTHONUNBUFFERED=1

# Default: mild correlated scoring variance (mean-preserving), tuned via eval sweeps.
export CONNECTED_CORRELATED_SCORING_ALPHA=${CONNECTED_CORRELATED_SCORING_ALPHA:-0.2}

echo "Using PORT=${PORT:-5000} WEB_CONCURRENCY=${WEB_CONCURRENCY:-1} WEB_THREADS=${WEB_THREADS:-4}"

# IMPORTANT (Render memory): boot should be as light as possible.
# The prestart refresh/export jobs can be CPU/memory heavy and can cause OOM
# on small Render instances. Keep them opt-in.
if [ "${RENDER_PRESTART_REFRESH:-0}" = "1" ]; then
  if [ -n "${ODDS_API_KEY:-}" ]; then
    REFRESH_DATE=$(date -u +%F)
    echo "[prestart] Refreshing odds and exporting recs for ${REFRESH_DATE}..."
    # Ignore failures so the web still comes up if the refresh times out
    python -m nba_betting.cli odds-refresh --date "${REFRESH_DATE}" || echo "[prestart] odds-refresh failed (non-fatal)"
    python -m nba_betting.cli export-recommendations --date "${REFRESH_DATE}" || echo "[prestart] export-recommendations failed (non-fatal)"
    python -m nba_betting.cli export-props-recommendations --date "${REFRESH_DATE}" || echo "[prestart] export-props-recommendations failed (non-fatal)"

    # Optionally commit and push refreshed artifacts back to Git for cross-env consistency
    if [ "${RENDER_PUSH_ON_DEPLOY:-0}" = "1" ]; then
      TOKEN="${GH_TOKEN:-${GIT_PAT:-}}"
      if [ -n "$TOKEN" ]; then
        echo "[prestart] Pushing refreshed odds/edges/recs to Git (date=${REFRESH_DATE})"
        git config user.name "${GH_NAME:-render-bot}" || true
        git config user.email "${GH_EMAIL:-render-bot@users.noreply.github.com}" || true
        git add -A || true
        if ! git diff --cached --quiet; then
          git commit -m "render: odds/edges/recs refresh ${REFRESH_DATE}" || true
        else
          echo "[prestart] No changes to commit"
        fi
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
    echo "[prestart] RENDER_PRESTART_REFRESH=1 but ODDS_API_KEY not set; skipping refresh"
  fi
else
  echo "[prestart] Skipping refresh/export (set RENDER_PRESTART_REFRESH=1 to enable)"
fi

exec gunicorn app:app \
  --bind 0.0.0.0:${PORT:-5000} \
  --workers ${WEB_CONCURRENCY:-1} \
  --worker-class gthread \
  --threads ${WEB_THREADS:-4} \
  --timeout 120 \
  --log-level info
