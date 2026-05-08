#!/usr/bin/env bash
set -euo pipefail

# Render Cron Job helper: call token-gated /api/cron/* endpoints reliably.
#
# Required env:
#   - WNBA_BETTING_BASE_URL (or BASE_URL)
#   - WNBA_BETTING_CRON_TOKEN (or CRON_TOKEN)
#
# Usage examples:
#   ./scripts/render_cron_call.sh /api/cron/ping
#   ./scripts/render_cron_call.sh "/api/cron/refresh-oddsapi-props?date=$(date -u +%F)&regions=us&edges=1&export=1"
#   ./scripts/render_cron_call.sh "/api/cron/run-all?push=0"

BASE_URL="${WNBA_BETTING_BASE_URL:-${NBA_BETTING_BASE_URL:-${BASE_URL:-${RENDER_EXTERNAL_URL:-}}}}"
TOKEN="${WNBA_BETTING_CRON_TOKEN:-${NBA_BETTING_CRON_TOKEN:-${CRON_TOKEN:-}}}"
PATH_QS="${1:-/api/cron/ping}"

if [[ -z "${BASE_URL}" ]]; then
  echo "Missing base URL. Set WNBA_BETTING_BASE_URL (recommended) or BASE_URL." >&2
  exit 2
fi
if [[ -z "${TOKEN}" ]]; then
  echo "Missing cron token. Set WNBA_BETTING_CRON_TOKEN (recommended) or CRON_TOKEN." >&2
  exit 2
fi

URL="${BASE_URL%/}${PATH_QS}"
if [[ "${PATH_QS}" != /* ]]; then
  URL="${BASE_URL%/}/${PATH_QS}"
fi

echo "[render-cron] GET ${URL}" >&2

max_attempts="${NBA_BETTING_CRON_MAX_ATTEMPTS:-6}"
sleep_base_seconds="${NBA_BETTING_CRON_SLEEP_BASE_SECONDS:-5}"

attempt=1
while true; do
  http=""
  curl_rc=0

  set +e
  http=$(curl -sS -o resp.txt -w "%{http_code}" -H "Authorization: Bearer ${TOKEN}" "${URL}")
  curl_rc=$?
  set -e

  if [[ ${curl_rc} -ne 0 ]]; then
    http="000"
  fi

  if [[ "${http}" =~ ^2[0-9][0-9]$ ]]; then
    cat resp.txt
    exit 0
  fi

  echo "[render-cron] attempt ${attempt}/${max_attempts} -> HTTP ${http}" >&2
  cat resp.txt >&2 || true

  if [[ ${attempt} -ge ${max_attempts} ]]; then
    exit 1
  fi

  # Retry transient failures: network errors (000), rate limit (429), gateway/app blips (5xx).
  if [[ "${http}" == "000" || "${http}" == "429" || "${http}" =~ ^5[0-9][0-9]$ ]]; then
    sleep_seconds=$(( attempt * sleep_base_seconds ))
    echo "[render-cron] retrying in ${sleep_seconds}s" >&2
    sleep "${sleep_seconds}"
    attempt=$((attempt + 1))
    continue
  fi

  # Non-retryable HTTP failures (typically auth/404).
  exit 1
done
