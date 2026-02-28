#!/usr/bin/env bash
# inga_universe300_build.sh
# Refresh universe300 from J-Quants /v2/equities/master (weekly: Sun 03:00)
#
# SKIP (exit 0):
#   api_key_missing  — JQ_API_KEY not set
#   no_data          — API returned < 100 Prime codes (keep existing file)
#
# FAIL (exit 1):
#   Network/curl hard failure
#
# Usage: inga_universe300_build.sh [--dry-run]
#
# Deploy: /srv/inga/SHUTDOWN/bin/inga_universe300_build.sh
#
# NOTE: No business-day guard here — this job runs intentionally on Sunday
#       to refresh the stock master. Trading-day awareness is not needed.

set -euo pipefail

# ─── arg parse ───────────────────────────────────────────────────────────────
DRY_RUN=0
for arg in "$@"; do [[ "$arg" == "--dry-run" ]] && DRY_RUN=1; done

# ─── helpers ─────────────────────────────────────────────────────────────────
_ts()       { date -u +%Y-%m-%dT%H:%M:%SZ; }
_log()      { echo "$(_ts) $*"; }
_log_skip() { _log "[SKIP] reason=${1} ${2:-}"; exit 0; }

# ─── paths (no I/O — SKIP checks come first) ─────────────────────────────────
BASE=/srv/inga/SHUTDOWN
ENV="${BASE}/conf/inga_signals.env"
OUT="${BASE}/conf/universe300.txt"

# ─── env / config (soft-fail: if file unreadable, env vars may stay unset) ───
if [[ -f "$ENV" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV" 2>/dev/null || true
  set +a
fi

# ─── SKIP: API key not set ───────────────────────────────────────────────────
[[ -z "${JQ_API_KEY:-}" ]] && _log_skip "api_key_missing" "JQ_API_KEY not set in env"

# ─── run setup (I/O: only after SKIP check passes) ───────────────────────────
TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT

_log "building universe300 from J-Quants /v2/equities/master"

if [[ "$DRY_RUN" -eq 1 ]]; then
  _log "[DRY] would fetch https://api.jquants.com/v2/equities/master and write ${OUT}"
  exit 0
fi

# Fetch master + filter Prime market codes
timeout 60s curl -sS \
  -H "x-api-key: ${JQ_API_KEY}" \
  "https://api.jquants.com/v2/equities/master" \
| jq -r '
    (.data // [])[]
    | (.MktNm // .Mkt // "") as $m
    | select($m | test("Prime|プライム|東証"; "i"))
    | (.Code // empty)
  ' 2>/dev/null \
| tr -d '\r' \
| sed 's/[[:space:]]//g' \
| grep -E '^[0-9]{4,5}$' \
| awk '{
    if (length($0) == 4) print $0
    else if (length($0) == 5 && $0 ~ /^[0-9]{4}0$/) print substr($0, 1, 4)
    else print $0
  }' \
| sort -u \
| head -n 300 >"$TMP"

cnt="$(wc -l <"$TMP" | tr -d ' ')"

# ─── SKIP: too few codes (API bad response, keep existing) ──────────────────
if [[ "$cnt" -lt 100 ]]; then
  _log_skip "no_data" "only ${cnt} codes from API (< 100); keeping existing ${OUT}"
fi

{
  echo "# auto-generated universe300 (Prime-ish). one code per line (4 digits preferred)."
  cat "$TMP"
} >"$OUT"

chmod 0640 "$OUT"
_log "OK: universe300 generated count=${cnt} -> ${OUT}"
