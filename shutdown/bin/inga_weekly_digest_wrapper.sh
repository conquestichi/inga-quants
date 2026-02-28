#!/usr/bin/env bash
# inga_weekly_digest_wrapper.sh
# Wraps notify_digest.py so that non-zero exits are converted to exit 0 (SKIP).
#
# Script resolution order (first existing file wins):
#   1. $DIGEST_SCRIPT env var (explicit override)
#   2. /srv/inga/SHUTDOWN/bin/notify_digest.py   (canonical deploy location)
#   3. /srv/inga-quants/shutdown/tools/notify_digest.py  (repo location)
#   4. /root/inga-context-public/tools/notify_digest.py  (legacy location)
#
# SKIP (exit 0):
#   script_missing  — no notify_digest.py found at any search path
#   notify_nonzero  — notify_digest.py exited non-zero (logged for debugging)
#
# FAIL (exit 1): never — this wrapper always exits 0
#
# Deploy:
#   sudo cp shutdown/bin/inga_weekly_digest_wrapper.sh /srv/inga/SHUTDOWN/bin/
#   sudo chmod 750 /srv/inga/SHUTDOWN/bin/inga_weekly_digest_wrapper.sh
#   sudo cp shutdown/tools/notify_digest.py /srv/inga/SHUTDOWN/bin/notify_digest.py
#   sudo chmod 640 /srv/inga/SHUTDOWN/bin/notify_digest.py

set -uo pipefail

_ts()  { date -u +%Y-%m-%dT%H:%M:%SZ; }
_log() { echo "$(_ts) $*"; }

PYTHON="${DIGEST_PYTHON:-/usr/bin/python3}"

# ── Resolve DIGEST_SCRIPT ────────────────────────────────────────────────────
if [[ -n "${DIGEST_SCRIPT:-}" ]]; then
  # Explicit override — use as-is (even if it doesn't exist; checked below)
  resolved_script="$DIGEST_SCRIPT"
else
  resolved_script=""
  for candidate in \
    "/srv/inga/SHUTDOWN/bin/notify_digest.py" \
    "/srv/inga-quants/shutdown/tools/notify_digest.py" \
    "/root/inga-context-public/tools/notify_digest.py"
  do
    if [[ -f "$candidate" ]]; then
      resolved_script="$candidate"
      break
    fi
  done
fi

_log "weekly-digest-wrapper: start resolved_script=${resolved_script:-<none>}"

if [[ -z "$resolved_script" || ! -f "$resolved_script" ]]; then
  _log "[SKIP] reason=script_missing searched=[/srv/inga/SHUTDOWN/bin/notify_digest.py, /srv/inga-quants/shutdown/tools/notify_digest.py, /root/inga-context-public/tools/notify_digest.py]"
  exit 0
fi

rc=0
"$PYTHON" "$resolved_script" || rc=$?

if [[ "$rc" -ne 0 ]]; then
  _log "[SKIP] reason=notify_nonzero exit_code=${rc} script=${resolved_script}"
  exit 0
fi

_log "OK: weekly-digest completed script=${resolved_script}"
