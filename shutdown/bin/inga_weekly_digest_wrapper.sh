#!/usr/bin/env bash
# inga_weekly_digest_wrapper.sh
# Wraps notify_digest.py so that non-zero exits are converted to exit 0 (SKIP).
#
# systemd SuccessExitStatus=0 1 covers exit 1, but if notify_digest.py exits ≥ 2
# (crash, unhandled exception, import error) the service lands in failed state.
# This wrapper catches any non-zero exit and logs it as [SKIP], keeping the
# service unit clean in systemctl --failed.
#
# SKIP (exit 0):
#   notify_nonzero — notify_digest.py exited with a non-zero code
#
# FAIL (exit 1): never — this wrapper always exits 0
#
# Deploy:
#   sudo cp shutdown/bin/inga_weekly_digest_wrapper.sh /srv/inga/SHUTDOWN/bin/
#   sudo chmod 750 /srv/inga/SHUTDOWN/bin/inga_weekly_digest_wrapper.sh
#   # then install the systemd override (see shutdown/systemd/README)

set -uo pipefail

_ts()       { date -u +%Y-%m-%dT%H:%M:%SZ; }
_log()      { echo "$(_ts) $*"; }

DIGEST_SCRIPT="${DIGEST_SCRIPT:-/root/inga-context-public/tools/notify_digest.py}"
PYTHON="${DIGEST_PYTHON:-/usr/bin/python3}"

_log "weekly-digest-wrapper: start script=${DIGEST_SCRIPT}"

if [[ ! -f "$DIGEST_SCRIPT" ]]; then
  _log "[SKIP] reason=script_missing path=${DIGEST_SCRIPT}"
  exit 0
fi

rc=0
"$PYTHON" "$DIGEST_SCRIPT" || rc=$?

if [[ "$rc" -ne 0 ]]; then
  _log "[SKIP] reason=notify_nonzero exit_code=${rc} script=${DIGEST_SCRIPT}"
  exit 0
fi

_log "OK: weekly-digest completed"
