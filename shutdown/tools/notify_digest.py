#!/usr/bin/env python3
"""
notify_digest.py — Weekly KPI digest stub for inga-weekly-digest.service.

This placeholder confirms the script path resolution works end-to-end.
Real digest logic (BigQuery queries, Slack notification, etc.) should be
added here in a subsequent PR.

Environment variables (all optional):
  AS_OF          Date override (YYYY-MM-DD). Defaults to today (JST).
  BASE           SHUTDOWN base path. Defaults to /srv/inga/SHUTDOWN.
  SLACK_WEBHOOK_URL  If set, POST digest payload here. Empty → skip Slack.
"""
from __future__ import annotations

import os
import sys
from datetime import date, timezone, timedelta

JST = timezone(timedelta(hours=9))


def _ts() -> str:
    from datetime import datetime
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _log(msg: str) -> None:
    print(f"{_ts()} {msg}", flush=True)


def main() -> int:
    as_of_str = os.environ.get("AS_OF", date.today().strftime("%Y-%m-%d"))
    base = os.environ.get("BASE", "/srv/inga/SHUTDOWN")
    slack_url = os.environ.get("SLACK_WEBHOOK_URL", "")

    _log(f"notify_digest: start as_of={as_of_str} base={base}")

    # Placeholder: real implementation will query BigQuery and post to Slack.
    _log("notify_digest: [STUB] digest logic not yet implemented — exit 0")

    if slack_url:
        _log("notify_digest: SLACK_WEBHOOK_URL set — would POST digest (stub)")
    else:
        _log("notify_digest: SLACK_WEBHOOK_URL not set — skipping Slack")

    _log("notify_digest: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
