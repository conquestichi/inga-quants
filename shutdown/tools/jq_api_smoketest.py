#!/usr/bin/env python3
"""
jq_api_smoketest.py
J-Quants API pre-flight auth verification.

Reads the API key from env (JQ_API_KEY / JQUANTS_API_KEY / JQUANTS_APIKEY, in
priority order) and makes a single lightweight GET request to verify that the
key is valid and the API is reachable.

Exit codes:
  0  — HTTP 200 (key valid, API up) — or stamp present (already verified)
  2  — API key not set in any of the supported env vars
  3  — HTTP 401 (authentication failure — key wrong or expired)
  4  — HTTP 403 (permission / plan restriction)
  5  — timeout or network error
  6  — any other error (unexpected HTTP status, DNS failure, etc.)

Never logs the API key value.

Stamp (one-shot optimization):
  On success (exit 0) a stamp file is written to $STATE/jq_api_smoketest.ok.json.
  Subsequent runs exit 0 immediately without hitting the API.
  Set FORCE=1 to bypass the stamp and re-run the full check.

Environment:
  JQ_API_KEY / JQUANTS_API_KEY / JQUANTS_APIKEY  — API key (first non-empty wins)
  STATE   — directory for stamp file (default: /srv/inga/SHUTDOWN/state)
  FORCE   — set to "1" to ignore the stamp and re-run
"""

import datetime
import json
import os
import sys
import urllib.error
import urllib.request

_BASE_URL = "https://api.jquants.com"
_PATH = "/v2/equities/master"
_TOTAL_TIMEOUT = 15

_URL = _BASE_URL + _PATH

_STATE = os.environ.get("STATE", "/srv/inga/SHUTDOWN/state")
_STAMP = os.path.join(_STATE, "jq_api_smoketest.ok.json")
_FORCE = os.environ.get("FORCE", "0").strip() == "1"


def _log(msg: str) -> None:
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"{ts} [smoketest] {msg}", flush=True)


def _truncate(text: str, max_len: int = 200) -> str:
    return text if len(text) <= max_len else text[:max_len] + "…"


def _write_stamp() -> None:
    try:
        os.makedirs(_STATE, exist_ok=True)
        payload = {
            "ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "result": "ok",
            "endpoint": _URL,
        }
        with open(_STAMP, "w") as fh:
            json.dump(payload, fh)
        _log(f"stamp written: {_STAMP}")
    except OSError as exc:
        _log(f"WARN: could not write stamp (non-fatal): {exc}")


def main() -> int:
    # ── stamp check (skip if already verified, unless FORCE=1) ─────────────
    if not _FORCE and os.path.isfile(_STAMP):
        _log(f"stamp present — skipping pre-flight check (FORCE=1 to re-run): {_STAMP}")
        return 0

    # ── key resolution (priority order) ────────────────────────────────────
    api_key = (
        os.environ.get("JQ_API_KEY")
        or os.environ.get("JQUANTS_API_KEY")
        or os.environ.get("JQUANTS_APIKEY")
        or ""
    )

    if not api_key:
        _log("key_missing: JQ_API_KEY / JQUANTS_API_KEY / JQUANTS_APIKEY not set in env")
        return 2

    _log(f"probe url={_URL}")

    req = urllib.request.Request(
        _URL,
        headers={"x-api-key": api_key},
    )

    try:
        with urllib.request.urlopen(req, timeout=_TOTAL_TIMEOUT) as resp:
            http_status = resp.status
            body_bytes = resp.read(512)
    except urllib.error.HTTPError as exc:
        http_status = exc.code
        try:
            body_bytes = exc.read(512)
        except Exception:
            body_bytes = b""
    except TimeoutError as exc:
        _log(f"timeout: {exc}")
        return 5
    except OSError as exc:
        # Covers socket.timeout (Python < 3.11), connection refused, DNS errors
        msg = str(exc)
        if "timed out" in msg.lower() or "timeout" in msg.lower():
            _log(f"timeout: {exc}")
            return 5
        _log(f"network_error: {exc}")
        return 5
    except Exception as exc:
        _log(f"unexpected_error: {type(exc).__name__}: {exc}")
        return 6

    try:
        body_text = body_bytes.decode("utf-8", errors="replace")
    except Exception:
        body_text = repr(body_bytes)

    _log(f"http_status={http_status} response={_truncate(body_text)}")

    if http_status == 200:
        _log("OK: API key valid")
        _write_stamp()
        return 0
    if http_status == 401:
        _log("auth_failure: HTTP 401 — key wrong or expired")
        return 3
    if http_status == 403:
        _log("permission_denied: HTTP 403 — plan restriction or access denied")
        return 4
    _log(f"unexpected_status: HTTP {http_status}")
    return 6


if __name__ == "__main__":
    sys.exit(main())
