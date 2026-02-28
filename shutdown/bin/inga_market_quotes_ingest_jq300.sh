#!/usr/bin/env bash
# inga_market_quotes_ingest_jq300.sh
# J-Quants daily bars ingest for universe300 → BigQuery
#
# SKIP (exit 0):
#   api_key_missing  — JQ_API_KEY not set
#   non_trading_day  — weekend or JP public holiday
#   no_data          — < 10 rows fetched (holiday, API no-data)
#
# FAIL (exit 1):
#   universe file missing / empty
#   API persistently unreachable (5 days × probe all fail) on a business day
#   BigQuery write failure
#
# Usage: inga_market_quotes_ingest_jq300.sh [--dry-run]
#
# Deploy: /srv/inga/SHUTDOWN/bin/inga_market_quotes_ingest_jq300.sh

set -euo pipefail

# ─── arg parse ───────────────────────────────────────────────────────────────
DRY_RUN=0
for arg in "$@"; do [[ "$arg" == "--dry-run" ]] && DRY_RUN=1; done

# AS_OF: date to evaluate for business-day check.
# Override via env for testing: AS_OF=2026-01-01 ./script.sh
# Default: today (JST) — used only for calendar guard, not for bar-date probe.
AS_OF="${AS_OF:-$(date +%Y-%m-%d)}"

# ─── helpers ─────────────────────────────────────────────────────────────────
_ts()       { date -u +%Y-%m-%dT%H:%M:%SZ; }
_log()      { echo "$(_ts) $*"; }
_log_skip() { _log "[SKIP] reason=${1} ${2:-}"; exit 0; }
_log_fail() { _log "[FAIL] ${1}" >&2; exit 1; }

# ─── paths (no I/O — SKIP checks come first) ─────────────────────────────────
# BASE/STATE/U300 are env-overridable for testing without root access.
BASE="${BASE:-/srv/inga/SHUTDOWN}"
ENV="${BASE}/conf/inga_signals.env"
STATE="${STATE:-${BASE}/state}"
U300="${U300:-${BASE}/conf/universe300.txt}"

# ─── env / config (soft-fail: if file unreadable, env vars may stay unset) ───
if [[ -f "$ENV" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV" 2>/dev/null || true
  set +a
fi

# Rate-limit / BQ knobs (env-overridable)
REQ_INTERVAL_SEC="${REQ_INTERVAL_SEC:-0.30}"
BQ_TABLE="${BQ_TABLE:-inga_advisor.market_quotes}"
STAGING="${STAGING_TABLE:-inga_advisor.market_quotes_ingest_tmp}"
SOURCE_NAME="jquants"

# ─── SKIP: API key not set ───────────────────────────────────────────────────
[[ -z "${JQ_API_KEY:-}" ]] && _log_skip "api_key_missing" "JQ_API_KEY not set in env"

# ─── SKIP: non-trading day ───────────────────────────────────────────────────
_is_jp_business_day() {
  local dow
  dow="$(date -d "${AS_OF}" +%u 2>/dev/null || date +%u)"  # 1=Mon … 7=Sun
  [[ "$dow" -le 5 ]] || return 1
  # JP public holiday via jpholiday.
  # Try venv python3 first (has jpholiday); fall back to system python3.
  # Exit codes: 0=not a holiday, 1=JP holiday, 2=jpholiday unavailable (fail-open).
  local py3="/srv/inga-quants/.venv/bin/python3"
  [[ -x "$py3" ]] || py3="python3"
  local py_rc=0
  "$py3" -c "
try:
    import jpholiday
    from datetime import date
    y, m, d = map(int, '${AS_OF}'.split('-'))
    exit(1 if jpholiday.is_holiday(date(y, m, d)) else 0)
except Exception:
    exit(2)
" 2>/dev/null; py_rc=$?
  # py_rc=1 → JP public holiday → not a business day
  # py_rc=0 → not a holiday → business day
  # py_rc=2 → jpholiday unavailable → fail-open (assume business day)
  [[ "$py_rc" -eq 1 ]] && return 1
  return 0
}
_is_jp_business_day || _log_skip "non_trading_day" "as_of=${AS_OF} is not a JP business day"

# ─── run setup (I/O: only after SKIP checks pass) ────────────────────────────
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
STATUS_TSV="${STATE}/jq300_ingest_status_${RUN_ID}.tsv"
ERRDIR="${STATE}/jq300_ingest_errors_${RUN_ID}"
DATA_JSON="${STATE}/jq_ingest_ndjson.tmp"

mkdir -p "$STATE" "$ERRDIR"
printf "ts\tcode\tapi_code\tdate\thttp_code\tkind\tbytes\tnote\n" >"$STATUS_TSV"
ln -sfn "$STATUS_TSV" "${STATE}/jq300_ingest_status_latest.tsv"
ln -sfn "$ERRDIR"     "${STATE}/jq300_ingest_errors_latest"

PROBE_BODY="$(mktemp)"
PROBE_HDR="$(mktemp)"
SQLF="$(mktemp)"
trap 'rm -f "$SQLF" "$DATA_JSON" "$PROBE_BODY" "$PROBE_HDR"' EXIT

_log "DBG: ingest start $(date -Is)"
_log "DBG: whoami=$(whoami) pwd=$(pwd)"
_log "DBG: tables BQ_TABLE=${BQ_TABLE} STAGING=${STAGING}"
[[ "$DRY_RUN" -eq 1 ]] && _log "[DRY] dry-run mode — no API calls or BQ writes"

# ─── universe check ──────────────────────────────────────────────────────────
[[ -f "$U300" ]] || _log_fail "universe file missing: $U300"
first_code="$(grep -E '^[0-9]{4,5}$' "$U300" | head -n 1 || true)"
[[ -n "$first_code" ]] || _log_fail "empty universe file: $U300"

# ─── 1) Determine target date ────────────────────────────────────────────────
# Probe today → back 5 days; first 200 with non-empty data wins.
# At 06:50 JST, today's bars are not yet published — yesterday's data is expected.
pick_date=""
probe_http=""
probe_url=""

for back in 0 1 2 3 4 5; do
  probe_d="$(date -d "today - ${back} day" +%Y%m%d)"
  probe_code="$first_code"
  [[ "$probe_code" =~ ^[0-9]{4}$ ]] && probe_code="${probe_code}0"
  probe_url="https://api.jquants.com/v2/equities/bars/daily?code=${probe_code}&date=${probe_d}"

  _log "DBG: probe back=${back} url=${probe_url}"

  if [[ "$DRY_RUN" -eq 1 ]]; then
    _log "[DRY] would curl ${probe_url}"
    continue
  fi

  probe_http="$(
    timeout 12s curl -sS \
      -o "$PROBE_BODY" -D "$PROBE_HDR" \
      -w "%{http_code}" \
      -H "x-api-key: ${JQ_API_KEY}" \
      "$probe_url" 2>/dev/null
  )" || probe_http=""

  _log "DBG: probe back=${back} http=${probe_http:-???}"

  if [[ "${probe_http:-}" == "200" ]]; then
    n="$(jq -r '(.data // []) | length' "$PROBE_BODY" 2>/dev/null || echo 0)"
    if [[ "${n:-0}" != "0" ]]; then
      pick_date="$probe_d"
      break
    fi
  fi
  sleep "${REQ_INTERVAL_SEC}"
done

if [[ "$DRY_RUN" -eq 1 ]]; then
  _log "[DRY] would determine pick_date from probe; exiting"
  exit 0
fi

if [[ -z "$pick_date" ]]; then
  _log_fail "could not determine latest business date (probed back 5 days) | last_http=${probe_http:-N/A} url=${probe_url:-N/A}"
fi

_log "target_date=${pick_date}"

# ─── 2) Ingest bars for all codes ────────────────────────────────────────────
: >"$DATA_JSON"
now_ts="$(date -Is)"
cnt_ok=0
cnt_try=0

while IFS= read -r c; do
  c="$(printf '%s' "$c" | tr -d '\r' | sed 's/[[:space:]]//g')"
  [[ "$c" =~ ^[0-9]{4,5}$ ]] || continue
  cnt_try=$((cnt_try + 1))

  c_req="$c"
  [[ "$c_req" =~ ^[0-9]{4}$ ]] && c_req="${c_req}0"
  url="https://api.jquants.com/v2/equities/bars/daily?code=${c_req}&date=${pick_date}"

  http="$(
    timeout 12s curl -sS \
      -o /tmp/jq_bar.json \
      -w '%{http_code}' \
      -H "x-api-key: ${JQ_API_KEY}" \
      "$url" 2>/dev/null
  )" || http=""

  sleep "${REQ_INTERVAL_SEC}"

  # Classify
  data_len=""; kind=""; note=""
  if [[ "${http:-}" == "200" ]]; then
    if jq -e . >/dev/null 2>&1 </tmp/jq_bar.json; then
      data_len="$(jq -r '(.data // []) | length' </tmp/jq_bar.json 2>/dev/null || echo "")"
      if [[ "${data_len:-0}" != "0" ]]; then
        kind="OK"
      else
        kind="NO_DATA"; note="200_empty_data"
      fi
    else
      kind="PARSE_FAIL"; note="invalid_json"
    fi
  elif [[ "${http:-}" == "429" ]]; then kind="HTTP_429"; note="rate_limited"
  elif [[ "${http:-}" == "404" ]]; then kind="HTTP_404"; note="not_found"
  elif [[ "${http:-}" =~ ^5[0-9][0-9]$ ]]; then kind="HTTP_5XX"; note="server_error"
  elif [[ -z "${http:-}" ]]; then kind="CURL_FAIL"; note="empty_http_code"
  else kind="HTTP_${http}"; note="status"
  fi

  printf "%s\t%s\t%s\t%s\t%s\t\t%s\t\t%s\n" \
    "$(_ts)" "$c" "$c_req" "$pick_date" \
    "${http:-}" "${kind}" "${note}" >>"$STATUS_TSV"

  [[ "$http" == "200" ]] || continue

  # Extract first row
  jq -c --arg src "$SOURCE_NAME" --arg ts "$now_ts" '
    (.data // [])[0] // empty
    | {
        code:       (.Code   // .code   // ""),
        date:       (.Date   // .date   // ""),
        open:       (.Open   // .O      // .AdjO   // null),
        high:       (.High   // .H      // .AdjH   // null),
        low:        (.Low    // .L      // .AdjL   // null),
        close:      (.Close  // .C      // .AdjC   // null),
        volume:     (.Volume // .Vo     // .AdjVo  // null),
        source:     $src,
        ingested_at: $ts
      }
  ' /tmp/jq_bar.json 2>/dev/null >>"$DATA_JSON" || true

  if [[ -s "$DATA_JSON" ]]; then
    tail -n 1 "$DATA_JSON" \
      | jq -e '.code != "" and .date != ""' >/dev/null 2>&1 \
      && cnt_ok=$((cnt_ok + 1)) || true
  fi
done < <(grep -E '^[0-9]{4,5}$' "$U300")

_log "ingest_candidates=${cnt_try} ok_rows=${cnt_ok}"

# ─── SKIP: too few rows (holiday / API no-data) ──────────────────────────────
if [[ "$cnt_ok" -lt 10 ]]; then
  _log_skip "no_data" "only ${cnt_ok} rows for date=${pick_date} (< 10 threshold)"
fi

# ─── 3) BigQuery: ensure staging table ──────────────────────────────────────
timeout 25s bq mk --table \
  --schema "code:STRING,date:STRING,open:FLOAT,high:FLOAT,low:FLOAT,close:FLOAT,volume:FLOAT,source:STRING,ingested_at:TIMESTAMP" \
  "$STAGING" >/dev/null 2>&1 || true

# ─── 4) BigQuery: load NDJSON → staging (truncate each run) ─────────────────
timeout 120s bq load --quiet --replace \
  --source_format=NEWLINE_DELIMITED_JSON \
  "$STAGING" "$DATA_JSON" >/dev/null

# ─── 5) BigQuery: merge staging → target (upsert by code+date+source) ────────
cat >"$SQLF" <<SQL
MERGE \`${BQ_TABLE}\` T
USING (
  SELECT code, date, open, high, low, close, volume, source, ingested_at
  FROM \`${STAGING}\`
  WHERE code IS NOT NULL AND code != ''
    AND date IS NOT NULL AND date != ''
    AND source='${SOURCE_NAME}'
) S
ON T.code = S.code AND T.date = SAFE_CAST(S.date AS DATE) AND T.source = S.source
WHEN MATCHED THEN
  UPDATE SET
    open        = S.open,
    high        = S.high,
    low         = S.low,
    close       = S.close,
    volume      = SAFE_CAST(S.volume AS INT64),
    ingested_at = S.ingested_at
WHEN NOT MATCHED THEN
  INSERT (code, date, open, high, low, close, volume, source, ingested_at)
  VALUES (S.code, SAFE_CAST(S.date AS DATE), S.open, S.high, S.low, S.close,
          SAFE_CAST(S.volume AS INT64), S.source, S.ingested_at);
SQL

timeout 180s bq query --use_legacy_sql=false --quiet <"$SQLF" >/dev/null

_log "OK: merged into ${BQ_TABLE} (staging=${STAGING}) rows=${cnt_ok} date=${pick_date}"
_log "OK: ingested to ${BQ_TABLE} for date=${pick_date} rows=${cnt_ok}"
