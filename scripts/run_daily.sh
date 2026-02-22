#!/usr/bin/env bash
# Daily pipeline runner — invoked by systemd timer at 18:00 JST
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"

# Load environment (credentials etc.)
if [[ -f .env ]]; then
    # shellcheck disable=SC1091
    set -a
    source .env
    set +a
fi

TODAY=$(date +%Y-%m-%d)
LOG_DIR="$REPO_DIR/logs"
mkdir -p "$LOG_DIR"

echo "[$(date -Iseconds)] Starting inga-quant daily run for as_of=$TODAY" | tee -a "$LOG_DIR/cron.log"

.venv/bin/python -m inga_quant.cli run \
    --as-of "$TODAY" \
    --config config/config.yaml \
    --out output \
    2>&1 | tee -a "$LOG_DIR/cron.log"

echo "[$(date -Iseconds)] Run complete" | tee -a "$LOG_DIR/cron.log"
