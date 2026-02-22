.PHONY: setup ingest_daily ingest_margin build_features_daily build_watchlist \
        fetch_minute_cache build_minute_features_daily train gate report slack \
        run demo prune_cache lint test clean

# ---------------------------------------------------------------------------
# Config (override on CLI: make run AS_OF=2026-02-10)
# ---------------------------------------------------------------------------
AS_OF       ?= $(shell date +%Y-%m-%d)
CONFIG      ?= config/config.yaml
BARS_PATH   ?= data/daily/latest.parquet
OUT_BASE    ?= output
CACHE_DAYS  ?= 20
PYTHON      := .venv/bin/python
PYTEST      := .venv/bin/pytest
RUFF        := .venv/bin/ruff

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------
setup:
	python -m venv .venv
	$(PYTHON) -m pip install -e ".[dev]" -q
	mkdir -p data/daily data/margin data/minute_features cache/minute_bars output logs
	@echo "Setup complete. Copy .env.example → .env and fill credentials."

# ---------------------------------------------------------------------------
# Data ingestion
# ---------------------------------------------------------------------------
ingest_daily:
	$(PYTHON) -c "\
from inga_quant.pipeline.ingest import JQuantsLoader; \
from datetime import date, timedelta; \
import pyarrow.parquet as pq, pyarrow as pa; \
loader = JQuantsLoader(); \
df = loader.fetch_daily(start_date=date.today()-timedelta(days=400), end_date=date.today()); \
pq.write_table(pa.Table.from_pandas(df, preserve_index=False), 'data/daily/latest.parquet'); \
print(f'Fetched {len(df)} rows')"

ingest_margin:
	@echo "J-Quants margin fetch — implement per Standard plan margin endpoint"

# ---------------------------------------------------------------------------
# Feature Store (Phase 1, production)
# ---------------------------------------------------------------------------
build_features_daily:
	$(PYTHON) -m inga_quant.cli build-features \
		--as-of $(AS_OF) \
		--bars $(BARS_PATH) \
		--out $(OUT_BASE)/features

# ---------------------------------------------------------------------------
# Watchlist (standalone — for debugging)
# ---------------------------------------------------------------------------
build_watchlist:
	@echo "Use 'make run AS_OF=...' for full pipeline including watchlist"

# ---------------------------------------------------------------------------
# Minute cache
# ---------------------------------------------------------------------------
fetch_minute_cache:
	@echo "Fetch minute bars for watchlist_50 — implement with J-Quants minute endpoint"

build_minute_features_daily:
	@echo "Build daily summary features from minute cache — implement in pipeline/minute.py"

prune_cache:
	$(PYTHON) -m inga_quant.cli prune-cache \
		--days $(CACHE_DAYS) \
		--cache-dir cache/minute_bars

# ---------------------------------------------------------------------------
# Model / Gate / Report / Notify (individual steps — for debugging)
# ---------------------------------------------------------------------------
train:
	@echo "Training is embedded in 'make run'. Use 'make run AS_OF=...' for full run."

gate:
	@echo "Gates are run as part of 'make run'. Use 'make run AS_OF=...' for full run."

report:
	@echo "Report is generated as part of 'make run'. Use 'make run AS_OF=...' for full run."

slack:
	@echo "Slack notification is sent as part of 'make run'. Use 'make run AS_OF=...' for full run."

# ---------------------------------------------------------------------------
# Full pipeline
# ---------------------------------------------------------------------------
run:
	$(PYTHON) -m inga_quant.cli run \
		--as-of $(AS_OF) \
		--out $(OUT_BASE) \
		--config $(CONFIG)

# Demo (no API calls, uses built-in fixture)
demo:
	$(PYTHON) -m inga_quant.cli run \
		--demo \
		--as-of $(AS_OF) \
		--out /tmp/inga-demo \
		--config $(CONFIG)
	@echo "Demo output in /tmp/inga-demo/"

# ---------------------------------------------------------------------------
# Quality
# ---------------------------------------------------------------------------
lint:
	$(RUFF) check .
	$(RUFF) format --check .

test:
	$(PYTEST) -q

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete
	rm -rf .pytest_cache .ruff_cache
