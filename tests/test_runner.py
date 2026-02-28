"""Tests for non-trading-day guard and empty-bars guard in run_pipeline."""
from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from inga_quant.pipeline.ingest import DataLoader, DemoLoader
from inga_quant.pipeline.runner import run_pipeline

_BARS_PATH = Path(__file__).parent / "fixtures" / "bars_small.parquet"

# A Saturday (non-trading day)
_SATURDAY = date(2026, 2, 21)
# JP New Year holiday (non-trading day)
_NEW_YEAR = date(2026, 1, 1)
# A known business day within the fixture range
_BUSINESS_DAY = date(2026, 2, 10)

# Env override: suppress real Slack calls in tests
_NO_SLACK = {"SLACK_WEBHOOK_URL": ""}


class _EmptyBarsLoader(DataLoader):
    """Stub loader that always returns an empty bars DataFrame."""

    def fetch_daily(
        self,
        start_date: date,
        end_date: date,
        tickers: list[str] | None = None,
    ) -> pd.DataFrame:
        return pd.DataFrame(columns=["as_of", "ticker", "close", "volume"])


@pytest.fixture(autouse=True)
def _no_slack(monkeypatch):
    """Prevent real Slack HTTP calls during runner tests."""
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "")


class TestNonTradingDayGuard:
    def test_saturday_returns_path(self, tmp_path):
        """Saturday as_of returns a Path without raising."""
        loader = DemoLoader(_BARS_PATH)
        result = run_pipeline(_SATURDAY, loader, out_base=tmp_path)
        assert isinstance(result, Path)

    def test_saturday_decision_card_no_trade(self, tmp_path):
        """Saturday as_of writes decision_card with action=NO_TRADE."""
        loader = DemoLoader(_BARS_PATH)
        out_dir = run_pipeline(_SATURDAY, loader, out_base=tmp_path)
        cards = list(out_dir.glob("decision_card_*.json"))
        assert cards, "decision_card must be written on non-trading day"
        card = json.loads(cards[0].read_text())
        assert card["action"] == "NO_TRADE"
        assert "non_trading_day" in card["no_trade_reasons"]

    def test_saturday_writes_quality_report(self, tmp_path):
        """Saturday as_of writes quality_report."""
        loader = DemoLoader(_BARS_PATH)
        out_dir = run_pipeline(_SATURDAY, loader, out_base=tmp_path)
        assert list(out_dir.glob("quality_report_*.json")), "quality_report must be written"

    def test_saturday_writes_manifest(self, tmp_path):
        """Saturday as_of writes manifest."""
        loader = DemoLoader(_BARS_PATH)
        out_dir = run_pipeline(_SATURDAY, loader, out_base=tmp_path)
        assert list(out_dir.glob("manifest_*.json")), "manifest must be written"

    def test_holiday_decision_card_no_trade(self, tmp_path):
        """JP New Year holiday writes decision_card with action=NO_TRADE."""
        loader = DemoLoader(_BARS_PATH)
        out_dir = run_pipeline(_NEW_YEAR, loader, out_base=tmp_path)
        cards = list(out_dir.glob("decision_card_*.json"))
        assert cards
        card = json.loads(cards[0].read_text())
        assert card["action"] == "NO_TRADE"
        assert "non_trading_day" in card["no_trade_reasons"]

    def test_decision_card_schema_valid(self, tmp_path):
        """decision_card on non-trading day must have required schema fields."""
        loader = DemoLoader(_BARS_PATH)
        out_dir = run_pipeline(_SATURDAY, loader, out_base=tmp_path)
        card = json.loads(next(out_dir.glob("decision_card_*.json")).read_text())
        for key in ("schema_version", "trade_date", "run_id", "action", "no_trade_reasons",
                    "top3", "key_metrics"):
            assert key in card, f"Missing key in decision_card: {key}"
        assert card["top3"] == []


class TestEmptyBarsGuard:
    def test_empty_bars_returns_path(self, tmp_path):
        """Empty bars loader returns a Path without raising."""
        loader = _EmptyBarsLoader()
        result = run_pipeline(_BUSINESS_DAY, loader, out_base=tmp_path)
        assert isinstance(result, Path)

    def test_empty_bars_decision_card_no_trade(self, tmp_path):
        """Empty bars writes decision_card with action=NO_TRADE and reason=no_data."""
        loader = _EmptyBarsLoader()
        out_dir = run_pipeline(_BUSINESS_DAY, loader, out_base=tmp_path)
        cards = list(out_dir.glob("decision_card_*.json"))
        assert cards, "decision_card must be written when bars are empty"
        card = json.loads(cards[0].read_text())
        assert card["action"] == "NO_TRADE"
        assert "no_data" in card["no_trade_reasons"]

    def test_empty_bars_writes_quality_report(self, tmp_path):
        """Empty bars writes quality_report."""
        loader = _EmptyBarsLoader()
        out_dir = run_pipeline(_BUSINESS_DAY, loader, out_base=tmp_path)
        assert list(out_dir.glob("quality_report_*.json")), "quality_report must be written"

    def test_empty_bars_writes_manifest(self, tmp_path):
        """Empty bars writes manifest."""
        loader = _EmptyBarsLoader()
        out_dir = run_pipeline(_BUSINESS_DAY, loader, out_base=tmp_path)
        assert list(out_dir.glob("manifest_*.json")), "manifest must be written"

    def test_empty_bars_manifest_has_required_fields(self, tmp_path):
        """Manifest written on empty bars has required fields."""
        loader = _EmptyBarsLoader()
        out_dir = run_pipeline(_BUSINESS_DAY, loader, out_base=tmp_path)
        manifest = json.loads(next(out_dir.glob("manifest_*.json")).read_text())
        for key in ("run_id", "code_hash", "inputs_digest", "as_of", "trade_date", "params"):
            assert key in manifest, f"Missing manifest key: {key}"
