"""Tests for ingest.py — JQuantsLoader (V2 /equities/bars/daily) and DemoLoader."""
from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests

from inga_quant.pipeline.ingest import (
    DemoLoader,
    JQuantsAuthError,
    JQuantsLoader,
    _extract_message,
)

BARS_PATH = Path(__file__).parent / "fixtures" / "bars_small.parquet"


# ---------------------------------------------------------------------------
# DemoLoader — no network
# ---------------------------------------------------------------------------

class TestDemoLoader:
    def test_returns_dataframe(self):
        loader = DemoLoader(BARS_PATH)
        df = loader.fetch_daily(date(2025, 8, 1), date(2026, 2, 10))
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    def test_filters_by_date_range(self):
        loader = DemoLoader(BARS_PATH)
        df = loader.fetch_daily(date(2026, 1, 1), date(2026, 2, 10))
        assert df["as_of"].min() >= date(2026, 1, 1)
        assert df["as_of"].max() <= date(2026, 2, 10)

    def test_filters_by_ticker(self):
        loader = DemoLoader(BARS_PATH)
        df = loader.fetch_daily(date(2025, 8, 1), date(2026, 2, 10), tickers=["AAA"])
        assert set(df["ticker"].unique()) == {"AAA"}

    def test_required_columns_present(self):
        loader = DemoLoader(BARS_PATH)
        df = loader.fetch_daily(date(2025, 8, 1), date(2026, 2, 10))
        for col in ("as_of", "ticker", "open", "high", "low", "close", "volume"):
            assert col in df.columns


# ---------------------------------------------------------------------------
# JQuantsLoader — constructor / auth error
# ---------------------------------------------------------------------------

class TestJQuantsLoaderAuth:
    def test_raises_when_no_api_key(self, monkeypatch):
        monkeypatch.delenv("JQUANTS_API_KEY", raising=False)
        monkeypatch.delenv("JQUANTS_APIKEY", raising=False)
        with pytest.raises(JQuantsAuthError, match="未設定"):
            JQuantsLoader()

    def test_reads_JQUANTS_API_KEY(self, monkeypatch):
        monkeypatch.setenv("JQUANTS_API_KEY", "test-key-primary")
        monkeypatch.delenv("JQUANTS_APIKEY", raising=False)
        loader = JQuantsLoader()
        assert loader._api_key == "test-key-primary"

    def test_reads_JQUANTS_APIKEY_compat(self, monkeypatch):
        monkeypatch.delenv("JQUANTS_API_KEY", raising=False)
        monkeypatch.setenv("JQUANTS_APIKEY", "test-key-compat")
        loader = JQuantsLoader()
        assert loader._api_key == "test-key-compat"

    def test_explicit_arg_takes_priority(self, monkeypatch):
        monkeypatch.setenv("JQUANTS_API_KEY", "env-key")
        loader = JQuantsLoader(api_key="explicit-key")
        assert loader._api_key == "explicit-key"


# ---------------------------------------------------------------------------
# JQuantsLoader — V2 endpoint + x-api-key header
# ---------------------------------------------------------------------------

class TestJQuantsLoaderHeaders:
    """Verify correct V2 endpoint URL and x-api-key header."""

    @pytest.fixture
    def loader(self, monkeypatch):
        monkeypatch.delenv("JQUANTS_API_KEY", raising=False)
        monkeypatch.delenv("JQUANTS_APIKEY", raising=False)
        return JQuantsLoader(api_key="dummy-key-for-tests")

    def _mock_resp(self, status: int, body: dict) -> MagicMock:
        resp = MagicMock(spec=requests.Response)
        resp.status_code = status
        resp.json.return_value = body
        resp.reason = "OK" if status == 200 else "Error"
        resp.raise_for_status = MagicMock()
        return resp

    def test_url_is_equities_bars_daily(self, loader):
        """Requests must go to /v2/equities/bars/daily — old /v2/prices/daily_quotes is gone."""
        resp = self._mock_resp(200, {"data": []})
        with patch("requests.get", return_value=resp) as mock_get:
            loader.fetch_daily(date(2026, 1, 5), date(2026, 1, 5), tickers=["72030"])
        url = mock_get.call_args[0][0]
        assert "/v2/equities/bars/daily" in url
        assert "/v2/prices/daily_quotes" not in url

    def test_x_api_key_header_sent(self, loader):
        resp = self._mock_resp(200, {"data": []})
        with patch("requests.get", return_value=resp) as mock_get:
            loader.fetch_daily(date(2026, 1, 5), date(2026, 1, 5), tickers=["72030"])
        call_kwargs = mock_get.call_args
        headers = call_kwargs.kwargs.get("headers") or call_kwargs[1].get("headers", {})
        assert "x-api-key" in headers, "x-api-key header must be present"
        assert headers["x-api-key"] == "dummy-key-for-tests"

    def test_403_raises_auth_error(self, loader):
        resp = self._mock_resp(403, {"message": "The incoming api key is invalid or expired."})
        with patch("requests.get", return_value=resp):
            with pytest.raises(JQuantsAuthError) as exc_info:
                loader.fetch_daily(date(2026, 1, 5), date(2026, 1, 5), tickers=["72030"])
        msg = str(exc_info.value)
        assert "無効" in msg or "invalid" in msg.lower()
        assert "ダッシュボード" in msg or ".env" in msg

    def test_403_message_does_not_include_full_body(self, loader):
        """Only .message field in error text, not entire response body."""
        long_body = {"message": "short msg", "extra_field": "x" * 5000}
        resp = self._mock_resp(403, long_body)
        with patch("requests.get", return_value=resp):
            with pytest.raises(JQuantsAuthError) as exc_info:
                loader.fetch_daily(date(2026, 1, 5), date(2026, 1, 5), tickers=["72030"])
        assert "x" * 100 not in str(exc_info.value), "Full body must not appear in error"

    def test_returns_empty_dataframe_on_no_records(self, loader):
        resp = self._mock_resp(200, {"data": []})
        with patch("requests.get", return_value=resp):
            df = loader.fetch_daily(date(2026, 1, 5), date(2026, 1, 5), tickers=["72030"])
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    def test_pagination_followed(self, loader):
        """Second call uses pagination_key; both pages are combined."""
        page1 = {
            "data": [
                {"Date": "2026-01-05", "Code": "72030", "O": 100, "H": 105,
                 "L": 99, "C": 103, "Vo": 1000}
            ],
            "pagination_key": "next-page-token",
        }
        page2 = {
            "data": [
                {"Date": "2026-01-06", "Code": "72030", "O": 103, "H": 108,
                 "L": 102, "C": 107, "Vo": 1200}
            ],
        }
        responses = [MagicMock(spec=requests.Response), MagicMock(spec=requests.Response)]
        for r, body in zip(responses, [page1, page2]):
            r.status_code = 200
            r.json.return_value = body
            r.raise_for_status = MagicMock()

        with patch("requests.get", side_effect=responses) as mock_get:
            df = loader.fetch_daily(date(2026, 1, 5), date(2026, 1, 6), tickers=["72030"])

        assert mock_get.call_count == 2
        assert len(df) == 2

    def test_column_mapping(self, loader):
        """V2 abbreviated column names are mapped to internal names."""
        body = {
            "data": [{
                "Date": "2026-01-05",
                "Code": "72030",
                "O": 100.0,
                "H": 105.0,
                "L": 98.0,
                "C": 103.0,
                "Vo": 5000,
                "AdjC": 103.0,
                "AdjFactor": 1.0,
            }]
        }
        resp = self._mock_resp(200, body)
        with patch("requests.get", return_value=resp):
            df = loader.fetch_daily(date(2026, 1, 5), date(2026, 1, 5), tickers=["72030"])
        assert "as_of" in df.columns
        assert "ticker" in df.columns
        assert "open" in df.columns
        assert "high" in df.columns
        assert "low" in df.columns
        assert "close" in df.columns
        assert "volume" in df.columns
        assert "adj_close" in df.columns
        assert df["ticker"].iloc[0] == "72030"

    def test_date_iteration_no_tickers(self, loader):
        """No tickers → one request per business day via date= param."""
        # 2026-01-05 Mon and 2026-01-06 Tue are both business days (no holiday)
        day1_body = {"data": [{"Date": "2026-01-05", "Code": "72030",
                                "O": 100, "H": 105, "L": 99, "C": 103, "Vo": 1000}]}
        day2_body = {"data": [{"Date": "2026-01-06", "Code": "72030",
                                "O": 103, "H": 108, "L": 102, "C": 107, "Vo": 1200}]}
        resp1 = self._mock_resp(200, day1_body)
        resp2 = self._mock_resp(200, day2_body)

        with patch("requests.get", side_effect=[resp1, resp2]) as mock_get:
            df = loader.fetch_daily(date(2026, 1, 5), date(2026, 1, 6))

        assert mock_get.call_count == 2
        assert len(df) == 2
        # Verify date= param was used (not code=)
        calls = mock_get.call_args_list
        dates_sent = [
            c.kwargs.get("params", c[1].get("params", {})).get("date")
            for c in calls
        ]
        assert "2026-01-05" in dates_sent
        assert "2026-01-06" in dates_sent


# ---------------------------------------------------------------------------
# JQuantsLoader — retry behaviour
# ---------------------------------------------------------------------------

class TestJQuantsLoaderRetry:
    @pytest.fixture
    def loader(self, monkeypatch):
        monkeypatch.delenv("JQUANTS_API_KEY", raising=False)
        monkeypatch.delenv("JQUANTS_APIKEY", raising=False)
        return JQuantsLoader(api_key="dummy-key")

    def test_retries_on_500(self, loader):
        err_resp = MagicMock(spec=requests.Response)
        err_resp.status_code = 500
        err_resp.raise_for_status = MagicMock()

        ok_resp = MagicMock(spec=requests.Response)
        ok_resp.status_code = 200
        ok_resp.json.return_value = {"data": []}
        ok_resp.raise_for_status = MagicMock()

        with patch("requests.get", side_effect=[err_resp, ok_resp]) as mock_get:
            with patch("time.sleep"):  # don't actually sleep
                df = loader.fetch_daily(date(2026, 1, 5), date(2026, 1, 5), tickers=["72030"])
        assert mock_get.call_count == 2
        assert len(df) == 0

    def test_no_retry_on_403(self, loader):
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 403
        resp.json.return_value = {"message": "invalid key"}
        resp.raise_for_status = MagicMock()

        with patch("requests.get", return_value=resp) as mock_get:
            with pytest.raises(JQuantsAuthError):
                loader.fetch_daily(date(2026, 1, 5), date(2026, 1, 5), tickers=["72030"])
        assert mock_get.call_count == 1

    def test_check_connectivity_raises_auth_error(self, loader):
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 403
        resp.json.return_value = {"message": "key expired"}
        resp.raise_for_status = MagicMock()

        with patch("requests.get", return_value=resp):
            with pytest.raises(JQuantsAuthError):
                loader.check_connectivity()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class TestExtractMessage:
    def test_extracts_message_field(self):
        resp = MagicMock(spec=requests.Response)
        resp.json.return_value = {"message": "invalid key"}
        resp.reason = "Forbidden"
        assert _extract_message(resp) == "invalid key"

    def test_falls_back_to_reason(self):
        resp = MagicMock(spec=requests.Response)
        resp.json.side_effect = ValueError("no json")
        resp.reason = "Forbidden"
        assert _extract_message(resp) == "Forbidden"
