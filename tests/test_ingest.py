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
    _equities_master_to_df,
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

    def test_retry_after_header_honoured(self, loader):
        """429 with Retry-After: 5 → sleeps 5s, not the default 1s backoff."""
        rate_resp = MagicMock(spec=requests.Response)
        rate_resp.status_code = 429
        rate_resp.headers = {"Retry-After": "5"}
        rate_resp.json.return_value = {}
        rate_resp.raise_for_status = MagicMock()

        ok_resp = MagicMock(spec=requests.Response)
        ok_resp.status_code = 200
        ok_resp.json.return_value = {"data": []}
        ok_resp.raise_for_status = MagicMock()

        sleep_calls: list[float] = []
        with patch("requests.get", side_effect=[rate_resp, ok_resp]):
            with patch("time.sleep", side_effect=lambda s: sleep_calls.append(s)):
                loader.fetch_daily(date(2026, 1, 5), date(2026, 1, 5), tickers=["72030"])

        assert any(s >= 5.0 for s in sleep_calls), f"Expected ≥5s sleep, got {sleep_calls}"


# ---------------------------------------------------------------------------
# JQuantsLoader — incremental cache
# ---------------------------------------------------------------------------

class TestJQuantsLoaderCache:
    """Verify that cache_path enables incremental fetch."""

    @pytest.fixture
    def loader_with_cache(self, tmp_path, monkeypatch):
        monkeypatch.delenv("JQUANTS_API_KEY", raising=False)
        monkeypatch.delenv("JQUANTS_APIKEY", raising=False)
        cache_file = tmp_path / "bars_cache.parquet"
        return JQuantsLoader(api_key="dummy", cache_path=cache_file), cache_file

    def _mock_resp(self, body: dict) -> MagicMock:
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 200
        resp.json.return_value = body
        resp.reason = "OK"
        resp.raise_for_status = MagicMock()
        return resp

    def _record(self, d: str, ticker: str = "72030") -> dict:
        return {"Date": d, "Code": ticker, "O": 100, "H": 105, "L": 99, "C": 103, "Vo": 1000}

    def test_cold_start_creates_cache(self, loader_with_cache):
        """First call (no cache file) fetches from API and writes cache."""
        loader, cache_file = loader_with_cache
        resp = self._mock_resp({"data": [self._record("2026-01-05")]})
        with patch("requests.get", return_value=resp):
            with patch("time.sleep"):
                df = loader.fetch_daily(date(2026, 1, 5), date(2026, 1, 5))
        assert len(df) == 1
        assert cache_file.exists()

    def test_warm_fetches_only_new_dates(self, loader_with_cache):
        """Cache covers day 1; only day 2 should be fetched from API."""
        loader, cache_file = loader_with_cache
        import pandas as pd
        # Pre-seed cache with day 1
        from datetime import date as _date
        seed = pd.DataFrame([{
            "as_of": _date(2026, 1, 5), "ticker": "72030",
            "open": 100.0, "high": 105.0, "low": 99.0, "close": 103.0, "volume": 1000.0,
        }])
        seed.to_parquet(cache_file, index=False)

        resp = self._mock_resp({"data": [self._record("2026-01-06")]})
        with patch("requests.get", return_value=resp) as mock_get:
            with patch("time.sleep"):
                df = loader.fetch_daily(date(2026, 1, 5), date(2026, 1, 6))

        assert mock_get.call_count == 1  # only 1 API call for the new day
        assert len(df) == 2              # both days in result

    def test_fresh_cache_needs_no_api_call(self, loader_with_cache):
        """Cache covers full range → zero API calls."""
        loader, cache_file = loader_with_cache
        import pandas as pd
        from datetime import date as _date
        seed = pd.DataFrame([
            {"as_of": _date(2026, 1, 5), "ticker": "72030",
             "open": 100.0, "high": 105.0, "low": 99.0, "close": 103.0, "volume": 1000.0},
            {"as_of": _date(2026, 1, 6), "ticker": "72030",
             "open": 103.0, "high": 108.0, "low": 102.0, "close": 107.0, "volume": 1200.0},
        ])
        seed.to_parquet(cache_file, index=False)

        with patch("requests.get") as mock_get:
            df = loader.fetch_daily(date(2026, 1, 5), date(2026, 1, 6))

        assert mock_get.call_count == 0  # no API calls
        assert len(df) == 2

    def test_cache_corruption_recovery(self, loader_with_cache):
        """Corrupt cache is renamed to .bak; full API refetch is triggered."""
        loader, cache_file = loader_with_cache
        cache_file.write_bytes(b"not a valid parquet file")

        resp = self._mock_resp({"data": [self._record("2026-01-05")]})
        with patch("requests.get", return_value=resp) as mock_get:
            with patch("time.sleep"):
                df = loader.fetch_daily(date(2026, 1, 5), date(2026, 1, 5))

        bak = cache_file.with_suffix(".parquet.bak")
        assert bak.exists(), ".parquet.bak must be created from corrupted cache"
        assert mock_get.call_count == 1, "Full API refetch must follow corruption"
        assert len(df) == 1


# ---------------------------------------------------------------------------
# JQuantsLoader — equities master fetch
# ---------------------------------------------------------------------------

class TestJQuantsLoaderMaster:
    @pytest.fixture
    def loader(self, monkeypatch):
        monkeypatch.delenv("JQUANTS_API_KEY", raising=False)
        monkeypatch.delenv("JQUANTS_APIKEY", raising=False)
        return JQuantsLoader(api_key="dummy-key")

    @pytest.fixture
    def loader_with_cache(self, tmp_path, monkeypatch):
        monkeypatch.delenv("JQUANTS_API_KEY", raising=False)
        monkeypatch.delenv("JQUANTS_APIKEY", raising=False)
        cache_file = tmp_path / "equities_master.parquet"
        return JQuantsLoader(api_key="dummy"), cache_file

    def _mock_resp(self, body: dict) -> MagicMock:
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 200
        resp.json.return_value = body
        resp.reason = "OK"
        resp.raise_for_status = MagicMock()
        return resp

    def test_demo_loader_fetch_master_returns_names(self):
        """DemoLoader.fetch_master() returns synthetic company names for fixture tickers."""
        from inga_quant.pipeline.ingest import DemoLoader
        loader = DemoLoader(BARS_PATH)
        df = loader.fetch_master()
        assert isinstance(df, pd.DataFrame)
        assert "ticker" in df.columns
        assert "name" in df.columns
        assert len(df) > 0, "DemoLoader must return non-empty master with company names"
        assert df["name"].notna().all(), "All names must be non-null"
        assert df["name"].str.endswith(" Corp").all(), "Demo names must follow '<TICKER> Corp' pattern"

    def test_cold_fetch_creates_cache(self, loader_with_cache):
        """First call fetches from API and writes cache parquet."""
        loader, cache_file = loader_with_cache
        body = {"data": [{"Code": "72030", "CompanyName": "トヨタ自動車"}]}
        resp = self._mock_resp(body)
        with patch("requests.get", return_value=resp):
            with patch("time.sleep"):
                df = loader.fetch_master(cache_path=cache_file)
        assert len(df) == 1
        assert df["ticker"].iloc[0] == "72030"
        assert df["name"].iloc[0] == "トヨタ自動車"
        assert cache_file.exists()

    def test_warm_cache_no_api_call(self, loader_with_cache):
        """Cache < 24h old → no API call."""
        loader, cache_file = loader_with_cache
        seed = pd.DataFrame([{"ticker": "72030", "name": "トヨタ自動車"}])
        seed.to_parquet(cache_file, index=False)
        with patch("requests.get") as mock_get:
            df = loader.fetch_master(cache_path=cache_file)
        assert mock_get.call_count == 0
        assert len(df) == 1

    def test_exception_returns_empty_df(self, loader):
        """Network error → returns empty df, does not raise."""
        with patch("requests.get", side_effect=ConnectionError("network down")):
            df = loader.fetch_master()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0


# ---------------------------------------------------------------------------
# _equities_master_to_df — unit tests
# ---------------------------------------------------------------------------

class TestEquitiesMasterToDf:
    """Unit tests for the _equities_master_to_df parsing helper."""

    def test_coname_preferred_over_companyname(self):
        """CoName takes priority when both CoName and CompanyName are present."""
        records = [{"Code": "72030", "CoName": "トヨタ自動車", "CompanyName": "Toyota Motor"}]
        df = _equities_master_to_df(records)
        assert df.loc[0, "name"] == "トヨタ自動車"

    def test_companyname_fallback_when_no_coname(self):
        """CompanyName is used when CoName is absent."""
        records = [{"Code": "72030", "CompanyName": "Toyota Motor"}]
        df = _equities_master_to_df(records)
        assert df.loc[0, "name"] == "Toyota Motor"

    def test_name_fallback(self):
        """Name field is used when CoName and CompanyName are absent."""
        records = [{"Code": "72030", "Name": "Toyota"}]
        df = _equities_master_to_df(records)
        assert df.loc[0, "name"] == "Toyota"

    def test_conamen_en_last_resort(self):
        """CoNameEn is used as last resort when all Japanese name fields are absent."""
        records = [{"Code": "72030", "CoNameEn": "TOYOTA MOTOR CORP"}]
        df = _equities_master_to_df(records)
        assert df.loc[0, "name"] == "TOYOTA MOTOR CORP"

    def test_name_none_when_no_name_field(self):
        """name is None when none of the candidate fields are present."""
        records = [{"Code": "72030", "SomeOtherField": "xyz"}]
        df = _equities_master_to_df(records)
        assert df.loc[0, "ticker"] == "72030"
        assert df.loc[0, "name"] is None

    def test_ticker_preserved_as_string(self):
        """Ticker codes like '285A0' must remain strings, not be coerced to int/NaN."""
        records = [{"Code": "285A0", "CoName": "Some Corp"}]
        df = _equities_master_to_df(records)
        assert df.loc[0, "ticker"] == "285A0"
        assert isinstance(df.loc[0, "ticker"], str)

    def test_numeric_code_also_string(self):
        """Even purely numeric codes like 72030 are returned as strings."""
        records = [{"Code": 72030, "CoName": "Toyota"}]
        df = _equities_master_to_df(records)
        assert df.loc[0, "ticker"] == "72030"
        assert isinstance(df.loc[0, "ticker"], str)

    def test_empty_records_returns_empty_df(self):
        """Empty input returns a DataFrame with required columns and zero rows."""
        df = _equities_master_to_df([])
        assert isinstance(df, pd.DataFrame)
        assert "ticker" in df.columns
        assert "name" in df.columns
        assert len(df) == 0

    def test_records_without_code_skipped(self):
        """Records without a 'Code' field are silently skipped."""
        records = [
            {"CoName": "No Code Corp"},
            {"Code": "72030", "CoName": "Toyota"},
        ]
        df = _equities_master_to_df(records)
        assert len(df) == 1
        assert df.loc[0, "ticker"] == "72030"

    def test_duplicates_deduplicated_keep_last(self):
        """Duplicate Code entries are deduplicated; last occurrence wins."""
        records = [
            {"Code": "72030", "CoName": "First"},
            {"Code": "72030", "CoName": "Second"},
        ]
        df = _equities_master_to_df(records)
        assert len(df) == 1
        assert df.loc[0, "name"] == "Second"

    def test_no_keyerror_on_unknown_fields(self):
        """No KeyError is raised when the record contains only unknown fields."""
        records = [{"Code": "72030", "UnknownField": "whatever"}]
        df = _equities_master_to_df(records)  # must not raise
        assert df.loc[0, "ticker"] == "72030"
        assert df.loc[0, "name"] is None

    def test_whitespace_stripped_from_name(self):
        """Leading/trailing whitespace in name values is stripped."""
        records = [{"Code": "72030", "CoName": "  Toyota  "}]
        df = _equities_master_to_df(records)
        assert df.loc[0, "name"] == "Toyota"

    def test_empty_string_name_falls_through_to_next_candidate(self):
        """An empty CoName is treated as absent; the next candidate is tried."""
        records = [{"Code": "72030", "CoName": "", "CompanyName": "Toyota Motor"}]
        df = _equities_master_to_df(records)
        assert df.loc[0, "name"] == "Toyota Motor"


class TestJQuantsLoaderMasterParsing:
    """Integration tests: fetch_master uses _equities_master_to_df via the API path."""

    @pytest.fixture
    def loader(self, monkeypatch):
        monkeypatch.delenv("JQUANTS_API_KEY", raising=False)
        monkeypatch.delenv("JQUANTS_APIKEY", raising=False)
        return JQuantsLoader(api_key="dummy-key")

    def _mock_resp(self, body: dict) -> MagicMock:
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 200
        resp.json.return_value = body
        resp.reason = "OK"
        resp.raise_for_status = MagicMock()
        return resp

    def test_coname_field_parsed_correctly(self, loader):
        """fetch_master correctly reads CoName (the real API field name)."""
        body = {"data": [{"Code": "72030", "CoName": "トヨタ自動車"}]}
        with patch("requests.get", return_value=self._mock_resp(body)):
            with patch("time.sleep"):
                df = loader.fetch_master()
        assert len(df) == 1
        assert df.loc[0, "ticker"] == "72030"
        assert df.loc[0, "name"] == "トヨタ自動車"

    def test_alphanumeric_ticker_preserved(self, loader):
        """Tickers like '285A0' are preserved as strings through the API path."""
        body = {"data": [{"Code": "285A0", "CoName": "Some Fintech Corp"}]}
        with patch("requests.get", return_value=self._mock_resp(body)):
            with patch("time.sleep"):
                df = loader.fetch_master()
        assert df.loc[0, "ticker"] == "285A0"
        assert isinstance(df.loc[0, "ticker"], str)

    def test_no_keyerror_when_name_field_absent(self, loader):
        """fetch_master must not raise when the response has no name-like field."""
        body = {"data": [{"Code": "72030", "Date": "2026-02-20"}]}
        with patch("requests.get", return_value=self._mock_resp(body)):
            with patch("time.sleep"):
                df = loader.fetch_master()  # must not raise
        assert df.loc[0, "ticker"] == "72030"
        assert df.loc[0, "name"] is None


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
