"""Data ingestion: DataLoader ABC, JQuantsLoader (V2 API key), DemoLoader (fixture)."""
from __future__ import annotations

import logging
import os
import time
from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from inga_quant.utils.io import load_bars

logger = logging.getLogger(__name__)

_JQUANTS_BASE = "https://api.jquants.com"
_MAX_RETRIES = 3
_BACKOFF_BASE = 1.0
_BACKOFF_CAP = 30.0

# J-Quants V2 column → internal name
_COL_MAP = {
    "Date": "as_of",
    "Code": "ticker",
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Volume": "volume",
    "AdjustmentClose": "adj_close",
    "AdjustmentFactor": "adj_factor",
}

_EMPTY_BARS = pd.DataFrame(
    columns=["as_of", "ticker", "open", "high", "low", "close", "volume"]
)


class JQuantsAuthError(RuntimeError):
    """Raised when API key is invalid or expired."""


class DataLoader(ABC):
    """Abstract interface for bar data sources."""

    @abstractmethod
    def fetch_daily(
        self,
        start_date: date,
        end_date: date,
        tickers: list[str] | None = None,
    ) -> pd.DataFrame:
        """Return bars_daily DataFrame with columns matching SPEC §1.1."""


class DemoLoader(DataLoader):
    """Loads fixture data for demo/test use — no network calls."""

    def __init__(self, bars_path: str | Path) -> None:
        self._bars_path = Path(bars_path)

    def fetch_daily(
        self,
        start_date: date,
        end_date: date,
        tickers: list[str] | None = None,
    ) -> pd.DataFrame:
        df = load_bars(self._bars_path, as_of=str(end_date))
        df = df[df["as_of"] >= start_date]
        if tickers:
            df = df[df["ticker"].isin(tickers)]
        return df.reset_index(drop=True)


class JQuantsLoader(DataLoader):
    """
    J-Quants V2 API client.

    Authentication: ``x-api-key: <api_key>`` header (no token refresh).
    API key is read from env ``JQUANTS_API_KEY`` (or compat ``JQUANTS_APIKEY``).
    Retry: exponential backoff on 429 / 5xx. 403 → immediate failure with guidance.
    """

    def __init__(self, api_key: str | None = None) -> None:
        self._api_key = (
            api_key
            or os.environ.get("JQUANTS_API_KEY")
            or os.environ.get("JQUANTS_APIKEY")
            or ""
        )
        if not self._api_key:
            raise JQuantsAuthError(
                "J-Quants APIキーが未設定です。"
                " .env に JQUANTS_API_KEY=<key> を設定してください。"
            )

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        return {"x-api-key": self._api_key}

    def _get(self, path: str, params: dict[str, str] | None = None) -> dict[str, Any]:
        url = f"{_JQUANTS_BASE}{path}"
        return self._request_with_retry(url, params=params)

    def _request_with_retry(
        self,
        url: str,
        params: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        delay = _BACKOFF_BASE
        last_exc: Exception | None = None

        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                resp = requests.get(
                    url,
                    headers=self._headers(),
                    params=params,
                    timeout=30,
                )

                # 403 → invalid key, no retry
                if resp.status_code == 403:
                    msg = _extract_message(resp)
                    raise JQuantsAuthError(
                        f"J-Quants 403: {msg} "
                        "— APIキーが無効か期限切れです。ダッシュボードで再発行して .env を更新してください。"
                    )

                # 429 → rate limit, backoff
                if resp.status_code == 429:
                    logger.warning("J-Quants rate limited (attempt %d). retry in %.0fs", attempt, delay)
                    time.sleep(delay)
                    delay = min(delay * 2, _BACKOFF_CAP)
                    continue

                # 5xx → transient, backoff
                if resp.status_code >= 500:
                    logger.warning("J-Quants server error %d (attempt %d)", resp.status_code, attempt)
                    time.sleep(delay)
                    delay = min(delay * 2, _BACKOFF_CAP)
                    last_exc = requests.HTTPError(response=resp)
                    continue

                resp.raise_for_status()
                return resp.json()

            except JQuantsAuthError:
                raise
            except requests.RequestException as exc:
                last_exc = exc
                logger.warning(
                    "J-Quants request error (attempt %d/%d): %s",
                    attempt, _MAX_RETRIES, type(exc).__name__,
                )
                time.sleep(delay)
                delay = min(delay * 2, _BACKOFF_CAP)

        raise RuntimeError(f"J-Quants: {_MAX_RETRIES}回リトライ失敗 ({url})") from last_exc

    # ------------------------------------------------------------------
    # Smoke check
    # ------------------------------------------------------------------

    def check_connectivity(self) -> bool:
        """
        Smoke test: call /v2/equities/master for one ticker.
        Logs one line. Returns True on success, raises JQuantsAuthError on 403.
        """
        try:
            self._get("/v2/equities/master", params={"code": "72030"})
            logger.info("J-Quants API: 接続OK")
            return True
        except JQuantsAuthError:
            raise
        except Exception as exc:
            logger.warning("J-Quants API: 接続失敗 (%s)", type(exc).__name__)
            return False

    # ------------------------------------------------------------------
    # Data fetch
    # ------------------------------------------------------------------

    def fetch_daily(
        self,
        start_date: date,
        end_date: date,
        tickers: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Fetch daily OHLCV bars from J-Quants V2 /v2/prices/daily_quotes.
        Handles pagination automatically.
        """
        params: dict[str, str] = {
            "date_from": start_date.strftime("%Y-%m-%d"),
            "date_to": end_date.strftime("%Y-%m-%d"),
        }
        # Single-ticker shortcut
        if tickers and len(tickers) == 1:
            params["code"] = tickers[0]

        all_records: list[dict] = []
        pagination_key: str | None = None

        while True:
            if pagination_key:
                params["pagination_key"] = pagination_key
            resp = self._get("/v2/prices/daily_quotes", params=params)
            records = resp.get("daily_quotes", [])
            all_records.extend(records)
            pagination_key = resp.get("pagination_key")
            if not pagination_key:
                break

        if not all_records:
            logger.info("J-Quants: 取得レコード 0件 (%s–%s)", start_date, end_date)
            return _EMPTY_BARS.copy()

        df = pd.DataFrame(all_records)
        df = df.rename(columns={k: v for k, v in _COL_MAP.items() if k in df.columns})
        df["as_of"] = pd.to_datetime(df["as_of"]).dt.date
        df["ticker"] = df["ticker"].astype(str)

        if tickers and len(tickers) > 1:
            df = df[df["ticker"].isin(tickers)]

        logger.info(
            "J-Quants: %d件取得 (%d銘柄, %s–%s)",
            len(df), df["ticker"].nunique(), start_date, end_date,
        )
        return df.reset_index(drop=True)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _extract_message(resp: requests.Response) -> str:
    """Extract 'message' from JSON body; fall back to HTTP status text. Never raises."""
    try:
        data = resp.json()
        if isinstance(data, dict) and "message" in data:
            return str(data["message"])
    except Exception:
        pass
    return getattr(resp, "reason", None) or str(resp.status_code)
