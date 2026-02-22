"""Data ingestion: DataLoader ABC, JQuantsLoader (production), DemoLoader (fixture)."""
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

_JQUANTS_BASE = "https://api.jquants.com/v1"
_MAX_RETRIES = 3
_BACKOFF_BASE = 1.0
_BACKOFF_CAP = 30.0


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
    J-Quants API client (Standard plan).

    Authentication: refresh token → ID token (refreshed automatically).
    Retry: exponential backoff on transient errors.
    """

    def __init__(
        self,
        mail_address: str | None = None,
        password: str | None = None,
    ) -> None:
        self._mail = mail_address or os.environ.get("JQUANTS_MAIL_ADDRESS", "")
        self._password = password or os.environ.get("JQUANTS_PASSWORD", "")
        self._refresh_token: str | None = None
        self._id_token: str | None = None

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def _get_refresh_token(self) -> str:
        resp = self._post_with_retry(
            f"{_JQUANTS_BASE}/token/auth_user",
            json={"mailaddress": self._mail, "password": self._password},
            authenticated=False,
        )
        return resp["refreshToken"]

    def _get_id_token(self, refresh_token: str) -> str:
        resp = self._post_with_retry(
            f"{_JQUANTS_BASE}/token/auth_refresh",
            params={"refreshtoken": refresh_token},
            authenticated=False,
        )
        return resp["idToken"]

    def _ensure_auth(self) -> None:
        if self._refresh_token is None:
            self._refresh_token = self._get_refresh_token()
        if self._id_token is None:
            self._id_token = self._get_id_token(self._refresh_token)

    # ------------------------------------------------------------------
    # HTTP helpers with retry / backoff
    # ------------------------------------------------------------------

    def _post_with_retry(
        self,
        url: str,
        *,
        json: dict[str, Any] | None = None,
        params: dict[str, str] | None = None,
        authenticated: bool = True,
    ) -> dict[str, Any]:
        headers = {}
        if authenticated and self._id_token:
            headers["Authorization"] = f"Bearer {self._id_token}"
        return self._request_with_retry("POST", url, json=json, params=params, headers=headers)

    def _get_with_retry(self, url: str, params: dict[str, str] | None = None) -> dict[str, Any]:
        self._ensure_auth()
        headers = {"Authorization": f"Bearer {self._id_token}"}
        return self._request_with_retry("GET", url, params=params, headers=headers)

    def _request_with_retry(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        delay = _BACKOFF_BASE
        last_exc: Exception | None = None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                resp = requests.request(method, url, timeout=30, **kwargs)
                if resp.status_code == 429:
                    logger.warning("Rate limited (attempt %d/%d). Sleeping %.1fs", attempt, _MAX_RETRIES, delay)
                    time.sleep(delay)
                    delay = min(delay * 2, _BACKOFF_CAP)
                    continue
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as exc:
                last_exc = exc
                err_type = _classify_error(exc)
                logger.warning("Request failed [%s] (attempt %d/%d): %s", err_type, attempt, _MAX_RETRIES, exc)
                if err_type == "auth_error":
                    # Re-authenticate
                    self._id_token = None
                    self._refresh_token = None
                    self._ensure_auth()
                time.sleep(delay)
                delay = min(delay * 2, _BACKOFF_CAP)
        raise RuntimeError(f"All {_MAX_RETRIES} retries failed for {url}: {last_exc}") from last_exc

    # ------------------------------------------------------------------
    # Data fetch
    # ------------------------------------------------------------------

    def fetch_daily(
        self,
        start_date: date,
        end_date: date,
        tickers: list[str] | None = None,
    ) -> pd.DataFrame:
        """Fetch daily OHLCV bars from J-Quants API."""
        self._ensure_auth()
        params: dict[str, str] = {
            "dateFrom": start_date.strftime("%Y%m%d"),
            "dateTo": end_date.strftime("%Y%m%d"),
        }
        resp = self._get_with_retry(f"{_JQUANTS_BASE}/prices/daily_quotes", params=params)
        records = resp.get("daily_quotes", [])
        if not records:
            return pd.DataFrame(columns=["as_of", "ticker", "open", "high", "low", "close", "volume"])
        df = pd.DataFrame(records)
        # Normalise column names from J-Quants response
        col_map = {
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
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
        df["as_of"] = pd.to_datetime(df["as_of"]).dt.date
        df["ticker"] = df["ticker"].astype(str)
        if tickers:
            df = df[df["ticker"].isin(tickers)]
        return df.reset_index(drop=True)


def _classify_error(exc: requests.RequestException) -> str:
    if isinstance(exc, requests.HTTPError):
        code = exc.response.status_code if exc.response is not None else 0
        if code in (401, 403):
            return "auth_error"
        if code == 429:
            return "rate_limit"
        return "data_error"
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return "connection_error"
    return "data_error"
