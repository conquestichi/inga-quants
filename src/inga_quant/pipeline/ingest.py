"""Data ingestion: DataLoader ABC, JQuantsLoader (V2 API key), DemoLoader (fixture).

V2 API constraint
-----------------
/v2/equities/bars/daily requires either ``code`` or ``date`` in every request.
There is no date-range-for-all-tickers mode, so all-market fetches iterate by
business day (one HTTP request per day).

Caching strategy (timer/cron)
------------------------------
Set ``cache_path`` on JQuantsLoader to enable an incremental parquet cache:
- Cold start: fetches entire date range (~252 calls for 365-day lookback), saves cache.
- Warm (daily cron): reads cache, fetches only new business days (typically 1–3 calls).
This makes daily cron nearly free after the first run.

Rate limit defence
------------------
- 429: honour Retry-After header if present, else exponential backoff.
- 0.2s inter-request sleep between consecutive HTTP calls in date/ticker loops.
- 5xx: exponential backoff, max 3 retries.
- 403: immediate failure (invalid key or endpoint).
"""
from __future__ import annotations

import logging
import os
import time
from abc import ABC, abstractmethod
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from inga_quant.pipeline.trade_date import is_business_day
from inga_quant.utils.io import load_bars

logger = logging.getLogger(__name__)

_JQUANTS_BASE = "https://api.jquants.com"
_MAX_RETRIES = 3
_BACKOFF_BASE = 1.0
_BACKOFF_CAP = 30.0
_REQUEST_INTERVAL = 0.2   # seconds between consecutive API calls (pacing)

# J-Quants V2 /equities/bars/daily abbreviated column → internal name
_COL_MAP = {
    "Date": "as_of",
    "Code": "ticker",
    "O": "open",
    "H": "high",
    "L": "low",
    "C": "close",
    "Vo": "volume",
    "AdjC": "adj_close",
    "AdjFactor": "adj_factor",
}

# J-Quants /v2/equities/master column → internal name
_MASTER_COL_MAP = {
    "Code": "ticker",
    "CompanyName": "name",
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

    def fetch_master(self, cache_path: Path | None = None) -> pd.DataFrame:
        """Return equities master DataFrame with 'ticker' and 'name' columns.

        Default implementation returns an empty DataFrame (no API access).
        Override in subclasses that support master data fetching.
        """
        return pd.DataFrame(columns=["ticker", "name"])


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

    Parameters
    ----------
    api_key:
        Override env ``JQUANTS_API_KEY`` / ``JQUANTS_APIKEY``.
    cache_path:
        Optional path to a parquet file used as an incremental bars cache.
        When set and ``tickers=None``, only missing business days are fetched
        from the API; all other data is served from the cache.
        Example: ``cache_path=Path("data/daily/bars_cache.parquet")``
    """

    def __init__(
        self,
        api_key: str | None = None,
        cache_path: str | Path | None = None,
    ) -> None:
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
        self._cache_path = Path(cache_path) if cache_path else None

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

                # 403 → invalid key or bad endpoint, no retry
                if resp.status_code == 403:
                    msg = _extract_message(resp)
                    raise JQuantsAuthError(
                        f"J-Quants 403: {msg} "
                        "— APIキーが無効か期限切れです。ダッシュボードで再発行して .env を更新してください。"
                    )

                # 429 → rate limit; honour Retry-After header if present
                if resp.status_code == 429:
                    raw_after = resp.headers.get("Retry-After", "")
                    wait = float(raw_after) if raw_after.strip().isdigit() else delay
                    logger.warning(
                        "J-Quants rate limited (attempt %d). retry in %.0fs", attempt, wait
                    )
                    time.sleep(wait)
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
    # Data fetch — internal helpers
    # ------------------------------------------------------------------

    def _fetch_all_pages(self, params: dict[str, str]) -> list[dict]:
        """
        Fetch all pages for the given base params from /v2/equities/bars/daily.
        Handles pagination_key automatically.
        """
        all_records: list[dict] = []
        p = dict(params)
        while True:
            resp = self._get("/v2/equities/bars/daily", params=p)
            records = resp.get("data", [])
            all_records.extend(records)
            pkey = resp.get("pagination_key")
            if not pkey:
                break
            p["pagination_key"] = pkey
            time.sleep(_REQUEST_INTERVAL)   # pace between pagination calls
        return all_records

    def _fetch_api_range(
        self,
        start_date: date,
        end_date: date,
        tickers: list[str] | None = None,
    ) -> pd.DataFrame:
        """Raw API fetch — no cache layer."""
        if tickers:
            # Per-ticker mode: code + from + to
            all_records: list[dict] = []
            for i, ticker in enumerate(tickers):
                if i > 0:
                    time.sleep(_REQUEST_INTERVAL)   # pace between tickers
                params: dict[str, str] = {
                    "code": ticker,
                    "from": start_date.isoformat(),
                    "to": end_date.isoformat(),
                }
                all_records.extend(self._fetch_all_pages(params))
        else:
            # All-market mode: one request per business day
            all_records = []
            current = start_date
            n_days = 0
            while current <= end_date:
                if is_business_day(current):
                    if n_days > 0:
                        time.sleep(_REQUEST_INTERVAL)   # pace between dates
                    records = self._fetch_all_pages({"date": current.isoformat()})
                    all_records.extend(records)
                    n_days += 1
                current += timedelta(days=1)
            logger.info(
                "J-Quants: %d営業日分リクエスト完了 (%s–%s)", n_days, start_date, end_date
            )

        if not all_records:
            logger.info("J-Quants: 取得レコード 0件 (%s–%s)", start_date, end_date)
            return _EMPTY_BARS.copy()

        df = pd.DataFrame(all_records)
        df = df.rename(columns={k: v for k, v in _COL_MAP.items() if k in df.columns})
        df["as_of"] = pd.to_datetime(df["as_of"]).dt.date
        df["ticker"] = df["ticker"].astype(str)

        logger.info(
            "J-Quants: %d件取得 (%d銘柄, %s–%s)",
            len(df), df["ticker"].nunique(), start_date, end_date,
        )
        return df.reset_index(drop=True)

    def _fetch_with_cache(self, start_date: date, end_date: date) -> pd.DataFrame:
        """
        Incremental fetch backed by a parquet cache file.

        On cold start (no cache): fetches full [start_date, end_date] and saves.
        On warm start: reads cache, fetches only business days after the last
        cached date, appends to cache, and returns the requested slice.
        If the cache doesn't reach back to start_date, the missing early range
        is fetched and prepended.
        """
        cache = _EMPTY_BARS.copy()

        if self._cache_path.exists():
            try:
                raw = pd.read_parquet(self._cache_path)
                if not raw.empty:
                    raw["as_of"] = pd.to_datetime(raw["as_of"]).dt.date
                    cache = raw
            except Exception:
                # Rename corrupt file for post-mortem; trigger cold-start refetch
                bak = self._cache_path.with_suffix(".parquet.bak")
                try:
                    self._cache_path.rename(bak)
                except Exception:
                    pass
                logger.warning(
                    "キャッシュ破損 → %s に退避しフルフェッチします", bak.name
                )
                cache = _EMPTY_BARS.copy()

        new_frames: list[pd.DataFrame] = []

        if cache.empty:
            # Cold start
            new_frames.append(self._fetch_api_range(start_date, end_date))
        else:
            cache_min: date = cache["as_of"].min()
            cache_max: date = cache["as_of"].max()

            # Fill gap at the tail (most common: new trading day)
            tail_start = cache_max + timedelta(days=1)
            if tail_start <= end_date:
                new_frames.append(self._fetch_api_range(tail_start, end_date))

            # Fill gap at the head (rare: train_days extended or cache rebuilt)
            if cache_min > start_date:
                logger.warning(
                    "キャッシュ先頭 %s > 要求開始 %s — 差分を先頭に追加します", cache_min, start_date
                )
                new_frames.append(self._fetch_api_range(start_date, cache_min - timedelta(days=1)))

        if new_frames:
            cache = pd.concat([cache] + new_frames, ignore_index=True)
            cache = cache.drop_duplicates(subset=["as_of", "ticker"], keep="last")
            cache = cache.sort_values(["as_of", "ticker"]).reset_index(drop=True)
            total_new = sum(len(f) for f in new_frames)
            tmp = self._cache_path.with_suffix(".parquet.tmp")
            try:
                self._cache_path.parent.mkdir(parents=True, exist_ok=True)
                cache.to_parquet(tmp, index=False)
                tmp.replace(self._cache_path)   # atomic on POSIX (same filesystem)
                logger.info(
                    "J-Quants: キャッシュ保存 (+%d行, 計%d行) → %s",
                    total_new, len(cache), self._cache_path.name,
                )
            except Exception:
                logger.warning("キャッシュ保存失敗 — tmpファイルを削除します")
                tmp.unlink(missing_ok=True)
        else:
            logger.info(
                "J-Quants: キャッシュ新鮮 (最終日=%s)", cache["as_of"].max()
            )

        # Return requested slice
        if cache.empty:
            return _EMPTY_BARS.copy()
        mask = (cache["as_of"] >= start_date) & (cache["as_of"] <= end_date)
        return cache[mask].reset_index(drop=True)

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def fetch_daily(
        self,
        start_date: date,
        end_date: date,
        tickers: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Fetch daily OHLCV bars from J-Quants V2 /v2/equities/bars/daily.

        When ``cache_path`` is set and ``tickers=None``, uses the incremental
        parquet cache — only new business days are fetched from the API.
        """
        if self._cache_path is not None and not tickers:
            return self._fetch_with_cache(start_date, end_date)
        return self._fetch_api_range(start_date, end_date, tickers)

    def fetch_master(self, cache_path: Path | None = None) -> pd.DataFrame:
        """
        Fetch equities master from /v2/equities/master and cache to parquet.

        Returns a DataFrame with 'ticker' (Code) and 'name' (CompanyName) columns.
        Cache is considered fresh for 24 hours.
        On any error, logs a warning and returns an empty DataFrame so the
        pipeline can continue without company names.
        """
        # Use cached version if fresh (< 24 h)
        if cache_path and cache_path.exists():
            try:
                age_h = (time.time() - cache_path.stat().st_mtime) / 3600
                if age_h < 24:
                    cached = pd.read_parquet(cache_path)
                    if not cached.empty and "ticker" in cached.columns:
                        logger.info("マスターキャッシュ使用 (%.1fh前)", age_h)
                        return cached
            except Exception:
                pass

        # Fetch from API with pagination
        try:
            all_records: list[dict] = []
            params: dict[str, str] = {}
            while True:
                resp = self._get("/v2/equities/master", params=params)
                all_records.extend(resp.get("data", []))
                pkey = resp.get("pagination_key")
                if not pkey:
                    break
                params["pagination_key"] = pkey
                time.sleep(_REQUEST_INTERVAL)

            if not all_records:
                logger.warning("マスター: 取得レコード 0件")
                return pd.DataFrame(columns=["ticker", "name"])

            df = pd.DataFrame(all_records)
            df = df.rename(columns={k: v for k, v in _MASTER_COL_MAP.items() if k in df.columns})
            df = df[["ticker", "name"]].drop_duplicates(subset=["ticker"])

            if cache_path:
                tmp = cache_path.with_suffix(".parquet.tmp")
                try:
                    cache_path.parent.mkdir(parents=True, exist_ok=True)
                    df.to_parquet(tmp, index=False)
                    tmp.replace(cache_path)
                    logger.info("マスター保存: %d銘柄 → %s", len(df), cache_path.name)
                except Exception:
                    logger.warning("マスターキャッシュ保存失敗")
                    tmp.unlink(missing_ok=True)

            return df

        except JQuantsAuthError:
            raise
        except Exception as exc:
            logger.warning("マスター取得失敗 (%s) — 社名なしで続行", type(exc).__name__)
            return pd.DataFrame(columns=["ticker", "name"])


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
