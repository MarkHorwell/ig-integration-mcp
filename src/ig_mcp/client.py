from __future__ import annotations

import asyncio
import hashlib
import json
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from .cache import PersistentCache
from .config import Settings


class IGApiError(RuntimeError):
    def __init__(self, response: httpx.Response):
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        error_code = (
            payload.get("errorCode", "unknown_error")
            if isinstance(payload, dict)
            else "unknown_error"
        )
        request_id = response.headers.get("X-REQUEST-ID")
        message = f"IG API error {response.status_code}: {error_code}"
        if request_id:
            message += f" (request id: {request_id})"
        super().__init__(message)
        self.status_code = response.status_code
        self.error_code = error_code
        self.request_id = request_id


class IGClient:
    """Minimal asynchronous client that owns an in-memory IG OAuth session."""

    def __init__(
        self, settings: Settings, http_client: httpx.AsyncClient | None = None
    ) -> None:
        self.settings = settings
        self._http = http_client or httpx.AsyncClient(
            base_url=settings.base_url, timeout=30.0
        )
        self._owns_http = http_client is None
        self._access_token: str | None = None
        self._refresh_token: str | None = None
        self._expires_at = 0.0
        self._account_id = settings.account_id
        self._cache = (
            PersistentCache(settings.cache_path) if settings.cache_enabled else None
        )
        self._cache_locks: dict[str, asyncio.Lock] = {}

    async def close(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def request(
        self,
        method: str,
        path: str,
        *,
        version: int,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        cache_ttl_seconds: float | None = None,
        cache_group: str | None = None,
    ) -> dict[str, Any]:
        await self._ensure_session()
        cache_key: str | None = None
        if (
            self._cache is not None
            and method == "GET"
            and cache_ttl_seconds is not None
            and cache_group is not None
        ):
            cache_key = self._response_cache_key(path, version, params)
            cached = await self._cache.get_response(cache_key)
            if cached is not None:
                return cached

            lock = self._cache_locks.setdefault(cache_key, asyncio.Lock())
            async with lock:
                cached = await self._cache.get_response(cache_key)
                if cached is not None:
                    return cached
                result = await self._request_from_ig(
                    method, path, version, params, body
                )
                await self._cache.set_response(
                    cache_key, cache_group, time.time() + cache_ttl_seconds, result
                )
                return result

        result = await self._request_from_ig(method, path, version, params, body)
        if self._cache is not None and method != "GET":
            await self._cache.invalidate_group(self._cache_scope(), "history")
        return result

    async def get_historical_prices(
        self, epic: str, resolution: str, from_date: str, to_date: str
    ) -> dict[str, Any]:
        """Return a price range, requesting only intervals absent from the cache."""
        start = self._parse_utc(from_date)
        end = self._parse_utc(to_date)
        if start >= end:
            raise ValueError("from_date must be earlier than to_date")

        await self._ensure_session()
        if self._cache is None:
            return await self._request_from_ig(
                "GET",
                f"/prices/{epic}/{resolution}",
                3,
                {"from": self._format_utc(start), "to": self._format_utc(end)},
                None,
            )

        scope = self._cache_scope()
        lock_key = f"prices:{scope}:{epic}:{resolution}"
        lock = self._cache_locks.setdefault(lock_key, asyncio.Lock())
        async with lock:
            coverage = await self._cache.price_coverage(scope, epic, resolution)
            missing = self._missing_intervals(start, end, coverage)
            for missing_start, missing_end in missing:
                response = await self._request_from_ig(
                    "GET",
                    f"/prices/{epic}/{resolution}",
                    3,
                    {
                        "from": self._format_utc(missing_start),
                        "to": self._format_utc(missing_end),
                    },
                    None,
                )
                prices = response.get("prices", [])
                if not isinstance(prices, list) or not all(
                    isinstance(price, dict) for price in prices
                ):
                    raise RuntimeError("Unexpected prices response from IG API")
                await self._store_price_coverage(
                    scope,
                    epic,
                    resolution,
                    missing_start,
                    missing_end,
                    prices,
                )

            prices = await self._cache.prices(
                scope,
                epic,
                resolution,
                self._format_utc(start),
                self._format_utc(end),
            )
            return {"prices": prices}

    async def _request_from_ig(
        self,
        method: str,
        path: str,
        version: int,
        params: dict[str, Any] | None,
        body: dict[str, Any] | None,
    ) -> dict[str, Any]:
        response = await self._send(
            method, path, version=version, params=params, body=body
        )
        if response.status_code == 401 and self._refresh_token:
            await self._refresh_session()
            response = await self._send(
                method, path, version=version, params=params, body=body
            )
        return self._decode(response)

    async def _store_price_coverage(
        self,
        scope: str,
        epic: str,
        resolution: str,
        start: datetime,
        end: datetime,
        prices: list[dict[str, Any]],
    ) -> None:
        if self._cache is None:
            return
        current_candle_start = self._current_candle_start(resolution)
        if start < current_candle_start:
            completed_end = min(end, current_candle_start)
            await self._cache.store_prices(
                scope,
                epic,
                resolution,
                self._format_utc(start),
                self._format_utc(completed_end),
                253402300799.0,
                prices,
            )
        if end > current_candle_start:
            volatile_start = max(start, current_candle_start)
            await self._cache.store_prices(
                scope,
                epic,
                resolution,
                self._format_utc(volatile_start),
                self._format_utc(end),
                time.time() + 60,
                prices,
            )

    @staticmethod
    def _current_candle_start(resolution: str) -> datetime:
        now = datetime.now(UTC)
        minute_resolutions = {
            "SECOND": 1 / 60,
            "MINUTE": 1,
            "MINUTE_2": 2,
            "MINUTE_3": 3,
            "MINUTE_5": 5,
            "MINUTE_10": 10,
            "MINUTE_15": 15,
            "MINUTE_30": 30,
            "HOUR": 60,
            "HOUR_2": 120,
            "HOUR_3": 180,
            "HOUR_4": 240,
        }
        if resolution == "DAY":
            return now.replace(hour=0, minute=0, second=0, microsecond=0)
        if resolution == "WEEK":
            day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            return day_start - timedelta(days=day_start.weekday())
        if resolution == "MONTH":
            return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        minutes = minute_resolutions.get(resolution)
        if minutes is None:
            # Unknown resolutions remain short-lived rather than becoming stale.
            return now
        seconds = int(minutes * 60)
        return datetime.fromtimestamp(int(now.timestamp() // seconds) * seconds, UTC)

    def _response_cache_key(
        self, path: str, version: int, params: dict[str, Any] | None
    ) -> str:
        canonical = json.dumps(params or {}, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(
            f"v1|GET|{version}|{path}|{canonical}".encode()
        ).hexdigest()
        return f"{self._cache_scope()}:{digest}"

    def _cache_scope(self) -> str:
        identity = (
            f"{self.settings.environment}|{self.settings.base_url}|"
            f"{self.settings.api_key}|{self.settings.identifier}|{self._account_id}"
        )
        return hashlib.sha256(identity.encode()).hexdigest()

    @staticmethod
    def _parse_utc(value: str) -> datetime:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise ValueError("dates must use ISO-8601 UTC format") from error
        if parsed.tzinfo is None:
            raise ValueError("dates must include a UTC offset")
        return parsed.astimezone(UTC)

    @staticmethod
    def _format_utc(value: datetime) -> str:
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")

    @classmethod
    def _missing_intervals(
        cls,
        start: datetime,
        end: datetime,
        coverage: list[tuple[str, str, float]],
    ) -> list[tuple[datetime, datetime]]:
        intervals = sorted(
            (cls._parse_utc(covered_start), cls._parse_utc(covered_end))
            for covered_start, covered_end, _ in coverage
        )
        missing: list[tuple[datetime, datetime]] = []
        cursor = start
        for covered_start, covered_end in intervals:
            if covered_end <= cursor or covered_start >= end:
                continue
            if covered_start > cursor:
                missing.append((cursor, min(covered_start, end)))
            cursor = max(cursor, covered_end)
            if cursor >= end:
                break
        if cursor < end:
            missing.append((cursor, end))
        return missing

    async def _ensure_session(self) -> None:
        if self._access_token and time.time() < self._expires_at - 10:
            return
        if self._refresh_token:
            await self._refresh_session()
        else:
            await self._login()

    async def _login(self) -> None:
        response = await self._http.post(
            "/session",
            headers=self._base_headers(version=3),
            json={
                "identifier": self.settings.identifier,
                "password": self.settings.password,
            },
        )
        payload = self._decode(response)
        self._store_tokens(payload)
        self._account_id = self._account_id or payload.get("accountId")
        if not self._account_id:
            raise RuntimeError(
                "IG login did not provide an active account ID; set IG_ACCOUNT_ID"
            )

    async def _refresh_session(self) -> None:
        response = await self._http.post(
            "/session/refresh-token",
            headers=self._base_headers(version=1),
            json={"refresh_token": self._refresh_token},
        )
        self._store_tokens(self._decode(response))

    def _store_tokens(self, payload: dict[str, Any]) -> None:
        token = payload.get("oauthToken", payload)
        self._access_token = token.get("access_token")
        self._refresh_token = token.get("refresh_token")
        if not self._access_token or not self._refresh_token:
            raise RuntimeError(
                "IG authentication response did not contain OAuth tokens"
            )
        self._expires_at = time.time() + int(token.get("expires_in", 60))

    async def _send(
        self,
        method: str,
        path: str,
        *,
        version: int,
        params: dict[str, Any] | None,
        body: dict[str, Any] | None,
    ) -> httpx.Response:
        headers = self._base_headers(version=version)
        headers["Authorization"] = f"Bearer {self._access_token}"
        headers["IG-ACCOUNT-ID"] = self._account_id or ""
        return await self._http.request(
            method, path, headers=headers, params=params, json=body
        )

    def _base_headers(self, *, version: int) -> dict[str, str]:
        return {
            "X-IG-API-KEY": self.settings.api_key,
            "Version": str(version),
            "Accept": "application/json; charset=UTF-8",
            "Content-Type": "application/json",
        }

    @staticmethod
    def _decode(response: httpx.Response) -> dict[str, Any]:
        if response.is_error:
            raise IGApiError(response)
        if not response.content:
            return {}
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError("Unexpected non-object response from IG API")
        return payload
