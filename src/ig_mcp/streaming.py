from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from lightstreamer.client import LightstreamerClient, Subscription, SubscriptionListener

from .client import StreamingCredentials

logger = logging.getLogger(__name__)

_SCALES = {
    "SECOND": ("SECOND", 1),
    "MINUTE": ("1MINUTE", 60),
    "MINUTE_5": ("5MINUTE", 300),
    "HOUR": ("HOUR", 3600),
}
_DERIVED_RESOLUTIONS = {
    "MINUTE_15": ("MINUTE_5", timedelta(minutes=15)),
    "HOUR_4": ("HOUR", timedelta(hours=4)),
    "DAY": ("HOUR", timedelta(days=1)),
}
_FIELDS = [
    "UTM",
    "BID_OPEN",
    "BID_HIGH",
    "BID_LOW",
    "BID_CLOSE",
    "OFR_OPEN",
    "OFR_HIGH",
    "OFR_LOW",
    "OFR_CLOSE",
    "LTV",
    "CONS_END",
    "CONS_TICK_COUNT",
]


@dataclass
class _SubscriptionState:
    subscription: Subscription
    ready: threading.Event = field(default_factory=threading.Event)
    candle: dict[str, Any] | None = None
    listener: Any | None = None
    last_requested: float = field(default_factory=time.monotonic)


class StreamingCandleManager:
    """Maintains IG consolidated candles received on one Lightstreamer connection."""

    def __init__(
        self,
        credentials: Callable[[], Any],
        *,
        client_factory: Callable[[str], Any] = lambda endpoint: LightstreamerClient(
            endpoint, None
        ),
        historical_prices: Callable[[str, str, str, str], Awaitable[dict[str, Any]]]
        | None = None,
        idle_seconds: float = 300,
    ) -> None:
        self._credentials = credentials
        self._client_factory = client_factory
        self._historical_prices = historical_prices
        self._idle_seconds = idle_seconds
        self._client: Any | None = None
        self._states: dict[tuple[str, str], _SubscriptionState] = {}
        self._segments: dict[tuple[str, str], dict[str, dict[str, Any]]] = {}
        self._seeded_until: dict[tuple[str, str, str], datetime] = {}
        self._window_starts: dict[tuple[str, str, str], datetime] = {}
        self._history_offsets: dict[str, timedelta] = {}
        self._lock = threading.RLock()

    async def current_candle(self, epic: str, resolution: str) -> dict[str, Any]:
        if resolution in _DERIVED_RESOLUTIONS:
            return await self._current_derived_candle(epic, resolution)
        if resolution not in _SCALES:
            supported = ", ".join((*_SCALES, *_DERIVED_RESOLUTIONS))
            raise ValueError(f"resolution must be one of: {supported}")
        state = await self._state_for(epic, resolution)
        if not state.ready.is_set():
            received = await asyncio.to_thread(state.ready.wait, 5)
            if not received:
                raise RuntimeError(
                    "Timed out waiting for IG's streaming candle snapshot"
                )
        with self._lock:
            state.last_requested = time.monotonic()
            if state.candle is None:
                raise RuntimeError(
                    "IG streaming candle snapshot contained no price data"
                )
            return dict(state.candle)

    async def _current_derived_candle(
        self, epic: str, resolution: str
    ) -> dict[str, Any]:
        source_resolution, duration = _DERIVED_RESOLUTIONS[resolution]
        current = await self.current_candle(epic, source_resolution)
        current_start = _timestamp(current["snapshotTimeUTC"])
        if resolution == "MINUTE_15":
            window_start = _window_start(current_start, duration)
        else:
            window_start = await self._ig_window_start(
                epic, resolution, current_start, duration
            )
        await self._seed_segments(epic, source_resolution, window_start, current_start)
        with self._lock:
            segments = dict(self._segments.get((epic, source_resolution), {}))
        source_seconds = _SCALES[source_resolution][1]
        end = None
        if resolution == "DAY":
            # A daylight-saving transition can make an IG trading day 25 hours.
            end = max(
                window_start + duration,
                current_start + timedelta(seconds=source_seconds),
            )
        candle = _aggregate_candles(
            window_start, segments, duration, source_seconds, end=end
        )
        segment_count = sum(
            window_start <= _timestamp(timestamp) < (end or window_start + duration)
            for timestamp in segments
        )
        logger.debug(
            "Derived streaming candle: epic=%s resolution=%s source=%s "
            "window_start=%s current_start=%s segments=%s expected_segments=%s "
            "consolidated=%s",
            epic,
            resolution,
            source_resolution,
            _format_timestamp(window_start),
            _format_timestamp(current_start),
            segment_count,
            int(duration.total_seconds() // source_seconds),
            candle["consolidated"],
        )
        return candle

    async def _ig_window_start(
        self, epic: str, resolution: str, current_start: datetime, duration: timedelta
    ) -> datetime:
        key = (epic, resolution, _format_timestamp(current_start))
        with self._lock:
            window_start = self._window_starts.get(key)
        if window_start is not None:
            logger.debug(
                "Using cached IG candle window: epic=%s resolution=%s "
                "source_start=%s window_start=%s",
                epic,
                resolution,
                _format_timestamp(current_start),
                _format_timestamp(window_start),
            )
            return window_start
        if self._historical_prices is None:
            raise RuntimeError(
                f"IG historical prices are required to derive {resolution} candles"
            )
        from_date = _format_timestamp(current_start - duration)
        to_date = _format_timestamp(current_start + timedelta(hours=1))
        logger.debug(
            "Resolving IG candle window: epic=%s resolution=%s from=%s to=%s",
            epic,
            resolution,
            from_date,
            to_date,
        )
        response = await self._historical_prices_for_utc_range(
            epic,
            resolution,
            current_start - duration,
            current_start + timedelta(hours=1),
        )
        prices = response.get("prices")
        if not isinstance(prices, list):
            raise RuntimeError("Unexpected historical price response from IG API")
        starts = [
            _timestamp(timestamp)
            for price in prices
            if isinstance(price, dict)
            and isinstance(timestamp := price.get("snapshotTimeUTC"), str)
            and _timestamp(timestamp) <= current_start
        ]
        if not starts:
            logger.warning(
                "IG returned no usable candle window: epic=%s resolution=%s "
                "source_start=%s returned_prices=%s",
                epic,
                resolution,
                _format_timestamp(current_start),
                len(prices),
            )
            raise RuntimeError(f"IG did not return a current {resolution} candle")
        window_start = max(starts)
        with self._lock:
            self._window_starts[key] = window_start
        logger.debug(
            "Resolved IG candle window: epic=%s resolution=%s source_start=%s "
            "window_start=%s returned_prices=%s",
            epic,
            resolution,
            _format_timestamp(current_start),
            _format_timestamp(window_start),
            len(prices),
        )
        return window_start

    async def _seed_segments(
        self,
        epic: str,
        resolution: str,
        window_start: datetime,
        current_start: datetime,
    ) -> None:
        if self._historical_prices is None or current_start <= window_start:
            return
        key = (epic, resolution, _format_timestamp(window_start))
        with self._lock:
            seeded_until = self._seeded_until.get(key, window_start)
        if seeded_until >= current_start:
            return
        from_date = _format_timestamp(seeded_until)
        to_date = _format_timestamp(current_start)
        response = await self._historical_prices_for_utc_range(
            epic, resolution, seeded_until, current_start
        )
        prices = response.get("prices")
        if not isinstance(prices, list):
            raise RuntimeError("Unexpected historical price response from IG API")
        seeded = 0
        with self._lock:
            for price in prices:
                if not isinstance(price, dict):
                    continue
                timestamp = price.get("snapshotTimeUTC")
                if not isinstance(timestamp, str):
                    continue
                start = _timestamp(timestamp)
                if window_start <= start < current_start:
                    self._segments.setdefault((epic, resolution), {})[
                        _format_timestamp(start)
                    ] = price
                    seeded += 1
            self._seeded_until[key] = current_start
        logger.debug(
            "Seeded streaming candle segments: epic=%s resolution=%s "
            "window_start=%s from=%s to=%s returned_prices=%s seeded=%s",
            epic,
            resolution,
            _format_timestamp(window_start),
            from_date,
            to_date,
            len(prices),
            seeded,
        )

    async def _historical_prices_for_utc_range(
        self, epic: str, resolution: str, start: datetime, end: datetime
    ) -> dict[str, Any]:
        if self._historical_prices is None:
            raise RuntimeError("IG historical prices are not configured")
        with self._lock:
            offset = self._history_offsets.get(epic, timedelta())
        response = await self._historical_prices(
            epic,
            resolution,
            _format_timestamp(start + offset),
            _format_timestamp(end + offset),
        )
        discovered_offset = _history_timestamp_offset(response)
        if discovered_offset is None or discovered_offset == offset:
            return response
        with self._lock:
            self._history_offsets[epic] = discovered_offset
        logger.debug(
            "Adjusted IG history query offset: epic=%s offset=%s",
            epic,
            discovered_offset,
        )
        return await self._historical_prices(
            epic,
            resolution,
            _format_timestamp(start + discovered_offset),
            _format_timestamp(end + discovered_offset),
        )

    async def _state_for(self, epic: str, resolution: str) -> _SubscriptionState:
        key = (epic, resolution)
        with self._lock:
            self._remove_idle_subscriptions()
            state = self._states.get(key)
            if state is not None:
                state.last_requested = time.monotonic()
                return state

        credentials = await self._credentials()
        return await asyncio.to_thread(self._create_subscription, key, credentials)

    def _create_subscription(
        self, key: tuple[str, str], credentials: StreamingCredentials
    ) -> _SubscriptionState:
        with self._lock:
            existing = self._states.get(key)
            if existing is not None:
                return existing
            if len(self._states) >= 40:
                raise RuntimeError(
                    "IG permits at most 40 simultaneous streaming subscriptions"
                )
            if self._client is None:
                self._client = self._client_factory(credentials.endpoint)
                self._client.connectionDetails.setUser(credentials.account_id)
                self._client.connectionDetails.setPassword(
                    f"CST-{credentials.cst}|XST-{credentials.xst}"
                )
                self._client.connect()

            scale, seconds = _SCALES[key[1]]
            subscription = Subscription("MERGE", [f"CHART:{key[0]}:{scale}"], _FIELDS)
            state = _SubscriptionState(subscription)
            state.listener = _CandleListener(
                state,
                seconds,
                self._lock,
                lambda candle: self._record_segment(key[0], key[1], candle),
            )
            subscription.addListener(state.listener)
            self._client.subscribe(subscription)
            self._states[key] = state
            logger.info(
                "Subscribed to IG streaming candle: epic=%s scale=%s", key[0], scale
            )
            return state

    def _record_segment(
        self, epic: str, resolution: str, candle: dict[str, Any]
    ) -> None:
        segments = self._segments.setdefault((epic, resolution), {})
        segments[candle["snapshotTimeUTC"]] = candle
        retention = 172800 if resolution == "HOUR" else 3600
        cutoff = datetime.now(UTC).timestamp() - retention
        for timestamp in list(segments):
            if _timestamp(timestamp).timestamp() < cutoff:
                del segments[timestamp]

    def _remove_idle_subscriptions(self) -> None:
        cutoff = time.monotonic() - self._idle_seconds
        for key, state in list(self._states.items()):
            if state.last_requested >= cutoff:
                continue
            self._client.unsubscribe(state.subscription)
            del self._states[key]
            logger.info("Unsubscribed idle IG streaming candle: epic=%s", key[0])

    async def close(self) -> None:
        await asyncio.to_thread(self._close)

    def _close(self) -> None:
        with self._lock:
            if self._client is not None:
                self._client.disconnect()
            self._client = None
            self._states.clear()
            self._segments.clear()
            self._seeded_until.clear()
            self._window_starts.clear()
            self._history_offsets.clear()


class _CandleListener(SubscriptionListener):
    def __init__(
        self,
        state: _SubscriptionState,
        seconds: int,
        lock: threading.RLock,
        on_candle: Callable[[dict[str, Any]], None],
    ) -> None:
        self._state = state
        self._seconds = seconds
        self._lock = lock
        self._on_candle = on_candle

    def onItemUpdate(self, update: Any) -> None:
        values = {field: update.getValue(field) for field in _FIELDS}
        candle = _candle_from_values(values, self._seconds)
        with self._lock:
            if candle is not None:
                self._state.candle = candle
                self._on_candle(candle)
            self._state.ready.set()


def _candle_from_values(values: dict[str, Any], seconds: int) -> dict[str, Any] | None:
    timestamp = _number(values.get("UTM"))
    if timestamp is None:
        return None
    start = int(timestamp / 1000 // seconds) * seconds
    bid = _price(values, "BID")
    ask = _price(values, "OFR")
    if bid["open"] is None and ask["open"] is None:
        return None
    result: dict[str, Any] = {
        "snapshotTimeUTC": datetime.fromtimestamp(start, UTC)
        .isoformat()
        .replace("+00:00", "Z"),
        "openPrice": {"bid": bid["open"], "ask": ask["open"]},
        "highPrice": {"bid": bid["high"], "ask": ask["high"]},
        "lowPrice": {"bid": bid["low"], "ask": ask["low"]},
        "closePrice": {"bid": bid["close"], "ask": ask["close"]},
        "updateTimeUTC": datetime.fromtimestamp(timestamp / 1000, UTC)
        .isoformat()
        .replace("+00:00", "Z"),
        "consolidated": values.get("CONS_END") == "1",
    }
    for source, target in (
        ("LTV", "lastTradedVolume"),
        ("CONS_TICK_COUNT", "tickCount"),
    ):
        value = _number(values.get(source))
        if value is not None:
            result[target] = value
    return result


def _price(values: dict[str, Any], prefix: str) -> dict[str, float | None]:
    return {
        name: _number(values.get(f"{prefix}_{name.upper()}"))
        for name in ("open", "high", "low", "close")
    }


def _number(value: Any) -> float | None:
    if value in (None, "", "-"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _history_timestamp_offset(response: dict[str, Any]) -> timedelta | None:
    prices = response.get("prices")
    if not isinstance(prices, list):
        return None
    for price in prices:
        if not isinstance(price, dict):
            continue
        local_time = price.get("snapshotTime")
        utc_time = price.get("snapshotTimeUTC")
        if not isinstance(local_time, str) or not isinstance(utc_time, str):
            continue
        try:
            local = datetime.strptime(local_time, "%Y/%m/%d %H:%M:%S").replace(
                tzinfo=UTC
            )
            offset = local - _timestamp(utc_time)
        except ValueError:
            continue
        if timedelta(hours=-14) <= offset <= timedelta(hours=14):
            return offset
    return None


def _window_start(start: datetime, duration: timedelta) -> datetime:
    seconds = int(duration.total_seconds())
    return datetime.fromtimestamp(int(start.timestamp() // seconds) * seconds, UTC)


def _aggregate_candles(
    start: datetime,
    segments: dict[str, dict[str, Any]],
    duration: timedelta,
    segment_seconds: int,
    *,
    end: datetime | None = None,
) -> dict[str, Any]:
    end = end or start + duration
    candles = sorted(
        (
            candle
            for timestamp, candle in segments.items()
            if start <= _timestamp(timestamp) < end
        ),
        key=lambda candle: candle["snapshotTimeUTC"],
    )
    if not candles:
        raise RuntimeError("No candle data is available for this interval")
    result: dict[str, Any] = {
        "snapshotTimeUTC": _format_timestamp(start),
        "openPrice": dict(candles[0]["openPrice"]),
        "highPrice": _extreme_price(candles, "highPrice", max),
        "lowPrice": _extreme_price(candles, "lowPrice", min),
        "closePrice": dict(candles[-1]["closePrice"]),
        "consolidated": (
            len(candles) == int(duration.total_seconds() // segment_seconds)
            and candles[-1]["snapshotTimeUTC"]
            == _format_timestamp(end - timedelta(seconds=segment_seconds))
            and candles[-1].get("consolidated") is True
        ),
    }
    update_times = [
        value
        for candle in candles
        if isinstance(value := candle.get("updateTimeUTC"), str)
    ]
    if update_times:
        result["updateTimeUTC"] = max(update_times)
    for source, target in (
        ("lastTradedVolume", "lastTradedVolume"),
        ("tickCount", "tickCount"),
    ):
        values = [
            value
            for candle in candles
            if isinstance(value := candle.get(source), (int, float))
        ]
        if values:
            result[target] = sum(values)
    return result


def _aggregate_five_minute_candles(
    start: datetime, segments: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    return _aggregate_candles(start, segments, timedelta(minutes=15), 300)


def _extreme_price(
    candles: list[dict[str, Any]], field: str, operation: Callable[[list[float]], float]
) -> dict[str, float | None]:
    return {
        side: operation(values)
        if (values := _price_values(candles, field, side))
        else None
        for side in ("bid", "ask")
    }


def _price_values(candles: list[dict[str, Any]], field: str, side: str) -> list[float]:
    return [
        value
        for candle in candles
        if isinstance(price := candle.get(field), dict)
        and isinstance(value := price.get(side), (int, float))
    ]
