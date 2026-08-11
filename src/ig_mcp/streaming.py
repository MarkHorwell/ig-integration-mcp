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
_DERIVED_RESOLUTIONS = {"MINUTE_15"}
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
        self._segments: dict[str, dict[str, dict[str, Any]]] = {}
        self._seeded_until: dict[tuple[str, str], datetime] = {}
        self._lock = threading.RLock()

    async def current_candle(self, epic: str, resolution: str) -> dict[str, Any]:
        if resolution == "MINUTE_15":
            return await self._current_fifteen_minute_candle(epic)
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

    async def _current_fifteen_minute_candle(self, epic: str) -> dict[str, Any]:
        current = await self.current_candle(epic, "MINUTE_5")
        current_start = _timestamp(current["snapshotTimeUTC"])
        window_start = datetime.fromtimestamp(
            int(current_start.timestamp() // 900) * 900, UTC
        )
        await self._seed_five_minute_segments(epic, window_start, current_start)
        with self._lock:
            segments = dict(self._segments.get(epic, {}))
        return _aggregate_five_minute_candles(window_start, segments)

    async def _seed_five_minute_segments(
        self, epic: str, window_start: datetime, current_start: datetime
    ) -> None:
        if self._historical_prices is None or current_start <= window_start:
            return
        key = (epic, _format_timestamp(window_start))
        with self._lock:
            seeded_until = self._seeded_until.get(key, window_start)
        if seeded_until >= current_start:
            return
        response = await self._historical_prices(
            epic,
            "MINUTE_5",
            _format_timestamp(seeded_until),
            _format_timestamp(current_start),
        )
        prices = response.get("prices")
        if not isinstance(prices, list):
            raise RuntimeError("Unexpected historical price response from IG API")
        with self._lock:
            for price in prices:
                if not isinstance(price, dict):
                    continue
                timestamp = price.get("snapshotTimeUTC")
                if not isinstance(timestamp, str):
                    continue
                start = _timestamp(timestamp)
                if window_start <= start < current_start:
                    self._segments.setdefault(epic, {})[_format_timestamp(start)] = (
                        price
                    )
            self._seeded_until[key] = current_start

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
                lambda candle: self._record_segment(key[0], candle),
            )
            subscription.addListener(state.listener)
            self._client.subscribe(subscription)
            self._states[key] = state
            logger.info(
                "Subscribed to IG streaming candle: epic=%s scale=%s", key[0], scale
            )
            return state

    def _record_segment(self, epic: str, candle: dict[str, Any]) -> None:
        segments = self._segments.setdefault(epic, {})
        segments[candle["snapshotTimeUTC"]] = candle
        cutoff = datetime.now(UTC).timestamp() - 3600
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


def _aggregate_five_minute_candles(
    start: datetime, segments: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    end = start + timedelta(minutes=15)
    candles = sorted(
        (
            candle
            for timestamp, candle in segments.items()
            if start <= _timestamp(timestamp) < end
        ),
        key=lambda candle: candle["snapshotTimeUTC"],
    )
    if not candles:
        raise RuntimeError("No five-minute candle data is available for this interval")
    result: dict[str, Any] = {
        "snapshotTimeUTC": _format_timestamp(start),
        "openPrice": dict(candles[0]["openPrice"]),
        "highPrice": _extreme_price(candles, "highPrice", max),
        "lowPrice": _extreme_price(candles, "lowPrice", min),
        "closePrice": dict(candles[-1]["closePrice"]),
        "consolidated": (
            len(candles) == 3
            and candles[-1]["snapshotTimeUTC"]
            == _format_timestamp(start + timedelta(minutes=10))
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
