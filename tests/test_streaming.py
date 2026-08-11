from __future__ import annotations

from datetime import UTC, datetime

from ig_mcp.client import StreamingCredentials
from ig_mcp.streaming import StreamingCandleManager, _aggregate_five_minute_candles


class FakeConnectionDetails:
    def __init__(self) -> None:
        self.user: str | None = None
        self.password: str | None = None

    def setUser(self, value: str) -> None:
        self.user = value

    def setPassword(self, value: str) -> None:
        self.password = value


class FakeLightstreamerClient:
    def __init__(self, endpoint: str) -> None:
        self.endpoint = endpoint
        self.connectionDetails = FakeConnectionDetails()
        self.subscriptions = []
        self.unsubscribed = []
        self.connected = False

    def connect(self) -> None:
        self.connected = True

    def disconnect(self) -> None:
        self.connected = False

    def subscribe(self, subscription) -> None:
        self.subscriptions.append(subscription)

    def unsubscribe(self, subscription) -> None:
        self.unsubscribed.append(subscription)


class FakeUpdate:
    def __init__(self, values: dict[str, str]) -> None:
        self.values = values

    def getValue(self, name: str) -> str | None:
        return self.values.get(name)


async def test_current_candle_subscribes_once_and_maps_chart_update() -> None:
    created: list[FakeLightstreamerClient] = []

    async def credentials() -> StreamingCredentials:
        return StreamingCredentials("https://stream", "account", "cst", "xst")

    def factory(endpoint: str) -> FakeLightstreamerClient:
        client = FakeLightstreamerClient(endpoint)
        created.append(client)
        return client

    manager = StreamingCandleManager(credentials, client_factory=factory)
    state = await manager._state_for("EPIC", "MINUTE")
    listener = state.listener
    assert listener is not None
    listener.onItemUpdate(
        FakeUpdate(
            {
                "UTM": "1767225630123",
                "BID_OPEN": "1.1",
                "BID_HIGH": "1.3",
                "BID_LOW": "1.0",
                "BID_CLOSE": "1.2",
                "OFR_OPEN": "1.1002",
                "OFR_HIGH": "1.3002",
                "OFR_LOW": "1.0002",
                "OFR_CLOSE": "1.2002",
                "LTV": "7",
                "CONS_TICK_COUNT": "8",
                "CONS_END": "0",
            }
        )
    )

    candle = await manager.current_candle("EPIC", "MINUTE")

    assert len(created) == 1
    assert created[0].endpoint == "https://stream"
    assert created[0].connectionDetails.user == "account"
    assert created[0].connectionDetails.password == "CST-cst|XST-xst"
    assert len(created[0].subscriptions) == 1
    assert candle == {
        "snapshotTimeUTC": "2026-01-01T00:00:00Z",
        "updateTimeUTC": "2026-01-01T00:00:30.123000Z",
        "openPrice": {"bid": 1.1, "ask": 1.1002},
        "highPrice": {"bid": 1.3, "ask": 1.3002},
        "lowPrice": {"bid": 1.0, "ask": 1.0002},
        "closePrice": {"bid": 1.2, "ask": 1.2002},
        "lastTradedVolume": 7.0,
        "tickCount": 8.0,
        "consolidated": False,
    }

    await manager.current_candle("EPIC", "MINUTE")
    assert len(created[0].subscriptions) == 1
    await manager.close()
    assert not created[0].connected


async def test_current_candle_rejects_unsupported_resolution() -> None:
    async def credentials() -> StreamingCredentials:
        raise AssertionError("credentials should not be requested")

    manager = StreamingCandleManager(credentials)

    try:
        await manager.current_candle("EPIC", "MINUTE_2")
    except ValueError as error:
        assert "MINUTE_5" in str(error)
    else:
        raise AssertionError("expected an unsupported resolution error")


def test_aggregate_five_minute_candles_builds_a_consolidated_fifteen_minute_bar() -> (
    None
):
    start = datetime(2026, 1, 1, tzinfo=UTC)
    candles = {
        "2026-01-01T00:00:00Z": _candle(
            "2026-01-01T00:00:00Z", 1.0, 1.4, 0.9, 1.2, 2, 3
        ),
        "2026-01-01T00:05:00Z": _candle(
            "2026-01-01T00:05:00Z", 1.2, 1.5, 1.1, 1.3, 4, 5
        ),
        "2026-01-01T00:10:00Z": _candle(
            "2026-01-01T00:10:00Z", 1.3, 1.6, 1.0, 1.4, 6, 7, True
        ),
    }

    candle = _aggregate_five_minute_candles(start, candles)

    assert candle == {
        "snapshotTimeUTC": "2026-01-01T00:00:00Z",
        "openPrice": {"bid": 1.0, "ask": 1.0002},
        "highPrice": {"bid": 1.6, "ask": 1.6002},
        "lowPrice": {"bid": 0.9, "ask": 0.9002},
        "closePrice": {"bid": 1.4, "ask": 1.4002},
        "updateTimeUTC": "2026-01-01T00:14:59Z",
        "lastTradedVolume": 12,
        "tickCount": 15,
        "consolidated": True,
    }


async def test_fifteen_minute_bootstrap_fetches_missing_segments_once() -> None:
    calls: list[tuple[str, str, str, str]] = []

    async def credentials() -> StreamingCredentials:
        raise AssertionError("stream credentials are not needed for bootstrap")

    async def history(
        epic: str, resolution: str, from_date: str, to_date: str
    ) -> dict[str, object]:
        calls.append((epic, resolution, from_date, to_date))
        return {
            "prices": [
                _candle("2026-01-01T00:00:00Z", 1.0, 1.1, 0.9, 1.05, 2, 3),
                _candle("2026-01-01T00:05:00Z", 1.05, 1.2, 1.0, 1.1, 4, 5),
            ]
        }

    manager = StreamingCandleManager(credentials, historical_prices=history)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    current_start = datetime(2026, 1, 1, 0, 10, tzinfo=UTC)

    await manager._seed_five_minute_segments("EPIC", start, current_start)
    await manager._seed_five_minute_segments("EPIC", start, current_start)

    assert calls == [
        ("EPIC", "MINUTE_5", "2026-01-01T00:00:00Z", "2026-01-01T00:10:00Z")
    ]
    assert sorted(manager._segments["EPIC"]) == [
        "2026-01-01T00:00:00Z",
        "2026-01-01T00:05:00Z",
    ]


def _candle(
    timestamp: str,
    open_price: float,
    high: float,
    low: float,
    close: float,
    volume: int,
    ticks: int,
    consolidated: bool = False,
) -> dict[str, object]:
    return {
        "snapshotTimeUTC": timestamp,
        "updateTimeUTC": "2026-01-01T00:14:59Z",
        "openPrice": {"bid": open_price, "ask": open_price + 0.0002},
        "highPrice": {"bid": high, "ask": high + 0.0002},
        "lowPrice": {"bid": low, "ask": low + 0.0002},
        "closePrice": {"bid": close, "ask": close + 0.0002},
        "lastTradedVolume": volume,
        "tickCount": ticks,
        "consolidated": consolidated,
    }
