from typing import Any

from ig_mcp.collector import chart_update_to_price


class ChartUpdate:
    def __init__(self, values: dict[str, str]) -> None:
        self._values = values

    def getValue(self, field: str) -> str | None:  # noqa: N802 - Lightstreamer API
        return self._values.get(field)


def test_chart_update_is_normalized_to_hour_candle() -> None:
    update = ChartUpdate(
        {
            "UTM": "1785629700000",
            "BID_OPEN": "1.1",
            "OFR_OPEN": "1.2",
            "BID_HIGH": "1.3",
            "OFR_HIGH": "1.4",
            "BID_LOW": "1.0",
            "OFR_LOW": "1.1",
            "BID_CLOSE": "1.2",
            "OFR_CLOSE": "1.3",
        }
    )

    price: dict[str, Any] = chart_update_to_price(update, "HOUR")

    assert price["snapshotTimeUTC"].endswith("T00:00:00Z")
    assert price["openPrice"] == {"bid": "1.1", "ask": "1.2", "lastTraded": None}
    assert price["closePrice"] == {"bid": "1.2", "ask": "1.3", "lastTraded": None}


def test_chart_update_without_timestamp_is_discarded() -> None:
    assert chart_update_to_price(ChartUpdate({}), "HOUR") is None


def test_five_minute_chart_update_is_rounded_to_candle_start() -> None:
    price = chart_update_to_price(ChartUpdate({"UTM": "1785629700000"}), "5MINUTE")

    assert price is not None
    assert price["snapshotTimeUTC"].endswith("T00:15:00Z")
