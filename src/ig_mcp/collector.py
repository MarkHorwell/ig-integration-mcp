from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from lightstreamer.client import (
    ClientListener,
    LightstreamerClient,
    Subscription,
    SubscriptionListener,
)

from .client import IGClient
from .config import Settings
from .logging_config import configure_logging

logger = logging.getLogger("ig_mcp.collector")

CHART_FIELDS = [
    "UTM",
    "OFR_OPEN",
    "OFR_HIGH",
    "OFR_LOW",
    "OFR_CLOSE",
    "BID_OPEN",
    "BID_HIGH",
    "BID_LOW",
    "BID_CLOSE",
    "LTP_OPEN",
    "LTP_HIGH",
    "LTP_LOW",
    "LTP_CLOSE",
    "CONS_END",
]
STREAM_TO_REST_RESOLUTION = {"5MINUTE": "MINUTE_5", "HOUR": "HOUR"}
REST_LOOKBACKS = {
    "DAY": timedelta(days=30),
    "HOUR": timedelta(days=2),
    "MINUTE_15": timedelta(hours=2),
    "MINUTE_5": timedelta(hours=1),
}


def chart_update_to_price(update: Any, stream_resolution: str) -> dict[str, Any] | None:
    """Normalize an IG chart update into the persisted REST candle shape."""
    value = update.getValue
    timestamp = value("UTM")
    try:
        interval_seconds = {"5MINUTE": 300, "HOUR": 3600}[stream_resolution]
        candle_time = datetime.fromtimestamp(
            int(timestamp) // 1000 // interval_seconds * interval_seconds, UTC
        )
    except (TypeError, ValueError, OSError):
        logger.warning("Ignoring chart update with invalid UTM")
        return None

    return {
        "snapshotTimeUTC": candle_time.isoformat().replace("+00:00", "Z"),
        "openPrice": {
            "bid": value("BID_OPEN"),
            "ask": value("OFR_OPEN"),
            "lastTraded": value("LTP_OPEN"),
        },
        "highPrice": {
            "bid": value("BID_HIGH"),
            "ask": value("OFR_HIGH"),
            "lastTraded": value("LTP_HIGH"),
        },
        "lowPrice": {
            "bid": value("BID_LOW"),
            "ask": value("OFR_LOW"),
            "lastTraded": value("LTP_LOW"),
        },
        "closePrice": {
            "bid": value("BID_CLOSE"),
            "ask": value("OFR_CLOSE"),
            "lastTraded": value("LTP_CLOSE"),
        },
        "lastTradedVolume": 0,
    }


class ChartListener(SubscriptionListener):
    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        client: IGClient,
        epic: str,
        stream_resolution: str,
    ) -> None:
        self._loop = loop
        self._client = client
        self._epic = epic
        self._stream_resolution = stream_resolution

    def onItemUpdate(self, update: Any) -> None:  # noqa: N802 - Lightstreamer API
        price = chart_update_to_price(update, self._stream_resolution)
        if price is None:
            return
        future = asyncio.run_coroutine_threadsafe(
            self._client.store_stream_prices(
                self._epic, STREAM_TO_REST_RESOLUTION[self._stream_resolution], [price]
            ),
            self._loop,
        )
        future.add_done_callback(self._log_persist_result)

    @staticmethod
    def _log_persist_result(future: Any) -> None:
        try:
            future.result()
        except Exception:
            logger.exception("Failed to persist streaming chart update")

    def onSubscriptionError(self, code: int, message: str) -> None:  # noqa: N802
        logger.error("Chart subscription failed code=%s message=%s", code, message)

    def onSubscription(self) -> None:  # noqa: N802 - Lightstreamer API
        logger.info(
            "Chart subscription succeeded epic=%s resolution=%s",
            self._epic,
            self._stream_resolution,
        )


class StreamConnectionListener(ClientListener):
    def onStatusChange(self, status: str) -> None:  # noqa: N802 - Lightstreamer API
        logger.info("Lightstreamer connection status=%s", status)

    def onServerError(self, code: int, message: str) -> None:  # noqa: N802
        logger.error("Lightstreamer server error code=%s message=%s", code, message)


async def refresh_rest_prices(
    client: IGClient, epic: str, resolutions: list[str]
) -> None:
    end = datetime.now(UTC).replace(second=0, microsecond=0)
    for resolution in resolutions:
        start = end - REST_LOOKBACKS[resolution]
        await client.get_historical_prices(
            epic,
            resolution,
            start.isoformat().replace("+00:00", "Z"),
            end.isoformat().replace("+00:00", "Z"),
        )


async def run_collector(
    epic: str,
    stream_resolutions: list[str],
    rest_resolutions: list[str],
    rest_refresh_seconds: int,
) -> None:
    settings = Settings.from_environment()
    if settings.log_enabled:
        configure_logging(settings.log_path, settings.log_level)
    client = IGClient(settings)
    stream_client: LightstreamerClient | None = None
    try:
        await refresh_rest_prices(client, epic, rest_resolutions)
        credentials = await client.streaming_credentials()
        stream_client = LightstreamerClient(credentials.endpoint, "DEFAULT")
        stream_client.connectionDetails.setUser(credentials.account_id)
        stream_client.connectionDetails.setPassword(
            f"CST-{credentials.cst}|XST-{credentials.security_token}"
        )
        for resolution in stream_resolutions:
            subscription = Subscription(
                "MERGE", [f"CHART:{epic}:{resolution}"], CHART_FIELDS
            )
            subscription.setDataAdapter("Pricing")
            subscription.addListener(
                ChartListener(asyncio.get_running_loop(), client, epic, resolution)
            )
            stream_client.subscribe(subscription)
        stream_client.addListener(StreamConnectionListener())
        stream_client.connect()
        logger.info(
            "Started chart streams epic=%s resolutions=%s",
            epic,
            ",".join(stream_resolutions),
        )
        while True:
            await asyncio.sleep(rest_refresh_seconds)
            await refresh_rest_prices(client, epic, rest_resolutions)
    finally:
        if stream_client is not None:
            stream_client.disconnect()
        await client.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Persist IG chart streams and REST candle reconciliation data."
    )
    parser.add_argument("--epic", required=True)
    parser.add_argument(
        "--stream-resolutions",
        nargs="+",
        choices=sorted(STREAM_TO_REST_RESOLUTION),
        default=["5MINUTE", "HOUR"],
    )
    parser.add_argument(
        "--rest-resolutions",
        nargs="+",
        choices=sorted(REST_LOOKBACKS),
        default=["DAY", "HOUR", "MINUTE_15", "MINUTE_5"],
    )
    parser.add_argument("--rest-refresh-seconds", type=int, default=900)
    parser.add_argument(
        "--run-seconds",
        type=int,
        help="Stop after this duration; useful for integration checks.",
    )
    arguments = parser.parse_args()
    if arguments.rest_refresh_seconds < 60:
        parser.error("--rest-refresh-seconds must be at least 60")
    if arguments.run_seconds is not None and arguments.run_seconds < 1:
        parser.error("--run-seconds must be at least 1")

    async def run() -> None:
        if arguments.run_seconds is None:
            await run_collector(
                arguments.epic,
                arguments.stream_resolutions,
                arguments.rest_resolutions,
                arguments.rest_refresh_seconds,
            )
            return
        try:
            await asyncio.wait_for(
                run_collector(
                    arguments.epic,
                    arguments.stream_resolutions,
                    arguments.rest_resolutions,
                    arguments.rest_refresh_seconds,
                ),
                timeout=arguments.run_seconds,
            )
        except TimeoutError:
            logger.info("Collector reached requested run duration")

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("Collector stopped")
