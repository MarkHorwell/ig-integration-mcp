from __future__ import annotations

import logging
import secrets
from contextlib import asynccontextmanager
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from typing import Any

from mcp.server.fastmcp import FastMCP
from pydantic import ValidationError

from .client import IGClient
from .config import Settings
from .models import (
    ClosePosition,
    CreatePosition,
    CreateWorkingOrder,
    UpdatePosition,
    UpdateWorkingOrder,
)
from .streaming import StreamingCandleManager
from .temporal import (
    format_ig_date,
    format_ig_datetime,
    format_response_timestamps,
    parse_offset_datetime,
    timezone_for,
)

_client: IGClient | None = None
_streaming: StreamingCandleManager | None = None


@asynccontextmanager
async def lifespan(_: FastMCP):
    try:
        yield
    finally:
        global _client, _streaming
        if _streaming is not None:
            await _streaming.close()
            _streaming = None
        if _client is not None:
            await _client.close()
            _client = None


mcp = FastMCP("IG Trading", lifespan=lifespan)


def configure_logging(settings: Settings) -> None:
    """Write application logs to a daily-rotated file, never MCP stdout."""
    logger = logging.getLogger("ig_mcp")
    logger.setLevel(getattr(logging, settings.log_level))
    logger.propagate = False

    for handler in list(logger.handlers):
        if getattr(handler, "_ig_mcp_file_handler", False):
            logger.removeHandler(handler)
            handler.close()

    if not settings.log_enabled:
        return

    settings.log_path.parent.mkdir(parents=True, exist_ok=True)
    handler = TimedRotatingFileHandler(
        settings.log_path,
        when="midnight",
        interval=1,
        backupCount=1,
        encoding="utf-8",
        utc=True,
    )
    handler._ig_mcp_file_handler = True  # type: ignore[attr-defined]
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    )
    logger.addHandler(handler)


def get_client() -> IGClient:
    global _client
    if _client is None:
        _client = IGClient(Settings.from_environment())
    return _client


def get_streaming() -> StreamingCandleManager:
    global _streaming
    if _streaming is None:
        client = get_client()
        _streaming = StreamingCandleManager(
            client.get_streaming_credentials,
            historical_prices=client.get_historical_prices,
        )
    return _streaming


def parse(model: type[Any], request: dict[str, Any]) -> dict[str, Any]:
    try:
        return model.model_validate(request).payload()
    except ValidationError as error:
        raise ValueError(error.json(include_url=False)) from error


def require_write_confirmation(confirm: bool, live_confirmation: str | None) -> None:
    if not confirm:
        raise ValueError(
            "This action changes an IG account. Set confirm=true to submit it."
        )
    if (
        Settings.from_environment().environment == "live"
        and live_confirmation != "LIVE_TRADE_CONFIRMED"
    ):
        raise ValueError(
            "Live trading requires live_confirmation='LIVE_TRADE_CONFIRMED'."
        )


def with_deal_reference(payload: dict[str, Any]) -> dict[str, Any]:
    """Provide an idempotency-friendly reference when the caller omitted one."""
    return {"dealReference": f"mcp-{secrets.token_hex(12)}", **payload}


def format_activity_datetime(value: str) -> tuple[str, datetime]:
    """Parse an offset-aware datetime and produce IG's query value."""
    parsed = parse_offset_datetime(value)
    return format_ig_datetime(parsed), parsed


def response(payload: dict[str, Any], timezone: str) -> dict[str, Any]:
    return format_response_timestamps(payload, timezone)


@mcp.tool()
async def ig_list_accounts(timezone: str) -> dict[str, Any]:
    """List the accounts available to the authenticated IG client."""
    timezone_for(timezone)
    return response(await get_client().request("GET", "/accounts", version=1), timezone)


@mcp.tool()
async def ig_get_account_preferences(timezone: str) -> dict[str, Any]:
    """Return preferences for the active IG account."""
    timezone_for(timezone)
    return response(
        await get_client().request("GET", "/accounts/preferences", version=1), timezone
    )


@mcp.tool()
async def ig_get_activity(
    from_date: str,
    to_date: str,
    timezone: str,
    detailed: bool = False,
    page_size: int = 20,
) -> dict[str, Any]:
    """Return active-account history for offset-aware ISO-8601 datetime inputs."""
    timezone_for(timezone)
    formatted_start, start = format_activity_datetime(from_date)
    formatted_end, end = format_activity_datetime(to_date)
    if start >= end:
        raise ValueError("from_date must be earlier than to_date")
    return response(
        await get_client().request(
            "GET",
            "/history/activity",
            version=3,
            params={
                "from": formatted_start,
                "to": formatted_end,
                "detailed": detailed,
                "pageSize": page_size,
            },
            cache_ttl_seconds=300,
            cache_group="history",
        ),
        timezone,
    )


@mcp.tool()
async def ig_get_transactions(
    transaction_type: str,
    from_date: str,
    to_date: str,
    timezone: str,
    page_size: int = 20,
) -> dict[str, Any]:
    """Return transactions for an offset-aware ISO-8601 datetime range."""
    timezone_for(timezone)
    start = parse_offset_datetime(from_date, "from_date")
    end = parse_offset_datetime(to_date, "to_date")
    if start >= end:
        raise ValueError("from_date must be earlier than to_date")
    path = (
        f"/history/transactions/{transaction_type}/"
        f"{format_ig_date(start)}/{format_ig_date(end)}"
    )
    return response(
        await get_client().request(
            "GET",
            path,
            version=1,
            params={"pageSize": page_size},
            cache_ttl_seconds=300,
            cache_group="history",
        ),
        timezone,
    )


@mcp.tool()
async def ig_search_markets(search_term: str, timezone: str) -> dict[str, Any]:
    """Search IG instruments by a market name or symbol."""
    timezone_for(timezone)
    return response(
        await get_client().request(
            "GET",
            "/markets",
            version=1,
            params={"searchTerm": search_term},
            cache_ttl_seconds=600,
            cache_group="market-search",
        ),
        timezone,
    )


@mcp.tool()
async def ig_get_market(epic: str, timezone: str) -> dict[str, Any]:
    """Get dealing rules, snapshot, and details for an instrument epic."""
    timezone_for(timezone)
    return response(
        await get_client().request("GET", f"/markets/{epic}", version=4), timezone
    )


@mcp.tool()
async def ig_get_historical_prices(
    epic: str, resolution: str, from_date: str, to_date: str, timezone: str
) -> dict[str, Any]:
    """Get historical OHLC prices for an offset-aware range, reusing cached periods."""
    timezone_for(timezone)
    return response(
        await get_client().get_historical_prices(epic, resolution, from_date, to_date),
        timezone,
    )


@mcp.tool()
async def ig_get_current_candle(
    epic: str, resolution: str, timezone: str
) -> dict[str, Any]:
    """Get the latest forming IG chart candle via a shared streaming subscription.

    Supported resolutions are SECOND, MINUTE, MINUTE_5, MINUTE_15, HOUR,
    HOUR_4, and DAY. Derived resolutions are built from IG streaming candles.
    Call again for a newer snapshot; MCP tool responses cannot be pushed after
    they return.
    """
    timezone_for(timezone)
    return response(
        {"candle": await get_streaming().current_candle(epic, resolution)}, timezone
    )


@mcp.tool()
async def ig_list_categories(timezone: str) -> dict[str, Any]:
    """List instrument categories enabled for the active account."""
    timezone_for(timezone)
    return response(
        await get_client().request(
            "GET",
            "/categories",
            version=1,
            cache_ttl_seconds=21600,
            cache_group="catalogue",
        ),
        timezone,
    )


@mcp.tool()
async def ig_list_category_instruments(
    category_id: str, timezone: str
) -> dict[str, Any]:
    """List the instruments within an IG category."""
    timezone_for(timezone)
    return response(
        await get_client().request(
            "GET",
            f"/categories/{category_id}/instruments",
            version=1,
            cache_ttl_seconds=3600,
            cache_group="catalogue",
        ),
        timezone,
    )


@mcp.tool()
async def ig_list_positions(timezone: str) -> dict[str, Any]:
    """List open positions in the active account."""
    timezone_for(timezone)
    return response(
        await get_client().request("GET", "/positions", version=2), timezone
    )


@mcp.tool()
async def ig_get_position(deal_id: str, timezone: str) -> dict[str, Any]:
    """Get a specific open position by deal ID."""
    timezone_for(timezone)
    return response(
        await get_client().request("GET", f"/positions/{deal_id}", version=2), timezone
    )


@mcp.tool()
async def ig_list_working_orders(timezone: str) -> dict[str, Any]:
    """List open working orders in the active account."""
    timezone_for(timezone)
    return response(
        await get_client().request("GET", "/workingorders", version=2), timezone
    )


@mcp.tool()
async def ig_get_deal_confirmation(
    deal_reference: str, timezone: str
) -> dict[str, Any]:
    """Get the confirmation of a submitted deal using its deal reference."""
    timezone_for(timezone)
    return response(
        await get_client().request("GET", f"/confirms/{deal_reference}", version=1),
        timezone,
    )


@mcp.tool()
async def ig_create_position(
    request: dict[str, Any],
    timezone: str,
    confirm: bool = False,
    live_confirmation: str | None = None,
) -> dict[str, Any]:
    """Create an OTC position. This may execute a leveraged trade."""
    timezone_for(timezone)
    require_write_confirmation(confirm, live_confirmation)
    return response(
        await get_client().request(
            "POST",
            "/positions/otc",
            version=2,
            body=with_deal_reference(parse(CreatePosition, request)),
        ),
        timezone,
    )


@mcp.tool()
async def ig_update_position(
    deal_id: str,
    request: dict[str, Any],
    timezone: str,
    confirm: bool = False,
    live_confirmation: str | None = None,
) -> dict[str, Any]:
    """Update an OTC position's stops or limits. This changes account risk."""
    timezone_for(timezone)
    require_write_confirmation(confirm, live_confirmation)
    return response(
        await get_client().request(
            "PUT",
            f"/positions/otc/{deal_id}",
            version=2,
            body=parse(UpdatePosition, request),
        ),
        timezone,
    )


@mcp.tool()
async def ig_close_position(
    deal_id: str,
    request: dict[str, Any],
    timezone: str,
    confirm: bool = False,
    live_confirmation: str | None = None,
) -> dict[str, Any]:
    """Close all or part of an OTC position. This may realize profit or loss."""
    timezone_for(timezone)
    require_write_confirmation(confirm, live_confirmation)
    return response(
        await get_client().request(
            "DELETE",
            "/positions/otc",
            version=1,
            body={"dealId": deal_id, **parse(ClosePosition, request)},
        ),
        timezone,
    )


@mcp.tool()
async def ig_create_working_order(
    request: dict[str, Any],
    timezone: str,
    confirm: bool = False,
    live_confirmation: str | None = None,
) -> dict[str, Any]:
    """Create an OTC working order that can open a leveraged position later."""
    timezone_for(timezone)
    require_write_confirmation(confirm, live_confirmation)
    return response(
        await get_client().request(
            "POST",
            "/workingorders/otc",
            version=2,
            body=with_deal_reference(parse(CreateWorkingOrder, request)),
        ),
        timezone,
    )


@mcp.tool()
async def ig_update_working_order(
    deal_id: str,
    request: dict[str, Any],
    timezone: str,
    confirm: bool = False,
    live_confirmation: str | None = None,
) -> dict[str, Any]:
    """Update an OTC working order."""
    timezone_for(timezone)
    require_write_confirmation(confirm, live_confirmation)
    return response(
        await get_client().request(
            "PUT",
            f"/workingorders/otc/{deal_id}",
            version=2,
            body=parse(UpdateWorkingOrder, request),
        ),
        timezone,
    )


@mcp.tool()
async def ig_cancel_working_order(
    deal_id: str,
    timezone: str,
    confirm: bool = False,
    live_confirmation: str | None = None,
) -> dict[str, Any]:
    """Cancel an OTC working order."""
    timezone_for(timezone)
    require_write_confirmation(confirm, live_confirmation)
    return response(
        await get_client().request(
            "DELETE", f"/workingorders/otc/{deal_id}", version=2
        ),
        timezone,
    )


def main() -> None:
    """Run the MCP server over standard input/output."""
    configure_logging(Settings.from_environment())
    mcp.run(transport="stdio")
