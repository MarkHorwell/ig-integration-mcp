from __future__ import annotations

import secrets
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

mcp = FastMCP("IG Trading")
_client: IGClient | None = None


def get_client() -> IGClient:
    global _client
    if _client is None:
        _client = IGClient(Settings.from_environment())
    return _client


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


@mcp.tool()
async def ig_list_accounts() -> dict[str, Any]:
    """List the accounts available to the authenticated IG client."""
    return await get_client().request("GET", "/accounts", version=1)


@mcp.tool()
async def ig_get_account_preferences() -> dict[str, Any]:
    """Return preferences for the active IG account."""
    return await get_client().request("GET", "/accounts/preferences", version=1)


@mcp.tool()
async def ig_get_activity(
    from_date: str, to_date: str, detailed: bool = False, page_size: int = 20
) -> dict[str, Any]:
    """Return active-account history. Dates must use IG's ISO-8601 UTC format."""
    return await get_client().request(
        "GET",
        "/history/activity",
        version=3,
        params={
            "from": from_date,
            "to": to_date,
            "detailed": detailed,
            "pageSize": page_size,
        },
        cache_ttl_seconds=300,
        cache_group="history",
    )


@mcp.tool()
async def ig_get_transactions(
    transaction_type: str, from_date: str, to_date: str, page_size: int = 20
) -> dict[str, Any]:
    """Return transactions for a type and date range."""
    path = f"/history/transactions/{transaction_type}/{from_date}/{to_date}"
    return await get_client().request(
        "GET",
        path,
        version=1,
        params={"pageSize": page_size},
        cache_ttl_seconds=300,
        cache_group="history",
    )


@mcp.tool()
async def ig_search_markets(search_term: str) -> dict[str, Any]:
    """Search IG instruments by a market name or symbol."""
    return await get_client().request(
        "GET",
        "/markets",
        version=1,
        params={"searchTerm": search_term},
        cache_ttl_seconds=600,
        cache_group="market-search",
    )


@mcp.tool()
async def ig_get_market(epic: str) -> dict[str, Any]:
    """Get dealing rules, snapshot, and details for an instrument epic."""
    return await get_client().request("GET", f"/markets/{epic}", version=4)


@mcp.tool()
async def ig_get_historical_prices(
    epic: str, resolution: str, from_date: str, to_date: str
) -> dict[str, Any]:
    """Get historical OHLC prices for a UTC range, reusing cached periods."""
    return await get_client().get_historical_prices(
        epic, resolution, from_date, to_date
    )


@mcp.tool()
async def ig_list_categories() -> dict[str, Any]:
    """List instrument categories enabled for the active account."""
    return await get_client().request(
        "GET",
        "/categories",
        version=1,
        cache_ttl_seconds=21600,
        cache_group="catalogue",
    )


@mcp.tool()
async def ig_list_category_instruments(category_id: str) -> dict[str, Any]:
    """List the instruments within an IG category."""
    return await get_client().request(
        "GET",
        f"/categories/{category_id}/instruments",
        version=1,
        cache_ttl_seconds=3600,
        cache_group="catalogue",
    )


@mcp.tool()
async def ig_list_positions() -> dict[str, Any]:
    """List open positions in the active account."""
    return await get_client().request("GET", "/positions", version=2)


@mcp.tool()
async def ig_get_position(deal_id: str) -> dict[str, Any]:
    """Get a specific open position by deal ID."""
    return await get_client().request("GET", f"/positions/{deal_id}", version=2)


@mcp.tool()
async def ig_list_working_orders() -> dict[str, Any]:
    """List open working orders in the active account."""
    return await get_client().request("GET", "/working-orders", version=2)


@mcp.tool()
async def ig_get_deal_confirmation(deal_reference: str) -> dict[str, Any]:
    """Get the confirmation of a submitted deal using its deal reference."""
    return await get_client().request("GET", f"/confirms/{deal_reference}", version=1)


@mcp.tool()
async def ig_create_position(
    request: dict[str, Any], confirm: bool = False, live_confirmation: str | None = None
) -> dict[str, Any]:
    """Create an OTC position. This may execute a leveraged trade."""
    require_write_confirmation(confirm, live_confirmation)
    return await get_client().request(
        "POST",
        "/positions/otc",
        version=2,
        body=with_deal_reference(parse(CreatePosition, request)),
    )


@mcp.tool()
async def ig_update_position(
    deal_id: str,
    request: dict[str, Any],
    confirm: bool = False,
    live_confirmation: str | None = None,
) -> dict[str, Any]:
    """Update an OTC position's stops or limits. This changes account risk."""
    require_write_confirmation(confirm, live_confirmation)
    return await get_client().request(
        "PUT",
        f"/positions/otc/{deal_id}",
        version=2,
        body=parse(UpdatePosition, request),
    )


@mcp.tool()
async def ig_close_position(
    deal_id: str,
    request: dict[str, Any],
    confirm: bool = False,
    live_confirmation: str | None = None,
) -> dict[str, Any]:
    """Close all or part of an OTC position. This may realize profit or loss."""
    require_write_confirmation(confirm, live_confirmation)
    return await get_client().request(
        "DELETE",
        "/positions/otc",
        version=1,
        body={"dealId": deal_id, **parse(ClosePosition, request)},
    )


@mcp.tool()
async def ig_create_working_order(
    request: dict[str, Any], confirm: bool = False, live_confirmation: str | None = None
) -> dict[str, Any]:
    """Create an OTC working order that can open a leveraged position later."""
    require_write_confirmation(confirm, live_confirmation)
    return await get_client().request(
        "POST",
        "/working-orders/otc",
        version=2,
        body=with_deal_reference(parse(CreateWorkingOrder, request)),
    )


@mcp.tool()
async def ig_update_working_order(
    deal_id: str,
    request: dict[str, Any],
    confirm: bool = False,
    live_confirmation: str | None = None,
) -> dict[str, Any]:
    """Update an OTC working order."""
    require_write_confirmation(confirm, live_confirmation)
    return await get_client().request(
        "PUT",
        f"/working-orders/otc/{deal_id}",
        version=2,
        body=parse(UpdateWorkingOrder, request),
    )


@mcp.tool()
async def ig_cancel_working_order(
    deal_id: str, confirm: bool = False, live_confirmation: str | None = None
) -> dict[str, Any]:
    """Cancel an OTC working order."""
    require_write_confirmation(confirm, live_confirmation)
    return await get_client().request(
        "DELETE", f"/working-orders/otc/{deal_id}", version=2
    )


def main() -> None:
    """Run the MCP server over standard input/output."""
    mcp.run(transport="stdio")
