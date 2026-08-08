# IG Trading MCP

A local Python MCP server for IG's REST Trading API. It provides account/history, market data, position, working-order, and trade-confirmation tools, plus guarded OTC trade actions. This project is not affiliated with, endorsed by, or sponsored by IG Group.

Trading leveraged products can result in losses exceeding deposits. Start with an IG demo account.

## Version 0.4.0

- Adds configurable daily-rotated file logging with one prior daily archive retained.
- Logs historical-price response counts, request parameters, and IG API allowance objects when supplied.
- Caches only finalized historical candles and requests missing ranges plus the active candle directly from IG.

## Setup

1. Create a demo or live IG API key in the IG trading platform.
2. Copy `.env.example` to `.env` and set the values, or export the same variables in the MCP client's environment.
3. Install and run with `uv`:

```bash
uv sync
uv run ig-mcp
```

Required environment variables:

| Variable | Description |
| --- | --- |
| `IG_API_KEY` | API key generated for the selected IG environment. |
| `IG_IDENTIFIER` | IG login identifier. |
| `IG_PASSWORD` | IG login password. |
| `IG_ENVIRONMENT` | `demo` or `live`; defaults to `demo`. |
| `IG_ACCOUNT_ID` | Optional active account ID. IG's login-selected account is used when omitted. |

The server keeps OAuth tokens in memory only. It sends credentials to IG only during login and never returns tokens from tools.

## API Call Cache

Persistent caching is enabled by default to reduce IG API usage. Cached data is stored in `~/.cache/ig-mcp/cache.sqlite3`; set `IG_CACHE_PATH` to use another location or `IG_CACHE_ENABLED=false` to disable cache reads and writes.

The server caches categories for 6 hours, category instruments for 1 hour, market searches for 10 minutes, and activity/transaction history for 5 minutes. A successful trade or working-order change invalidates cached activity and transactions for the active account.

Trading-sensitive data is never cached: account details, market snapshots, positions, working orders, confirmations, and all write operations. OAuth tokens, API keys, passwords, and HTTP headers are never written to the cache.

Historical price requests use a persistent candle cache. Request an explicit UTC range and the server calls IG only for completed periods that are not already covered. The current, potentially incomplete candle is never cached and is fetched from IG for every request.

```text
ig_get_historical_prices(
  epic="CS.D.EURUSD.CFD.IP",
  resolution="MINUTE",
  from_date="2026-08-01T00:00:00Z",
  to_date="2026-08-01T12:00:00Z",
)
```

## File Logging

Operational logging is enabled by default and writes only to `~/.cache/ig-mcp/ig-mcp.log`, keeping MCP stdout reserved for the protocol. Set `IG_LOG_PATH` to use another location, `IG_LOG_LEVEL` to select `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`, or `IG_LOG_ENABLED=false` to disable it.

Logs rotate at midnight UTC. The active log and one prior daily archive are retained; older archives are removed automatically.

## MCP Client Configuration

Example configuration for a local stdio-capable MCP client:

```json
{
  "mcpServers": {
    "ig": {
      "command": "uv",
      "args": ["--directory", "/absolute/path/to/ig-integration-mcp", "run", "ig-mcp"],
      "env": {
        "IG_API_KEY": "your-demo-key",
        "IG_IDENTIFIER": "your-identifier",
        "IG_PASSWORD": "your-password",
        "IG_ENVIRONMENT": "demo"
      }
    }
  }
}
```

## Trade Safety

All tools that create, update, close, or cancel a trade/order require `confirm: true`.

When `IG_ENVIRONMENT=live`, they additionally require this exact parameter:

```json
{"live_confirmation": "LIVE_TRADE_CONFIRMED"}
```

Use `ig_get_market` to inspect a market's dealing rules before submitting an order. Trade tools validate the main IG field dependencies before making API calls, but IG remains the authority for account-specific eligibility, sizes, and price rules.

## Testing

```bash
uv run pytest
```

Tests mock all IG API traffic and do not need credentials.
