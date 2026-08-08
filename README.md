# IG Trading MCP

A local Python MCP server for IG's REST Trading API. It provides account/history, market data, position, working-order, and trade-confirmation tools, plus guarded OTC trade actions. This project is not affiliated with, endorsed by, or sponsored by IG Group.

Trading leveraged products can result in losses exceeding deposits. Start with an IG demo account.

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

Optional operational variables:

| Variable | Description |
| --- | --- |
| `IG_CACHE_ENABLED` | Set to `false` to disable persistent cache reads and writes. Defaults to `true`. |
| `IG_CACHE_PATH` | SQLite database path. Defaults to `~/.cache/ig-mcp/cache.sqlite3`. |
| `IG_LOG_ENABLED` | Set to `false` to disable file logging. Defaults to `true`. |
| `IG_LOG_PATH` | Log file path. Defaults to `~/.cache/ig-mcp/ig-mcp.log`. |
| `IG_LOG_LEVEL` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`. Defaults to `INFO`. |

The server keeps OAuth tokens in memory only. It sends credentials to IG only during login and never returns tokens from tools.

## API Call Cache

Persistent caching is enabled by default to reduce IG API usage. Cached data is stored in `~/.cache/ig-mcp/cache.sqlite3`; set `IG_CACHE_PATH` to use another location or `IG_CACHE_ENABLED=false` to disable cache reads and writes.

The server caches categories for 6 hours, category instruments for 1 hour, market searches for 10 minutes, and activity/transaction history for 5 minutes. A successful trade or working-order change invalidates cached activity and transactions for the active account.

Trading-sensitive data is never cached: account details, market snapshots, positions, working orders, confirmations, and all write operations. OAuth tokens, API keys, passwords, and HTTP headers are never written to the cache.

Historical price requests use a persistent candle cache. Request an explicit UTC range and the server calls IG only for periods that are not already covered. The current, potentially incomplete candle is refreshed after one minute. An empty REST `prices` response is rejected: it creates no cache coverage and the interval is retried by the next request or collector refresh.

Each stored candle records its source. Stream candles are provisional and remain stored until matching REST data reconciles them. REST candles are authoritative and replace a candle previously written by the streaming collector; a later stream update cannot overwrite a REST candle.

```text
ig_get_historical_prices(
  epic="CS.D.EURUSD.CFD.IP",
  resolution="MINUTE",
  from_date="2026-08-01T00:00:00Z",
  to_date="2026-08-01T12:00:00Z",
)
```

## Logging

Operational logging is enabled by default and writes to `~/.cache/ig-mcp/ig-mcp.log`. The log rotates hourly in UTC and retains the active log plus 23 completed hourly files, limiting retention to approximately 24 hours. It records server startup, authentication and refresh events, cache decisions, IG response status/timing, historical-price ranges and counts, empty price responses, and exceptions. It never logs request/response bodies, tokens, credentials, API keys, or HTTP headers.

Set `IG_LOG_PATH` to change the location, `IG_LOG_LEVEL` to one of `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`, or `IG_LOG_ENABLED=false` to disable it.

## Market Data Collector

Run the collector independently of the MCP stdio server to persist five-minute and hourly streaming chart updates, while reconciling daily, hourly, 15-minute, and five-minute REST candles every 15 minutes:

```bash
uv run ig-collect --epic CS.D.EURUSD.CFD.IP
```

The collector authenticates with IG's REST API, obtains Lightstreamer CST/XST session tokens, and subscribes to `CHART:{epic}:5MINUTE` and `CHART:{epic}:HOUR`. It persists `DAY`, `HOUR`, `MINUTE_15`, and `MINUTE_5` REST candles immediately at startup, then refreshes them every 900 seconds. Use `--rest-refresh-seconds` to change that interval; the minimum is 60 seconds.

Override the default stream or REST resolutions when required:

```bash
uv run ig-collect --epic CS.D.EURUSD.CFD.IP \
  --stream-resolutions 5MINUTE HOUR \
  --rest-resolutions DAY HOUR MINUTE_15 MINUTE_5
```

The collector normally runs until stopped. For a short demo-only verification, use:

```bash
uv run ig-collect --epic CS.D.EURUSD.CFD.IP --run-seconds 45
```

### Sources And Precedence

The `prices` table stores `source` and `observed_at` alongside each candle.

| Source | Resolution | Role |
| --- | --- | --- |
| `stream` | `MINUTE_5`, `HOUR` | Live, provisional Lightstreamer chart updates. |
| `rest` | `DAY`, `HOUR`, `MINUTE_15`, `MINUTE_5` | Historical and reconciliation data returned by IG REST. |

REST replaces a stream candle only when both writes use the same epic, resolution, and timestamp. This applies to `HOUR` and `MINUTE_5`; daily and 15-minute rows are REST-only.

No `stream` row is expected while IG does not publish updates, for example when the selected market is closed. Connection and subscription status is recorded in the configured log file.

### Inspecting The Database

Set a project-local cache path to keep a visible database while testing:

```bash
IG_CACHE_PATH=data/ig-market-data.sqlite3 \
  uv run ig-collect --epic CS.D.EURUSD.CFD.IP --run-seconds 45
```

Inspect the resulting structure and stored sources with SQLite:

```bash
sqlite3 data/ig-market-data.sqlite3 ".schema prices"
sqlite3 data/ig-market-data.sqlite3 \
  "SELECT resolution, source, COUNT(*) FROM prices GROUP BY resolution, source;"
```

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
