# IG Trading MCP

A local Python MCP server for IG's REST Trading API. It provides account/history, market data, position, working-order, and trade-confirmation tools, plus guarded OTC trade actions. This project is not affiliated with, endorsed by, or sponsored by IG Group.

Trading leveraged products can result in losses exceeding deposits. Start with an IG demo account.

## Version 0.8.5

- At `DEBUG`, logs every IG REST request's path, version, parameters, and body, and each response's complete payload.
- Redacts passwords and OAuth tokens from debug logs.

## Version 0.8.4

- Makes `snapshotTime` and `snapshotTimeUTC` represent the same instant in the requested timezone, using IG's authoritative UTC candle timestamp.
- Caches completed historical candles only for their actual resolution intervals, so sparse or capped IG responses leave gaps eligible for refetch rather than marking an entire request as complete.
- Automatically clears legacy historical-price coverage when opening an existing cache database.

## Version 0.8.3

- Adds `HOUR_4` and `DAY` support to `ig_get_current_candle`, aggregating hourly stream candles in IG-aligned market-session windows.
- Aligns REST-seeded stream segments with IG's market-local history timestamps and adds derived-candle debug diagnostics.

## Version 0.8.2

- Applies Ruff formatting required by CI.

## Version 0.8.1

- Adds `MINUTE_15` support to `ig_get_current_candle` by aggregating IG's five-minute streaming candles, with REST seeding for completed segments in the active interval.

## Version 0.8.0

- Adds `ig_get_current_candle`, backed by IG Lightstreamer consolidated chart subscriptions for forming OHLC candles at second, one-minute, five-minute, and hourly resolutions.
- Reuses in-memory subscriptions and latest candle snapshots, with clean shutdown and idle-subscription cleanup.

## Version 0.7.0

- All tools require an IANA `timezone`, such as `Australia/Sydney`.
- Temporal inputs require ISO-8601 datetimes with an explicit UTC offset.
- IG response timestamps are converted to the requested timezone, including daylight-saving offsets.

## Version 0.6.2

- Normalizes all historical-price `snapshotTimeUTC` response values to ISO-8601 UTC with a `Z` suffix, matching request and cache timestamps.

## Version 0.6.1

- Sends historical-price datetimes in IG's required offset-free UTC format.
- Retries empty or malformed historical-price responses instead of caching them as permanently covered periods.
- Automatically ignores existing empty historical-price cache coverage.

## Version 0.6.0

- Normalizes activity-history timestamps to IG's offset-free datetime format and validates date ranges before making the request.
- Corrects all working-order API routes for IG's live and demo gateways.

## Version 0.5.0

- Includes safe request method, URL, and body details in non-2xx IG API errors; credentials are redacted.
- Logs each successful API response with its allowance object.
- Logs historical-price cache and IG API candle counts and byte totals at `DEBUG` level.

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

All tools require an IANA `timezone` so timestamp responses use the caller's local offset, including daylight-saving changes. Temporal inputs must be ISO-8601 datetimes with an explicit UTC offset. The service converts them to IG's required UTC format before forwarding the request:

```text
ig_get_activity(
  from_date="2026-08-10T10:00:00+10:00",
  to_date="2026-08-10T15:16:00+10:00",
  timezone="Australia/Sydney",
)
```

Trading-sensitive data is never cached: account details, market snapshots, positions, working orders, confirmations, and all write operations. OAuth tokens, API keys, passwords, and HTTP headers are never written to the cache.

Historical price requests use a persistent candle cache. Request an explicit offset-aware range and the server calls IG only for completed candle intervals that are not already covered. Sparse or capped responses do not mark missing intervals as cached. The current, potentially incomplete candle is never cached and is fetched from IG for every request. Returned candle timestamps use the requested timezone; `snapshotTime` and `snapshotTimeUTC` identify the same instant; cache timestamps remain UTC internally.

Version 0.8.4 clears legacy coverage metadata automatically. To force a complete historical-price refresh, stop the server and clear both price-cache tables, then restart it:

```bash
sqlite3 "$IG_CACHE_PATH" \
  "BEGIN; DELETE FROM price_coverage; DELETE FROM prices; COMMIT; VACUUM;"
```

When `IG_CACHE_PATH` is unset, use `~/.cache/ig-mcp/cache.sqlite3` instead.

```text
ig_get_historical_prices(
  epic="CS.D.EURUSD.CFD.IP",
  resolution="MINUTE",
  from_date="2026-08-01T10:00:00+10:00",
  to_date="2026-08-01T22:00:00+10:00",
  timezone="Australia/Sydney",
)
```

## Streaming Current Candles

`ig_get_current_candle` subscribes to IG's Lightstreamer chart feed the first time
an epic and resolution are requested. The server keeps the latest forming candle
in memory and later calls return the newest received snapshot without a REST
price request. It supports `SECOND`, `MINUTE`, `MINUTE_5`, `MINUTE_15`, `HOUR`,
`HOUR_4`, and `DAY`. `MINUTE_15` combines IG's five-minute stream candles.
`HOUR_4` and `DAY` combine hourly stream candles, using IG's current `HOUR_4`
or `DAY` candle to align the active time window to the instrument's market
session. Their first requests may make REST history requests to seed completed
segments in the active interval.

```text
ig_get_current_candle(
  epic="CS.D.EURUSD.CFD.IP",
  resolution="MINUTE",
  timezone="Australia/Sydney",
)
```

The response includes IG's bid and ask OHLC values, update time, tick count, and
`consolidated`, which becomes true when IG closes the candle. Call the tool again
to get later updates: MCP tool responses cannot be pushed to an agent after a
tool call returns. Set `IG_LOG_LEVEL=DEBUG` to log the derived window chosen from
IG, REST segment-seeding ranges and counts, and the assembled candle's segment
count. Idle subscriptions are removed when a later streaming request arrives;
streaming data is never persisted.

## File Logging

Operational logging is enabled by default and writes only to `~/.cache/ig-mcp/ig-mcp.log`, keeping MCP stdout reserved for the protocol. Set `IG_LOG_PATH` to use another location, `IG_LOG_LEVEL` to select `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`, or `IG_LOG_ENABLED=false` to disable it. Each successful API response log includes its `allowance` object. At `DEBUG`, every IG REST request logs its method, path, API version, parameters, and body; every response logs its complete payload. Passwords and OAuth tokens are redacted. Historical-price logs also include returned candle counts and serialized byte totals from the SQLite cache and IG API separately.

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
