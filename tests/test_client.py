import logging
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx

from ig_mcp.client import IGApiError, IGClient
from ig_mcp.config import DEMO_BASE_URL, Settings


@pytest.fixture
def settings() -> Settings:
    return Settings("key", "user", "password", "demo", None)


def cached_settings(cache_path: Path) -> Settings:
    return Settings("key", "user", "password", "demo", None, True, cache_path)


def login_response() -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "accountId": "ABC",
            "oauthToken": {
                "access_token": "access",
                "refresh_token": "refresh",
                "expires_in": "60",
            },
        },
    )


@respx.mock
async def test_client_logs_in_then_makes_authenticated_request(
    settings: Settings, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger="ig_mcp.client")
    respx.post(f"{DEMO_BASE_URL}/session").mock(
        return_value=httpx.Response(
            200,
            json={
                "accountId": "ABC",
                "oauthToken": {
                    "access_token": "access",
                    "refresh_token": "refresh",
                    "expires_in": "60",
                },
            },
        )
    )
    accounts = respx.get(f"{DEMO_BASE_URL}/accounts").mock(
        return_value=httpx.Response(
            200,
            json={"accounts": [], "allowance": {"remainingAllowance": 42}},
        )
    )
    async with httpx.AsyncClient(base_url=DEMO_BASE_URL) as http:
        client = IGClient(settings, http)
        result = await client.request("GET", "/accounts", version=1)

    assert result == {"accounts": [], "allowance": {"remainingAllowance": 42}}
    assert accounts.calls[0].request.headers["Authorization"] == "Bearer access"
    assert accounts.calls[0].request.headers["IG-ACCOUNT-ID"] == "ABC"
    assert (
        "IG API response: method=GET version=1 status=200 "
        "allowance={'remainingAllowance': 42}"
    ) in caplog.messages


@respx.mock
async def test_client_raises_structured_ig_error(settings: Settings) -> None:
    respx.post(f"{DEMO_BASE_URL}/session").mock(
        return_value=httpx.Response(
            200,
            json={
                "accountId": "ABC",
                "oauthToken": {
                    "access_token": "access",
                    "refresh_token": "refresh",
                    "expires_in": "60",
                },
            },
        )
    )
    respx.get(f"{DEMO_BASE_URL}/accounts").mock(
        return_value=httpx.Response(
            403,
            headers={"X-REQUEST-ID": "request-1"},
            json={"errorCode": "error.security.api-key-invalid"},
        )
    )
    async with httpx.AsyncClient(base_url=DEMO_BASE_URL) as http:
        client = IGClient(settings, http)
        with pytest.raises(IGApiError, match="error.security.api-key-invalid") as error:
            await client.request("GET", "/accounts", version=1)

    assert error.value.request_id == "request-1"
    assert error.value.request == {
        "method": "GET",
        "url": f"{DEMO_BASE_URL}/accounts",
        "body": None,
    }


@respx.mock
async def test_client_redacts_credentials_from_failed_login_request(
    settings: Settings,
) -> None:
    respx.post(f"{DEMO_BASE_URL}/session").mock(
        return_value=httpx.Response(
            403, json={"errorCode": "error.security.invalid-details"}
        )
    )
    async with httpx.AsyncClient(base_url=DEMO_BASE_URL) as http:
        client = IGClient(settings, http)
        with pytest.raises(IGApiError) as error:
            await client.request("GET", "/accounts", version=1)

    assert error.value.request == {
        "method": "POST",
        "url": f"{DEMO_BASE_URL}/session",
        "body": {"identifier": "user", "password": "[REDACTED]"},
    }


@respx.mock
async def test_client_refreshes_after_an_unauthorized_request(
    settings: Settings,
) -> None:
    respx.post(f"{DEMO_BASE_URL}/session").mock(
        return_value=httpx.Response(
            200,
            json={
                "accountId": "ABC",
                "oauthToken": {
                    "access_token": "old-access",
                    "refresh_token": "refresh",
                    "expires_in": "60",
                },
            },
        )
    )
    respx.post(f"{DEMO_BASE_URL}/session/refresh-token").mock(
        return_value=httpx.Response(
            200,
            json={
                "access_token": "new-access",
                "refresh_token": "new-refresh",
                "expires_in": "60",
            },
        )
    )
    accounts = respx.get(f"{DEMO_BASE_URL}/accounts").mock(
        side_effect=[
            httpx.Response(
                401, json={"errorCode": "error.security.oauth-token-invalid"}
            ),
            httpx.Response(200, json={"accounts": []}),
        ]
    )
    async with httpx.AsyncClient(base_url=DEMO_BASE_URL) as http:
        client = IGClient(settings, http)
        result = await client.request("GET", "/accounts", version=1)

    assert result == {"accounts": []}
    assert accounts.calls[1].request.headers["Authorization"] == "Bearer new-access"


@respx.mock
async def test_cached_get_persists_across_clients(tmp_path: Path) -> None:
    respx.post(f"{DEMO_BASE_URL}/session").mock(return_value=login_response())
    categories = respx.get(f"{DEMO_BASE_URL}/categories").mock(
        return_value=httpx.Response(200, json={"categories": []})
    )
    settings = cached_settings(tmp_path / "cache.sqlite3")

    async with httpx.AsyncClient(base_url=DEMO_BASE_URL) as http:
        client = IGClient(settings, http)
        assert await client.request(
            "GET",
            "/categories",
            version=1,
            cache_ttl_seconds=60,
            cache_group="catalogue",
        ) == {"categories": []}

    async with httpx.AsyncClient(base_url=DEMO_BASE_URL) as http:
        client = IGClient(settings, http)
        assert await client.request(
            "GET",
            "/categories",
            version=1,
            cache_ttl_seconds=60,
            cache_group="catalogue",
        ) == {"categories": []}

    assert len(categories.calls) == 1


@respx.mock
async def test_historical_prices_fetch_only_uncovered_range(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(
        IGClient,
        "_current_candle_start",
        staticmethod(lambda resolution: datetime(2026, 8, 1, 3, tzinfo=UTC)),
    )
    caplog.set_level(logging.DEBUG, logger="ig_mcp.client")
    respx.post(f"{DEMO_BASE_URL}/session").mock(return_value=login_response())
    prices = respx.get(f"{DEMO_BASE_URL}/prices/EPIC").mock(
        side_effect=[
            httpx.Response(
                200,
                json={
                    "prices": [
                        {"snapshotTimeUTC": "2026/08/01 00:30:00", "closePrice": {}}
                    ]
                },
            ),
            httpx.Response(
                200,
                json={
                    "prices": [
                        {"snapshotTimeUTC": "2026/08/01 01:30:00", "closePrice": {}}
                    ]
                },
            ),
        ]
    )
    settings = cached_settings(tmp_path / "cache.sqlite3")
    async with httpx.AsyncClient(base_url=DEMO_BASE_URL) as http:
        client = IGClient(settings, http)
        first = await client.get_historical_prices(
            "EPIC", "MINUTE", "2026-08-01T00:00:00Z", "2026-08-01T01:00:00Z"
        )
        second = await client.get_historical_prices(
            "EPIC", "MINUTE", "2026-08-01T00:00:00Z", "2026-08-01T02:00:00Z"
        )

    assert len(first["prices"]) == 1
    assert len(second["prices"]) == 2
    assert first["prices"][0]["snapshotTimeUTC"] == "2026-08-01T00:30:00Z"
    assert second["prices"][0]["snapshotTimeUTC"] == "2026-08-01T00:30:00Z"
    assert len(prices.calls) == 2
    assert prices.calls[0].request.url.params["resolution"] == "MINUTE"
    assert prices.calls[1].request.url.params["from"] == "2026-08-01T01:00:00"
    assert prices.calls[1].request.url.params["to"] == "2026-08-01T02:00:00"
    source_logs = [
        message
        for message in caplog.messages
        if message.startswith("IG historical price sources:")
    ]
    assert "cache_candles=0 cache_bytes=0 api_candles=1 api_bytes=" in source_logs[0]
    assert "cache_candles=1 cache_bytes=" in source_logs[1]
    assert "api_candles=1 api_bytes=" in source_logs[1]


@respx.mock
async def test_historical_prices_never_cache_current_candle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    current_candle_start = datetime(2026, 8, 1, 1, tzinfo=UTC)
    monkeypatch.setattr(
        IGClient,
        "_current_candle_start",
        staticmethod(lambda resolution: current_candle_start),
    )
    respx.post(f"{DEMO_BASE_URL}/session").mock(return_value=login_response())
    prices = respx.get(f"{DEMO_BASE_URL}/prices/EPIC").mock(
        side_effect=[
            httpx.Response(
                200,
                json={
                    "prices": [
                        {"snapshotTimeUTC": "2026-08-01T00:30:00Z", "closePrice": {}}
                    ]
                },
            ),
            httpx.Response(
                200,
                json={
                    "prices": [
                        {"snapshotTimeUTC": "2026-08-01T01:00:00Z", "closePrice": {}}
                    ]
                },
            ),
            httpx.Response(
                200,
                json={
                    "prices": [
                        {"snapshotTimeUTC": "2026-08-01T01:00:00Z", "closePrice": {}}
                    ]
                },
            ),
        ]
    )
    settings = cached_settings(tmp_path / "cache.sqlite3")
    async with httpx.AsyncClient(base_url=DEMO_BASE_URL) as http:
        client = IGClient(settings, http)
        first = await client.get_historical_prices(
            "EPIC", "MINUTE", "2026-08-01T00:00:00Z", "2026-08-01T01:30:00Z"
        )
        second = await client.get_historical_prices(
            "EPIC", "MINUTE", "2026-08-01T00:00:00Z", "2026-08-01T01:30:00Z"
        )
        cached = await client._cache.prices(
            client._cache_scope(),
            "EPIC",
            "MINUTE",
            "2026-08-01T00:00:00Z",
            "2026-08-01T01:30:00Z",
        )

    assert len(first["prices"]) == 2
    assert len(second["prices"]) == 2
    assert len(prices.calls) == 3
    assert prices.calls[1].request.url.params["from"] == "2026-08-01T01:00:00"
    assert prices.calls[2].request.url.params["from"] == "2026-08-01T01:00:00"
    assert cached == [{"snapshotTimeUTC": "2026-08-01T00:30:00Z", "closePrice": {}}]


@respx.mock
async def test_historical_prices_refetch_only_deleted_interval(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        IGClient,
        "_current_candle_start",
        staticmethod(lambda resolution: datetime(2026, 8, 1, 4, tzinfo=UTC)),
    )
    respx.post(f"{DEMO_BASE_URL}/session").mock(return_value=login_response())
    prices = respx.get(f"{DEMO_BASE_URL}/prices/EPIC").mock(
        side_effect=[
            httpx.Response(
                200,
                json={
                    "prices": [
                        {"snapshotTimeUTC": "2026-08-01T00:30:00Z", "closePrice": {}}
                    ]
                },
            ),
            httpx.Response(
                200,
                json={
                    "prices": [
                        {"snapshotTimeUTC": "2026-08-01T01:30:00Z", "closePrice": {}}
                    ]
                },
            ),
            httpx.Response(
                200,
                json={
                    "prices": [
                        {"snapshotTimeUTC": "2026-08-01T02:30:00Z", "closePrice": {}}
                    ]
                },
            ),
            httpx.Response(
                200,
                json={
                    "prices": [
                        {"snapshotTimeUTC": "2026-08-01T01:30:00Z", "closePrice": {}}
                    ]
                },
            ),
        ]
    )
    cache_path = tmp_path / "cache.sqlite3"
    settings = cached_settings(cache_path)
    async with httpx.AsyncClient(base_url=DEMO_BASE_URL) as http:
        client = IGClient(settings, http)
        for hour in range(3):
            await client.get_historical_prices(
                "EPIC",
                "MINUTE",
                f"2026-08-01T0{hour}:00:00Z",
                f"2026-08-01T0{hour + 1}:00:00Z",
            )

        scope = client._cache_scope()
        with sqlite3.connect(cache_path) as connection:
            connection.execute(
                """
                DELETE FROM price_coverage
                WHERE scope = ? AND epic = ? AND resolution = ?
                AND start_at = ? AND end_at = ?
                """,
                (
                    scope,
                    "EPIC",
                    "MINUTE",
                    "2026-08-01T01:00:00Z",
                    "2026-08-01T02:00:00Z",
                ),
            )
            connection.execute(
                """
                DELETE FROM prices
                WHERE scope = ? AND epic = ? AND resolution = ?
                AND snapshot_time >= ? AND snapshot_time < ?
                """,
                (
                    scope,
                    "EPIC",
                    "MINUTE",
                    "2026-08-01T01:00:00Z",
                    "2026-08-01T02:00:00Z",
                ),
            )

        result = await client.get_historical_prices(
            "EPIC", "MINUTE", "2026-08-01T00:00:00Z", "2026-08-01T03:00:00Z"
        )

    assert len(result["prices"]) == 3
    assert len(prices.calls) == 4
    assert dict(prices.calls[3].request.url.params) == {
        "resolution": "MINUTE",
        "from": "2026-08-01T01:00:00",
        "to": "2026-08-01T02:00:00",
    }


@respx.mock
async def test_historical_prices_retries_an_empty_response(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        IGClient,
        "_current_candle_start",
        staticmethod(lambda resolution: datetime(2026, 8, 1, 2, tzinfo=UTC)),
    )
    respx.post(f"{DEMO_BASE_URL}/session").mock(return_value=login_response())
    prices = respx.get(f"{DEMO_BASE_URL}/prices/EPIC").mock(
        side_effect=[
            httpx.Response(200, json={"prices": []}),
            httpx.Response(
                200,
                json={
                    "prices": [
                        {"snapshotTimeUTC": "2026/08/01 00:30:00", "closePrice": {}}
                    ]
                },
            ),
        ]
    )
    settings = cached_settings(tmp_path / "cache.sqlite3")
    async with httpx.AsyncClient(base_url=DEMO_BASE_URL) as http:
        client = IGClient(settings, http)
        first = await client.get_historical_prices(
            "EPIC", "MINUTE", "2026-08-01T00:00:00Z", "2026-08-01T01:00:00Z"
        )
        second = await client.get_historical_prices(
            "EPIC", "MINUTE", "2026-08-01T00:00:00Z", "2026-08-01T01:00:00Z"
        )

    assert first == {"prices": []}
    assert len(second["prices"]) == 1
    assert len(prices.calls) == 2


@respx.mock
async def test_historical_prices_ignores_existing_empty_coverage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        IGClient,
        "_current_candle_start",
        staticmethod(lambda resolution: datetime(2026, 8, 1, 2, tzinfo=UTC)),
    )
    respx.post(f"{DEMO_BASE_URL}/session").mock(return_value=login_response())
    prices = respx.get(f"{DEMO_BASE_URL}/prices/EPIC").mock(
        return_value=httpx.Response(
            200,
            json={
                "prices": [{"snapshotTimeUTC": "2026/08/01 00:30:00", "closePrice": {}}]
            },
        )
    )
    settings = cached_settings(tmp_path / "cache.sqlite3")
    async with httpx.AsyncClient(base_url=DEMO_BASE_URL) as http:
        client = IGClient(settings, http)
        await client._ensure_session()
        await client._cache.store_prices(
            client._cache_scope(),
            "EPIC",
            "MINUTE",
            "2026-08-01T00:00:00Z",
            "2026-08-01T01:00:00Z",
            253402300799.0,
            [],
        )
        result = await client.get_historical_prices(
            "EPIC", "MINUTE", "2026-08-01T00:00:00Z", "2026-08-01T01:00:00Z"
        )

    assert len(result["prices"]) == 1
    assert len(prices.calls) == 1
