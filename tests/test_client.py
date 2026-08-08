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
    settings: Settings,
) -> None:
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
        return_value=httpx.Response(200, json={"accounts": []})
    )
    async with httpx.AsyncClient(base_url=DEMO_BASE_URL) as http:
        client = IGClient(settings, http)
        result = await client.request("GET", "/accounts", version=1)

    assert result == {"accounts": []}
    assert accounts.calls[0].request.headers["Authorization"] == "Bearer access"
    assert accounts.calls[0].request.headers["IG-ACCOUNT-ID"] == "ABC"


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
async def test_client_gets_lightstreamer_credentials(settings: Settings) -> None:
    respx.post(f"{DEMO_BASE_URL}/session").mock(return_value=login_response())
    session = respx.get(f"{DEMO_BASE_URL}/session").mock(
        return_value=httpx.Response(
            200,
            headers={"CST": "client-token", "X-SECURITY-TOKEN": "account-token"},
            json={"lightstreamerEndpoint": "https://stream.example.test"},
        )
    )
    async with httpx.AsyncClient(base_url=DEMO_BASE_URL) as http:
        client = IGClient(settings, http)
        credentials = await client.streaming_credentials()

    assert credentials.endpoint == "https://stream.example.test"
    assert credentials.account_id == "ABC"
    assert credentials.cst == "client-token"
    assert credentials.security_token == "account-token"
    assert session.calls[0].request.url.params["fetchSessionTokens"] == "true"


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
async def test_historical_prices_fetch_only_uncovered_range(tmp_path: Path) -> None:
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
    assert len(prices.calls) == 2
    assert prices.calls[1].request.headers["Version"] == "3"
    assert prices.calls[1].request.url.params["resolution"] == "MINUTE"
    assert prices.calls[1].request.url.params["from"] == "2026-08-01T01:00:00"
    assert prices.calls[1].request.url.params["to"] == "2026-08-01T02:00:00"


@respx.mock
async def test_empty_historical_prices_response_is_not_cached(tmp_path: Path) -> None:
    respx.post(f"{DEMO_BASE_URL}/session").mock(return_value=login_response())
    prices = respx.get(f"{DEMO_BASE_URL}/prices/EPIC").mock(
        return_value=httpx.Response(200, json={"prices": []})
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
    assert second == {"prices": []}
    assert len(prices.calls) == 2
