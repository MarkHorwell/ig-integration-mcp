import httpx
import pytest
import respx

from ig_mcp.client import IGApiError, IGClient
from ig_mcp.config import DEMO_BASE_URL, Settings


@pytest.fixture
def settings() -> Settings:
    return Settings("key", "user", "password", "demo", None)


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
