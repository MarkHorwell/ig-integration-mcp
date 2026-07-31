from __future__ import annotations

import time
from typing import Any

import httpx

from .config import Settings


class IGApiError(RuntimeError):
    def __init__(self, response: httpx.Response):
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        error_code = (
            payload.get("errorCode", "unknown_error")
            if isinstance(payload, dict)
            else "unknown_error"
        )
        request_id = response.headers.get("X-REQUEST-ID")
        message = f"IG API error {response.status_code}: {error_code}"
        if request_id:
            message += f" (request id: {request_id})"
        super().__init__(message)
        self.status_code = response.status_code
        self.error_code = error_code
        self.request_id = request_id


class IGClient:
    """Minimal asynchronous client that owns an in-memory IG OAuth session."""

    def __init__(
        self, settings: Settings, http_client: httpx.AsyncClient | None = None
    ) -> None:
        self.settings = settings
        self._http = http_client or httpx.AsyncClient(
            base_url=settings.base_url, timeout=30.0
        )
        self._owns_http = http_client is None
        self._access_token: str | None = None
        self._refresh_token: str | None = None
        self._expires_at = 0.0
        self._account_id = settings.account_id

    async def close(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def request(
        self,
        method: str,
        path: str,
        *,
        version: int,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        await self._ensure_session()
        response = await self._send(
            method, path, version=version, params=params, body=body
        )
        if response.status_code == 401 and self._refresh_token:
            await self._refresh_session()
            response = await self._send(
                method, path, version=version, params=params, body=body
            )
        return self._decode(response)

    async def _ensure_session(self) -> None:
        if self._access_token and time.time() < self._expires_at - 10:
            return
        if self._refresh_token:
            await self._refresh_session()
        else:
            await self._login()

    async def _login(self) -> None:
        response = await self._http.post(
            "/session",
            headers=self._base_headers(version=3),
            json={
                "identifier": self.settings.identifier,
                "password": self.settings.password,
            },
        )
        payload = self._decode(response)
        self._store_tokens(payload)
        self._account_id = self._account_id or payload.get("accountId")
        if not self._account_id:
            raise RuntimeError(
                "IG login did not provide an active account ID; set IG_ACCOUNT_ID"
            )

    async def _refresh_session(self) -> None:
        response = await self._http.post(
            "/session/refresh-token",
            headers=self._base_headers(version=1),
            json={"refresh_token": self._refresh_token},
        )
        self._store_tokens(self._decode(response))

    def _store_tokens(self, payload: dict[str, Any]) -> None:
        token = payload.get("oauthToken", payload)
        self._access_token = token.get("access_token")
        self._refresh_token = token.get("refresh_token")
        if not self._access_token or not self._refresh_token:
            raise RuntimeError(
                "IG authentication response did not contain OAuth tokens"
            )
        self._expires_at = time.time() + int(token.get("expires_in", 60))

    async def _send(
        self,
        method: str,
        path: str,
        *,
        version: int,
        params: dict[str, Any] | None,
        body: dict[str, Any] | None,
    ) -> httpx.Response:
        headers = self._base_headers(version=version)
        headers["Authorization"] = f"Bearer {self._access_token}"
        headers["IG-ACCOUNT-ID"] = self._account_id or ""
        return await self._http.request(
            method, path, headers=headers, params=params, json=body
        )

    def _base_headers(self, *, version: int) -> dict[str, str]:
        return {
            "X-IG-API-KEY": self.settings.api_key,
            "Version": str(version),
            "Accept": "application/json; charset=UTF-8",
            "Content-Type": "application/json",
        }

    @staticmethod
    def _decode(response: httpx.Response) -> dict[str, Any]:
        if response.is_error:
            raise IGApiError(response)
        if not response.content:
            return {}
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError("Unexpected non-object response from IG API")
        return payload
