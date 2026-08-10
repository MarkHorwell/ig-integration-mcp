import logging
from logging.handlers import TimedRotatingFileHandler

import pytest

from ig_mcp.config import Settings
from ig_mcp.server import (
    configure_logging,
    ig_cancel_working_order,
    ig_create_working_order,
    ig_get_activity,
    ig_list_working_orders,
    ig_update_working_order,
    require_write_confirmation,
    with_deal_reference,
)


def configure(monkeypatch: pytest.MonkeyPatch, environment: str) -> None:
    monkeypatch.setenv("IG_API_KEY", "key")
    monkeypatch.setenv("IG_IDENTIFIER", "user")
    monkeypatch.setenv("IG_PASSWORD", "password")
    monkeypatch.setenv("IG_ENVIRONMENT", environment)


class ActivityClient:
    def __init__(self) -> None:
        self.params: dict[str, object] | None = None

    async def request(self, *args, **kwargs) -> dict[str, object]:
        self.params = kwargs["params"]
        return {"activities": []}


class RequestClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, object]]] = []

    async def request(self, method: str, path: str, **kwargs) -> dict[str, object]:
        self.calls.append((method, path, kwargs))
        return {}


async def test_activity_converts_utc_datetimes_to_ig_format(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = ActivityClient()
    monkeypatch.setattr("ig_mcp.server.get_client", lambda: client)

    result = await ig_get_activity(
        "2026-08-10T00:00:00+00:00",
        "2026-08-10T05:16:00Z",
        detailed=True,
        page_size=100,
    )

    assert result == {"activities": []}
    assert client.params == {
        "from": "2026-08-10T00:00:00",
        "to": "2026-08-10T05:16:00",
        "detailed": True,
        "pageSize": 100,
    }


async def test_activity_rejects_an_invalid_date_range(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = ActivityClient()
    monkeypatch.setattr("ig_mcp.server.get_client", lambda: client)

    with pytest.raises(ValueError, match="from_date must be earlier than to_date"):
        await ig_get_activity("2026-08-10T05:16:00Z", "2026-08-10T05:16:00Z")

    assert client.params is None


async def test_activity_preserves_date_only_end_of_day_semantics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = ActivityClient()
    monkeypatch.setattr("ig_mcp.server.get_client", lambda: client)

    await ig_get_activity("2026-08-10", "2026-08-10")

    assert client.params == {
        "from": "2026-08-10",
        "to": "2026-08-10",
        "detailed": False,
        "pageSize": 20,
    }


async def test_working_order_tools_use_ig_workingorders_routes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure(monkeypatch, "demo")
    client = RequestClient()
    monkeypatch.setattr("ig_mcp.server.get_client", lambda: client)
    monkeypatch.setattr("ig_mcp.server.parse", lambda model, request: request)

    await ig_list_working_orders()
    await ig_create_working_order({}, confirm=True)
    await ig_update_working_order("deal-id", {}, confirm=True)
    await ig_cancel_working_order("deal-id", confirm=True)

    assert [(method, path) for method, path, _ in client.calls] == [
        ("GET", "/workingorders"),
        ("POST", "/workingorders/otc"),
        ("PUT", "/workingorders/otc/deal-id"),
        ("DELETE", "/workingorders/otc/deal-id"),
    ]


def test_mutations_require_confirmation(monkeypatch: pytest.MonkeyPatch) -> None:
    configure(monkeypatch, "demo")
    with pytest.raises(ValueError, match="confirm=true"):
        require_write_confirmation(False, None)


def test_live_mutations_require_phrase(monkeypatch: pytest.MonkeyPatch) -> None:
    configure(monkeypatch, "live")
    with pytest.raises(ValueError, match="LIVE_TRADE_CONFIRMED"):
        require_write_confirmation(True, None)

    require_write_confirmation(True, "LIVE_TRADE_CONFIRMED")


def test_deal_reference_is_preserved_or_created() -> None:
    assert (
        with_deal_reference({"dealReference": "provided"})["dealReference"]
        == "provided"
    )
    assert with_deal_reference({})["dealReference"].startswith("mcp-")


def test_file_logging_rotates_daily_and_keeps_one_backup(tmp_path) -> None:
    log_path = tmp_path / "logs" / "ig-mcp.log"
    settings = Settings(
        api_key="key",
        identifier="user",
        password="password",
        environment="demo",
        account_id=None,
        log_path=log_path,
    )

    configure_logging(settings)

    logger = logging.getLogger("ig_mcp")
    handlers = [
        handler
        for handler in logger.handlers
        if getattr(handler, "_ig_mcp_file_handler", False)
    ]
    assert len(handlers) == 1
    handler = handlers[0]
    assert isinstance(handler, TimedRotatingFileHandler)
    assert handler.backupCount == 1
    assert handler.utc is True

    configure_logging(
        Settings(
            api_key="key",
            identifier="user",
            password="password",
            environment="demo",
            account_id=None,
            log_enabled=False,
        )
    )
