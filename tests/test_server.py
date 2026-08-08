import logging
from logging.handlers import TimedRotatingFileHandler

import pytest

from ig_mcp.config import Settings
from ig_mcp.server import (
    configure_logging,
    require_write_confirmation,
    with_deal_reference,
)


def configure(monkeypatch: pytest.MonkeyPatch, environment: str) -> None:
    monkeypatch.setenv("IG_API_KEY", "key")
    monkeypatch.setenv("IG_IDENTIFIER", "user")
    monkeypatch.setenv("IG_PASSWORD", "password")
    monkeypatch.setenv("IG_ENVIRONMENT", environment)


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
