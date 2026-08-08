from pathlib import Path

import pytest

from ig_mcp.config import DEMO_BASE_URL, LIVE_BASE_URL, Settings


def test_settings_reads_demo_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("ig_mcp.config.load_dotenv", lambda: False)
    monkeypatch.setenv("IG_API_KEY", "key")
    monkeypatch.setenv("IG_IDENTIFIER", "user")
    monkeypatch.setenv("IG_PASSWORD", "password")
    monkeypatch.setenv("IG_ENVIRONMENT", "demo")
    monkeypatch.setenv("IG_ACCOUNT_ID", "")
    monkeypatch.delenv("IG_CACHE_ENABLED", raising=False)
    monkeypatch.delenv("IG_CACHE_PATH", raising=False)
    monkeypatch.delenv("IG_LOG_ENABLED", raising=False)
    monkeypatch.delenv("IG_LOG_PATH", raising=False)
    monkeypatch.delenv("IG_LOG_LEVEL", raising=False)

    settings = Settings.from_environment()

    assert settings.base_url == DEMO_BASE_URL
    assert settings.account_id is None
    assert settings.cache_enabled is True
    assert settings.cache_path == Path("~/.cache/ig-mcp/cache.sqlite3").expanduser()
    assert settings.log_enabled is True
    assert settings.log_path == Path("~/.cache/ig-mcp/ig-mcp.log").expanduser()
    assert settings.log_level == "INFO"


def test_settings_uses_live_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IG_API_KEY", "key")
    monkeypatch.setenv("IG_IDENTIFIER", "user")
    monkeypatch.setenv("IG_PASSWORD", "password")
    monkeypatch.setenv("IG_ENVIRONMENT", "live")
    monkeypatch.setenv("IG_ACCOUNT_ID", "ABC123")

    settings = Settings.from_environment()

    assert settings.base_url == LIVE_BASE_URL
    assert settings.account_id == "ABC123"


def test_settings_allows_cache_to_be_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IG_API_KEY", "key")
    monkeypatch.setenv("IG_IDENTIFIER", "user")
    monkeypatch.setenv("IG_PASSWORD", "password")
    monkeypatch.setenv("IG_CACHE_ENABLED", "false")
    monkeypatch.setenv("IG_CACHE_PATH", "/tmp/ig-cache.sqlite3")

    settings = Settings.from_environment()

    assert settings.cache_enabled is False
    assert settings.cache_path == Path("/tmp/ig-cache.sqlite3")


def test_settings_rejects_invalid_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IG_API_KEY", "key")
    monkeypatch.setenv("IG_IDENTIFIER", "user")
    monkeypatch.setenv("IG_PASSWORD", "password")
    monkeypatch.setenv("IG_ENVIRONMENT", "test")

    with pytest.raises(RuntimeError, match="IG_ENVIRONMENT"):
        Settings.from_environment()


def test_settings_rejects_invalid_log_level(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IG_API_KEY", "key")
    monkeypatch.setenv("IG_IDENTIFIER", "user")
    monkeypatch.setenv("IG_PASSWORD", "password")
    monkeypatch.setenv("IG_LOG_LEVEL", "verbose")

    with pytest.raises(RuntimeError, match="IG_LOG_LEVEL"):
        Settings.from_environment()
