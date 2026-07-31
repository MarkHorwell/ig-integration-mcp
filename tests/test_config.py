import pytest

from ig_mcp.config import DEMO_BASE_URL, LIVE_BASE_URL, Settings


def test_settings_reads_demo_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IG_API_KEY", "key")
    monkeypatch.setenv("IG_IDENTIFIER", "user")
    monkeypatch.setenv("IG_PASSWORD", "password")
    monkeypatch.setenv("IG_ENVIRONMENT", "demo")
    monkeypatch.setenv("IG_ACCOUNT_ID", "")

    settings = Settings.from_environment()

    assert settings.base_url == DEMO_BASE_URL
    assert settings.account_id is None


def test_settings_uses_live_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IG_API_KEY", "key")
    monkeypatch.setenv("IG_IDENTIFIER", "user")
    monkeypatch.setenv("IG_PASSWORD", "password")
    monkeypatch.setenv("IG_ENVIRONMENT", "live")
    monkeypatch.setenv("IG_ACCOUNT_ID", "ABC123")

    settings = Settings.from_environment()

    assert settings.base_url == LIVE_BASE_URL
    assert settings.account_id == "ABC123"


def test_settings_rejects_invalid_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IG_API_KEY", "key")
    monkeypatch.setenv("IG_IDENTIFIER", "user")
    monkeypatch.setenv("IG_PASSWORD", "password")
    monkeypatch.setenv("IG_ENVIRONMENT", "test")

    with pytest.raises(RuntimeError, match="IG_ENVIRONMENT"):
        Settings.from_environment()
