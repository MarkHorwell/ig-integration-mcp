import pytest

from ig_mcp.server import require_write_confirmation, with_deal_reference


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
