import pytest
from pydantic import ValidationError

from ig_mcp.models import CreatePosition, Direction, OrderType


def position_request(**overrides: object) -> dict[str, object]:
    request: dict[str, object] = {
        "currency_code": "GBP",
        "direction": Direction.BUY,
        "epic": "CS.D.EURUSD.CFD.IP",
        "expiry": "DFB",
        "force_open": False,
        "guaranteed_stop": False,
        "size": 1,
        "order_type": OrderType.MARKET,
    }
    request.update(overrides)
    return request


def test_position_payload_uses_ig_camel_case() -> None:
    payload = CreatePosition.model_validate(
        position_request(force_open=True, stop_distance=10)
    ).payload()

    assert payload["forceOpen"] is True
    assert payload["stopDistance"] == 10
    assert "currency_code" not in payload


def test_position_rejects_market_level() -> None:
    with pytest.raises(ValidationError, match="MARKET orders"):
        CreatePosition.model_validate(position_request(level=1.2))


def test_position_requires_force_open_for_stop() -> None:
    with pytest.raises(ValidationError, match="force_open"):
        CreatePosition.model_validate(position_request(stop_distance=10))
