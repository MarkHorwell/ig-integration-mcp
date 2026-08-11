import pytest

from ig_mcp.temporal import format_response_timestamps, timezone_for


def test_response_dates_include_requested_timezone_offset() -> None:
    result = format_response_timestamps({"valueDate": "2026-08-01"}, "Australia/Sydney")

    assert result == {"valueDate": "2026-08-01T10:00:00+10:00"}


def test_price_snapshot_times_represent_the_same_instant() -> None:
    result = format_response_timestamps(
        {
            "snapshotTime": "2026/08/11 00:30:00",
            "snapshotTimeUTC": "2026-08-10T14:30:00",
        },
        "Australia/Sydney",
    )

    assert result == {
        "snapshotTime": "2026-08-11T00:30:00+10:00",
        "snapshotTimeUTC": "2026-08-11T00:30:00+10:00",
    }


def test_invalid_iana_timezone_is_rejected() -> None:
    with pytest.raises(ValueError, match="valid IANA timezone"):
        timezone_for("Sydney")
