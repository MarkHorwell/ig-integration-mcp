from __future__ import annotations

from datetime import UTC, date, datetime, time
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def timezone_for(value: str) -> ZoneInfo:
    try:
        return ZoneInfo(value)
    except ZoneInfoNotFoundError as error:
        raise ValueError(
            f"timezone must be a valid IANA timezone, got {value!r}"
        ) from error


def parse_offset_datetime(value: str, field_name: str = "dates") -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{field_name} must use ISO-8601 datetime format") from error
    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must include a UTC offset")
    return parsed.astimezone(UTC)


def format_ig_datetime(value: datetime) -> str:
    """Format a UTC instant as the offset-free datetime IG expects."""
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S")


def format_ig_date(value: datetime) -> str:
    return value.astimezone(UTC).date().isoformat()


def format_response_timestamps(
    payload: dict[str, Any], timezone: str
) -> dict[str, Any]:
    """Convert IG date and timestamp values to the caller's requested timezone."""
    zone = timezone_for(timezone)
    return _convert_value(payload, zone)


def _convert_value(value: Any, zone: ZoneInfo) -> Any:
    if isinstance(value, dict):
        return {
            key: _convert_temporal_value(key, item, zone) for key, item in value.items()
        }
    if isinstance(value, list):
        return [_convert_value(item, zone) for item in value]
    return value


def _convert_temporal_value(key: str, value: Any, zone: ZoneInfo) -> Any:
    if isinstance(value, (dict, list)):
        return _convert_value(value, zone)
    if not isinstance(value, str) or not _is_temporal_key(key):
        return value
    parsed = _parse_ig_datetime(value)
    if parsed is None:
        return value
    return parsed.astimezone(zone).isoformat()


def _is_temporal_key(key: str) -> bool:
    normalized = key.lower()
    return normalized == "date" or normalized.endswith(
        ("date", "dateutc", "time", "timeutc", "timestamp")
    )


def _parse_ig_datetime(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.strptime(value, "%Y/%m/%d %H:%M:%S")
        except ValueError:
            try:
                parsed_date = date.fromisoformat(value)
            except ValueError:
                return None
            parsed = datetime.combine(parsed_date, time.min)
    if parsed.tzinfo is None:
        # IG emits its offset-free timestamp fields in UTC.
        parsed = parsed.replace(tzinfo=UTC)
    return parsed
