from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


class PersistentCache:
    """Best-effort SQLite cache that never prevents an IG API request."""

    def __init__(self, path: Path) -> None:
        self._path = path

    async def get_response(self, key: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._get_response, key)

    async def set_response(
        self, key: str, group_name: str, expires_at: float, payload: dict[str, Any]
    ) -> None:
        await asyncio.to_thread(
            self._set_response, key, group_name, expires_at, payload
        )

    async def invalidate_group(self, scope: str, group_name: str) -> None:
        await asyncio.to_thread(self._invalidate_group, scope, group_name)

    async def price_coverage(
        self, scope: str, epic: str, resolution: str
    ) -> list[tuple[str, str, float]]:
        return await asyncio.to_thread(self._price_coverage, scope, epic, resolution)

    async def store_prices(
        self,
        scope: str,
        epic: str,
        resolution: str,
        start: str,
        end: str,
        expires_at: float,
        prices: list[dict[str, Any]],
    ) -> None:
        await asyncio.to_thread(
            self._store_prices,
            scope,
            epic,
            resolution,
            start,
            end,
            expires_at,
            prices,
        )

    async def prices(
        self, scope: str, epic: str, resolution: str, start: str, end: str
    ) -> list[dict[str, Any]]:
        return await asyncio.to_thread(
            self._prices, scope, epic, resolution, start, end
        )

    def _connect(self) -> sqlite3.Connection:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self._path, timeout=5)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=5000")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS responses (
                key TEXT PRIMARY KEY,
                group_name TEXT NOT NULL,
                expires_at REAL NOT NULL,
                payload TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS price_coverage (
                scope TEXT NOT NULL,
                epic TEXT NOT NULL,
                resolution TEXT NOT NULL,
                start_at TEXT NOT NULL,
                end_at TEXT NOT NULL,
                expires_at REAL NOT NULL,
                PRIMARY KEY (scope, epic, resolution, start_at, end_at)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS prices (
                scope TEXT NOT NULL,
                epic TEXT NOT NULL,
                resolution TEXT NOT NULL,
                snapshot_time TEXT NOT NULL,
                payload TEXT NOT NULL,
                PRIMARY KEY (scope, epic, resolution, snapshot_time)
            )
            """
        )
        cache_version = connection.execute("PRAGMA user_version").fetchone()[0]
        if cache_version < 2:
            # Earlier versions marked every non-empty response as complete coverage.
            connection.execute("DELETE FROM price_coverage")
            connection.execute("PRAGMA user_version = 2")
        return connection

    def _get_response(self, key: str) -> dict[str, Any] | None:
        try:
            with self._connect() as connection:
                row = connection.execute(
                    "SELECT payload, expires_at FROM responses WHERE key = ?", (key,)
                ).fetchone()
                if row is None:
                    return None
                if row[1] <= time.time():
                    connection.execute("DELETE FROM responses WHERE key = ?", (key,))
                    return None
                payload = json.loads(row[0])
                return payload if isinstance(payload, dict) else None
        except (OSError, sqlite3.Error, ValueError, TypeError):
            return None

    def _set_response(
        self, key: str, group_name: str, expires_at: float, payload: dict[str, Any]
    ) -> None:
        try:
            with self._connect() as connection:
                connection.execute(
                    "INSERT OR REPLACE INTO responses VALUES (?, ?, ?, ?)",
                    (
                        key,
                        group_name,
                        expires_at,
                        json.dumps(payload, separators=(",", ":")),
                    ),
                )
        except (OSError, sqlite3.Error, TypeError, ValueError):
            pass

    def _invalidate_group(self, scope: str, group_name: str) -> None:
        try:
            with self._connect() as connection:
                connection.execute(
                    "DELETE FROM responses WHERE group_name = ? AND key LIKE ?",
                    (group_name, f"{scope}:%"),
                )
        except (OSError, sqlite3.Error):
            pass

    def _price_coverage(
        self, scope: str, epic: str, resolution: str
    ) -> list[tuple[str, str, float]]:
        try:
            with self._connect() as connection:
                rows = connection.execute(
                    """
                    SELECT start_at, end_at, expires_at FROM price_coverage
                    WHERE scope = ? AND epic = ? AND resolution = ?
                    AND expires_at > ?
                    AND EXISTS (
                        SELECT 1 FROM prices
                        WHERE prices.scope = price_coverage.scope
                        AND prices.epic = price_coverage.epic
                        AND prices.resolution = price_coverage.resolution
                        AND prices.snapshot_time >= price_coverage.start_at
                        AND prices.snapshot_time < price_coverage.end_at
                    )
                    """,
                    (scope, epic, resolution, time.time()),
                ).fetchall()
                return [(row[0], row[1], row[2]) for row in rows]
        except (OSError, sqlite3.Error):
            return []

    def _store_prices(
        self,
        scope: str,
        epic: str,
        resolution: str,
        start: str,
        end: str,
        expires_at: float,
        prices: list[dict[str, Any]],
    ) -> None:
        try:
            with self._connect() as connection:
                coverage = self._price_intervals(resolution, prices)
                for covered_start, covered_end in coverage:
                    connection.execute(
                        "INSERT OR REPLACE INTO price_coverage "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            scope,
                            epic,
                            resolution,
                            covered_start,
                            covered_end,
                            expires_at,
                        ),
                    )
                for price in prices:
                    timestamp = self._snapshot_time(price)
                    if timestamp is None:
                        continue
                    connection.execute(
                        "INSERT OR REPLACE INTO prices VALUES (?, ?, ?, ?, ?)",
                        (
                            scope,
                            epic,
                            resolution,
                            timestamp,
                            json.dumps(price, separators=(",", ":")),
                        ),
                    )
        except (OSError, sqlite3.Error, TypeError, ValueError):
            pass

    def _prices(
        self, scope: str, epic: str, resolution: str, start: str, end: str
    ) -> list[dict[str, Any]]:
        try:
            with self._connect() as connection:
                rows = connection.execute(
                    """
                    SELECT payload FROM prices
                    WHERE scope = ? AND epic = ? AND resolution = ?
                    AND snapshot_time >= ? AND snapshot_time < ?
                    ORDER BY snapshot_time
                    """,
                    (scope, epic, resolution, start, end),
                ).fetchall()
                return [json.loads(row[0]) for row in rows]
        except (OSError, sqlite3.Error, TypeError, ValueError):
            return []

    @staticmethod
    def _snapshot_time(price: dict[str, Any]) -> str | None:
        value = price.get("snapshotTimeUTC")
        if not isinstance(value, str):
            return None
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            try:
                parsed = datetime.strptime(value, "%Y/%m/%d %H:%M:%S").replace(
                    tzinfo=UTC
                )
            except ValueError:
                return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")

    @classmethod
    def _price_intervals(
        cls, resolution: str, prices: list[dict[str, Any]]
    ) -> list[tuple[str, str]]:
        duration = _resolution_duration(resolution)
        if duration is None:
            return []
        timestamps = sorted(
            {
                datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                for price in prices
                if (timestamp := cls._snapshot_time(price)) is not None
            }
        )
        if not timestamps:
            return []

        return [
            (
                timestamp.astimezone(UTC).isoformat().replace("+00:00", "Z"),
                (timestamp + duration)
                .astimezone(UTC)
                .isoformat()
                .replace("+00:00", "Z"),
            )
            for timestamp in timestamps
        ]


def _resolution_duration(resolution: str) -> timedelta | None:
    minutes = {
        "SECOND": 1 / 60,
        "MINUTE": 1,
        "MINUTE_2": 2,
        "MINUTE_3": 3,
        "MINUTE_5": 5,
        "MINUTE_10": 10,
        "MINUTE_15": 15,
        "MINUTE_30": 30,
        "HOUR": 60,
        "HOUR_2": 120,
        "HOUR_3": 180,
        "HOUR_4": 240,
        "DAY": 1440,
        "WEEK": 10080,
    }
    value = minutes.get(resolution)
    return timedelta(minutes=value) if value is not None else None
