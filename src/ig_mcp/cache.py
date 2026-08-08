from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger("ig_mcp.cache")


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
        source: str = "rest",
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
            source,
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
                source TEXT NOT NULL DEFAULT 'rest',
                observed_at TEXT NOT NULL,
                PRIMARY KEY (scope, epic, resolution, snapshot_time)
            )
            """
        )
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(prices)").fetchall()
        }
        if "source" not in columns:
            connection.execute(
                "ALTER TABLE prices ADD COLUMN source TEXT NOT NULL DEFAULT 'rest'"
            )
        if "observed_at" not in columns:
            connection.execute(
                "ALTER TABLE prices ADD COLUMN observed_at TEXT NOT NULL DEFAULT ''"
            )
            connection.execute(
                "UPDATE prices SET observed_at = snapshot_time WHERE observed_at = ''"
            )
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
            logger.exception("Response cache read failed")
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
            logger.exception("Response cache write failed")

    def _invalidate_group(self, scope: str, group_name: str) -> None:
        try:
            with self._connect() as connection:
                connection.execute(
                    "DELETE FROM responses WHERE group_name = ? AND key LIKE ?",
                    (group_name, f"{scope}:%"),
                )
        except (OSError, sqlite3.Error):
            logger.exception("Response cache invalidation failed")

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
                    """,
                    (scope, epic, resolution, time.time()),
                ).fetchall()
                return [(row[0], row[1], row[2]) for row in rows]
        except (OSError, sqlite3.Error):
            logger.exception("Price coverage read failed")
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
        source: str,
    ) -> None:
        if source not in {"rest", "stream"}:
            raise ValueError("Price source must be either 'rest' or 'stream'")
        try:
            with self._connect() as connection:
                connection.execute(
                    "INSERT OR REPLACE INTO price_coverage VALUES (?, ?, ?, ?, ?, ?)",
                    (scope, epic, resolution, start, end, expires_at),
                )
                observed_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
                for price in prices:
                    timestamp = self._snapshot_time(price)
                    if timestamp is None:
                        continue
                    connection.execute(
                        """
                        INSERT INTO prices (
                            scope, epic, resolution, snapshot_time, payload,
                            source, observed_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(scope, epic, resolution, snapshot_time)
                        DO UPDATE SET
                            payload = CASE
                                WHEN excluded.source = 'rest'
                                    OR prices.source = 'stream'
                                THEN excluded.payload ELSE prices.payload END,
                            source = CASE
                                WHEN excluded.source = 'rest'
                                    OR prices.source = 'stream'
                                THEN excluded.source ELSE prices.source END,
                            observed_at = CASE
                                WHEN excluded.source = 'rest'
                                    OR prices.source = 'stream'
                                THEN excluded.observed_at ELSE prices.observed_at END
                        """,
                        (
                            scope,
                            epic,
                            resolution,
                            timestamp,
                            json.dumps(price, separators=(",", ":")),
                            source,
                            observed_at,
                        ),
                    )
            logger.info(
                "Stored %d %s price candles for epic=%s resolution=%s range=%s..%s",
                len(prices),
                source,
                epic,
                resolution,
                start,
                end,
            )
        except (OSError, sqlite3.Error, TypeError, ValueError):
            logger.exception(
                "Price persistence failed for source=%s epic=%s resolution=%s",
                source,
                epic,
                resolution,
            )

    def _prices(
        self, scope: str, epic: str, resolution: str, start: str, end: str
    ) -> list[dict[str, Any]]:
        try:
            with self._connect() as connection:
                rows = connection.execute(
                    """
                    SELECT payload FROM prices
                    WHERE scope = ? AND epic = ? AND resolution = ?
                    AND snapshot_time >= ? AND snapshot_time <= ?
                    ORDER BY snapshot_time
                    """,
                    (scope, epic, resolution, start, end),
                ).fetchall()
                return [json.loads(row[0]) for row in rows]
        except (OSError, sqlite3.Error, TypeError, ValueError):
            logger.exception("Stored price read failed")
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
