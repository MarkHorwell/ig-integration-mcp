import asyncio
import json
import sqlite3
from pathlib import Path

from ig_mcp.cache import PersistentCache


def price(close: float) -> dict[str, object]:
    return {
        "snapshotTimeUTC": "2026-08-01T00:00:00Z",
        "closePrice": {"bid": close},
    }


def test_rest_replaces_stream_prices_but_stream_cannot_replace_rest(
    tmp_path: Path,
) -> None:
    path = tmp_path / "cache.sqlite3"
    cache = PersistentCache(path)
    arguments = (
        "scope",
        "EPIC",
        "MINUTE",
        "2026-08-01T00:00:00Z",
        "2026-08-01T00:01:00Z",
        0.0,
    )

    asyncio.run(cache.store_prices(*arguments, [price(1.0)], source="stream"))
    asyncio.run(cache.store_prices(*arguments, [price(2.0)], source="rest"))
    asyncio.run(cache.store_prices(*arguments, [price(3.0)], source="stream"))

    with sqlite3.connect(path) as connection:
        source, payload, observed_at = connection.execute(
            "SELECT source, payload, observed_at FROM prices"
        ).fetchone()

    assert source == "rest"
    assert json.loads(payload)["closePrice"]["bid"] == 2.0
    assert observed_at


def test_existing_price_database_migrates_source_metadata(tmp_path: Path) -> None:
    path = tmp_path / "cache.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE prices (
                scope TEXT NOT NULL,
                epic TEXT NOT NULL,
                resolution TEXT NOT NULL,
                snapshot_time TEXT NOT NULL,
                payload TEXT NOT NULL,
                PRIMARY KEY (scope, epic, resolution, snapshot_time)
            )
            """
        )
        connection.execute(
            "INSERT INTO prices VALUES (?, ?, ?, ?, ?)",
            ("scope", "EPIC", "MINUTE", "2026-08-01T00:00:00Z", "{}"),
        )

    cache = PersistentCache(path)
    asyncio.run(
        cache.prices(
            "scope",
            "EPIC",
            "MINUTE",
            "2026-08-01T00:00:00Z",
            "2026-08-01T00:00:00Z",
        )
    )

    with sqlite3.connect(path) as connection:
        source, observed_at = connection.execute(
            "SELECT source, observed_at FROM prices"
        ).fetchone()

    assert source == "rest"
    assert observed_at == "2026-08-01T00:00:00Z"
