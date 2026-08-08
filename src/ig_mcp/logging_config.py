from __future__ import annotations

import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path


def configure_logging(path: Path, level: str) -> None:
    """Configure file-only logging so MCP stdout remains protocol-safe."""
    logger = logging.getLogger("ig_mcp")
    logger.setLevel(level)
    logger.propagate = False

    resolved_path = path.resolve()
    if any(
        isinstance(handler, TimedRotatingFileHandler)
        and Path(handler.baseFilename) == resolved_path
        for handler in logger.handlers
    ):
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    handler = TimedRotatingFileHandler(
        resolved_path,
        when="H",
        interval=1,
        backupCount=23,
        encoding="utf-8",
        utc=True,
    )
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%SZ",
        )
    )
    logger.addHandler(handler)
