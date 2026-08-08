import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from ig_mcp.logging_config import configure_logging


def test_file_logs_rotate_hourly_and_retain_24_hours(tmp_path: Path) -> None:
    path = tmp_path / "ig-mcp.log"
    logger = logging.getLogger("ig_mcp")
    original_handlers = logger.handlers[:]
    logger.handlers.clear()
    try:
        configure_logging(path, "INFO")

        handler = next(
            handler
            for handler in logger.handlers
            if isinstance(handler, TimedRotatingFileHandler)
        )
        assert handler.interval == 3600
        assert handler.backupCount == 23
        assert handler.utc is True
    finally:
        for handler in logger.handlers:
            handler.close()
        logger.handlers[:] = original_handlers
