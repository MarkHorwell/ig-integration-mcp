from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

DEMO_BASE_URL = "https://demo-api.ig.com/gateway/deal"
LIVE_BASE_URL = "https://api.ig.com/gateway/deal"


@dataclass(frozen=True, slots=True)
class Settings:
    api_key: str
    identifier: str
    password: str
    environment: str
    account_id: str | None
    cache_enabled: bool = True
    cache_path: Path = Path("~/.cache/ig-mcp/cache.sqlite3")
    log_enabled: bool = True
    log_path: Path = Path("~/.cache/ig-mcp/ig-mcp.log")
    log_level: str = "INFO"

    @property
    def base_url(self) -> str:
        return DEMO_BASE_URL if self.environment == "demo" else LIVE_BASE_URL

    @classmethod
    def from_environment(cls) -> Settings:
        load_dotenv()
        values = {
            "api_key": os.getenv("IG_API_KEY"),
            "identifier": os.getenv("IG_IDENTIFIER"),
            "password": os.getenv("IG_PASSWORD"),
        }
        missing = [f"IG_{name.upper()}" for name, value in values.items() if not value]
        if missing:
            variable_names = ", ".join(missing)
            raise RuntimeError(f"Missing environment variables: {variable_names}")

        environment = os.getenv("IG_ENVIRONMENT", "demo").lower()
        if environment not in {"demo", "live"}:
            raise RuntimeError("IG_ENVIRONMENT must be either 'demo' or 'live'")

        cache_enabled = os.getenv("IG_CACHE_ENABLED", "true").lower()
        if cache_enabled not in {"true", "false"}:
            raise RuntimeError("IG_CACHE_ENABLED must be either 'true' or 'false'")

        log_enabled = os.getenv("IG_LOG_ENABLED", "true").lower()
        if log_enabled not in {"true", "false"}:
            raise RuntimeError("IG_LOG_ENABLED must be either 'true' or 'false'")

        log_level = os.getenv("IG_LOG_LEVEL", "INFO").upper()
        if log_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise RuntimeError(
                "IG_LOG_LEVEL must be DEBUG, INFO, WARNING, ERROR, or CRITICAL"
            )

        return cls(
            api_key=values["api_key"],
            identifier=values["identifier"],
            password=values["password"],
            environment=environment,
            account_id=os.getenv("IG_ACCOUNT_ID") or None,
            cache_enabled=cache_enabled == "true",
            cache_path=Path(
                os.getenv("IG_CACHE_PATH", "~/.cache/ig-mcp/cache.sqlite3")
            ).expanduser(),
            log_enabled=log_enabled == "true",
            log_path=Path(
                os.getenv("IG_LOG_PATH", "~/.cache/ig-mcp/ig-mcp.log")
            ).expanduser(),
            log_level=log_level,
        )
