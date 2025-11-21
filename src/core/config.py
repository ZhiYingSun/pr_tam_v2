"""Configuration helpers for environment variables and constants."""
from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class AppConfig:
    zyte_api_key: str | None
    openai_api_key: str | None
    default_country: str = "US"


def load_config() -> AppConfig:
    """Load config values from environment variables."""

    return AppConfig(
        zyte_api_key=os.getenv("ZYTE_API_KEY"),
        openai_api_key=os.getenv("OPENAI_API_KEY"),
    )
