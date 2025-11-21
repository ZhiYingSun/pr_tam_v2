from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class AppConfig:
    zyte_api_key: str
    openai_api_key: str

def load_config() -> AppConfig:
    return AppConfig(
        zyte_api_key=os.getenv("ZYTE_API_KEY"),
        openai_api_key=os.getenv("OPENAI_API_KEY"),
    )
