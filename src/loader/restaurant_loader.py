from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, Iterator, Protocol

import pandas as pd
from pydantic import BaseModel, Field, ValidationError, field_validator

from src.core.models import RestaurantRecord

logger = logging.getLogger(__name__)

class RestaurantLoader(Protocol):
    def stream(self) -> Iterator[RestaurantRecord]:
        ...

    def load(self) -> list[RestaurantRecord]:
        ...



