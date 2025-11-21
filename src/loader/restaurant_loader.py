from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, Iterator, Protocol

import pandas as pd
from pydantic import BaseModel, Field, ValidationError, field_validator

from src.core.models import RestaurantRecord, RawRestaurantRow

logger = logging.getLogger(__name__)

class RestaurantLoader(Protocol):
    def stream(self) -> Iterator[RestaurantRecord]:
        ...

    def load(self) -> list[RestaurantRecord]:
        ...


def to_restaurant(raw: RawRestaurantRow) -> RestaurantRecord:
    return RestaurantRecord(
        name=raw.name,
        address=(raw.full_address or "").strip(),
        city=(raw.city or "").strip(),
        postal_code=(raw.postal_code or "").strip(),
        coordinates=(raw.longitude, raw.latitude),
        rating=raw.reviews_rating,
        reviews_count=raw.reviews_count,
        google_id=(raw.google_id or "").strip() or None,
        phone=(raw.phone or "").strip() or None,
        website=(raw.website or "").strip() or None,
        main_type=(raw.main_type or "").strip() or None,
    )

class CSVLoaderConfig:
    path: Path
    chunk_size: int = 1000
    encoding: str = "utf-8"
    drop_empty_names: bool = True

class CSVRestaurantLoader(RestaurantLoader):
    def __init__(self, config: CSVLoaderConfig):
        self.config = config
        if not config.path.exists():
            raise FileNotFoundError(f"CSV file not found: {config.path}")

    def stream(self) -> Iterator[RestaurantRecord]:
        logger.info("Streaming restaurants from %s", self.config.path)

        chunks: Iterable[pd.DataFrame] = pd.read_csv(
            self.config.path,
            chunksize=self.config.chunk_size,
            encoding=self.config.encoding,
        )

        count, skipped, rows_read  = 0, 0, 0

        for chunk in chunks:
            for _, row in chunk.iterrows():
                rows_read += 1

                try:
                    restaurant_row = RawRestaurantRow.model_validate(row.to_dict())
                    restaurant = to_restaurant(restaurant_row)
                    count += 1
                    yield restaurant
                except ValidationError as exc:
                    skipped += 1
                    logger.debug(
                        f"Skipping row {rows_read} due to validation: {exc}"
                    )
                except Exception as exc:
                    skipped += 1
                    logger.warning(
                        f"Error converting row {rows_read}: {exc}"
                    )

                logger.info(
                    f"Streaming finished. Rows read={rows_read} loaded={count} skipped={skipped}"
                )
    def load(self) -> list[RestaurantRecord]:
        return list(self.stream())