from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterator, Union, Optional

import pandas as pd
from pydantic import ValidationError

from src.core.models import RestaurantRecord, RawRestaurantRow

logger = logging.getLogger(__name__)

def _to_restaurant(raw: RawRestaurantRow) -> RestaurantRecord:
    return RestaurantRecord(
        name=raw.name,
        address=(raw.full_address or "").strip(),
        city=(raw.city or "").strip(),
        postal_code=(raw.postal_code or "").strip(),
        coordinates=(raw.longitude, raw.latitude),
        rating=raw.reviews_rating,
        reviews_count=raw.reviews_count,
        google_id=raw.google_id,
        phone=raw.phone,
        website=(raw.website or "").strip() or None,
        main_type=(raw.main_type or "").strip() or None,
    )


def stream_restaurants_from_csv(
    csv_path: Union[str, Path],
    *,
    chunk_size: int = 1000,
    encoding: str = "utf-8",
) -> Iterator[RestaurantRecord]:
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")

    chunks = pd.read_csv(
        path,
        chunksize=chunk_size,
        encoding=encoding,
        dtype=str,
        keep_default_na=False,
    )

    rows_read = loaded = skipped = 0

    for chunk in chunks:
        for _, row in chunk.iterrows():
            rows_read += 1
            try:
                restaurant_row = RawRestaurantRow.model_validate(row.to_dict())
                restaurant = _to_restaurant(restaurant_row)
                loaded += 1
                yield restaurant
            except ValidationError as exc:
                skipped += 1
                logger.debug(f"Skipping row {rows_read} due to validation error: {exc}")
            except Exception as exc:
                skipped += 1
                logger.warning(f"Error converting row {rows_read}: {exc}")

    logger.info(
        f"CSV load finished. rows={rows_read} loaded={loaded} skipped={skipped}"
    )


def load_restaurants_from_csv(
    csv_path: Union[str, Path],
    *,
    chunk_size: int = 1000,
    encoding: str = "utf-8",
) -> list[RestaurantRecord]:
    return list(
        stream_restaurants_from_csv(
            csv_path,
            chunk_size=chunk_size,
            encoding=encoding,
        )
    )

