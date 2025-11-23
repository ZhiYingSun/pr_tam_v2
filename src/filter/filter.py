from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Iterator, Protocol, Union

from src.core.models import RestaurantRecord

logger = logging.getLogger(__name__)

class FilterResult:
    name: list[str]
    processed: int = 0
    passed: int = 0
    removed: int = 0
    removal_reasons: Counter[str]

class BusinessFilter(Protocol):
    def match(self, restaurant: RestaurantRecord) -> bool:
        ...


# TODO: add filter result data into return tuple
def apply_all_filters(restaurants: Iterable[RestaurantRecord], filters: Iterable[BusinessFilter]) -> Iterator[
    RestaurantRecord]:
    for record in restaurants:
        if all(filter.match(record) for filter in filters):
            yield record


class InactiveBusinessFilter:
    def match(self, restaurant: RestaurantRecord) -> bool:
        if restaurant.is_closed:
            return False
        return True

class BusinessTypeFilter:
    # TODO: could make it more flexible and make the types optional
    def __init__(self, inclusion_types_file: Union[str, Path], exclusion_types_file: Union[str, Path]):
        inclusion = load_types_from_file(inclusion_types_file)
        exclusion = load_types_from_file(exclusion_types_file)
        if not inclusion or not exclusion:
            raise ValueError("Both inclusion_types and exclusion_types must be provided")
        self.inclusion_types = inclusion
        self.exclusion_types = exclusion

    def match(self, restaurant: RestaurantRecord) -> bool:
        types = _collect_types(restaurant)
        matches_exclusion = bool(self.exclusion_types.intersection(types))
        matches_inclusion = bool(self.inclusion_types.intersection(types))
        if matches_exclusion and not matches_inclusion:
            return False
        return True

def _collect_types(record: RestaurantRecord) -> set[str]:
    types: set[str] = set()
    if record.main_type:
        types.add(record.main_type.lower())
    extra_types = record.all_types
    if extra_types:
       types.update({str(t).strip().lower() for t in extra_types if str(t).strip()})
    return types

def load_types_from_file(path: Path) -> set[str]:
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open("r", encoding="utf-8") as handle:
        return {line.strip() for line in handle if line.strip()}