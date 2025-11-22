from __future__ import annotations

from pathlib import Path

import pytest

from ...core.models import RestaurantRecord
from ..filter import (
    apply_all_filters,
    BusinessTypeFilter,
    InactiveBusinessFilter,
    _collect_types,
    load_types_from_file,
)


def _make_record(**overrides) -> RestaurantRecord:
    base = dict(
        name="Condal",
        address="1104 Magdalena Ave",
        phone="+1 787-725-0023",
        city="San Juan",
        postal_code="00908",
        coordinates=(-66.0741174, 18.4563899),
        google_id="gid",
        is_closed=False,
        rating=4.1,
        reviews_count=700,
        website=None,
        main_type="restaurant",
        all_types=["restaurant", "cocktail bar"],
    )
    base.update(overrides)
    return RestaurantRecord(**base)


def test_inactive_business_filter_blocks_closed_records() -> None:
    filter_ = InactiveBusinessFilter()
    open_record = _make_record(is_closed=False)
    closed_record = _make_record(is_closed=True)

    assert filter_.match(open_record) is True
    assert filter_.match(closed_record) is False


def test_business_type_filter_respects_inclusion_and_exclusion() -> None:
    filter_ = BusinessTypeFilter(
        inclusion_types=["restaurant", "food court"],
        exclusion_types=["bar", "nightclub"],
    )

    allowed = _make_record(main_type="restaurant", all_types=["restaurant", "bar"])
    excluded = _make_record(main_type="bar", all_types=["bar"])

    assert filter_.match(allowed) is True  # inclusion wins when both match
    assert filter_.match(excluded) is False


def test_apply_all_filters_combines_checks() -> None:
    filters = [
        InactiveBusinessFilter(),
        BusinessTypeFilter(["restaurant"], ["bar"]),
    ]
    records = [
        _make_record(name="Allowed", is_closed=False, all_types=["restaurant"]),
        _make_record(name="Closed", is_closed=True, all_types=["restaurant"]),
        _make_record(
            name="BarOnly", is_closed=False, main_type="bar", all_types=["bar"]
        ),
    ]

    filtered = list(apply_all_filters(records, filters))

    assert [r.name for r in filtered] == ["Allowed"]


def test_collect_types_merges_main_and_all_types() -> None:
    record = _make_record(main_type="Restaurant", all_types=["Cafe", ""])
    types = _collect_types(record)

    assert types == {"restaurant", "cafe"}


def test_load_types_from_file_reads_values() -> None:
    path = (
        Path(__file__).resolve().parents[3]
        / "data"
        / "included_business_types.txt"
    )
    types = load_types_from_file(path)

    assert "restaurant" in {t.lower() for t in types}
    assert len(types) > 0


def test_load_types_from_file_missing_path(tmp_path: Path) -> None:
    missing = tmp_path / "missing.txt"
    with pytest.raises(FileNotFoundError):
        load_types_from_file(missing)

