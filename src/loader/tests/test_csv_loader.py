from __future__ import annotations

from pathlib import Path

from ..csv_loader import load_restaurants_from_csv, stream_restaurants_from_csv

DATA_PATH = (
    Path(__file__).resolve().parents[3]
    / "data"
    / "Puerto Rico Data_ v1109_50_without_LLC.csv"
)


def test_stream_restaurants_from_csv_reads_all_rows() -> None:
    records = list(stream_restaurants_from_csv(DATA_PATH, chunk_size=10))

    assert len(records) == 50

    first = records[0]
    assert first.name == "Condal Tapas Restaurant & Rooftop Lounge"
    assert first.google_id == "0x8c036f39c8756047:0x38f4d248a30bcf3d"
    assert first.coordinates == (-66.0741174, 18.4563899)
    assert first.address == (
        "Condal Tapas Restaurant & Rooftop Lounge, 1104 Magdalena Ave, "
        "San Juan, 00908, Puerto Rico"
    )
    assert first.city == "San Juan"
    assert first.postal_code == "908"
    assert first.phone == "+1 787-725-0023"
    assert first.rating == 4.1
    assert first.reviews_count == 702
    assert first.main_type == "Tapas restaurant"


def test_load_restaurants_from_csv_returns_list() -> None:
    records = load_restaurants_from_csv(DATA_PATH)

    assert len(records) == 50

    first = records[0]
    assert first.name == "Condal Tapas Restaurant & Rooftop Lounge"
    assert first.google_id == "0x8c036f39c8756047:0x38f4d248a30bcf3d"
    assert first.coordinates == (-66.0741174, 18.4563899)
    assert first.address == (
        "Condal Tapas Restaurant & Rooftop Lounge, 1104 Magdalena Ave, "
        "San Juan, 00908, Puerto Rico"
    )
    assert first.city == "San Juan"
    assert first.postal_code == "908"
    assert first.phone == "+1 787-725-0023"
    assert first.rating == 4.1
    assert first.reviews_count == 702

