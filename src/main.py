"""Entry point wiring ingest → filter → search → match → export."""
from __future__ import annotations

from src.clients.openai_client import OpenAIClient
from src.clients.zyte_client import ZyteClient
from src.core.config import load_config
from src.filter.filter_service import FilterService
from src.loader.csv_loader import load_restaurants_from_csv
from src.matcher.matcher import RestaurantMatcher
from src.searcher.searcher import RestaurantSearcher


def main(csv_path: str, output_path: str) -> None:
    config = load_config()
    records = load_restaurants_from_csv(csv_path)
    filtered = FilterService().apply(records)

    searcher = RestaurantSearcher(
        clients=[
            ZyteClient(api_key=config.zyte_api_key),
            OpenAIClient(api_key=config.openai_api_key),
        ]
    )
    candidates = searcher.search(filtered)
    matcher = RestaurantMatcher()
    matched = [c for c in candidates if matcher.match(c, c.record)]

    # TODO: send `matched` to export layer once format finalized.
    print(f"Matched {len(matched)} candidates")


if __name__ == "__main__":
    # Placeholder manual invocation
    main("data/restaurants.csv", "data/output.csv")
