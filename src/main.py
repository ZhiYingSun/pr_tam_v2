"""Entry point wiring ingest → filter → search → match → export."""
from __future__ import annotations

import asyncio
import datetime
from typing import Tuple, Optional, List, Union

import pandas as pd

from src.clients.openai_client import OpenAIClient
from src.clients.zyte_client import ZyteClient
from src.core.config import load_config
from src.core.models import RestaurantRecord, MatchResult, GeneratedOutputFiles
from src.core.validation_models import ValidationResult
from src.export.report_generator import ReportGenerator, export_restaurant_records_to_csv
from src.filter.filter import BusinessTypeFilter, InactiveBusinessFilter, apply_all_filters
from src.loader.csv_loader import load_restaurants_from_csv, stream_restaurants_from_csv
from pathlib import Path

import sys
import logging

from src.matcher.matcher import RestaurantMatcher
from src.searcher.searcher import IncorporationSearcher

logger = logging.getLogger(__name__)

async def main(input_path: str, output_path: str) -> None:
    input_path = Path(input_path)
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        sys.exit(1)

    config = load_config()

    openai_api_key = config.openai_api_key
    if not openai_api_key:
        logger.error("OPENAI_API_KEY not found in environment. Validation is required.")
        sys.exit(1)

    zyte_api_key = config.zyte_api_key
    if not zyte_api_key:
        logger.error("ZYTE_API_KEY not found in environment.")
        sys.exit(1)


    openai_client = OpenAIClient(api_key=openai_api_key)
    zyte_client = ZyteClient(api_key=zyte_api_key)

    start_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    records = stream_restaurants_from_csv(input_path)

    business_type_filter = BusinessTypeFilter("data/included_business_types.txt", "data/excluded_business_types.txt")
    inactive_filter = InactiveBusinessFilter()

    filtered_restaurants = list(apply_all_filters(records, [business_type_filter, inactive_filter]))
    filtered_restaurants_csv = export_restaurant_records_to_csv(filtered_restaurants)

    searcher = IncorporationSearcher(zyte_client)


    async with searcher:
        matcher = RestaurantMatcher(searcher, openai_client)

        tasks = [match_restaurant_to_legal_entity(restaurant, matcher) for restaurant in filtered_restaurants]
        restaurant_results = await asyncio.gather(*tasks, return_exceptions=True)

    # Process results and handle exceptions
    match_results, validation_results, error_count, error_rate = process_restaurant_results(
        restaurant_results, filtered_restaurants
    )

    # Generate output files
    output_files = generate_all_outputs(match_results, str(output_path))

    if validation_results:
        validation_file = save_validation_results(validation_results, output_path, start_timestamp)
    else:
        validation_file = None

    # Transformation - always run to include all filtered businesses
    report_generator = ReportGenerator()
    final_output = run_transformation(
        report_generator, start_timestamp, validation_file, filtered_restaurants_csv
    )

    duration = datetime.now() - start_timestamp
    logger.info("PIPELINE COMPLETED")
    logger.info(f"Duration: {duration}")
    logger.info(f"Output File: {final_output}")


if __name__ == "__main__":
    main("data/restaurants.csv", "data/output.csv")

async def match_restaurant_to_legal_entity(
        self,
        restaurant: RestaurantRecord,
        matcher: RestaurantMatcher
) -> Tuple[Optional[MatchResult], Optional[ValidationResult]]:
    # Step 1: Find top 3 candidate matches
    match_results = []
    try:
        match_results = await matcher.find_best_match(restaurant)
    except Exception as e:
        logger.error(f"Error matching restaurant '{restaurant.name}': {e}", exc_info=True)

    # Step 2: Validate all candidates and select the best one
    validation_result = None
    selected_match = None

    if match_results:
        try:
            selected_match, validation_result = await self.validator.validate_best_match_from_candidates(
                match_results)
            return selected_match, validation_result
        except Exception as e:
            logger.error(f"Error validating matches for '{restaurant.name}': {e}", exc_info=True)

    return selected_match, validation_result

def process_restaurant_results(
        restaurant_results: List[Union[Tuple[Optional[MatchResult], Optional[ValidationResult]], Exception]],
        restaurants: List[RestaurantRecord]
) -> Tuple[List[MatchResult], List[ValidationResult], int, float]:
    match_results = []
    validation_results = []
    error_count = 0

    for i, result in enumerate(restaurant_results):
        if isinstance(result, Exception):
            logger.error(f"Error processing restaurant '{restaurants[i].name}': {result}")
            error_count += 1
            continue

        match_result, validation_result = result

        # Add the match result if it exists
        if match_result:
            match_results.append(match_result)

        if validation_result:
            validation_results.append(validation_result)

    # Calculate error rate and check threshold
    error_rate = error_count / len(restaurants) if restaurants else 0.0
    if error_rate > 0.05:
        raise RuntimeError(
            f"Pipeline failure: {error_count}/{len(restaurants)} restaurants failed "
        )

    if error_count > 0:
        logger.warning(f"Encountered {error_count} errors during processing ({error_rate * 100:.1f}% error rate)")

    logger.info(f" Processed {len(restaurants)} restaurants")
    logger.info(f"   Matches found: {len(match_results)}")
    logger.info(f"   Validated: {len(validation_results)}")

    return match_results, validation_results, error_count, error_rate


def generate_all_outputs(matches: List[MatchResult], output_dir: str = "data/output") -> GeneratedOutputFiles:
    logger.info("Generating output files...")

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Generate timestamp for unique filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # File paths
    matched_csv = output_path / f"matched_restaurants_{timestamp}.csv"
    unmatched_csv = output_path / f"unmatched_restaurants_{timestamp}.csv"

    # Generate files
    generate_matched_restaurants_csv(matches, str(matched_csv))
    generate_unmatched_restaurants_csv(matches, str(unmatched_csv))

    generated_files = GeneratedOutputFiles(
        matched_csv=str(matched_csv),
        unmatched_csv=str(unmatched_csv)
    )

    return generated_files


def generate_matched_restaurants_csv(matches: List[MatchResult], output_path: str) -> None:
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Filter only accepted matches
    accepted_matches = [match for match in matches if match.is_accepted and match.business]

    logger.info(f"Generating matched restaurants CSV with {len(accepted_matches)} matches")

    # Prepare data for CSV
    data = []
    for match in accepted_matches:
        restaurant = match.restaurant
        business = match.business

        row = {
            # Restaurant information (from Google Maps)
            'restaurant_name': restaurant.name,
            'restaurant_address': restaurant.address,
            'restaurant_city': restaurant.city,
            'restaurant_postal_code': restaurant.postal_code,
            'restaurant_coordinates': f"{restaurant.coordinates[0]}, {restaurant.coordinates[1]}",
            'restaurant_rating': restaurant.rating,
            'restaurant_reviews_count': restaurant.reviews_count,
            'restaurant_phone': restaurant.phone,
            'restaurant_website': restaurant.website,
            'restaurant_main_type': restaurant.main_type,
            'restaurant_google_id': restaurant.google_id,

            # Business information (from PR incorporation docs)
            'business_legal_name': business.legal_name,
            'business_registration_number': business.registration_number,
            'business_registration_index': business.registration_index,
            'business_address': business.business_address,
            'business_status': business.status,
            'business_resident_agent_name': business.resident_agent_name,
            'business_resident_agent_address': business.resident_agent_address,

            # Match information
            'match_confidence_score': match.confidence_score,
            'match_type': match.match_type,
            'name_score': match.name_score,
            'postal_code_match': match.postal_code_match,
            'city_match': match.city_match,
            'match_reason': match.match_reason
        }
        data.append(row)

    # Create DataFrame and save
    df = pd.DataFrame(data)
    df.to_csv(output_path, index=False)

    logger.info(f"Saved {len(accepted_matches)} matched restaurants to {output_path}")


def generate_unmatched_restaurants_csv(matches: List[MatchResult], output_path: str) -> None:
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Filter unmatched restaurants
    unmatched = [match for match in matches if not match.is_accepted]

    logger.info(f"Generating unmatched restaurants CSV with {len(unmatched)} restaurants")

    # Prepare data for CSV
    data = []
    for match in unmatched:
        restaurant = match.restaurant

        row = {
            'restaurant_name': restaurant.name,
            'restaurant_address': restaurant.address,
            'restaurant_city': restaurant.city,
            'restaurant_postal_code': restaurant.postal_code,
            'restaurant_coordinates': f"{restaurant.coordinates[0]}, {restaurant.coordinates[1]}",
            'restaurant_rating': restaurant.rating,
            'restaurant_reviews_count': restaurant.reviews_count,
            'restaurant_phone': restaurant.phone,
            'restaurant_website': restaurant.website,
            'restaurant_main_type': restaurant.main_type,
            'restaurant_google_id': restaurant.google_id,
            'match_confidence_score': match.confidence_score if match.confidence_score else 0,
            'match_type': match.match_type,
            'match_reason': match.match_reason,
            # Include best candidate business info if available
            'best_candidate_legal_name': match.business.legal_name if match.business else '',
            'best_candidate_registration_number': match.business.registration_number if match.business else '',
            'best_candidate_registration_index': match.business.registration_index if match.business else '',
            'best_candidate_address': match.business.business_address if match.business else '',
            'best_candidate_status': match.business.status if match.business else '',
            'best_candidate_resident_agent_name': match.business.resident_agent_name if match.business else '',
            'best_candidate_resident_agent_address': match.business.resident_agent_address if match.business else '',
            'name_score': match.name_score if match.name_score else 0,
            'postal_code_match': match.postal_code_match if match.postal_code_match is not None else False,
            'city_match': match.city_match if match.city_match is not None else False
        }
        data.append(row)

    # Create DataFrame and save
    df = pd.DataFrame(data)
    df.to_csv(output_path, index=False)

    logger.info(f"Saved {len(unmatched)} unmatched restaurants to {output_path}")

def save_validation_results(
        validation_results: List[ValidationResult],
        output_path: Path,
        timestamp: str
) -> Optional[str]:
    import pandas as pd

    validation_path = output_path / "validation"
    validation_path.mkdir(parents=True, exist_ok=True)

    validation_df = pd.DataFrame([r.__dict__ for r in validation_results])
    validation_file_path = validation_path / f"validation_results_all_{timestamp}.csv"
    validation_df.to_csv(validation_file_path, index=False)
    logger.info(f"Saved {len(validation_df)} validation results to {validation_file_path}")

    # Save medium and high confidence matches and return its path for final report generation
    validated_df = validation_df[validation_df['openai_confidence'].isin(['medium', 'high'])]
    if not validated_df.empty:
        validated_path = validation_path / f"validated_matches_accept_{timestamp}.csv"
        validated_df.to_csv(validated_path, index=False)
        logger.info(f"Saved {len(validated_df)} validated matches (medium/high confidence) to {validated_path}")
        return str(validated_path)

    # No medium/high confidence matches - return None (final report won't be generated)
    logger.warning("No medium or high confidence matches found - final report will not be generated")
    return None

def run_transformation(
        report_generator: ReportGenerator,
        timestamp: str,
        validation_file: Optional[str],
        filtered_csv: str
) -> Optional[str]:
    if not report_generator:
        return None

    final_output_path = f"final_output_{timestamp}.csv"
    transform_result = report_generator.run(
        output_csv_path=str(final_output_path),
        validation_csv_path=validation_file,
        filtered_csv_path=filtered_csv
    )

    if transform_result.get('success'):
        logger.info(f"Transformation completed: {final_output_path}")
        return str(final_output_path)

    return None