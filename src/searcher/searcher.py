import logging
from typing import Dict, List, Any, Optional

from src.core.models import BusinessRecord
from src.core.api_models import (
    CorporationSearchResponse,
    CorporationDetailResponse,
    CorporationDetailResponseData,
    CorporationSearchRecord,
)
from src.clients.zyte_client import ZyteClient
from src.clients.client_protocols import ZyteClientProtocol
from pydantic import ValidationError

logger = logging.getLogger(__name__)


class IncorporationSearcher:
    BASE_URL = "https://rceapi.estado.pr.gov/api"

    def __init__(self, zyte_client: ZyteClientProtocol):
        self.zyte_client = zyte_client
        self.search_url = f"{self.BASE_URL}/corporation/search"

    def get_detail_url(self, registration_index: str) -> str:
        return f"{self.BASE_URL}/corporation/info/{registration_index}"

    async def __aenter__(self):
        await self.zyte_client.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.zyte_client.close()

    async def search_business(self, business_name: str, limit: int = 250) -> List[CorporationSearchRecord]:
        payload = {
            "cancellationMode": False,
            "comparisonType": 1,
            "corpName": business_name,
            "isWorkFlowSearch": False,
            "limit": limit,
            "matchType": 4,
            "onlyActive": True
        }

        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/problem+json; charset=UTF-8',
            'Origin': 'https://rcp.estado.pr.gov',
            'Authorization': 'null'
        }

        try:
            response = await self._make_corporation_search_request(payload, headers)
            logger.debug(f"Search response for '{business_name}': {response}")

            if response and response.response and response.response.records:
                records = response.response.records
                logger.info(f"Found {len(records)} records for '{business_name}'")
                return records
            else:
                logger.warning(f"No records found for '{business_name}'")
                return []

        except Exception as e:
            logger.error(f"Error searching for business '{business_name}': {e}")
            return []

    async def _make_corporation_search_request(self, payload: Dict, headers: Dict) -> Optional[
        CorporationSearchResponse]:
        try:
            zyte_response = await self.zyte_client.post_request(
                url=self.search_url,
                request_body=payload,
                headers=headers
            )

            try:
                decoded_body = zyte_response.decode_body()
                search_response = CorporationSearchResponse(**decoded_body)
                return search_response
            except ValidationError as e:
                logger.error(f"Failed to parse PR search response: {e}")
                return None

        except Exception as e:
            logger.error(f"Zyte POST request failed: {e}")
            return None

    async def _get_business_details(self, business_entity_id: int, registration_index: str = None) -> Optional[
        CorporationDetailResponseData]:
        try:
            url = self.get_detail_url(registration_index)
            headers = {
                'Accept': 'application/json, text/plain, */*',
                'Origin': 'https://rcp.estado.pr.gov',
                'Authorization': 'null'
            }

            response = await self._make_corporation_detail_get_request(url, headers)

            if response and response.response and response.response.corporation:
                return response.response
            else:
                logger.debug(f"URL {url} failed, trying next...")

            return None

        except Exception as e:
            logger.error(f"Error getting business details for entity {business_entity_id}: {e}")
            return None

    async def _make_corporation_detail_get_request(self, url: str, headers: Dict) -> Optional[
        CorporationDetailResponse]:
        try:
            zyte_response = await self.zyte_client.get_request(
                url=url,
                headers=headers
            )

            try:
                decoded_body = zyte_response.decode_body()
                corporation_detail = CorporationDetailResponse(**decoded_body)
                return corporation_detail
            except ValidationError as e:
                logger.error(f"Failed to parse PR detail response: {e}")
                return None

        except Exception as e:
            logger.error(f"Zyte GET request failed: {e}")
            return None

    def _create_business_record_from_details(self, details: CorporationDetailResponseData) -> BusinessRecord:
        corporation = details.corporation
        main_location = details.mainLocation
        resident_agent = details.residentAgent

        # Extract address information
        business_address = ''

        if main_location and main_location.streetAddress:
            street_addr = main_location.streetAddress
            address_parts = []
            if street_addr.address1:
                address_parts.append(street_addr.address1)
            if street_addr.address2:
                address_parts.append(street_addr.address2)
            if street_addr.city:
                address_parts.append(street_addr.city)
            if street_addr.zip:
                address_parts.append(street_addr.zip)
            business_address = ', '.join(address_parts)

        # Extract resident agent information
        resident_agent_name = ''
        resident_agent_address = ''

        if resident_agent:
            if resident_agent.isIndividual and resident_agent.individualName:
                individual = resident_agent.individualName
                name_parts = []
                if individual.firstName:
                    name_parts.append(individual.firstName)
                if individual.middleName:
                    name_parts.append(individual.middleName)
                if individual.lastName:
                    name_parts.append(individual.lastName)
                if individual.surName:
                    name_parts.append(individual.surName)
                resident_agent_name = ' '.join(name_parts)
            elif resident_agent.organizationName:
                resident_agent_name = resident_agent.organizationName.name or ''

            if resident_agent.streetAddress:
                agent_addr = resident_agent.streetAddress
                address_parts = []
                if agent_addr.address1:
                    address_parts.append(agent_addr.address1)
                if agent_addr.address2:
                    address_parts.append(agent_addr.address2)
                resident_agent_address = ' '.join(address_parts).strip()

        return BusinessRecord.from_corporation(
            corporation,
            business_address=business_address,
            resident_agent_name=resident_agent_name,
            resident_agent_address=resident_agent_address,
        )

    async def get_business_details_for_records(self, records: List[CorporationSearchRecord]) -> List[BusinessRecord]:
        business_records = []

        for record in records:
            try:
                business_entity_id = record.businessEntityId

                if business_entity_id:
                    # Get detailed business information using registrationIndex
                    registration_index = record.registrationIndex
                    detailed_record = await self._get_business_details(business_entity_id, registration_index)
                    if detailed_record:
                        business_record = self._create_business_record_from_details(detailed_record)
                    else:
                        # Fallback to search result if details fail
                        business_record = self._create_business_record_from_search(record)
                        logger.debug(f"Fell back to search result for {business_entity_id}")
                else:
                    # Fallback to search result if no businessEntityId
                    business_record = self._create_business_record_from_search(record)
                    logger.debug(f"No businessEntityId, using search result")

                business_records.append(business_record)
                logger.debug(f"Created business record: {business_record.legal_name}")
            except Exception as e:
                logger.warning(f"Failed to create business record: {e}")
                continue

        return business_records

    def _create_business_record_from_search(self, record: CorporationSearchRecord) -> BusinessRecord:
        return BusinessRecord(
            legal_name=record.corpName or '',
            registration_number=str(record.registrationNumber) if record.registrationNumber else '',
            registration_index=record.registrationIndex or '',
            status=record.statusEn or '',
        )
