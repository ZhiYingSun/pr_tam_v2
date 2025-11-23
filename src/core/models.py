from __future__ import annotations

import json
import base64
import binascii
from typing import Optional, Tuple, List, Dict, Any, Union
from pydantic import BaseModel, Field, field_validator

class RestaurantRecord(BaseModel):
    name: str
    address: str
    phone: str
    city: str
    postal_code: str
    coordinates: Tuple[float, float]

    google_id: str
    is_closed: bool
    rating: float
    reviews_count: int

    website: Optional[str] = None
    main_type: Optional[str] = None
    all_types: list[str]

class RawRestaurantRow(BaseModel):
    google_id: str = Field(alias="Google ID")
    name: str = Field(alias="Name")
    full_address: str = Field(alias="Full address")
    is_closed: bool = Field(alias="Is closed")
    description_1: Optional[str] = Field(alias="Description 1", default=None)
    main_type: Optional[str] = Field(alias="Main type", default=None)
    all_types: list[str] = Field(alias="All types", default=list)
    website: Optional[str] = Field(alias="Website", default=None)
    website_root: Optional[str] = Field(alias="Website (root url)", default=None)
    phone: str = Field(alias="Phone")
    phone_international: Optional[str] = Field(alias="Phone international", default=None)
    borough: Optional[str] = Field(alias="Borough", default=None)
    street: Optional[str] = Field(alias="Street", default=None)
    city: str = Field(alias="City")
    postal_code: str = Field(alias="Postal code")
    state: Optional[str] = Field(alias="State", default=None)
    country: Optional[str] = Field(alias="Country", default=None)
    country_code: Optional[str] = Field(alias="Country code", default=None)
    longitude: float = Field(alias="Longitude")
    latitude: float = Field(alias="Latitude")
    link: Optional[str] = Field(alias="Link", default=None)
    first_seen_on: Optional[str] = Field(alias="First seen on", default=None)
    reviews_count: int = Field(alias="Reviews count")
    reviews_rating: float = Field(alias="Reviews rating")
    reviews_per_score: Optional[str] = Field(alias="Reviews per score", default=None)
    photos_count: Optional[int] = Field(alias="Photos count", default=None)
    photo_1: Optional[str] = Field(alias="Photo 1", default=None)
    photo_2: Optional[str] = Field(alias="Photo 2", default=None)
    all_photos: Optional[str] = Field(alias="All photos", default=None)

    @field_validator("all_types", mode="before")
    def parse_all_types(cls, value: Union[list[str], str]) -> list[str]:
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            return [t.strip() for t in value.split(",") if t.strip()]
        return []

class ZyteHttpResponse(BaseModel):
    httpResponseBody: Optional[str] = Field(None, description="Base64 encoded response body")

    def decode_body(self) -> Dict[str, Any]:
        if not self.httpResponseBody:
            raise ValueError("httpResponseBody is missing or empty")

        try:
            decoded_body = base64.b64decode(self.httpResponseBody).decode('utf-8')
            return json.loads(decoded_body)
        except (binascii.Error, UnicodeDecodeError) as e:
            raise ValueError(f"Failed to decode base64 response body: {e}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse JSON from decoded body: {e}")

