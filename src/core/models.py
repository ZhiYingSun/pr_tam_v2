from __future__ import annotations

from typing import Optional, Tuple, List, Dict, Any
from pydantic import BaseModel, Field

class RestaurantRecord(BaseModel):
    name: str
    address: str
    phone: str
    city: str
    postal_code: str
    coordinates: Tuple[float, float]

    google_id: str
    rating: float
    reviews_count: int

    website: Optional[str] = None
    main_type: Optional[str] = None

class RawRestaurantRow(BaseModel):
    google_id: str = Field(alias="Google ID")
    name: str = Field(alias="Name")
    full_address: Optional[str] = Field(alias="Full address", default=None)
    is_closed: bool = Field(alias="Is closed")
    description_1: Optional[str] = Field(alias="Description 1", default=None)
    main_type: Optional[str] = Field(alias="Main type", default=None)
    all_types: Optional[str] = Field(alias="All types", default=None)
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
