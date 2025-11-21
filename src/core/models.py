from __future__ import annotations

import pandas as pd
from typing import Optional, Tuple, List, Dict, Any
from pydantic import BaseModel

class RestaurantRecord(BaseModel):
    """Represents a restaurant from Google Maps data"""
    name: str
    address: str
    city: str
    postal_code: str
    coordinates: Tuple[float, float]
    rating: float

    google_id: Optional[str] = None
    phone: Optional[str] = None
    website: Optional[str] = None
    reviews_count: Optional[int] = None
    main_type: Optional[str] = None