"""
Ad event models — the raw impression/click/win/conversion stream that flows
through Kafka into ClickHouse.
"""

from enum import Enum
from typing import Optional
from datetime import datetime, timezone
import uuid

from pydantic import BaseModel, Field


class EventType(str, Enum):
    IMPRESSION = "impression"
    CLICK = "click"
    WIN = "win"
    CONVERSION = "conversion"
    VIDEO_START = "video_start"
    VIDEO_COMPLETE = "video_complete"


class AdEventCreate(BaseModel):
    event_type: EventType
    timestamp: Optional[datetime] = None        # defaults to ingest time
    campaign_id: str
    line_item_id: str = ""
    creative_id: str = ""
    advertiser_id: str = ""

    bid_price_cpm: Optional[float] = None
    clearing_price_cpm: Optional[float] = None

    publisher_domain: str = ""
    placement_type: str = ""                     # banner / video / native
    device_type: str = ""
    geo_country: str = ""
    user_id: str = ""

    conversion_value_usd: Optional[float] = None


class AdEvent(AdEventCreate):
    event_id: str = Field(default_factory=lambda: f"evt_{uuid.uuid4().hex[:16]}")

    def with_defaults(self) -> "AdEvent":
        if self.timestamp is None:
            self.timestamp = datetime.now(timezone.utc)
        return self
