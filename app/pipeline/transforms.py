"""
Transform stage — maps a raw ad event into a flat ClickHouse row.

Adds the derived columns the analytics layer relies on:
  * event_date    — partition key (Date)
  * is_*          — per-event-type flags so rollups are plain SUMs
  * revenue_usd   — spend booked on a win (clearing CPM / 1000)

Keeping the booleans and revenue precomputed means both the ClickHouse SQL
rollups and the in-memory rollups are simple sums over columns.
"""

from datetime import datetime, timezone

from app.models.events import EventType

# Column order matches the ClickHouse `ad_events` table (schema.sql) so we can
# insert rows positionally via clickhouse-connect.
COLUMNS = [
    "event_id", "event_type", "event_time", "event_date",
    "campaign_id", "line_item_id", "creative_id", "advertiser_id",
    "publisher_domain", "placement_type", "device_type", "geo_country", "user_id",
    "bid_price_cpm", "clearing_price_cpm", "revenue_usd", "conversion_value_usd",
    "is_impression", "is_click", "is_win", "is_conversion",
]


def _parse_time(value) -> datetime:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        dt = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def to_row(event: dict) -> dict:
    """Validate and flatten one event dict into a ClickHouse-ready row."""
    etype = event.get("event_type")
    if etype not in {e.value for e in EventType}:
        raise ValueError(f"Unknown event_type '{etype}'")
    if not event.get("campaign_id"):
        raise ValueError("campaign_id is required")

    event_time = _parse_time(event.get("timestamp"))
    clearing = event.get("clearing_price_cpm")
    is_win = 1 if etype == EventType.WIN.value else 0
    revenue = (clearing / 1000.0) if (is_win and clearing is not None) else 0.0

    return {
        "event_id": event.get("event_id", ""),
        "event_type": etype,
        "event_time": event_time,
        "event_date": event_time.date(),
        "campaign_id": event["campaign_id"],
        "line_item_id": event.get("line_item_id", ""),
        "creative_id": event.get("creative_id", ""),
        "advertiser_id": event.get("advertiser_id", ""),
        "publisher_domain": event.get("publisher_domain", ""),
        "placement_type": event.get("placement_type", ""),
        "device_type": event.get("device_type", ""),
        "geo_country": event.get("geo_country", ""),
        "user_id": event.get("user_id", ""),
        "bid_price_cpm": float(event.get("bid_price_cpm") or 0.0),
        "clearing_price_cpm": float(clearing or 0.0),
        "revenue_usd": round(revenue, 6),
        "conversion_value_usd": float(event.get("conversion_value_usd") or 0.0),
        "is_impression": 1 if etype == EventType.IMPRESSION.value else 0,
        "is_click": 1 if etype == EventType.CLICK.value else 0,
        "is_win": is_win,
        "is_conversion": 1 if etype == EventType.CONVERSION.value else 0,
    }


def to_row_tuple(row: dict) -> tuple:
    """Positional tuple in COLUMNS order, for bulk inserts."""
    return tuple(row[c] for c in COLUMNS)
