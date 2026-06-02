"""
Rollups — campaign KPI aggregations over the landed events.

In live mode these run as SQL against ClickHouse's `campaign_daily` rollup
table. In mock mode they aggregate the sink's in-memory rows in Python. Both
paths return the same shape, so callers don't care which backend is active.
"""

from datetime import date
from typing import Optional

from app.config import settings, clickhouse_enabled
from app.pipeline.sink import ClickHouseSink

try:
    import clickhouse_connect  # type: ignore
except ImportError:  # pragma: no cover
    clickhouse_connect = None


def _safe_div(n: float, d: float) -> float:
    return round(n / d, 4) if d else 0.0


def _summary_from_counts(campaign_id: str, impressions: int, clicks: int,
                         wins: int, conversions: int, spend: float) -> dict:
    return {
        "campaign_id": campaign_id,
        "impressions": impressions,
        "clicks": clicks,
        "wins": wins,
        "conversions": conversions,
        "spend_usd": round(spend, 4),
        "ctr": _safe_div(clicks, impressions),
        "cvr": _safe_div(conversions, clicks),
        "cpc_usd": _safe_div(spend, clicks),
        "cpa_usd": _safe_div(spend, conversions),
        "win_rate": _safe_div(wins, impressions),
    }


def campaign_summary(
    campaign_id: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> list[dict]:
    if clickhouse_enabled() and clickhouse_connect is not None:
        return _campaign_summary_ch(campaign_id, date_from, date_to)
    return _campaign_summary_memory(campaign_id, date_from, date_to)


def _campaign_summary_memory(campaign_id, date_from, date_to) -> list[dict]:
    buckets: dict[str, dict] = {}
    for row in ClickHouseSink.memory_rows():
        if campaign_id and row["campaign_id"] != campaign_id:
            continue
        if date_from and row["event_date"] < date_from:
            continue
        if date_to and row["event_date"] > date_to:
            continue
        b = buckets.setdefault(row["campaign_id"], {
            "impressions": 0, "clicks": 0, "wins": 0, "conversions": 0, "spend": 0.0,
        })
        b["impressions"] += row["is_impression"]
        b["clicks"] += row["is_click"]
        b["wins"] += row["is_win"]
        b["conversions"] += row["is_conversion"]
        b["spend"] += row["revenue_usd"]

    return [
        _summary_from_counts(cid, b["impressions"], b["clicks"], b["wins"],
                             b["conversions"], b["spend"])
        for cid, b in sorted(buckets.items())
    ]


def _campaign_summary_ch(campaign_id, date_from, date_to) -> list[dict]:  # pragma: no cover - needs live CH
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host, port=settings.clickhouse_port,
        database=settings.clickhouse_db, username=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )
    where = []
    params: dict = {}
    if campaign_id:
        where.append("campaign_id = %(cid)s")
        params["cid"] = campaign_id
    if date_from:
        where.append("event_date >= %(from)s")
        params["from"] = date_from
    if date_to:
        where.append("event_date <= %(to)s")
        params["to"] = date_to
    clause = ("WHERE " + " AND ".join(where)) if where else ""

    sql = f"""
        SELECT campaign_id,
               sum(impressions), sum(clicks), sum(wins),
               sum(conversions), sum(spend_usd)
        FROM campaign_daily
        {clause}
        GROUP BY campaign_id
        ORDER BY campaign_id
    """
    rows = client.query(sql, parameters=params).result_rows
    return [_summary_from_counts(r[0], r[1], r[2], r[3], r[4], r[5]) for r in rows]
