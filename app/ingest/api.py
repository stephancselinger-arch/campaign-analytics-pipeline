"""
Ingestion gateway — the HTTP front door of the pipeline.

Validates incoming ad events and produces them to Kafka. The consumer worker
(app.pipeline.worker) drains the topic into ClickHouse independently. KPI
rollups are exposed for convenience and read from ClickHouse in live mode.
"""

from datetime import date
from typing import Optional

from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import kafka_enabled, clickhouse_enabled, settings
from app.models.events import AdEventCreate
from app.pipeline import rollups, stream
from app.pipeline.producer import producer

app = FastAPI(
    title="Campaign Analytics Pipeline",
    description=(
        "Event ingestion → Kafka → ClickHouse pipeline for ad impression/click "
        "data. This service is the ingestion gateway; it validates events and "
        "produces them to Kafka for the consumer worker to sink into ClickHouse."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

router = APIRouter(prefix="/v1")


@router.post("/events/ingest")
def ingest(events: list[AdEventCreate]) -> dict:
    produced = [producer.produce(ev) for ev in events]
    producer.flush()
    return {"produced": len(produced), "topic": settings.topic}


@router.get("/reports/campaigns")
def campaign_reports(
    campaign_id: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> list[dict]:
    return rollups.campaign_summary(campaign_id, date_from, date_to)


app.include_router(router)


@app.get("/health")
def health() -> dict:
    return {
        "status": "ok",
        "kafka": "live" if kafka_enabled() else "in-memory",
        "clickhouse": "live" if clickhouse_enabled() else "in-memory",
        "topic": settings.topic,
        "stream_depth": stream.depth(settings.topic),
    }
