"""
Event producer — publishes ad events to Kafka (or the in-memory stream).

Serializes each event to JSON keyed by campaign_id so all events for a
campaign land on the same partition (ordered per campaign).
"""

import json
from datetime import datetime

from app.config import settings, kafka_enabled
from app.models.events import AdEvent, AdEventCreate
from app.pipeline import stream

try:
    from confluent_kafka import Producer as _KafkaProducer  # type: ignore
except ImportError:  # pragma: no cover - kafka client optional at runtime
    _KafkaProducer = None


def _serialize(event: AdEvent) -> str:
    return json.dumps(event.model_dump(), default=_json_default)


def _json_default(o):
    if isinstance(o, datetime):
        return o.isoformat()
    return str(o)


class EventProducer:
    def __init__(self):
        self.live = kafka_enabled() and _KafkaProducer is not None
        self._producer = None
        if self.live:
            self._producer = _KafkaProducer({
                "bootstrap.servers": settings.kafka_bootstrap,
                "linger.ms": 50,
                "acks": "all",
            })

    def produce(self, event: AdEventCreate) -> AdEvent:
        full = AdEvent(**event.model_dump()).with_defaults()
        payload = _serialize(full)
        if self.live:
            self._producer.produce(
                settings.topic,
                key=full.campaign_id.encode(),
                value=payload.encode(),
            )
            self._producer.poll(0)
        else:
            stream.publish(settings.topic, payload)
        return full

    def flush(self, timeout: float = 5.0) -> None:
        if self.live and self._producer is not None:
            self._producer.flush(timeout)


# Shared instance used by the ingestion API.
producer = EventProducer()
