"""
Event consumer — pulls batches of ad events from Kafka (or the in-memory
stream) and hands back decoded dicts for the transform + sink stages.
"""

import json
from typing import Optional

from app.config import settings, kafka_enabled
from app.pipeline import stream

try:
    from confluent_kafka import Consumer as _KafkaConsumer  # type: ignore
except ImportError:  # pragma: no cover - kafka client optional at runtime
    _KafkaConsumer = None


class EventConsumer:
    def __init__(self):
        self.live = kafka_enabled() and _KafkaConsumer is not None
        self._consumer = None
        if self.live:
            self._consumer = _KafkaConsumer({
                "bootstrap.servers": settings.kafka_bootstrap,
                "group.id": settings.consumer_group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            })
            self._consumer.subscribe([settings.topic])

    def poll_batch(self, max_messages: Optional[int] = None) -> list[dict]:
        max_messages = max_messages or settings.batch_size
        if not self.live:
            raw = stream.poll(settings.topic, max_messages)
            return [json.loads(r) for r in raw]

        out: list[dict] = []
        while len(out) < max_messages:
            msg = self._consumer.poll(settings.poll_timeout_s)
            if msg is None:
                break
            if msg.error():
                continue
            out.append(json.loads(msg.value()))
        return out

    def commit(self) -> None:
        if self.live and self._consumer is not None:
            self._consumer.commit(asynchronous=False)

    def close(self) -> None:
        if self.live and self._consumer is not None:
            self._consumer.close()
