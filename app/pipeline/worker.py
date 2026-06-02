"""
Consumer worker — the Kafka → ClickHouse sink stage.

Polls batches of events, transforms them into rows, bulk-writes to ClickHouse,
then commits offsets. Run as a long-lived process:

    python -m app.pipeline.worker

Skips rows that fail validation (a poison message never stalls the stream).
"""

import logging
import signal
import time

from app.config import settings, kafka_enabled, clickhouse_enabled
from app.pipeline.consumer import EventConsumer
from app.pipeline.sink import ClickHouseSink
from app.pipeline.transforms import to_row

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("pipeline.worker")

_running = True


def _transform_batch(events: list[dict]) -> list[dict]:
    rows = []
    for ev in events:
        try:
            rows.append(to_row(ev))
        except ValueError as e:
            log.warning("dropping invalid event: %s", e)
    return rows


def process_once(consumer: EventConsumer, sink: ClickHouseSink) -> int:
    """Poll one batch, write it, commit. Returns rows written."""
    events = consumer.poll_batch()
    if not events:
        return 0
    rows = _transform_batch(events)
    written = sink.write(rows)
    consumer.commit()
    if written:
        log.info("wrote %d rows (clickhouse total=%d)", written, sink.count())
    return written


def drain(consumer: EventConsumer, sink: ClickHouseSink) -> int:
    """Process batches until the stream is empty. Returns total rows written."""
    total = 0
    while True:
        n = process_once(consumer, sink)
        if n == 0:
            break
        total += n
    return total


def run_forever() -> None:
    consumer = EventConsumer()
    sink = ClickHouseSink()
    log.info("worker started | kafka=%s clickhouse=%s topic=%s group=%s",
             kafka_enabled(), clickhouse_enabled(), settings.topic, settings.consumer_group)

    def _stop(*_):
        global _running
        _running = False
        log.info("shutdown signal received")

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    while _running:
        wrote = process_once(consumer, sink)
        if wrote == 0:
            time.sleep(settings.poll_timeout_s)
    consumer.close()


if __name__ == "__main__":  # pragma: no cover
    run_forever()
