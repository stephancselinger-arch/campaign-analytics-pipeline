import pytest
from datetime import datetime, timezone

from app.models.events import AdEventCreate, EventType
from app.pipeline import stream, sink as sink_mod
from app.pipeline.producer import EventProducer
from app.pipeline.consumer import EventConsumer
from app.pipeline.sink import ClickHouseSink
from app.pipeline.worker import process_once, drain


@pytest.fixture(autouse=True)
def clean_backends():
    stream.reset()
    sink_mod.reset()
    yield
    stream.reset()
    sink_mod.reset()


def _ev(etype, cid="cmp_1", **over):
    return AdEventCreate(event_type=etype, campaign_id=cid,
                         timestamp=datetime.now(timezone.utc), **over)


def test_producer_publishes_to_stream():
    producer = EventProducer()
    assert producer.live is False  # no kafka configured in tests
    producer.produce(_ev(EventType.IMPRESSION))
    producer.produce(_ev(EventType.CLICK))
    assert stream.depth("ad-events") == 2


def test_producer_assigns_event_id_and_timestamp():
    producer = EventProducer()
    full = producer.produce(AdEventCreate(event_type=EventType.IMPRESSION, campaign_id="cmp_1"))
    assert full.event_id.startswith("evt_")
    assert full.timestamp is not None


def test_consumer_drains_in_batches():
    producer = EventProducer()
    for _ in range(5):
        producer.produce(_ev(EventType.IMPRESSION))
    consumer = EventConsumer()
    batch = consumer.poll_batch(max_messages=3)
    assert len(batch) == 3
    assert stream.depth("ad-events") == 2


def test_process_once_writes_rows_and_drains_stream():
    producer = EventProducer()
    producer.produce(_ev(EventType.IMPRESSION))
    producer.produce(_ev(EventType.WIN, clearing_price_cpm=5.0))

    consumer = EventConsumer()
    sink = ClickHouseSink()
    assert sink.live is False
    written = process_once(consumer, sink)
    assert written == 2
    assert sink.count() == 2


def test_drain_processes_everything():
    producer = EventProducer()
    for _ in range(10):
        producer.produce(_ev(EventType.IMPRESSION))
    total = drain(EventConsumer(), ClickHouseSink())
    assert total == 10
    assert stream.depth("ad-events") == 0


def test_invalid_event_is_dropped_not_fatal():
    # Publish a raw invalid payload directly to the stream.
    stream.publish("ad-events", '{"event_type": "bogus", "campaign_id": "cmp_1"}')
    stream.publish("ad-events", '{"event_type": "impression", "campaign_id": "cmp_1", "timestamp": "2026-06-01T00:00:00Z"}')
    written = process_once(EventConsumer(), ClickHouseSink())
    assert written == 1  # invalid one dropped, valid one written
