"""
Local end-to-end demo — runs the whole pipeline in one process with the
in-memory backends (no Kafka, no ClickHouse required):

    python -m app.pipeline.run_local

Produces a batch of synthetic ad events, drains them through the consumer +
sink, and prints the campaign KPI rollup.
"""

import json
import random
from datetime import datetime, timezone, timedelta

from app.models.events import AdEventCreate, EventType
from app.pipeline import rollups
from app.pipeline.consumer import EventConsumer
from app.pipeline.producer import EventProducer
from app.pipeline.sink import ClickHouseSink
from app.pipeline.worker import drain


def synth_events(n: int = 2000, campaigns=("cmp_alpha", "cmp_beta")) -> list[AdEventCreate]:
    events: list[AdEventCreate] = []
    now = datetime.now(timezone.utc)
    for _ in range(n):
        cid = random.choice(campaigns)
        roll = random.random()
        if roll < 0.45:
            etype, clearing = EventType.IMPRESSION, None
        elif roll < 0.70:
            etype, clearing = EventType.WIN, round(random.uniform(1.5, 6.0), 2)
        elif roll < 0.90:
            etype, clearing = EventType.CLICK, None
        else:
            etype, clearing = EventType.CONVERSION, None
        events.append(AdEventCreate(
            event_type=etype,
            timestamp=now - timedelta(minutes=random.randint(0, 60 * 24)),
            campaign_id=cid,
            creative_id=random.choice(["cr_1", "cr_2", "cr_3"]),
            advertiser_id="adv_1",
            clearing_price_cpm=clearing,
            geo_country=random.choice(["USA", "CAN", "GBR"]),
            device_type=random.choice(["mobile", "desktop", "ctv"]),
        ))
    return events


def main() -> None:
    producer = EventProducer()
    consumer = EventConsumer()
    sink = ClickHouseSink()

    events = synth_events()
    for ev in events:
        producer.produce(ev)
    producer.flush()
    print(f"produced {len(events)} events")

    written = drain(consumer, sink)
    print(f"sank {written} rows into ClickHouse ({sink.count()} total)")

    print("\ncampaign rollup:")
    print(json.dumps(rollups.campaign_summary(), indent=2))


if __name__ == "__main__":  # pragma: no cover
    main()
