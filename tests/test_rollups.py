import pytest
from datetime import datetime, timezone, date

from app.models.events import AdEventCreate, EventType
from app.pipeline import stream, sink as sink_mod
from app.pipeline.producer import EventProducer
from app.pipeline.consumer import EventConsumer
from app.pipeline.sink import ClickHouseSink
from app.pipeline.worker import drain
from app.pipeline import rollups


@pytest.fixture(autouse=True)
def clean_backends():
    stream.reset()
    sink_mod.reset()
    yield
    stream.reset()
    sink_mod.reset()


def _ev(etype, cid, **over):
    return AdEventCreate(event_type=etype, campaign_id=cid,
                         timestamp=datetime.now(timezone.utc), **over)


def _load(events):
    producer = EventProducer()
    for e in events:
        producer.produce(e)
    drain(EventConsumer(), ClickHouseSink())


def test_campaign_summary_kpis():
    _load([
        _ev(EventType.IMPRESSION, "cmp_1"),
        _ev(EventType.IMPRESSION, "cmp_1"),
        _ev(EventType.IMPRESSION, "cmp_1"),
        _ev(EventType.IMPRESSION, "cmp_1"),
        _ev(EventType.CLICK, "cmp_1"),
        _ev(EventType.CONVERSION, "cmp_1"),
        _ev(EventType.WIN, "cmp_1", clearing_price_cpm=4.0),
    ])
    [summary] = rollups.campaign_summary(campaign_id="cmp_1")
    assert summary["impressions"] == 4
    assert summary["clicks"] == 1
    assert summary["conversions"] == 1
    assert summary["wins"] == 1
    assert summary["ctr"] == pytest.approx(0.25)        # 1 / 4
    assert summary["cvr"] == pytest.approx(1.0)         # 1 / 1
    assert summary["spend_usd"] == pytest.approx(0.004)  # 4.0 / 1000


def test_campaign_summary_groups_and_filters():
    _load([
        _ev(EventType.IMPRESSION, "cmp_1"),
        _ev(EventType.IMPRESSION, "cmp_2"),
        _ev(EventType.CLICK, "cmp_2"),
    ])
    all_campaigns = rollups.campaign_summary()
    assert {c["campaign_id"] for c in all_campaigns} == {"cmp_1", "cmp_2"}

    only_two = rollups.campaign_summary(campaign_id="cmp_2")
    assert len(only_two) == 1
    assert only_two[0]["clicks"] == 1


def test_zero_division_safe():
    _load([_ev(EventType.IMPRESSION, "cmp_1")])  # impressions but no clicks
    [summary] = rollups.campaign_summary(campaign_id="cmp_1")
    assert summary["ctr"] == 0.0
    assert summary["cpc_usd"] == 0.0


def test_date_filtering_excludes_out_of_range():
    _load([_ev(EventType.IMPRESSION, "cmp_1")])
    future = date(2099, 1, 1)
    assert rollups.campaign_summary(campaign_id="cmp_1", date_from=future) == []
