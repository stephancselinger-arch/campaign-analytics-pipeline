import pytest
from datetime import datetime, timezone

from app.pipeline.transforms import to_row, to_row_tuple, COLUMNS


def _impression(**over):
    base = {"event_type": "impression", "campaign_id": "cmp_1",
            "timestamp": "2026-06-01T10:00:00Z"}
    base.update(over)
    return base


def test_to_row_sets_flags_and_date():
    row = to_row(_impression())
    assert row["is_impression"] == 1
    assert row["is_click"] == 0
    assert row["event_date"].isoformat() == "2026-06-01"
    assert row["revenue_usd"] == 0.0


def test_win_books_revenue_from_clearing_cpm():
    row = to_row(_impression(event_type="win", clearing_price_cpm=4.0))
    assert row["is_win"] == 1
    # 4.0 CPM / 1000 = 0.004 per impression
    assert row["revenue_usd"] == pytest.approx(0.004)


def test_non_win_never_books_revenue():
    row = to_row(_impression(event_type="click", clearing_price_cpm=4.0))
    assert row["is_click"] == 1
    assert row["revenue_usd"] == 0.0


def test_unknown_event_type_rejected():
    with pytest.raises(ValueError):
        to_row(_impression(event_type="banana"))


def test_missing_campaign_rejected():
    with pytest.raises(ValueError):
        to_row({"event_type": "impression", "timestamp": "2026-06-01T10:00:00Z"})


def test_naive_timestamp_assumed_utc():
    row = to_row(_impression(timestamp="2026-06-01T10:00:00"))
    assert row["event_time"].tzinfo is not None


def test_row_tuple_matches_column_order():
    row = to_row(_impression())
    tup = to_row_tuple(row)
    assert len(tup) == len(COLUMNS)
    assert tup[COLUMNS.index("campaign_id")] == "cmp_1"
