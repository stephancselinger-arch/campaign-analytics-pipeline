# Campaign Analytics Pipeline

Event ingestion → Kafka → ClickHouse streaming pipeline for ad impression/click data. Validates a high-volume stream of ad events at the edge, buffers them through Kafka, and sinks them into ClickHouse where day-partitioned rollups make campaign KPIs (CTR, CVR, CPC, CPA, win rate, spend) cheap to query.

Runs two ways: against **real Kafka + ClickHouse** (via `docker compose`), or fully **in-memory with zero infrastructure** for local demos and CI — the producer, consumer, and sink each fall back to an in-process backend, exactly like the other services in this stack.

## Features

- **Ingestion Gateway** — FastAPI front door that validates events and produces them to Kafka, keyed by `campaign_id` for per-campaign ordering
- **Streaming Buffer** — Kafka decouples spiky ingestion from the sink; the consumer commits offsets only after a successful ClickHouse write (at-least-once)
- **Transform Stage** — flattens events to columnar rows with derived fields (`event_date` partition, `is_*` flags, booked `revenue_usd`) so rollups are plain SUMs
- **ClickHouse Sink** — batched bulk inserts into a `MergeTree` table partitioned by day
- **Incremental Rollups** — a `SummingMergeTree` materialized view keeps `campaign_daily` aggregates current; KPI reads scan a tiny table
- **Poison-message safe** — invalid events are dropped with a warning, never stalling the stream
- **Zero-infra mode** — no `KAFKA_BOOTSTRAP_SERVERS` / `CLICKHOUSE_HOST`? It runs in-memory end to end

## Architecture

```
                    POST /v1/events/ingest
Ad Servers / DSP ─────────────────────────▶  Ingestion Gateway (FastAPI)
                                                      │  produce (key=campaign_id)
                                                      ▼
                                              Kafka topic: ad-events
                                                      │  poll batches
                                                      ▼
                                              Consumer Worker
                                                ├── transform → columnar rows
                                                └── bulk insert
                                                      ▼
                                              ClickHouse: ad_events (MergeTree)
                                                      │  materialized view
                                                      ▼
                                              campaign_daily (SummingMergeTree)
                                                      ▼
                                              KPI queries (CTR/CVR/CPC/CPA)
```

## Quickstart (no infrastructure)

Runs the whole pipeline in one process with in-memory Kafka + ClickHouse stand-ins:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# end-to-end demo: produce synthetic events → sink → print rollup
PYTHONPATH=. python -m app.pipeline.run_local
```

Or run just the ingestion gateway:

```bash
uvicorn app.ingest.api:app --port 8004 --reload
```

API docs: http://localhost:8004/docs

## Full stack (Kafka + ClickHouse)

```bash
docker compose up
```

This starts four services: `kafka` (KRaft mode), `clickhouse` (schema auto-loaded from `app/clickhouse/schema.sql`), the `ingest` gateway on :8004, and the `consumer` worker. Setting `KAFKA_BOOTSTRAP_SERVERS` and `CLICKHOUSE_HOST` flips every stage from in-memory to live automatically.

## Configuration

| Env var | Default | Effect |
|---------|---------|--------|
| `KAFKA_BOOTSTRAP_SERVERS` | _(unset)_ | Set → use real Kafka; unset → in-memory stream |
| `KAFKA_TOPIC` | `ad-events` | Topic events are produced to / consumed from |
| `KAFKA_CONSUMER_GROUP` | `ch-sink` | Consumer group id for the sink worker |
| `CLICKHOUSE_HOST` | _(unset)_ | Set → use real ClickHouse; unset → in-memory rows |
| `CLICKHOUSE_PORT` | `8123` | ClickHouse HTTP port |
| `CLICKHOUSE_DB` | `analytics` | Target database |
| `BATCH_SIZE` | `500` | Max events pulled + inserted per batch |

## Components

| Module | Responsibility |
|--------|----------------|
| `app/ingest/api.py` | FastAPI ingestion gateway + KPI report endpoints |
| `app/pipeline/producer.py` | Kafka producer (in-memory fallback) |
| `app/pipeline/consumer.py` | Kafka consumer (in-memory fallback) |
| `app/pipeline/transforms.py` | Event → ClickHouse row mapping + derived fields |
| `app/pipeline/sink.py` | Batched ClickHouse writer (in-memory fallback) |
| `app/pipeline/worker.py` | Consumer loop: poll → transform → write → commit |
| `app/pipeline/rollups.py` | Campaign KPI aggregations (SQL or in-memory) |
| `app/clickhouse/schema.sql` | Table, rollup table, and materialized view DDL |

## API Reference

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/v1/events/ingest` | Validate a batch of ad events and produce to Kafka |
| `GET` | `/v1/reports/campaigns` | Campaign KPI rollups (filter by `campaign_id`, `date_from`, `date_to`) |
| `GET` | `/health` | Status + active backend modes + stream depth |

## Example: Ingest Events

```json
POST /v1/events/ingest
[
  {
    "event_type": "win",
    "timestamp": "2026-06-01T10:00:00Z",
    "campaign_id": "cmp_alpha",
    "creative_id": "cr_1",
    "advertiser_id": "adv_1",
    "clearing_price_cpm": 4.00,
    "geo_country": "USA",
    "device_type": "mobile"
  },
  {
    "event_type": "impression",
    "timestamp": "2026-06-01T10:00:01Z",
    "campaign_id": "cmp_alpha",
    "creative_id": "cr_1"
  }
]
```

```json
{ "produced": 2, "topic": "ad-events" }
```

## Example: Campaign Rollup

```
GET /v1/reports/campaigns?campaign_id=cmp_alpha
```

```json
[
  {
    "campaign_id": "cmp_alpha",
    "impressions": 431,
    "clicks": 211,
    "wins": 268,
    "conversions": 111,
    "spend_usd": 0.9818,
    "ctr": 0.4896,
    "cvr": 0.5261,
    "cpc_usd": 0.0047,
    "cpa_usd": 0.0088,
    "win_rate": 0.6218
  }
]
```

In live mode this reads from the `campaign_daily` rollup table. The equivalent SQL is documented at the bottom of `app/clickhouse/schema.sql`.

## Running Tests

```bash
PYTHONPATH=. pytest tests/ -v
```

Covers the transform stage (flags, revenue, validation, column ordering), the producer → consumer → sink path, poison-message handling, and KPI rollups with zero-division and date-filter edge cases — all against the in-memory backends, so no Kafka or ClickHouse is needed.

## Production Considerations

| Concern | Dev (current) | Production |
|---------|--------------|------------|
| Transport | In-memory deque | Kafka, partitioned by `campaign_id`, replicated |
| Delivery | Drain-once | At-least-once; offsets committed post-write |
| Storage | In-memory rows | ClickHouse `MergeTree`, partitioned by day, TTL'd |
| Rollups | Python aggregation | `SummingMergeTree` materialized view |
| Sink throughput | Per-batch insert | Async inserts / buffer tables, tuned `BATCH_SIZE` |
| Schema drift | N/A | Schema registry (Avro/Protobuf) on the topic |
| Dedup | None | `ReplacingMergeTree` on `event_id` or upstream idempotency |

## Tech Stack

- **Apache Kafka** (KRaft) — durable, partitioned event transport
- **ClickHouse** — columnar OLAP store with materialized-view rollups
- **confluent-kafka** — producer/consumer client
- **clickhouse-connect** — ClickHouse client
- **FastAPI** + **Pydantic v2** — ingestion gateway + validation
- Python 3.12+

<!-- Last updated: 2026-06-03 -->

<!-- Last updated: 2026-06-05 -->

<!-- Last updated: 2026-06-07 -->

<!-- Last updated: 2026-06-09 -->

<!-- Last updated: 2026-06-11 -->

<!-- Last updated: 2026-06-13 -->
