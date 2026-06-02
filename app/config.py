"""
Runtime configuration, driven entirely by environment variables.

The pipeline runs in two modes per backend, mirroring how the other services
in this stack fall back to in-memory implementations:

  * Kafka      — live when KAFKA_BOOTSTRAP_SERVERS is set, else an in-process
                 in-memory stream (single process; great for demos and tests).
  * ClickHouse — live when CLICKHOUSE_HOST is set, else an in-memory row store
                 that supports the same rollup queries.
"""

import os


class Settings:
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")
    topic: str = os.getenv("KAFKA_TOPIC", "ad-events")
    consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "ch-sink")

    clickhouse_host: str = os.getenv("CLICKHOUSE_HOST", "")
    clickhouse_port: int = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    clickhouse_db: str = os.getenv("CLICKHOUSE_DB", "analytics")
    clickhouse_user: str = os.getenv("CLICKHOUSE_USER", "default")
    clickhouse_password: str = os.getenv("CLICKHOUSE_PASSWORD", "")
    clickhouse_table: str = os.getenv("CLICKHOUSE_TABLE", "ad_events")

    batch_size: int = int(os.getenv("BATCH_SIZE", "500"))
    poll_timeout_s: float = float(os.getenv("POLL_TIMEOUT_S", "1.0"))


settings = Settings()


def kafka_enabled() -> bool:
    return bool(settings.kafka_bootstrap)


def clickhouse_enabled() -> bool:
    return bool(settings.clickhouse_host)
