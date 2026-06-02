"""
ClickHouse sink — bulk-writes transformed rows into the `ad_events` table.

Live mode uses clickhouse-connect; without CLICKHOUSE_HOST it keeps rows in an
in-memory list that the rollup layer can query, so the whole pipeline runs with
no infrastructure.
"""

from typing import Optional

from app.config import settings, clickhouse_enabled
from app.pipeline.transforms import COLUMNS, to_row_tuple

try:
    import clickhouse_connect  # type: ignore
except ImportError:  # pragma: no cover - clickhouse client optional at runtime
    clickhouse_connect = None


# In-memory store used in mock mode (list of row dicts).
_rows: list[dict] = []


class ClickHouseSink:
    def __init__(self):
        self.live = clickhouse_enabled() and clickhouse_connect is not None
        self._client = None
        if self.live:
            self._client = clickhouse_connect.get_client(
                host=settings.clickhouse_host,
                port=settings.clickhouse_port,
                database=settings.clickhouse_db,
                username=settings.clickhouse_user,
                password=settings.clickhouse_password,
            )

    def write(self, rows: list[dict]) -> int:
        if not rows:
            return 0
        if self.live:
            data = [to_row_tuple(r) for r in rows]
            self._client.insert(settings.clickhouse_table, data, column_names=COLUMNS)
        else:
            _rows.extend(rows)
        return len(rows)

    def count(self) -> int:
        if self.live:
            result = self._client.query(f"SELECT count() FROM {settings.clickhouse_table}")
            return int(result.result_rows[0][0])
        return len(_rows)

    @staticmethod
    def memory_rows() -> list[dict]:
        """Rows held in mock mode — used by the in-memory rollup queries."""
        return _rows


def reset() -> None:
    _rows.clear()
