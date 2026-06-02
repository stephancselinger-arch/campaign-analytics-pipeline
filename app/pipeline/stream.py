"""
In-memory transport — a process-local stand-in for a Kafka topic.

Used when KAFKA_BOOTSTRAP_SERVERS is unset so the producer, consumer, and
sink can run end-to-end in one process (demos, tests, `python -m
app.pipeline.run_local`). The producer appends JSON-encoded events; the
consumer drains them in FIFO order.
"""

from collections import deque
from typing import Optional


_topics: dict[str, deque] = {}


def _topic(name: str) -> deque:
    return _topics.setdefault(name, deque())


def publish(topic: str, payload: str) -> None:
    _topic(topic).append(payload)


def poll(topic: str, max_messages: int) -> list[str]:
    q = _topic(topic)
    out: list[str] = []
    while q and len(out) < max_messages:
        out.append(q.popleft())
    return out


def depth(topic: Optional[str] = None) -> int:
    if topic is not None:
        return len(_topic(topic))
    return sum(len(q) for q in _topics.values())


def reset() -> None:
    _topics.clear()
