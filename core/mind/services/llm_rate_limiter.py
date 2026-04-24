"""Concurrency and back-pressure helpers for routed LLM calls."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import email.utils
import threading
from typing import Iterator


DEFAULT_CONCURRENCY = 4
MAX_CONCURRENCY = 8


@dataclass(frozen=True)
class RateLimitKey:
    provider: str
    model: str


class LLMRateLimiter:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._semaphores: dict[RateLimitKey, threading.BoundedSemaphore] = {}

    @contextmanager
    def acquire(self, *, provider: str, model: str, configured_cap: int | None) -> Iterator[None]:
        key = RateLimitKey(provider=provider, model=model)
        semaphore = self._semaphore(key, configured_cap=configured_cap)
        semaphore.acquire()
        try:
            yield
        finally:
            semaphore.release()

    def _semaphore(self, key: RateLimitKey, *, configured_cap: int | None) -> threading.BoundedSemaphore:
        with self._lock:
            if key not in self._semaphores:
                cap = normalize_concurrency(configured_cap)
                self._semaphores[key] = threading.BoundedSemaphore(cap)
            return self._semaphores[key]


GLOBAL_RATE_LIMITER = LLMRateLimiter()


def normalize_concurrency(value: int | None) -> int:
    if value is None:
        return DEFAULT_CONCURRENCY
    return max(1, min(int(value), MAX_CONCURRENCY))


def parse_retry_after_seconds(value: object) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        seconds = int(float(text))
    except ValueError:
        try:
            parsed = email.utils.parsedate_to_datetime(text)
        except (TypeError, ValueError):
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        seconds = int((parsed - datetime.now(timezone.utc)).total_seconds())
    return max(seconds, 0)
