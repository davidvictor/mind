from __future__ import annotations

import threading
import time

from mind.services.llm_rate_limiter import GLOBAL_RATE_LIMITER, normalize_concurrency, parse_retry_after_seconds


def test_normalize_concurrency_bounds_values() -> None:
    assert normalize_concurrency(None) == 4
    assert normalize_concurrency(0) == 1
    assert normalize_concurrency(99) == 8


def test_parse_retry_after_seconds_handles_numeric_and_http_date() -> None:
    assert parse_retry_after_seconds("30") == 30
    assert parse_retry_after_seconds("Wed, 21 Oct 2030 07:28:00 GMT") is not None


def test_global_rate_limiter_caps_concurrent_access() -> None:
    current = 0
    peak = 0
    lock = threading.Lock()

    def worker() -> None:
        nonlocal current, peak
        with GLOBAL_RATE_LIMITER.acquire(provider="anthropic", model="anthropic/claude-sonnet-4.6-rate-test", configured_cap=2):
            with lock:
                current += 1
                peak = max(peak, current)
            time.sleep(0.05)
            with lock:
                current -= 1

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert peak <= 2
