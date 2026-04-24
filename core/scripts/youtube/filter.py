"""YouTube cheap pre-filter — duration + shorts + music exclusion.

This module is the no-API-call pre-filter that drops obvious-no's before
they reach the LLM classifier in scripts.youtube.enrich.classify().

The full content classification (business | personal | ignore) lives in
``LLMService.classify_video`` and is wrapped by ``enrich.classify()``. This
file is intentionally tiny.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Final

from scripts.youtube.parse import YouTubeRecord


SHORTS_RE = re.compile(r"#shorts\b|\bshorts\b$", re.IGNORECASE)
MUSIC_TOKENS: Final[tuple[str, ...]] = ("music", "vevo")


@dataclass(frozen=True)
class Filter:
    min_duration_minutes: int = 5

    def cheap_drop(self, record: YouTubeRecord, duration_minutes: float | None = None) -> bool:
        """Return True if the video should be dropped without calling the classifier.

        Drops:
          - videos shorter than min_duration_minutes when duration is known
          - shorts (title contains #shorts or ends with "shorts")
          - music channels (channel name contains "music" or "vevo")
        """
        if duration_minutes is not None and duration_minutes < self.min_duration_minutes:
            return True
        if SHORTS_RE.search(record.title):
            return True
        channel_lower = record.channel.lower()
        if any(token in channel_lower for token in MUSIC_TOKENS):
            return True
        return False


def duration_minutes(record: YouTubeRecord) -> float | None:
    if record.duration_seconds is None:
        return None
    return float(record.duration_seconds) / 60.0


def should_skip_record(
    record: YouTubeRecord,
    *,
    filter_: Filter | None = None,
    duration_minutes_override: float | None = None,
) -> bool:
    active_filter = filter_ or Filter()
    duration = duration_minutes(record)
    if duration is None and duration_minutes_override is not None:
        duration = float(duration_minutes_override)
    return active_filter.cheap_drop(record, duration)
