"""Parse Google Takeout YouTube watch-history.json into normalized records.

Watch-history JSON is an array of activity entries. We keep only entries that:
- have a YouTube video URL (titleUrl matching watch?v=)
- have a non-empty title

Output is an iterable of YouTubeRecord dataclasses, one per video.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable, Iterator


VIDEO_ID_RE = re.compile(r"watch\?v=([A-Za-z0-9_\-]{11})")
WATCHED_PREFIX = "Watched "


@dataclass(frozen=True)
class YouTubeRecord:
    video_id: str
    title: str
    channel: str
    watched_at: str  # ISO 8601 string
    duration_seconds: int | None = None
    description: str = ""
    tags: tuple[str, ...] = ()
    category: str = ""
    categories: tuple[str, ...] = ()
    title_url: str = ""
    channel_url: str = ""
    channel_id: str = ""
    published_at: str = ""
    thumbnail_url: str = ""


def parse_takeout(entries: Iterable[dict[str, Any]]) -> Iterator[YouTubeRecord]:
    for entry in entries:
        title_url = entry.get("titleUrl") or ""
        match = VIDEO_ID_RE.search(title_url)
        if not match:
            continue
        video_id = match.group(1)
        title = entry.get("title") or ""
        if title.startswith(WATCHED_PREFIX):
            title = title[len(WATCHED_PREFIX):]
        if not title:
            continue
        subtitles = entry.get("subtitles") or []
        channel = subtitles[0].get("name", "") if subtitles else ""
        channel_url = subtitles[0].get("url", "") if subtitles else ""
        watched_at = entry.get("time", "")
        category = str(entry.get("category", "") or "")
        yield YouTubeRecord(
            video_id=video_id,
            title=title,
            channel=channel,
            watched_at=watched_at,
            description=str(entry.get("description", "") or ""),
            tags=tuple(entry.get("tags") or ()),
            category=category,
            categories=(category,) if category else (),
            title_url=title_url,
            channel_url=channel_url,
        )
