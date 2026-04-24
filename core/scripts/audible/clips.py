"""Fetch and parse Audible bookmarks/clips for a single book.

The internal Audible API exposes annotations (bookmarks + clips) per ASIN
through the `library/{asin}/annotations` endpoint. We use the mkb79/audible
Python library to make the call and a defensive parser to normalize the
response into ClipRecord instances.

ClipRecord shape is intentionally minimal:
  - chapter: name of the chapter the clip is in (best-effort)
  - note: the user's typed note (may be empty)
  - start_seconds: position in seconds from the start of the book
  - end_seconds: end position (same as start for instant bookmarks)
  - position_hms: human-readable H:MM:SS for display in the wiki
  - created_at: ISO date the clip was made (YYYY-MM-DD)
  - annotation_id: opaque ID, useful for deduping if we ever resync

Audio transcription of the clip is intentionally NOT done here. That's
Tier 3 territory and out of scope for this plan.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator


@dataclass(frozen=True)
class ClipRecord:
    annotation_id: str
    chapter: str
    note: str
    start_seconds: float
    end_seconds: float
    position_hms: str
    created_at: str  # YYYY-MM-DD


def _ms_to_seconds(value: Any) -> float:
    try:
        return float(value) / 1000.0
    except (TypeError, ValueError):
        return 0.0


def _format_hms(seconds: float) -> str:
    total = int(seconds)
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    return f"{h}:{m:02d}:{s:02d}"


def parse_clips(payload: dict[str, Any], *, asin: str) -> Iterator[ClipRecord]:
    """Defensive parser for the audible annotations payload.

    Skips entries with no usable position. Empty notes are kept (the position
    itself is the highlight).
    """
    annotations = payload.get("annotations") or []
    for annotation in annotations:
        annotation_id = (annotation.get("annotationId") or "").strip()
        chapter = ""
        metadata = annotation.get("metadata") or {}
        if isinstance(metadata, dict):
            chapter = (metadata.get("chapterTitle") or metadata.get("title") or "").strip()

        note_obj = annotation.get("note") or {}
        if isinstance(note_obj, dict):
            note = (note_obj.get("text") or "").strip()
        elif isinstance(note_obj, str):
            note = note_obj.strip()
        else:
            note = ""

        start_seconds = _ms_to_seconds(annotation.get("startPosition"))
        end_seconds = _ms_to_seconds(annotation.get("endPosition")) or start_seconds

        created_raw = annotation.get("creationTime") or ""
        created_at = created_raw[:10]

        yield ClipRecord(
            annotation_id=annotation_id,
            chapter=chapter,
            note=note,
            start_seconds=start_seconds,
            end_seconds=end_seconds,
            position_hms=_format_hms(start_seconds),
            created_at=created_at,
        )


def fetch_clips(client: Any, asin: str) -> list[ClipRecord]:
    """Hit the Audible internal annotations endpoint via the mkb79/audible client.

    Returns an empty list on any failure (the user may not have any clips for
    a particular book — that's not an error).
    """
    try:
        response = client.get(f"library/{asin}/annotations", response_groups="metadata,note")
    except Exception:
        return []
    if not isinstance(response, dict):
        return []
    return list(parse_clips(response, asin=asin))
