"""Parse audible-cli library export JSON into BookRecord instances.

The audible-cli library export shape is documented in the audible-cli README:
each entry has asin, title, authors[], narrators[], runtime_length_min,
purchase_date, is_finished, rating.overall_rating, publisher_summary, and a
nested category_ladders structure.

We map this to the existing scripts.books.parse.BookRecord dataclass with two
extensions:
  - asin: the Audible Standard Identification Number (used to fetch clips)
  - clips: optional list of ClipRecord populated by the clips fetcher

The BookRecord extension is additive — existing callers that don't care about
audible-specific fields continue to work.
"""
from __future__ import annotations

from typing import Any, Iterable, Iterator

from scripts.books.parse import BookRecord


def _format_runtime(minutes: int) -> str:
    if not minutes:
        return ""
    hours = minutes // 60
    mins = minutes % 60
    if hours == 0:
        return f"{mins}m"
    return f"{hours}h {mins}m"


def _normalize_purchase_date(value: str) -> str:
    """Audible dates are ISO 8601 with timezone. Strip to YYYY-MM-DD."""
    if not value:
        return ""
    return value[:10]


def _extract_authors(value: Any) -> list[str]:
    """Handle both shapes audible-cli has shipped over time:
    - 0.3.x:   "First Last, Second Author" (comma-separated string)
    - 0.4+:    [{"name": "First Last"}, {"name": "Second"}]  (list of dicts)
    """
    if value is None:
        return []
    if isinstance(value, str):
        return [name.strip() for name in value.split(",") if name.strip()]
    if isinstance(value, list):
        names: list[str] = []
        for item in value:
            if isinstance(item, dict):
                name = (item.get("name") or "").strip()
                if name:
                    names.append(name)
            elif isinstance(item, str):
                stripped = item.strip()
                if stripped:
                    names.append(stripped)
        return names
    return []


def _extract_rating(value: Any) -> int | None:
    """Handle both shapes audible-cli has shipped:
    - 0.3.x:   "4.7" or "0.0" (string)
    - 0.4+:    {"overall_rating": 4.7}  (nested dict)
    Returns int 1-5 or None.
    """
    if value is None:
        return None
    raw: Any = value
    if isinstance(value, dict):
        raw = value.get("overall_rating")
    if raw is None or raw == "":
        return None
    try:
        rounded = int(round(float(raw)))
    except (TypeError, ValueError):
        return None
    if rounded < 1 or rounded > 5:
        return None
    return rounded


def parse_audible_library(entries: Iterable[dict[str, Any]]) -> Iterator[BookRecord]:
    """Convert audible-cli library export entries into BookRecord instances."""
    for entry in entries:
        asin = entry.get("asin", "").strip()
        title = (entry.get("title") or "").strip()
        authors = _extract_authors(entry.get("authors"))
        runtime = entry.get("runtime_length_min") or 0
        is_finished = bool(entry.get("is_finished"))
        purchased = _normalize_purchase_date(entry.get("purchase_date") or "")

        rating_value = _extract_rating(entry.get("rating"))

        status: str = "finished" if is_finished else "in-progress"

        yield BookRecord(
            title=title,
            author=authors,
            publisher=str(entry.get("publisher_name") or entry.get("publisher") or ""),
            rating=rating_value,
            finished_date=purchased if is_finished else "",
            started_date=purchased,
            length=_format_runtime(runtime),
            format="audiobook",
            status=status,  # type: ignore[arg-type]
            asin=asin,
            clips=[],
            document_path=_extract_path_field(entry, "document"),
            audio_path=_extract_path_field(entry, "audio"),
        )


def _extract_path_field(entry: dict[str, Any], kind: str) -> str:
    keys = (
        f"{kind}_path",
        f"{kind}Path",
        "local_path" if kind == "audio" else "pdf_path",
        "localPath" if kind == "audio" else "pdfPath",
        "file_path" if kind == "audio" else "document_path",
        "filePath" if kind == "audio" else "documentPath",
    )
    for key in keys:
        value = entry.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""
