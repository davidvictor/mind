"""Parse book lists into a normalized BookRecord shape.

Supports three input flavors:
- Goodreads CSV export (https://www.goodreads.com/review/import)
- OpenAudible CSV export
- Free-form markdown bullet list
"""
from __future__ import annotations

import csv
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, Literal


@dataclass
class BookRecord:
    title: str
    author: list[str]
    publisher: str = ""
    rating: int | None = None
    finished_date: str = ""
    started_date: str = ""
    length: str = ""
    format: str = "ebook"
    status: Literal["finished", "in-progress", "to-read"] = "finished"
    # Audible-only extensions (always set when source is audible-cli; empty
    # otherwise). Additive — Goodreads/OpenAudible/markdown parsers leave
    # both as defaults.
    asin: str = ""
    clips: list = field(default_factory=list)
    document_path: str = ""
    audio_path: str = ""


_GOODREADS_DATE_RE = re.compile(r"(\d{4})/(\d{1,2})/(\d{1,2})")


def _normalize_goodreads_date(value: str) -> str:
    if not value:
        return ""
    m = _GOODREADS_DATE_RE.match(value.strip())
    if not m:
        return ""
    y, mo, d = m.groups()
    return f"{y}-{int(mo):02d}-{int(d):02d}"


def parse_csv(path: Path, *, flavor: str) -> Iterator[BookRecord]:
    with path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            if flavor == "goodreads":
                yield _row_to_goodreads(row)
            elif flavor == "openaudible":
                yield _row_to_openaudible(row)
            else:
                raise ValueError(f"Unknown flavor: {flavor}")


def _row_to_goodreads(row: dict) -> BookRecord:
    title = (row.get("Title") or "").strip()
    author = (row.get("Author") or "").strip()
    rating_str = (row.get("My Rating") or "").strip()
    rating = int(rating_str) if rating_str.isdigit() and int(rating_str) > 0 else None
    finished = _normalize_goodreads_date(row.get("Date Read") or "")
    pages = (row.get("Number of Pages") or "").strip()
    length = f"{pages}p" if pages else ""
    shelves = (row.get("Bookshelves") or "").lower()
    if "to-read" in shelves:
        status = "to-read"
    elif "currently-reading" in shelves:
        status = "in-progress"
    else:
        status = "finished"
    return BookRecord(
        title=title,
        author=[author] if author else [],
        publisher=(row.get("Publisher") or "").strip(),
        rating=rating,
        finished_date=finished,
        length=length,
        format="ebook",
        status=status,
        document_path=_extract_path_field(row, "document"),
        audio_path=_extract_path_field(row, "audio"),
    )


def _row_to_openaudible(row: dict) -> BookRecord:
    title = (row.get("Title") or "").strip()
    author = (row.get("Author") or "").strip()
    rating_str = (row.get("Rating") or "").strip()
    rating = int(rating_str) if rating_str.isdigit() and int(rating_str) > 0 else None
    length = (row.get("Length") or "").strip()
    finished = (row.get("Date Finished") or "").strip()
    started = (row.get("Date Added") or "").strip()
    status_raw = (row.get("Status") or "").strip().lower()
    if "finished" in status_raw:
        status = "finished"
    elif "progress" in status_raw:
        status = "in-progress"
    else:
        status = "to-read"
    return BookRecord(
        title=title,
        author=[author] if author else [],
        publisher=(row.get("Publisher") or "").strip(),
        rating=rating,
        finished_date=finished,
        started_date=started,
        length=length,
        format="audiobook",
        status=status,
        document_path=_extract_path_field(row, "document"),
        audio_path=_extract_path_field(row, "audio"),
    )


_MARKDOWN_RE = re.compile(
    r'^[-*]\s+["\u201c]?(?P<title>[^"\u201d]+?)["\u201d]?\s+by\s+(?P<author>[^—\-]+?)\s*'
    r'(?:[—\-]\s*(?P<note>.*))?$'
)


def parse_markdown(path: Path) -> Iterator[BookRecord]:
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line.startswith(("-", "*")):
            continue
        m = _MARKDOWN_RE.match(line)
        if not m:
            continue
        title = m.group("title").strip()
        author = m.group("author").strip()
        raw_note = (m.group("note") or "").strip()
        note = raw_note.lower()
        rating = None
        rating_match = re.search(r"(\d)\s*stars?", note)
        if rating_match:
            rating = int(rating_match.group(1))
        finished_match = re.search(r"finished\s+(\d{4}-\d{2}-\d{2})", note)
        finished = finished_match.group(1) if finished_match else ""
        if "to read" in note or "to-read" in note:
            status: Literal["finished", "in-progress", "to-read"] = "to-read"
        elif "in progress" in note or "reading" in note:
            status = "in-progress"
        else:
            status = "finished" if finished else "to-read"
        yield BookRecord(
            title=title,
            author=[author] if author else [],
            publisher="",
            rating=rating,
            finished_date=finished,
            format="paper",  # markdown lists don't say, default to paper
            status=status,
            document_path=_extract_inline_asset(raw_note, "pdf"),
            audio_path=_extract_inline_asset(raw_note, "audio"),
        )


def _extract_path_field(row: dict, kind: str) -> str:
    keys = (
        f"{kind}_path",
        f"{kind}Path",
        f"{kind.capitalize()} Path",
        f"{kind.capitalize()}Path",
        "local_path" if kind == "audio" else "pdf_path",
        "Local Path" if kind == "audio" else "PDF Path",
    )
    for key in keys:
        value = (row.get(key) or "").strip()
        if value:
            return value
    return ""


def _extract_inline_asset(note: str, label: str) -> str:
    match = re.search(rf"{label}\s*:\s*(\S+)", note)
    return match.group(1).strip() if match else ""
