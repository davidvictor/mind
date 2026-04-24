"""Parse a Substack reader-API export JSON into SubstackRecord dataclasses."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator


@dataclass(frozen=True)
class SubstackRecord:
    id: str
    title: str
    subtitle: str | None
    slug: str
    published_at: str   # ISO-8601
    saved_at: str       # ISO-8601
    url: str
    author_name: str
    author_id: str
    publication_name: str
    publication_slug: str
    body_html: str | None
    is_paywalled: bool


def _first_byline(post: dict[str, Any]) -> dict[str, Any]:
    bylines = post.get("publishedBylines") or post.get("published_bylines") or []
    if not bylines:
        return {"id": "", "name": "Unknown"}
    return bylines[0]


def parse_export(data: dict[str, Any]) -> Iterator[SubstackRecord]:
    """Yield SubstackRecord per post in a reader-API saved-posts response."""
    for post in data.get("posts", []) or []:
        pub = post.get("publication") or {}
        byline = _first_byline(post)
        yield SubstackRecord(
            id=str(post.get("id", "")),
            title=post.get("title") or "",
            subtitle=post.get("subtitle"),
            slug=post.get("slug") or "",
            published_at=post.get("post_date") or "",
            saved_at=post.get("saved_at") or "",
            url=post.get("canonical_url") or "",
            author_name=byline.get("name") or "Unknown",
            author_id=str(byline.get("id") or ""),
            publication_name=pub.get("name") or "",
            publication_slug=pub.get("subdomain") or "",
            body_html=post.get("body_html"),
            is_paywalled=(post.get("audience") == "only_paid"),
        )
