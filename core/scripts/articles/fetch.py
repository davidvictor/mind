"""Fetch web articles via trafilatura's reader-mode extractor.

Two-step:
1. trafilatura.fetch_url(url) — downloads the HTML
2. trafilatura.bare_extraction(html) — extracts main content + metadata

Cached at raw/transcripts/articles/<slug>.html where the cached body is the
extracted markdown text. Metadata sidecar at <slug>.meta.json.

On any failure (network, empty body, no extraction), returns None and the
caller logs to inbox.

NOTE on trafilatura 2.0: bare_extraction returns a Document object, not a
dict. We normalize via .as_dict() with attribute-access fallback. Tests can
mock with plain dicts — they pass through unchanged.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import trafilatura

from scripts.articles.parse import ArticleDropEntry
from scripts.common.vault import raw_path


@dataclass(frozen=True)
class ArticleFetchResult:
    body_text: str             # markdown-ish text from trafilatura
    title: str | None
    author: str | None
    sitename: str | None       # outlet
    published: str | None      # ISO date if extractable
    raw_html_path: Path        # cache path


@dataclass(frozen=True)
class ArticleFetchFailure:
    failure_kind: str
    detail: str
    url: str


_UNSUPPORTED_HOSTS = frozenset({
    "twitter.com",
    "x.com",
    "instagram.com",
    "www.instagram.com",
    "youtube.com",
    "www.youtube.com",
})

_PAYWALL_MARKERS = (
    "subscribe to continue",
    "member-only",
    "become a subscriber",
    "upgrade to paid",
    "sign in to read",
    "this article is for subscribers",
)


def _cache_dir(repo_root: Path) -> Path:
    return raw_path(repo_root, "transcripts", "articles")


def html_cache_path(repo_root: Path, slug: str) -> Path:
    return _cache_dir(repo_root) / f"{slug}.html"


def _slug_for_entry(entry: ArticleDropEntry) -> str:
    from scripts.articles.write_pages import slugify_url
    return slugify_url(entry.url, entry.discovered_at)


def fetch_article(
    entry: ArticleDropEntry,
    *,
    repo_root: Path,
) -> ArticleFetchResult | ArticleFetchFailure:
    """Fetch and extract a single article.

    Cached: raw/transcripts/articles/<slug>.html holds the extracted body.
    Metadata at <slug>.meta.json sidecar.
    """
    import json

    slug = _slug_for_entry(entry)
    cache = html_cache_path(repo_root, slug)
    metadata_cache = cache.with_suffix(".meta.json")

    # Cache hit: rehydrate body + metadata.
    if cache.exists() and metadata_cache.exists():
        meta = json.loads(metadata_cache.read_text(encoding="utf-8"))
        return ArticleFetchResult(
            body_text=cache.read_text(encoding="utf-8"),
            title=meta.get("title"),
            author=meta.get("author"),
            sitename=meta.get("sitename"),
            published=meta.get("published"),
            raw_html_path=cache,
        )

    if not is_supported_article_url(entry.url):
        return ArticleFetchFailure(
            failure_kind="unsupported_format",
            detail="unsupported host for article extraction",
            url=entry.url,
        )

    # Cache miss: full fetch + extract.
    try:
        downloaded = trafilatura.fetch_url(entry.url)
    except Exception as exc:
        return ArticleFetchFailure(
            failure_kind="network_failed",
            detail=f"{type(exc).__name__}: {exc}",
            url=entry.url,
        )
    if downloaded is None:
        return ArticleFetchFailure(
            failure_kind="network_failed",
            detail="fetch_url returned no content",
            url=entry.url,
        )

    try:
        extracted = trafilatura.bare_extraction(downloaded, url=entry.url)
    except Exception as exc:
        return ArticleFetchFailure(
            failure_kind="extraction_failed",
            detail=f"{type(exc).__name__}: {exc}",
            url=entry.url,
        )
    if extracted is None:
        return ArticleFetchFailure(
            failure_kind="paywalled" if _looks_paywalled(downloaded) else "extraction_failed",
            detail="bare_extraction returned no document",
            url=entry.url,
        )

    # trafilatura 2.0+ returns a Document object, not a dict.
    # Normalize via .as_dict() if available; if it's already a dict (e.g.
    # from a test mock), pass through.
    if hasattr(extracted, "as_dict"):
        extracted_dict = extracted.as_dict()
    elif isinstance(extracted, dict):
        extracted_dict = extracted
    else:
        # Fallback: pull common attributes off the object
        extracted_dict = {
            "text": getattr(extracted, "text", "") or "",
            "title": getattr(extracted, "title", None),
            "author": getattr(extracted, "author", None),
            "sitename": getattr(extracted, "sitename", None),
            "date": getattr(extracted, "date", None),
        }

    body_text = extracted_dict.get("text") or ""
    if not body_text.strip():
        return ArticleFetchFailure(
            failure_kind="paywalled" if _looks_paywalled(downloaded) else "empty_body",
            detail="extracted body was empty",
            url=entry.url,
        )

    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text(body_text, encoding="utf-8")

    meta = {
        "title": extracted_dict.get("title"),
        "author": extracted_dict.get("author"),
        "sitename": extracted_dict.get("sitename"),
        "published": extracted_dict.get("date"),
    }
    metadata_cache.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    return ArticleFetchResult(
        body_text=body_text,
        title=meta["title"],
        author=meta["author"],
        sitename=meta["sitename"],
        published=meta["published"],
        raw_html_path=cache,
    )


def is_supported_article_url(url: str) -> bool:
    return not _is_unsupported_format(url)


def _is_unsupported_format(url: str) -> bool:
    parsed = urlparse(url)
    if (parsed.hostname or "").lower() in _UNSUPPORTED_HOSTS:
        return True
    if parsed.scheme not in {"http", "https"}:
        return True
    return parsed.path.strip("/") == "" and not parsed.query


def _looks_paywalled(downloaded: str) -> bool:
    lowered = downloaded.lower()
    return any(marker in lowered for marker in _PAYWALL_MARKERS)
