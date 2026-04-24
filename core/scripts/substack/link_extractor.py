"""Extract external and Substack-internal links from a post body HTML.

Returns a dict with two lists:
  external_links  — links to non-substack.com domains
  substack_links  — links to any *.substack.com or substack.com hosted post

Each link entry is {"url", "anchor_text", "context_snippet"}.

Link filtering:
- Substack chrome (subscribe widgets, share buttons) is stripped before extraction
  by reusing scripts.substack.html_to_markdown.strip_chrome.
- Substack redirect wrappers (substack.com/redirect/...?u=<encoded>) are unwrapped
  to the canonical destination URL.
- Fragment-only links (#footnote-1) and non-http(s) schemes are dropped.
- Duplicates by URL (after unwrap) are coalesced to the first occurrence.
"""
from __future__ import annotations

from urllib.parse import parse_qs, unquote, urlparse

from bs4 import BeautifulSoup

from scripts.substack.html_to_markdown import strip_chrome


def _is_substack_host(host: str) -> bool:
    """True for substack.com or any *.substack.com subdomain (not evilsubstack.com)."""
    host = host.lower()
    return host == "substack.com" or host.endswith(".substack.com")


def _unwrap_redirect(url: str) -> str | None:
    """Unwrap substack.com/redirect/...?u=<target> to <target>.

    Returns:
      - The unwrapped destination URL if the ?u= param is present and decodable.
      - None if this is a substack /redirect/ URL with no usable ?u= param
        (caller should drop the link — it points at substack chrome, not content).
      - The original URL unchanged if it's not a redirect wrapper at all.
    """
    parsed = urlparse(url)
    if _is_substack_host(parsed.netloc) and "/redirect/" in parsed.path:
        qs = parse_qs(parsed.query)
        if "u" in qs and qs["u"]:
            return unquote(qs["u"][0])
        return None  # malformed redirect — drop
    return url


def _is_substack_post(url: str) -> bool:
    """True if url looks like a Substack-hosted POST (not a Note, not a settings page).

    Posts live at <subdomain>.substack.com/p/<slug>. Notes at substack.com/@user/note-X
    and settings pages at substack.com/subscribe etc. are excluded.
    """
    parsed = urlparse(url)
    if not _is_substack_host(parsed.netloc):
        return False
    return parsed.path.startswith("/p/")


def _context_snippet(anchor_text: str, parent_text: str, max_len: int = 200) -> str:
    """One-sentence context around the anchor; up to max_len chars centered on it."""
    if not parent_text:
        return anchor_text
    parent_text = " ".join(parent_text.split())  # collapse whitespace
    if len(parent_text) <= max_len:
        return parent_text
    idx = parent_text.find(anchor_text)
    if idx < 0:
        return parent_text[:max_len]
    half = max_len // 2
    start = max(0, idx - half)
    end = min(len(parent_text), start + max_len)
    return parent_text[start:end]


def extract(html: str | None) -> dict[str, list[dict[str, str]]]:
    """Extract external and substack-internal links from post body HTML."""
    if not html:
        return {"external_links": [], "substack_links": []}

    soup = BeautifulSoup(html, "html.parser")
    strip_chrome(soup)

    external: list[dict[str, str]] = []
    substack: list[dict[str, str]] = []
    seen_urls: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith("#"):
            continue
        if not (href.startswith("http://") or href.startswith("https://")):
            continue

        url = _unwrap_redirect(href)
        if url is None:
            # Malformed substack /redirect/ with no ?u= destination — drop.
            continue
        if url in seen_urls:
            continue
        seen_urls.add(url)

        anchor_text = a.get_text(strip=True) or url
        parent = a.find_parent(["p", "li", "blockquote", "div"])
        parent_text = parent.get_text(" ", strip=True) if parent else anchor_text
        context = _context_snippet(anchor_text, parent_text)

        entry = {"url": url, "anchor_text": anchor_text, "context_snippet": context}

        if _is_substack_post(url):
            substack.append(entry)
        else:
            external.append(entry)

    return {"external_links": external, "substack_links": substack}
