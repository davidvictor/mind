"""Convert Substack post body HTML into clean markdown.

Two steps:
1. Strip Substack-specific chrome (subscribe widgets, share buttons, paywall CTAs).
2. Convert remaining HTML to markdown via the `markdownify` library.

Images stay as ![alt](src) pointing to Substack CDN URLs — we don't download.
External links are preserved as inline [text](url).

Chrome-stripping is done with EXACT class-token CSS selectors, not substring
matches. Substring matching would catch `pencraft` (Substack's generic body
primitive) and `shared-note-preview` etc., which are legitimate content.
"""
from __future__ import annotations

from bs4 import BeautifulSoup
from markdownify import markdownify


# Exact CSS selectors (class tokens) for Substack non-content chrome.
# Each selector matches only elements whose class list contains EXACTLY that
# class token — not substrings. Notable omissions:
#   - `pencraft` is Substack's generic body primitive (wraps paragraphs, headings,
#     images). Stripping it would delete most real post content.
#   - `share-widget` is folded into `.share` if present; Substack uses both.
STRIP_CSS_SELECTORS = [
    ".subscribe-widget",
    ".subscribe-prompt",
    ".share",
    ".share-widget",
    ".post-footer",
    ".footer-actions",
    ".paywall",
    ".button-wrapper",
]


def strip_chrome(soup: BeautifulSoup) -> None:
    """Remove Substack UI chrome (subscribe, share, paywall CTAs) in place."""
    for selector in STRIP_CSS_SELECTORS:
        for el in soup.select(selector):
            el.decompose()


def convert(html: str | None) -> str:
    """Convert Substack post body HTML to markdown. Returns '' for None/empty."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    strip_chrome(soup)
    md = markdownify(str(soup), heading_style="ATX")
    # Collapse excessive blank lines (markdownify is liberal).
    lines = [line.rstrip() for line in md.splitlines()]
    out: list[str] = []
    blank = 0
    for line in lines:
        if line.strip() == "":
            blank += 1
            if blank <= 1:
                out.append("")
        else:
            blank = 0
            out.append(line)
    return "\n".join(out).strip() + "\n" if out else ""
