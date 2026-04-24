"""Single canonical slugifier for the brain.

Replaces the four separate slugify implementations in:
  - scripts/articles/write_pages.py
  - scripts/substack/write_pages.py
  - scripts/youtube/write_pages.py
  - scripts/books/enrich.py

Goal: every wiki-link, filename, and id in the brain comes from this one
function so 'founder vs. employee' and 'Founder VS. Employee' always
produce the same slug.

Lowercase, ASCII-only, dash-separated, max length capped, idempotent.
"""
from __future__ import annotations

import re
import unicodedata


_NON_ALNUM = re.compile(r"[^a-z0-9]+")
_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]+")


def normalize_text(value: str) -> str:
    """Normalize text into a stable Unicode form before further processing."""
    if not value:
        return ""
    text = unicodedata.normalize("NFKC", value)
    return _CONTROL_CHARS.sub("", text)


def ascii_fold(value: str) -> str:
    """Convert text into an ASCII-safe approximation."""
    if not value:
        return ""
    text = normalize_text(value)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    return text.encode("ascii", "ignore").decode("ascii")


def slugify(text: str, *, max_len: int = 80) -> str:
    """Convert arbitrary text to a canonical lowercase-dashed slug.

    Args:
        text: Input string. May contain unicode, punctuation, whitespace.
        max_len: Maximum result length. Default 80.

    Returns:
        Lowercase ASCII slug with dashes between word groups, no leading
        or trailing dashes, no consecutive dashes, capped at max_len.
        Returns empty string for empty/whitespace/punctuation-only input.
    """
    if not text:
        return ""

    # Normalize first, then fold into an ASCII-safe working space.
    text = ascii_fold(text)

    # Lowercase and replace runs of non-alphanumeric with single dashes.
    text = text.lower()
    text = _NON_ALNUM.sub("-", text)

    # Strip leading/trailing dashes.
    text = text.strip("-")

    if not text:
        return ""

    # Truncate to max_len, then strip trailing dash if truncation landed there.
    if len(text) > max_len:
        text = text[:max_len].rstrip("-")

    return text


def normalize_identifier(text: str, *, max_len: int = 120) -> str:
    """Canonical identifier normalizer used for ids and graph keys."""
    return slugify(text.strip().replace("_", "-"), max_len=max_len)
