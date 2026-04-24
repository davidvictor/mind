"""Shared wikilink regex patterns and extraction utilities."""
from __future__ import annotations

import re


# Base pattern: extracts target from [[target]]
WIKILINK_RE = re.compile(r"\[\[([^\]]+)\]\]")

# Handles [[target|display]] syntax, capturing both groups
WIKILINK_DISPLAY_RE = re.compile(r"\[\[([^|\]]+)(?:\|([^\]]+))?\]\]")

# Handles [[target|display]] and [[target#anchor]] with full extraction
WIKILINK_FULL_RE = re.compile(r"\[\[([^\]|#]+?)(?:\|[^\]]*)?(?:#[^\]]*)?\]\]")

# Detects malformed nested [[[[target]]]]
NESTED_WIKILINK_RE = re.compile(r"\[\[\[\[([^\[\]]+)\]\]\]\]")


def extract_wikilinks(text: str) -> list[str]:
    """Extract wikilink targets from text, stripping display text and anchors."""
    return re.findall(r"\[\[([^\]|#]+)", text)
