"""Shared frontmatter parsing utilities."""
from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import yaml


def split_frontmatter(text: str) -> tuple[dict[str, Any], str]:
    """Split YAML frontmatter from markdown body."""
    if not text.startswith("---\n"):
        return {}, text
    end = text.find("\n---\n", 4)
    if end == -1:
        return {}, text
    try:
        frontmatter = yaml.safe_load(text[4:end]) or {}
    except yaml.YAMLError:
        return {}, text
    if not isinstance(frontmatter, dict):
        return {}, text
    return frontmatter, text[end + 5:]


def read_page(path: Path) -> tuple[dict[str, Any], str]:
    """Read a markdown file and split its frontmatter."""
    return split_frontmatter(path.read_text(encoding="utf-8"))


def today_str() -> str:
    """Return today's date as an ISO string."""
    return date.today().isoformat()
