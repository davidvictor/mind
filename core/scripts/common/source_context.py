"""Reusable prior-source context loading helpers for ingestion lanes."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Callable

from scripts.common.frontmatter import split_frontmatter as parse_frontmatter


def extract_tldr(body: str) -> str:
    match = re.search(r"^## TL;DR\s*\n(.*)", body, re.MULTILINE | re.DOTALL)
    if match:
        for paragraph in match.group(1).split("\n"):
            paragraph = paragraph.strip()
            if paragraph and not paragraph.startswith("#"):
                return paragraph[:200]
    for paragraph in body.split("\n"):
        paragraph = paragraph.strip()
        if paragraph and not paragraph.startswith("#") and not paragraph.startswith("---"):
            return paragraph[:200]
    return ""


def strip_wiki_link(value: str) -> str:
    match = re.match(r"^\[\[(.+)\]\]$", value.strip())
    return match.group(1) if match else value.strip()


def build_prior_context(
    *,
    root: Path,
    matcher: Callable[[dict[str, str]], bool],
    heading: str,
    budget: int = 2000,
) -> str:
    if not root.exists():
        return ""

    candidates: list[tuple[str, str, str, str]] = []
    for path in root.glob("**/*.md"):
        frontmatter, body = parse_frontmatter(path.read_text(encoding="utf-8"))
        if not matcher(frontmatter):
            continue
        candidates.append((
            frontmatter.get("last_updated", ""),
            frontmatter.get("title", path.stem),
            path.stem,
            extract_tldr(body),
        ))
    if not candidates:
        return ""

    candidates.sort(key=lambda item: item[0], reverse=True)

    def render(limit: int) -> str:
        lines = [heading, ""]
        for _, title, slug, tldr in candidates[:limit]:
            lines.append(f'- [[{slug}]] "{title}" — {tldr}' if tldr else f'- [[{slug}]] "{title}"')
        return "\n".join(lines) + "\n"

    for limit in (5, 3, 2):
        block = render(limit)
        if len(block) <= budget or limit == 2:
            return block
    return render(2)
