"""Generic stance document reader.

Serves both legacy stance docs (for example ``wiki/people/<slug>-stance.md``)
and canonical person/channel pages that embed stance sections inline. The
`kind` parameter selects the directory.

This module is READ-ONLY. Writes that require source-kind-specific record
types live in their respective ingestor packages:

  - scripts/substack/stance.py — substack post → person stance updates
  - scripts/youtube/stance.py  — video → channel stance updates (Phase E)
  - scripts/books/stance.py    — book → author stance updates (Phase F)

These will all eventually delegate frontmatter parsing and section assembly
to a shared helper, but that consolidation is deferred.
"""
from __future__ import annotations

from pathlib import Path
import re
from typing import Literal

from scripts.common.frontmatter import split_frontmatter
from scripts.common.section_rewriter import parse_markdown_body
from scripts.common.vault import wiki_path

StanceKind = Literal["person", "channel"]
STANCE_SECTIONS = (
    "Core beliefs",
    "Open questions",
    "Recent shifts",
    "Contradictions observed",
)
CHANGELOG_SECTION = "Changelog"
DEFAULT_STANCE_SNAPSHOT_MAX_BULLETS = 12
DEFAULT_STANCE_SNAPSHOT_MAX_CHARS = 8000


def stance_page_path(repo_root: Path, *, slug: str, kind: StanceKind) -> Path:
    """Return the expected path for a stance document.

    Args:
        repo_root: Brain repo root.
        slug: Pre-slugified entity name (e.g. result of slugify(author_name)).
        kind: 'person' for wiki/people/, 'channel' for wiki/channels/.

    Raises:
        ValueError: if kind is not 'person' or 'channel'.
    """
    if kind == "person":
        return wiki_path(repo_root, "people", f"{slug}-stance.md")
    if kind == "channel":
        return wiki_path(repo_root, "channels", f"{slug}-stance.md")
    raise ValueError(f"kind must be 'person' or 'channel', got {kind!r}")


def _canonical_person_page_path(repo_root: Path, *, slug: str) -> Path:
    return wiki_path(repo_root, "people", f"{slug}.md")


def _extract_stance_sections(text: str) -> str:
    _frontmatter, body = split_frontmatter(text)
    parsed = parse_markdown_body(body)
    kept: list[str] = []
    for section in parsed.sections:
        if section.heading in {
            *(f"## {name}" for name in STANCE_SECTIONS),
            f"## {CHANGELOG_SECTION}",
        }:
            kept.append(f"{section.heading}\n\n{section.content.strip()}\n")
    return "\n\n".join(block.rstrip() for block in kept).strip()


def _bullet_blocks(content: str) -> list[str]:
    blocks: list[str] = []
    current: list[str] = []
    for raw_line in content.splitlines():
        stripped = raw_line.strip()
        if stripped.startswith("- "):
            if current:
                blocks.append("\n".join(current).strip())
            current = [stripped]
            continue
        if current:
            if stripped:
                current.append(stripped)
            else:
                current.append("")
    if current:
        blocks.append("\n".join(current).strip())
    return [block for block in blocks if block]


def _compact_section_content(content: str, *, max_bullets: int) -> str:
    bullets = _bullet_blocks(content)
    if not bullets:
        return content.strip()
    kept = bullets[:max_bullets]
    omitted = max(0, len(bullets) - len(kept))
    if omitted:
        kept.append(f"- ({omitted} earlier bullets omitted for brevity)")
    return "\n\n".join(kept).strip()


def compact_stance_markdown(
    markdown: str,
    *,
    max_bullets_per_section: int = DEFAULT_STANCE_SNAPSHOT_MAX_BULLETS,
    max_chars: int = DEFAULT_STANCE_SNAPSHOT_MAX_CHARS,
    include_changelog: bool = False,
) -> str:
    allowed = [*STANCE_SECTIONS, *( [CHANGELOG_SECTION] if include_changelog else [] )]
    parsed = parse_markdown_body(markdown)
    section_map = {
        section.heading.removeprefix("## ").strip(): section.content.strip()
        for section in parsed.sections
        if section.heading.startswith("## ")
    }
    bullet_limit = max(1, max_bullets_per_section)
    rendered = ""
    while bullet_limit >= 1:
        blocks: list[str] = []
        for name in allowed:
            content = section_map.get(name, "").strip()
            if not content:
                continue
            compact = _compact_section_content(content, max_bullets=bullet_limit)
            if compact:
                blocks.append(f"## {name}\n\n{compact}")
        rendered = "\n\n".join(blocks).strip()
        if len(rendered) <= max_chars or bullet_limit == 1:
            break
        bullet_limit -= 1
    if len(rendered) <= max_chars:
        return rendered
    truncated = rendered[: max_chars - 32].rstrip()
    boundary = max(truncated.rfind("\n## "), truncated.rfind("\n- "))
    if boundary > max_chars // 2:
        truncated = truncated[:boundary].rstrip()
    return f"{truncated}\n\n(truncated for prompt budget)"


def load_stance_snapshot(
    *,
    slug: str,
    kind: StanceKind,
    repo_root: Path,
    max_bullets_per_section: int = DEFAULT_STANCE_SNAPSHOT_MAX_BULLETS,
    max_chars: int = DEFAULT_STANCE_SNAPSHOT_MAX_CHARS,
    include_changelog: bool = False,
) -> str:
    body = ""
    if kind == "person":
        canonical_person = _canonical_person_page_path(repo_root, slug=slug)
        if canonical_person.exists():
            body = _extract_stance_sections(canonical_person.read_text(encoding="utf-8"))
    if not body:
        path = stance_page_path(repo_root, slug=slug, kind=kind)
        if not path.exists():
            return ""
        text = path.read_text(encoding="utf-8")
        _frontmatter, raw_body = split_frontmatter(text)
        body = raw_body.strip()
    compact = compact_stance_markdown(
        body,
        max_bullets_per_section=max_bullets_per_section,
        max_chars=max_chars,
        include_changelog=include_changelog,
    ).strip()
    return compact


def load_stance_context(
    *,
    slug: str,
    kind: StanceKind,
    repo_root: Path,
    max_bullets_per_section: int = DEFAULT_STANCE_SNAPSHOT_MAX_BULLETS,
    max_chars: int = DEFAULT_STANCE_SNAPSHOT_MAX_CHARS,
    include_changelog: bool = False,
) -> str:
    """Load and format a stance document for prompt injection.

    Returns an empty string when the stance doc doesn't exist. Otherwise
    reads the file, strips schema-v2 frontmatter (between the leading ---
    fences), wraps the remaining body in a standard header block, and
    returns the result.

    The wrapping header reads "What this author believed last time you read
    them" — even for channels, since the consuming prompt is asking the LLM
    to think about the creator's prior positions regardless of medium.
    """
    body = load_stance_snapshot(
        slug=slug,
        kind=kind,
        repo_root=repo_root,
        max_bullets_per_section=max_bullets_per_section,
        max_chars=max_chars,
        include_changelog=include_changelog,
    )
    if not body:
        return ""
    return f"## What this author believed last time you read them\n\n{body}\n"
