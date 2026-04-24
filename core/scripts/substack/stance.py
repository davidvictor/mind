"""Helpers for loading and maintaining per-author stance state.

Substack authors now use a single canonical person page at
``wiki/people/<author-slug>.md``. The author's evolving stance lives inside
that page under stance-specific sections rather than a separate
``<author-slug>-stance.md`` file.
"""
from __future__ import annotations

from pathlib import Path
import re
from typing import TYPE_CHECKING

from scripts.common.frontmatter import read_page, split_frontmatter
from scripts.common.section_rewriter import SectionOperation, apply_section_operations, parse_markdown_body
from scripts.common.stance import compact_stance_markdown
from scripts.common.default_tags import default_domains, default_tags
from scripts.common.vault import raw_path, wiki_path

if TYPE_CHECKING:
    from scripts.substack.parse import SubstackRecord


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STANCE_SECTIONS = (
    "Core beliefs",
    "Open questions",
    "Recent shifts",
    "Contradictions observed",
)


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------


def stance_page_path(repo_root: Path, author_slug: str) -> Path:
    """Compatibility shim: return the canonical author page path."""
    return wiki_path(repo_root, "people", f"{author_slug}.md")


def legacy_stance_page_path(repo_root: Path, author_slug: str) -> Path:
    """Return the legacy per-author stance-doc path."""
    return wiki_path(repo_root, "people", f"{author_slug}-stance.md")


def stance_cache_path(repo_root: Path, post_id: str) -> Path:
    """Return the cache path for a post's stance update result.

    Cached at raw/transcripts/substack/<id>.stance.json.
    """
    return raw_path(repo_root, "transcripts", "substack", f"{post_id}.stance.json")


# ---------------------------------------------------------------------------
# Read helpers
# ---------------------------------------------------------------------------


def load_stance_context(author_slug: str, repo_root: Path) -> str:
    """Load and format an author's stance document for prompt injection.

    Returns an empty string when the stance doc doesn't exist.
    Otherwise reads the file, strips schema-v2 frontmatter (between the
    leading ``---`` fences), wraps the remaining body in a standard header
    block, and returns the result.

    Thin wrapper around scripts.common.stance.load_stance_context — the
    substack signature (positional author_slug) is preserved so all existing
    callers continue to work.
    """
    from scripts.common.stance import load_stance_context as _load_stance_context

    return _load_stance_context(slug=author_slug, kind="person", repo_root=repo_root)


def read_stance_body(repo_root: Path, author_slug: str) -> str:
    """Read raw body (frontmatter-stripped) from the stance doc.

    Unlike load_stance_context(), this does NOT wrap the body in a header —
    it's meant for the update pass which needs the raw body to compare
    against the updated version.

    Returns empty string if file doesn't exist.
    """
    path = stance_page_path(repo_root, author_slug)
    if path.exists():
        _frontmatter, body = split_frontmatter(path.read_text(encoding="utf-8"))
        extracted = _extract_stance_body(body)
        if extracted:
            return extracted
    legacy = legacy_stance_page_path(repo_root, author_slug)
    if legacy.exists():
        _frontmatter, body = split_frontmatter(legacy.read_text(encoding="utf-8"))
        return body.strip()
    return ""


def read_stance_update_snapshot(
    repo_root: Path,
    author_slug: str,
    *,
    max_bullets_per_section: int = 12,
    max_chars: int = 8000,
) -> str:
    body = read_stance_body(repo_root, author_slug)
    if not body:
        return ""
    return compact_stance_markdown(
        body,
        max_bullets_per_section=max_bullets_per_section,
        max_chars=max_chars,
        include_changelog=False,
    )


# ---------------------------------------------------------------------------
# Stub / write helpers
# ---------------------------------------------------------------------------


def stub_stance_doc(record: "SubstackRecord") -> tuple[dict, str]:
    """Return a canonical person-page stub with stance sections."""
    from scripts.substack.write_pages import slugify

    author_slug = slugify(record.author_name)
    today = record.saved_at[:10] if record.saved_at else ""
    frontmatter = {
        "id": author_slug,
        "type": "person",
        "title": record.author_name,
        "aliases": [],
        "created": today,
        "last_updated": today,
        "status": "active",
        "tags": default_tags("person"),
        "domains": default_domains("person"),
        "sources": [],
        "name": record.author_name,
        "substack_author_id": record.author_id,
    }
    body = (
        f"# {record.author_name}\n\n"
        f"Substack author at [[{record.publication_slug}|{record.publication_name}]].\n\n"
        "## Core beliefs\n\n"
        "(no posts ingested yet)\n\n"
        "## Open questions\n\n"
        "(no posts ingested yet)\n\n"
        "## Recent shifts\n\n"
        "(no posts ingested yet)\n\n"
        "## Contradictions observed\n\n"
        "(no posts ingested yet)\n\n"
        "## Changelog\n\n"
    )
    return frontmatter, body


def _parse_sections(body: str) -> dict[str, str]:
    """Split a markdown body into {heading_text: section_body} mapping.

    Top-level ``# Title`` lines are skipped. Only ``## Heading`` lines
    are treated as section boundaries.
    """
    sections: dict[str, str] = {}
    current_heading: str | None = None
    current_lines: list[str] = []

    for line in body.splitlines():
        if line.startswith("## "):
            if current_heading is not None:
                sections[current_heading] = "\n".join(current_lines).strip()
            current_heading = line[3:].strip()
            current_lines = []
        elif line.startswith("# "):
            # top-level title — skip, don't treat as a section boundary
            continue
        else:
            current_lines.append(line)

    if current_heading is not None:
        sections[current_heading] = "\n".join(current_lines).strip()

    return sections


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


def _assemble_body(
    existing_body: str,
    *,
    intro_text: str,
    section_bodies: dict[str, str],
    changelog_text: str,
) -> str:
    """Reassemble the canonical author page body, preserving non-stance sections."""
    operations: list[SectionOperation] = []
    previous_heading: str | None = None
    for section in STANCE_SECTIONS:
        heading = f"## {section}"
        operations.append(
            SectionOperation(
                heading=heading,
                mode="replace",
                content=section_bodies.get(section, "(no content)"),
                insert_after=previous_heading,
            )
        )
        previous_heading = heading
    operations.append(
        SectionOperation(
            heading="## Changelog",
            mode="replace",
            content=changelog_text,
            insert_after=previous_heading,
        )
    )
    return apply_section_operations(
        text=existing_body,
        intro_mode="replace",
        intro_content=intro_text,
        section_operations=operations,
    )


def _extract_stance_body(body: str) -> str:
    parsed = parse_markdown_body(body)
    kept: list[str] = []
    for section in parsed.sections:
        heading_text = section.heading.removeprefix("## ").strip()
        if heading_text in (*STANCE_SECTIONS, "Changelog"):
            kept.append(f"{section.heading}\n\n{section.content.strip()}\n")
    return "\n\n".join(block.rstrip() for block in kept).strip()


def _canonical_author_intro(record: "SubstackRecord", existing_intro: str) -> str:
    canonical = (
        f"# {record.author_name}\n\n"
        f"Substack author at [[{record.publication_slug}|{record.publication_name}]].\n"
    )
    stripped = existing_intro.strip()
    if not stripped or "Stub created by /ingest-substack" in stripped:
        return canonical
    return existing_intro.rstrip("\n") + "\n"


def _normalize_existing_author_intro(existing_intro: str, *, author_name: str) -> str:
    stripped = existing_intro.strip()
    if not stripped:
        return f"# {author_name}\n"
    if "Stub created by /ingest-substack" not in stripped:
        return existing_intro.rstrip("\n") + "\n"
    kept_lines = [line for line in stripped.splitlines() if "Stub created by /ingest-substack" not in line]
    cleaned = "\n".join(kept_lines).strip()
    return (cleaned or f"# {author_name}").rstrip("\n") + "\n"


def _merge_sources(existing_sources: object, *, new_source: str) -> list[str]:
    if isinstance(existing_sources, str):
        values = [existing_sources] if existing_sources else []
    elif isinstance(existing_sources, list):
        values = [str(item).strip() for item in existing_sources if str(item).strip()]
    else:
        values = []
    if new_source and new_source not in values:
        values.append(new_source)
    return values


_PLACEHOLDER_SECTION_VALUES = {
    "(no posts ingested yet)",
    "(no content)",
    "(none)",
    "- (none)",
    "- (none yet)",
}


def _normalize_bullet(block: str) -> str:
    text = re.sub(r"\s+", " ", block.strip()).strip()
    return text.lower()


def _meaningful_bullets(content: str) -> list[str]:
    blocks = _bullet_blocks(content)
    filtered: list[str] = []
    for block in blocks:
        normalized = _normalize_bullet(block)
        if normalized in _PLACEHOLDER_SECTION_VALUES:
            continue
        filtered.append(block)
    return filtered


def apply_stance_delta(
    record: "SubstackRecord",
    delta_body: str,
    change_note: str,
    post_slug: str,
    repo_root: Path,
) -> None:
    """Append a bounded stance delta onto the canonical author page."""
    from scripts.common.wiki_writer import write_page
    from scripts.substack.write_pages import ensure_author_page, slugify

    author_slug = slugify(record.author_name)
    today = record.saved_at[:10] if record.saved_at else ""
    post_wikilink = f"[[{post_slug}]]"
    target = stance_page_path(repo_root, author_slug)
    ensure_author_page(record, repo_root=repo_root)

    frontmatter, existing_body_raw = read_page(target)
    existing_sections = _parse_sections(existing_body_raw)
    delta_sections = _parse_sections(delta_body)

    merged_sections: dict[str, str] = {}
    for section in STANCE_SECTIONS:
        existing_bullets = _meaningful_bullets(existing_sections.get(section, ""))
        delta_bullets = _meaningful_bullets(delta_sections.get(section, ""))
        if delta_bullets:
            known = {_normalize_bullet(block) for block in existing_bullets}
            for block in delta_bullets:
                normalized = _normalize_bullet(block)
                if normalized not in known:
                    existing_bullets.append(block)
                    known.add(normalized)
        if existing_bullets:
            merged_sections[section] = "\n\n".join(existing_bullets)
        else:
            merged_sections[section] = existing_sections.get(section, "(no posts ingested yet)").strip() or "(no posts ingested yet)"

    existing_changelog = existing_sections.get("Changelog", "").strip()
    changelog_entry = f"- {today} — {change_note} (from {post_wikilink})"
    new_changelog_text = f"{existing_changelog}\n{changelog_entry}\n".strip() if existing_changelog else f"{changelog_entry}\n"

    frontmatter["last_updated"] = today
    frontmatter["name"] = record.author_name
    frontmatter["substack_author_id"] = record.author_id
    frontmatter["sources"] = _merge_sources(frontmatter.get("sources", []), new_source=post_wikilink)

    body = _assemble_body(
        existing_body_raw,
        intro_text=_canonical_author_intro(record, parse_markdown_body(existing_body_raw).intro),
        section_bodies=merged_sections,
        changelog_text=new_changelog_text,
    )
    write_page(target, frontmatter=frontmatter, body=body, force=True)


def _pick_created(existing_created: object, legacy_created: object) -> str:
    candidates = [str(value).strip() for value in (existing_created, legacy_created) if str(value).strip()]
    return min(candidates) if candidates else ""


def _pick_last_updated(existing_updated: object, legacy_updated: object) -> str:
    candidates = [str(value).strip() for value in (existing_updated, legacy_updated) if str(value).strip()]
    return max(candidates) if candidates else ""


def write_stance_doc(
    record: "SubstackRecord",
    updated_body: str,
    change_note: str,
    post_slug: str,
    repo_root: Path,
) -> None:
    """Write or update the per-author stance doc.

    Handles two modes:
    - Mode A (first ingest): file doesn't exist — create from stub, inject
      Gemini-returned sections and first changelog entry.
    - Mode B (subsequent ingest): file exists — parse existing frontmatter,
      replace section bodies with new content, append new changelog entry,
      update ``last_updated`` and ``sources``.
    """
    from scripts.common.wiki_writer import write_page
    from scripts.substack.write_pages import ensure_author_page, slugify

    author_slug = slugify(record.author_name)
    today = record.saved_at[:10] if record.saved_at else ""
    post_wikilink = f"[[{post_slug}]]"

    target = stance_page_path(repo_root, author_slug)
    ensure_author_page(record, repo_root=repo_root)

    # Parse the Gemini-returned updated body into sections
    new_sections = _parse_sections(updated_body)

    # Build the changelog entry
    changelog_entry = f"- {today} — {change_note} (from {post_wikilink})"

    frontmatter, existing_body_raw = read_page(target)
    legacy_target = legacy_stance_page_path(repo_root, author_slug)
    if legacy_target.exists():
        legacy_frontmatter, legacy_body = read_page(legacy_target)
    else:
        legacy_frontmatter, legacy_body = {}, ""

    # Update mutable frontmatter fields on the canonical person page
    frontmatter["last_updated"] = today
    frontmatter["name"] = record.author_name
    frontmatter["substack_author_id"] = record.author_id
    frontmatter["sources"] = _merge_sources(
        _merge_sources(legacy_frontmatter.get("sources", []), new_source=post_wikilink),
        new_source=post_wikilink,
    )

    # Parse existing author page into sections; fall back to any legacy stance doc
    existing_sections = _parse_sections(existing_body_raw)
    if not any(section in existing_sections for section in (*STANCE_SECTIONS, "Changelog")) and legacy_body:
        existing_sections = _parse_sections(legacy_body)

    merged_sections: dict[str, str] = {}
    for section in STANCE_SECTIONS:
        merged_sections[section] = new_sections.get(section) or existing_sections.get(section, "(no content)")

    existing_changelog = existing_sections.get("Changelog", "").strip()
    if existing_changelog:
        new_changelog_text = existing_changelog + "\n" + changelog_entry + "\n"
    else:
        new_changelog_text = changelog_entry + "\n"

    body = _assemble_body(
        existing_body_raw,
        intro_text=_canonical_author_intro(record, parse_markdown_body(existing_body_raw).intro),
        section_bodies=merged_sections,
        changelog_text=new_changelog_text,
    )
    write_page(target, frontmatter=frontmatter, body=body, force=True)


def migrate_legacy_stance_pages(repo_root: Path) -> list[Path]:
    """Merge legacy ``people/<slug>-stance.md`` pages into canonical person pages."""
    from scripts.common.wiki_writer import write_page

    migrated: list[Path] = []
    people_root = wiki_path(repo_root, "people")
    for legacy_path in sorted(people_root.glob("*-stance.md")):
        author_slug = legacy_path.stem.removesuffix("-stance")
        legacy_frontmatter, legacy_body = read_page(legacy_path)
        target = stance_page_path(repo_root, author_slug)
        if target.exists():
            frontmatter, existing_body = read_page(target)
        else:
            author_name = str(legacy_frontmatter.get("title") or author_slug).replace(" — Current Stance", "", 1).strip()
            frontmatter = {
                "id": author_slug,
                "type": "person",
                "title": author_name or author_slug,
                "aliases": [],
                "created": str(legacy_frontmatter.get("created") or ""),
                "last_updated": str(legacy_frontmatter.get("last_updated") or ""),
                "status": "active",
                "tags": default_tags("person"),
                "domains": default_domains("person"),
                "sources": [],
                "name": author_name or author_slug,
            }
            existing_body = f"# {author_name or author_slug}\n"

        legacy_sections = _parse_sections(legacy_body)
        changelog_text = str(legacy_sections.get("Changelog") or "").strip() + "\n"
        merged_sections = {
            section: str(legacy_sections.get(section) or "(no content)")
            for section in STANCE_SECTIONS
        }
        body = _assemble_body(
            existing_body,
            intro_text=_normalize_existing_author_intro(
                parse_markdown_body(existing_body).intro,
                author_name=str(frontmatter.get("title") or author_slug),
            ),
            section_bodies=merged_sections,
            changelog_text=changelog_text,
        )
        merged_sources = _merge_sources(frontmatter.get("sources", []), new_source="")
        for source in _merge_sources(legacy_frontmatter.get("sources", []), new_source=""):
            if source not in merged_sources:
                merged_sources.append(source)
        frontmatter["sources"] = merged_sources
        frontmatter["created"] = _pick_created(frontmatter.get("created"), legacy_frontmatter.get("created"))
        frontmatter["last_updated"] = _pick_last_updated(frontmatter.get("last_updated"), legacy_frontmatter.get("last_updated"))
        write_page(target, frontmatter=frontmatter, body=body, force=True)
        legacy_path.unlink()
        migrated.append(target)
    return migrated
