"""Generate wiki pages from an enriched book record.

For each book, writes one durable page:
1. wiki/sources/books/<category>/<slug>.md  (type: book)

The category becomes a tag and also drives the directory the page lives in,
so the Obsidian graph naturally clusters business vs personal books.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Literal

from mind.services.content_policy import canonical_policy_fields, content_policy_from_classification
from scripts.common import env
from scripts.common.default_tags import default_domains
from scripts.common.frontmatter import read_page
from scripts.common.wikilink_sanitizer import sanitize_wikilinks
from scripts.common.vault import raw_path, relative_markdown_path, wiki_path
from mind.services.durable_write import DurableLinkTarget, write_contract_page
from mind.services.materialization import MaterializationCandidate
from scripts.books.parse import BookRecord
from scripts.books.enrich import slugify


Category = Literal["business", "personal", "fiction"]
Subcategory = Literal[
    "history", "science", "biography", "politics",
    "memoir", "self-help", "culture",
]
VALID_SUBCATEGORIES: set[str] = {
    "history", "science", "biography", "politics",
    "memoir", "self-help", "culture",
}


def _preserved_source_write_dates(target: Path, *, today: str) -> tuple[str, str]:
    if not target.exists():
        return today, today
    frontmatter, _body = read_page(target)
    created = str(frontmatter.get("created") or "").strip() or today
    ingested = str(frontmatter.get("ingested") or "").strip() or today
    return created, ingested


def book_page_path(repo_root: Path, book: BookRecord, category: Category) -> Path:
    author_slug = slugify(book.author[0]) if book.author else "unknown"
    title_slug = slugify(book.title)
    slug = f"{author_slug}-{title_slug}"
    return wiki_path(repo_root, "sources", "books", category, f"{slug}.md")


def canonical_page_id(repo_root: Path, book: BookRecord) -> str:
    return book_page_path(repo_root, book, "business").stem


def summary_page_path(repo_root: Path, book: BookRecord) -> Path:
    existing = _existing_summary_page_path(repo_root, book)
    if existing is not None:
        return existing
    author_slug = slugify(book.author[0]) if book.author else "unknown"
    title_slug = slugify(book.title)
    return wiki_path(repo_root, "summaries", f"summary-{author_slug}-{title_slug}.md")

def _domains_for_category(category: Category) -> list[str]:
    if category == "business":
        return ["business"]
    if category == "fiction":
        return ["personal"]
    return ["personal"]


def _frontmatter_policy_fields(policy: dict[str, Any] | None) -> dict[str, Any]:
    fields = canonical_policy_fields(policy)
    fields.pop("domains", None)
    return fields


def _existing_summary_page_path(repo_root: Path, book: BookRecord) -> Path | None:
    root = wiki_path(repo_root, "summaries")
    if not root.exists():
        return None
    author_slug = slugify(book.author[0]) if book.author else "unknown"
    title_slug = slugify(book.title)
    for candidate in (
        root / f"summary-{author_slug}-{title_slug}.md",
        root / f"summary-book-{author_slug}-{title_slug}.md",
    ):
        if candidate.exists():
            return candidate
    external_id = f"audible-{book.asin}" if book.asin else ""
    if external_id:
        for path in sorted(root.glob("summary-*.md")):
            text = path.read_text(encoding="utf-8")
            if f"external_id: {external_id}" in text:
                return path
    return None


def _render_clips(book: BookRecord) -> list[str]:
    """Render the My Highlights section body lines from book.clips.

    Returns an empty list if there are no clips.
    """
    clips = getattr(book, "clips", None) or []
    if not clips:
        return []
    lines = ["", "## My Highlights", ""]
    lines.append(f"_{len(clips)} bookmarks/clips from Audible_")
    lines.append("")
    for clip in clips:
        chapter = clip.chapter or "Unknown chapter"
        position = clip.position_hms or ""
        note = clip.note or ""
        header = f"### {chapter}"
        if position:
            header += f" — {position}"
        lines.append(header)
        if note:
            lines.append("")
            lines.append(f"> {note}")
        lines.append("")
    return lines


def _render_deep_body(
    enriched: dict[str, Any],
    applied: dict[str, Any] | None,
    stance_change_note: str | None,
    book: BookRecord,
) -> list[str]:
    """Render the markdown body for a deep-research book page.

    Sections, in order:
      ## TL;DR
      ## Core Argument
      ## Key Frameworks (each with worked example)
      ## Memorable Stories
      ## Counterarguments
      ## Famous Quotes (only if the summary returned any)
      ## Applied to You (only if applied is non-empty)
      ## In Conversation With
      ## My Highlights (from Audible clips, if any)
      ## Connections
      ## My Notes
    """
    parts: list[str] = []

    parts += ["## TL;DR", "", enriched.get("tldr", "").strip(), ""]

    core = (enriched.get("core_argument") or "").strip()
    if core:
        parts += ["## Core Argument", "", core, ""]

    frameworks = enriched.get("key_frameworks") or []
    if frameworks:
        parts += ["## Key Frameworks", ""]
        for fw in frameworks:
            if not isinstance(fw, dict):
                parts.append(f"- {fw}")
                continue
            name = (fw.get("name") or "").strip() or "Untitled framework"
            summary = (fw.get("summary") or "").strip()
            example = (fw.get("worked_example") or "").strip()
            parts.append(f"### {name}")
            parts.append("")
            if summary:
                parts.append(summary)
                parts.append("")
            if example:
                parts.append(f"_Example:_ {example}")
                parts.append("")

    stories = enriched.get("memorable_examples") or []
    if stories:
        parts += ["## Memorable Examples", ""]
        for story in stories:
            if not isinstance(story, dict):
                parts.append(f"- {story}")
                continue
            title = (story.get("title") or "").strip() or "Story"
            body = (story.get("story") or "").strip()
            lesson = (story.get("lesson") or "").strip()
            parts.append(f"**{title}.** {body}")
            if lesson:
                parts.append("")
                parts.append(f"_Lesson:_ {lesson}")
            parts.append("")

    counterargs = enriched.get("counterarguments") or []
    if counterargs:
        parts += ["## Counterarguments", ""]
        for c in counterargs:
            text = c.strip() if isinstance(c, str) else str(c)
            if text:
                parts.append(f"- {text}")
        parts.append("")

    quotes = enriched.get("notable_quotes") or []
    if quotes:
        parts += ["## Notable Quotes", ""]
        for q in quotes:
            if isinstance(q, dict):
                text = (q.get("quote") or "").strip()
                ctx = (q.get("context") or "").strip()
                if text:
                    parts.append(f"> {text}")
                    if ctx:
                        parts.append(f"> — _{ctx}_")
                    parts.append("")
            elif isinstance(q, str) and q.strip():
                parts.append(f"> {q.strip()}")
                parts.append("")

    if applied and (applied.get("applied_paragraph") or applied.get("applied_bullets")):
        parts += ["## Applied to You", ""]
        para = (applied.get("applied_paragraph") or "").strip()
        if para:
            parts.append(para)
            parts.append("")
        for bullet in applied.get("applied_bullets") or []:
            if not isinstance(bullet, dict):
                parts.append(f"- {bullet}")
                continue
            claim = (bullet.get("claim") or "").strip()
            why = (bullet.get("why_it_matters") or "").strip()
            action = (bullet.get("action") or "").strip()
            line = f"- **{claim}**"
            if why:
                line += f" — {why}"
            if action:
                line += f" _Action:_ {action}"
            parts.append(line)
        parts.append("")
        threads = applied.get("thread_links") or []
        if threads:
            cleaned = []
            for t in threads:
                if not isinstance(t, str):
                    continue
                s = t.strip().lstrip("[").rstrip("]").strip()
                if s:
                    cleaned.append(s)
            if cleaned:
                parts.append("_Touches:_ " + ", ".join(f"[[{t}]]" for t in cleaned))
                parts.append("")

    if stance_change_note and stance_change_note.strip():
        parts += ["## Author Stance Update", "", stance_change_note.strip(), ""]

    convo = enriched.get("in_conversation_with") or []
    if convo:
        parts += ["## In Conversation With", ""]
        for entry in convo:
            parts.append(f"- {entry}")
        parts.append("")

    parts.extend(_render_clips(book))
    parts += ["", "## Connections", "", "## My Notes", ""]
    return parts


def _render_thin_body(enriched: dict[str, Any], book: BookRecord) -> list[str]:
    """Legacy thin renderer for old-shape research files."""
    body_parts = ["## TL;DR", "", enriched.get("tldr", ""), "", "## Key Claims", ""]
    for entry in enriched.get("key_claims") or enriched.get("key_ideas") or []:
        claim = entry.get("claim", entry.get("idea", "")) if isinstance(entry, dict) else str(entry)
        context = entry.get("evidence_context", entry.get("explanation", "")) if isinstance(entry, dict) else ""
        body_parts.append(f"- **{claim}** — {context}" if context else f"- {claim}")
    body_parts.extend(["", "## Frameworks Introduced", ""])
    for fw in enriched.get("frameworks_introduced", []) or []:
        body_parts.append(f"- {fw}")
    body_parts.extend(["", "## In Conversation With", ""])
    for entry in enriched.get("in_conversation_with", []) or []:
        body_parts.append(f"- {entry}")
    body_parts.extend(_render_clips(book))
    body_parts.extend(["", "## Connections", "", "## My Notes", ""])
    return body_parts


def write_book_page(
    book: BookRecord,
    enriched: dict[str, Any],
    category: Category,
    policy: dict[str, Any] | None = None,
    subcategory: str | None = None,
    applied: dict[str, Any] | None = None,
    stance_change_note: str | None = None,
    summary: dict[str, Any] | None = None,
    creator_target: MaterializationCandidate | None = None,
    publisher_target: MaterializationCandidate | None = None,
    source_kind: str = "research",
    source_asset_path: str = "",
    force: bool = False,
) -> Path:
    """Write a wiki book page.

    enriched: either the legacy thin shape (key_ideas[]) or the new deep shape
        (key_frameworks[]). Renderer auto-detects.
    applied: optional Pass B output. When present, an "Applied to You" section
        is rendered.
    force: when True, overwrite existing pages. Used by deep-pass re-runs.
    """
    cfg = env.load()
    today = date.today().isoformat()
    target = book_page_path(cfg.repo_root, book, category)
    if target.exists() and not force:
        return target
    created_on, ingested_on = _preserved_source_write_dates(target, today=today)
    frontmatter_domains = list(content_policy_from_classification(policy).domains) if policy is not None else _domains_for_category(category)
    # Sanity-check subcategory: only valid for personal, must be in vocab
    if category != "personal":
        subcategory = None
    elif subcategory not in VALID_SUBCATEGORIES:
        subcategory = None
    author_slug = slugify(book.author[0]) if book.author else "unknown"
    title_slug = slugify(book.title)
    author_page_id = creator_target.resolved_page_id() if creator_target is not None else author_slug
    author_values = []
    if book.author:
        author_values.append(f"[[{author_page_id}]]")
        author_values.extend(book.author[1:])
    summary = summary or {}
    is_deep = "key_frameworks" in enriched or "core_argument" in enriched
    if is_deep:
        body_parts = _render_deep_body(enriched, applied, stance_change_note, book)
    else:
        body_parts = _render_thin_body(enriched, book)
    if summary.get("tldr"):
        body_parts[0:0] = ["## Summary Snapshot", "", summary.get("tldr", "").strip(), ""]
    if source_kind != "research":
        source_line = f"Source-grounded from local {source_kind}"
        if source_asset_path:
            source_line += f": `{source_asset_path}`"
        body_parts[0:0] = ["## Source Grounding", "", source_line, ""]
    research_rel = relative_markdown_path(
        target,
        Path(source_asset_path) if source_asset_path else raw_path(cfg.repo_root, "research", "books", f"{author_slug}-{title_slug}.summary.json"),
    )
    legacy_aliases = [summary_page_path(cfg.repo_root, book).stem, f"summary-book-{author_slug}-{title_slug}"]
    body_text = sanitize_wikilinks("\n".join(body_parts), repo_root=cfg.repo_root)
    write_contract_page(
        target,
        page_type="book",
        title=book.title,
        body=body_text,
        status="active" if book.status == "finished" else book.status,
        created=created_on,
        last_updated=today,
        aliases=legacy_aliases,
        tags=list(enriched.get("topics") or []),
        domains=frontmatter_domains,
        sources=[],
        extra_frontmatter={
            **_frontmatter_policy_fields(policy),
            **({"external_id": f"audible-{book.asin}"} if book.asin else {}),
            "source_type": "book",
            "source_date": book.finished_date or today,
            "ingested": ingested_on,
            "source_path": research_rel,
            "author": author_values,
            "publisher": f"[[{publisher_target.resolved_page_id()}]]" if publisher_target is not None else book.publisher,
            "category": category,
            "subcategory": subcategory or "",
            "published": "",
            "format": book.format,
            "length": book.length,
            "started": book.started_date,
            "finished": book.finished_date,
            "rating": book.rating,
            "recommended_by": [],
            "chapters": [],
            "key_claims": [],
            "connects_to": [],
            "source_kind": source_kind,
            "source_asset_path": source_asset_path,
        },
        force=force,
    )
    return target


def write_summary_page(
    book: BookRecord,
    enriched: dict[str, Any],
    category: Category,
    policy: dict[str, Any] | None = None,
    subcategory: str | None = None,
    applied: dict[str, Any] | None = None,
    stance_change_note: str | None = None,
    creator_target: MaterializationCandidate | None = None,
    publisher_target: MaterializationCandidate | None = None,
    source_path_override: Path | None = None,
    source_kind: str = "research",
    source_asset_path: str = "",
    force: bool = False,
) -> Path:
    """Compatibility wrapper that now returns the canonical book page."""
    return write_book_page(
        book,
        enriched,
        category=category,
        policy=policy,
        subcategory=subcategory,
        applied=applied,
        stance_change_note=stance_change_note,
        summary=enriched if isinstance(enriched, dict) else None,
        creator_target=creator_target,
        publisher_target=publisher_target,
        source_kind=source_kind,
        source_asset_path=source_asset_path or (str(source_path_override) if source_path_override else ""),
        force=force,
    )


def ensure_author_page(
    book: BookRecord,
    *,
    repo_root: Path,
    creator_target: MaterializationCandidate | None,
    source_link: str,
) -> Path | None:
    if creator_target is None or creator_target.page_type != "person":
        return None
    target = wiki_path(repo_root, "people", f"{creator_target.resolved_page_id()}.md")
    if target.exists():
        return target
    today = date.today().isoformat()
    body = f"# {creator_target.name}\n\nPrimary book author materialized from [[{source_link}]].\n"
    write_contract_page(
        target,
        page_type="person",
        title=creator_target.name,
        body=body,
        status="active",
        created=today,
        last_updated=today,
        aliases=[],
        domains=default_domains("person"),
        sources=[DurableLinkTarget(page_type="book", page_id=source_link)],
        extra_frontmatter={"name": creator_target.name},
    )
    return target


def ensure_publisher_page(
    book: BookRecord,
    *,
    repo_root: Path,
    publisher_target: MaterializationCandidate | None,
    source_link: str,
) -> Path | None:
    if publisher_target is None or publisher_target.page_type != "company":
        return None
    target = wiki_path(repo_root, "companies", f"{publisher_target.resolved_page_id()}.md")
    if target.exists():
        return target
    today = date.today().isoformat()
    body = f"# {publisher_target.name}\n\nPrimary book publisher materialized from [[{source_link}]].\n"
    write_contract_page(
        target,
        page_type="company",
        title=publisher_target.name,
        body=body,
        status="active",
        created=today,
        last_updated=today,
        aliases=[],
        domains=default_domains("company"),
        sources=[DurableLinkTarget(page_type="book", page_id=source_link)],
        extra_frontmatter={"name": publisher_target.name},
    )
    return target
