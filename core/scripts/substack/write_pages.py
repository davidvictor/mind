"""Generate durable wiki pages from a Substack post and its enrichment outputs.

This module writes three durable page types:
  1. article
  2. person stub for the primary creator
  3. company stub for the publication

Writes are idempotent. Existing pages are left untouched.
"""
from __future__ import annotations

import json
from datetime import date as _date_type
from pathlib import Path
from typing import Any

from scripts.common.frontmatter import read_page
from scripts.common.section_rewriter import replace_or_insert_section
from scripts.common.default_tags import default_domains, default_tags
from scripts.common.section_renderers import (
    render_tldr,
    render_core_argument,
    render_argument_structure,
    render_key_claims,
    render_memorable_examples,
    render_notable_quotes,
    render_strongest_fight,
    render_in_conversation_with as _render_in_conversation_with_base,
    render_applied_to_you,
    render_socratic_questions,
)
from scripts.common.wikilink_sanitizer import sanitize_wikilinks
from scripts.common.vault import raw_path, wiki_path
from mind.services.durable_write import DurableLinkTarget, write_contract_page
from mind.services.materialization import MaterializationCandidate
from scripts.substack.parse import SubstackRecord

def slugify(text: str, max_len: int = 60) -> str:
    """Convert text to a lowercase-hyphen-separated slug.

    Thin wrapper around scripts.common.slugify. Returns 'untitled' for
    empty or entirely-non-alphanumeric input. Truncates to max_len.
    """
    from scripts.common.slugify import slugify as _slugify
    return _slugify(text, max_len=max_len) or "untitled"


def _date_prefix(iso: str) -> str:
    """Extract YYYY-MM-DD from an ISO-8601 timestamp."""
    return iso[:10] if iso else "unknown-date"


def article_page_path(repo_root: Path, record: SubstackRecord) -> Path:
    date = _date_prefix(record.published_at)
    post_slug = record.slug or slugify(record.title)
    return (
        wiki_path(repo_root, "sources", "substack", record.publication_slug, f"{date}-{post_slug}.md")
    )


def summary_page_path(repo_root: Path, record: SubstackRecord) -> Path:
    return wiki_path(repo_root, "summaries", f"summary-{article_page_path(repo_root, record).stem}.md")


def canonical_page_id(repo_root: Path, record: SubstackRecord) -> str:
    return article_page_path(repo_root, record).stem


def author_page_path(repo_root: Path, record: SubstackRecord) -> Path:
    return wiki_path(repo_root, "people", f"{slugify(record.author_name)}.md")


def publication_page_path(repo_root: Path, record: SubstackRecord) -> Path:
    return wiki_path(repo_root, "companies", f"{record.publication_slug}.md")


def article_slug(repo_root: Path, record: SubstackRecord) -> str:
    """Return the wiki-link slug (basename without .md) for a post's article page.

    E.g. ``2026-03-15-on-trust`` — matches the filename produced by
    ``article_page_path``.
    """
    return article_page_path(repo_root, record).stem


def _render_referenced_links(classified_links: dict[str, Any]) -> str:
    """Render the Referenced Links section grouping by category.

    Hides 'ignore' links entirely. Returns empty string if no non-ignored links.
    """
    external = classified_links.get("external_classified") or []
    substack_internal = classified_links.get("substack_internal") or []

    business = [L for L in external if L.get("category") == "business"]
    personal = [L for L in external if L.get("category") == "personal"]
    # 'ignore' is intentionally hidden

    if not (business or personal or substack_internal):
        return ""

    parts = ["## Referenced Links\n"]
    if business:
        parts.append("### Business\n")
        for L in business:
            parts.append(f"- [{L['anchor_text']}]({L['url']})")
        parts.append("")
    if personal:
        parts.append("### Personal\n")
        for L in personal:
            parts.append(f"- [{L['anchor_text']}]({L['url']})")
        parts.append("")
    if substack_internal:
        parts.append("### Substack (internal)\n")
        for L in substack_internal:
            parts.append(f"- [{L['anchor_text']}]({L['url']})")
        parts.append("")
    return "\n".join(parts).rstrip() + "\n"


# ---------------------------------------------------------------------------
# Substack-specific section renderer — extends the shared in_conversation_with
# with relates_to_prior (Substack-only feature).
# ---------------------------------------------------------------------------


def _render_in_conversation_with(summary: dict[str, Any]) -> str:
    """Substack-specific: renders both in_conversation_with and relates_to_prior."""
    in_conv = summary.get("in_conversation_with") or []
    relates_to_prior = summary.get("relates_to_prior") or []

    if not in_conv and not relates_to_prior:
        return ""

    # Start with base shared rendering for in_conversation_with
    parts = ["## In Conversation With\n"]
    if in_conv:
        for entry in in_conv:
            parts.append(f"- {entry}")
        parts.append("")

    # Substack-specific: prior post cross-references
    if relates_to_prior:
        parts.append("**Prior posts in your wiki:**\n")
        for entry in relates_to_prior:
            if not isinstance(entry, dict):
                continue
            post_title = entry.get("post_title", "")
            post_id = entry.get("post_id", "")
            wiki_target = slugify(post_title) if post_title else str(post_id)
            relation = (entry.get("relation") or "").strip()
            note = (entry.get("note") or "").strip()
            line = f"- [[{wiki_target}]]"
            if relation:
                line += f" ({relation})"
            if note:
                line += f" — {note}"
            parts.append(line)
        parts.append("")

    return "\n".join(parts).rstrip() + "\n"


def _render_author_stance_update(record: SubstackRecord, stance_change_note: str | None) -> str:
    if not stance_change_note or not stance_change_note.strip():
        return ""

    author_slug = slugify(record.author_name)
    author_page = f"[[{author_slug}]]"
    return f"## Author Stance Update\n\n{stance_change_note.strip()} See {author_page}.\n"


def _render_discovered_via(
    *,
    discovered_via_page_id: str | None = None,
    discovered_via_url: str | None = None,
) -> str:
    if discovered_via_page_id:
        return f"_Discovered via [[{discovered_via_page_id}|the linking Substack post]]._\n"
    if discovered_via_url:
        return f"_Discovered via [the linking Substack post]({discovered_via_url})._\n"
    return ""


def _source_page_path(repo_root: Path, page_id: str) -> Path | None:
    root = wiki_path(repo_root, "sources", "substack")
    if not root.exists():
        return None
    matches = list(root.rglob(f"{page_id}.md"))
    if not matches:
        return None
    return sorted(matches)[0]


def _parse_materialized_links(content: str) -> dict[str, list[str]]:
    buckets = {"Articles": [], "Substack Posts": []}
    current: str | None = None
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if line == "### Articles":
            current = "Articles"
            continue
        if line == "### Substack Posts":
            current = "Substack Posts"
            continue
        if current and line.startswith("- "):
            item = line[2:].strip()
            if item and item not in buckets[current]:
                buckets[current].append(item)
    return buckets


def _render_materialized_links(content: dict[str, list[str]]) -> str:
    parts: list[str] = []
    for heading in ("Articles", "Substack Posts"):
        items = content.get(heading) or []
        if not items:
            continue
        parts.append(f"### {heading}")
        parts.append("")
        parts.extend(f"- {item}" for item in items)
        parts.append("")
    return "\n".join(parts).rstrip() + "\n"


def add_materialized_link_to_source_page(
    *,
    repo_root: Path,
    source_page_id: str,
    target_page_id: str,
    target_kind: str,
) -> bool:
    source_path = _source_page_path(repo_root, source_page_id)
    if source_path is None or not source_path.exists():
        return False

    _frontmatter, body = read_page(source_path)
    existing_content = ""
    in_section = False
    for line in body.splitlines():
        stripped = line.lstrip()
        if line.rstrip() == "## Materialized Linked Pages":
            in_section = True
            continue
        if in_section and stripped.startswith("## ") and not stripped.startswith("### "):
            break
        if in_section:
            existing_content += line + "\n"

    buckets = _parse_materialized_links(existing_content)
    heading = "Articles" if target_kind == "article" else "Substack Posts"
    bullet = f"[[{target_page_id}]]"
    if bullet in buckets[heading]:
        return False
    buckets[heading].append(bullet)

    content = _render_materialized_links(buckets)
    return replace_or_insert_section(
        file_path=source_path,
        section_heading="## Materialized Linked Pages",
        new_content=content,
        insert_after="## Referenced Links",
    )


# ---------------------------------------------------------------------------
# Public writers
# ---------------------------------------------------------------------------


def write_article_page(
    record: SubstackRecord,
    *,
    summary: dict[str, Any],
    classified_links: dict[str, Any],
    body_markdown: str,
    repo_root: Path,
    applied: dict[str, Any] | None = None,          # Pass B output
    stance_change_note: str | None = None,          # Pass C change note
    creator_target: MaterializationCandidate | None = None,
    publisher_target: MaterializationCandidate | None = None,
    discovered_via_page_id: str | None = None,
    discovered_via_url: str | None = None,
    force: bool = False,
) -> Path:
    """Write the substack article page. Skips if it already exists."""
    target = article_page_path(repo_root, record)
    if target.exists() and not force:
        return target

    author_page_id = creator_target.resolved_page_id() if creator_target else slugify(record.author_name)
    author_label = creator_target.name if creator_target else record.author_name
    publication_page_id = publisher_target.resolved_page_id() if publisher_target else record.publication_slug
    publication_label = publisher_target.name if publisher_target else record.publication_name

    # Section 1 — Header block (title, subtitle, author, publication, published, source)
    header_lines: list[str] = []
    header_lines.append(f"# {record.title}\n")
    if record.subtitle:
        header_lines.append(f"_{record.subtitle}_\n")
    header_lines.append(f"**Author:** [[{author_page_id}|{author_label}]]  ")
    header_lines.append(f"**Publication:** [[{publication_page_id}|{publication_label}]]  ")
    published_display = record.published_at[:10] if record.published_at else "unknown"
    header_lines.append(f"**Published:** {published_display}  ")
    header_lines.append(f"**Source:** [{record.url}]({record.url})\n")
    header_block = "\n".join(header_lines)

    body_parts = [header_block]
    discovered_via = _render_discovered_via(
        discovered_via_page_id=discovered_via_page_id,
        discovered_via_url=discovered_via_url,
    )
    if discovered_via:
        body_parts.append(discovered_via)

    # Sections 2–11 via renderers (filter empties)
    body_parts.extend(filter(None, [
        render_tldr(summary),                        # 2
        render_core_argument(summary),               # 3
        render_argument_structure(summary),          # 4
        render_key_claims(summary),                  # 5
        render_memorable_examples(summary),          # 6
        render_notable_quotes(summary),              # 7
        render_strongest_fight(summary),             # 8
        _render_in_conversation_with(summary),       # 9 (substack-specific)
        render_applied_to_you(applied),              # 10
        render_socratic_questions(applied),          # 11
    ]))

    # Section 12 — Referenced Links (signature UNCHANGED per Plan B constraint)
    refs_md = _render_referenced_links(classified_links)
    if refs_md:
        body_parts.append(refs_md)

    # Section 13 — Author Stance Update
    stance_md = _render_author_stance_update(record, stance_change_note)
    if stance_md:
        body_parts.append(stance_md)

    # Section 14 — Full Body
    if body_markdown.strip():
        body_parts.append("## Full Body\n\n" + body_markdown.strip() + "\n")

    today_iso = _date_type.today().isoformat()
    legacy_aliases = [summary_page_path(repo_root, record).stem, f"summary-substack-{record.id}"]
    body_text = sanitize_wikilinks("\n\n".join(body_parts), repo_root=repo_root)
    write_contract_page(
        target,
        page_type="article",
        title=record.title,
        body=body_text,
        status="active",
        created=record.saved_at[:10] if record.saved_at else "",
        last_updated=record.saved_at[:10] if record.saved_at else "",
        aliases=legacy_aliases,
        tags=list(summary.get("topics") or []),
        domains=["learning", "craft"],
        sources=[DurableLinkTarget(page_type="article", page_id=discovered_via_page_id)] if discovered_via_page_id else [],
        extra_frontmatter={
            "external_id": f"substack-{record.id}",
            "source_type": "substack",
            "source_date": record.published_at[:10] if record.published_at else (record.saved_at[:10] if record.saved_at else today_iso),
            "ingested": today_iso,
            "author": f"[[{author_page_id}]]",
            "outlet": f"[[{publication_page_id}]]",
            "published": record.published_at[:10] if record.published_at else "",
            "source_url": record.url,
            "saved_at": record.saved_at,
            **({"discovered_via": discovered_via_url} if discovered_via_url else {}),
        },
        force=force,
    )
    return target


def write_summary_page(
    record: SubstackRecord,
    *,
    summary: dict[str, Any],
    repo_root: Path,
    applied: dict[str, Any] | None = None,          # Pass B output
    stance_change_note: str | None = None,          # Pass C change note
    creator_target: MaterializationCandidate | None = None,
    publisher_target: MaterializationCandidate | None = None,
    discovered_via_page_id: str | None = None,
    discovered_via_url: str | None = None,
    force: bool = False,
) -> Path:
    """Compatibility wrapper that now returns the canonical article page."""
    return write_article_page(
        record,
        summary=summary,
        classified_links={},
        body_markdown="",
        repo_root=repo_root,
        applied=applied,
        stance_change_note=stance_change_note,
        creator_target=creator_target,
        publisher_target=publisher_target,
        discovered_via_page_id=discovered_via_page_id,
        discovered_via_url=discovered_via_url,
        force=force,
    )


def ensure_author_page(
    record: SubstackRecord,
    *,
    repo_root: Path,
    creator_target: MaterializationCandidate | None = None,
    publisher_target: MaterializationCandidate | None = None,
) -> Path:
    """Create a canonical person page for the author if it does not exist."""
    author_slug = creator_target.resolved_page_id() if creator_target else slugify(record.author_name)
    publication_page_id = publisher_target.resolved_page_id() if publisher_target else record.publication_slug
    publication_label = publisher_target.name if publisher_target else record.publication_name
    target = wiki_path(repo_root, "people", f"{author_slug}.md")
    if target.exists():
        return target

    if creator_target is not None and creator_target.page_type != "person":
        raise ValueError(f"author candidate must be person, got {creator_target.page_type!r}")

    today = _date_type.today().isoformat()
    author_name = creator_target.name if creator_target else record.author_name
    body = (
        f"# {author_name}\n\n"
        f"Substack author at [[{publication_page_id}|{publication_label}]].\n\n"
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
    write_contract_page(
        target,
        page_type="person",
        title=author_name,
        body=body,
        status="active",
        created=today,
        last_updated=today,
        aliases=[],
        domains=default_domains("person"),
        relates_to=[DurableLinkTarget(page_type="company", page_id=publication_page_id)],
        extra_frontmatter={
            "name": author_name,
            "substack_author_id": record.author_id,
        },
    )
    return target


def ensure_publication_page(
    record: SubstackRecord,
    *,
    repo_root: Path,
    publisher_target: MaterializationCandidate | None = None,
    source_link: str | None = None,
) -> Path:
    """Create a publication stub if it does not exist. Never mutates."""
    publication_slug = publisher_target.resolved_page_id() if publisher_target else record.publication_slug
    publication_name = publisher_target.name if publisher_target else (record.publication_name or record.publication_slug)
    target = wiki_path(repo_root, "companies", f"{publication_slug}.md")
    if target.exists():
        return target

    if publisher_target is not None and publisher_target.page_type != "company":
        raise ValueError(f"publication candidate must be company, got {publisher_target.page_type!r}")

    today = _date_type.today().isoformat()
    body = (
        f"# {publication_name}\n\n"
        f"Stub created by /ingest-substack on {today}.\n\n"
        f"Substack publication at "
        f"[{publication_slug}.substack.com](https://{publication_slug}.substack.com).\n"
    )
    write_contract_page(
        target,
        page_type="company",
        title=publication_name,
        body=body,
        status="active",
        created=today,
        last_updated=today,
        aliases=[],
        domains=default_domains("company"),
        sources=[DurableLinkTarget(page_type="article", page_id=source_link)] if source_link else [],
        extra_frontmatter={
            "name": publication_name,
            "substack_publication_slug": publication_slug,
            "website": f"https://{publication_slug}.substack.com",
        },
    )
    return target


def append_links_to_drop_queue(
    record: SubstackRecord,
    *,
    classified_links: dict[str, Any],
    repo_root: Path,
    today: str,
) -> Path:
    """Append non-ignored external links to raw/drops/articles-from-substack-YYYY-MM-DD.jsonl.

    Ignored links are excluded. The drop file is line-delimited JSON, appended across
    multiple posts in the same run AND across re-runs of the same day.

    **Idempotent**: dedupes by (source_post_id, url). If an entry for this post+url
    already exists in today's drop file, it is not re-appended. This makes
    /ingest-substack safe to re-run on the same export without bloating Plan B's
    input queue with duplicates.

    If there are no non-ignored links to queue, still touches the file so downstream
    tooling can detect "no new links for today".
    """
    drop_dir = raw_path(repo_root, "drops")
    drop_dir.mkdir(parents=True, exist_ok=True)
    target = drop_dir / f"articles-from-substack-{today}.jsonl"

    externals = classified_links.get("external_classified") or []
    keep = [L for L in externals if L.get("category") in ("business", "personal")]
    if not keep:
        target.touch(exist_ok=True)
        return target

    # Load existing entry keys so we can dedupe across runs + intra-run calls.
    existing_keys: set[tuple[str, str]] = set()
    if target.exists():
        for line in target.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue  # corrupt line — skip, don't crash
            key = (entry.get("source_post_id", ""), entry.get("url", ""))
            existing_keys.add(key)

    with target.open("a", encoding="utf-8") as fh:
        for L in keep:
            key = (record.id, L["url"])
            if key in existing_keys:
                continue  # already queued — skip
            existing_keys.add(key)
            entry = {
                "url": L["url"],
                "source_post_id": record.id,
                "source_post_url": record.url,
                "source_page_id": canonical_page_id(repo_root, record),
                "anchor_text": L.get("anchor_text", ""),
                "context_snippet": L.get("context_snippet", ""),
                "category": L["category"],
                "discovered_at": record.saved_at,
                "source_type": "substack-link",
            }
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
    return target
