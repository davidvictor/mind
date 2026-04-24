"""Write contract-aligned article pages.

The shared lifecycle materializer uses these helpers to:
1. rewrite article pages under ``wiki/sources/articles/<slug>.md``
2. materialize the narrow primary actor pages allowed for Phase 4

Callers can preserve existing pages by leaving ``force=False`` or rewrite
legacy flat pages during migration by passing ``force=True``.
"""
from __future__ import annotations

from datetime import date as _date_type
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from mind.services.durable_write import DurableLinkTarget, write_contract_page
from mind.services.materialization import MaterializationCandidate
from scripts.articles.parse import ArticleDropEntry
from scripts.articles.fetch import ArticleFetchResult
from scripts.common.default_tags import default_domains
from scripts.common.section_renderers import (
    render_tldr,
    render_core_argument,
    render_argument_structure,
    render_key_claims,
    render_memorable_examples,
    render_notable_quotes,
    render_strongest_fight,
    render_in_conversation_with,
    render_entities,
    render_applied_to_you,
    render_socratic_questions,
)
from scripts.common.wikilink_sanitizer import sanitize_wikilinks
from scripts.common.vault import wiki_path


def _date_prefix(iso: str) -> str:
    return iso[:10] if iso else "unknown-date"


def slugify_url(url: str, discovered_at: str) -> str:
    """Stable, filesystem-safe slug for an article URL.

    Format: <YYYY-MM-DD>-<hostname>-<path segments>
    Truncated to 60 chars after the date prefix. Strips 'www.', query strings,
    and fragments.
    """
    parsed = urlparse(url)
    host = parsed.netloc.replace("www.", "")
    parts = [host]
    for seg in parsed.path.strip("/").split("/"):
        if seg:
            parts.append(seg)
    raw = "-".join(parts)
    from scripts.common.slugify import slugify as _slugify
    slug = _slugify(raw, max_len=60) or "untitled"
    date = _date_prefix(discovered_at)
    return f"{date}-{slug}"


def article_page_path(repo_root: Path, entry: ArticleDropEntry) -> Path:
    slug = slugify_url(entry.url, entry.discovered_at)
    return wiki_path(repo_root, "sources", "articles", f"{slug}.md")


def canonical_page_id(repo_root: Path, entry: ArticleDropEntry) -> str:
    return article_page_path(repo_root, entry).stem


def summary_page_path(repo_root: Path, entry: ArticleDropEntry) -> Path:
    slug = slugify_url(entry.url, entry.discovered_at)
    return wiki_path(repo_root, "summaries", f"summary-{slug}.md")


def _author_value(
    fetch_result: ArticleFetchResult,
    creator_target: MaterializationCandidate | None,
) -> str:
    if creator_target is not None:
        return f"[[{creator_target.resolved_page_id()}]]"
    return fetch_result.author or ""


def _outlet_value(
    fetch_result: ArticleFetchResult,
    publisher_target: MaterializationCandidate | None,
    entry: ArticleDropEntry,
) -> str:
    if publisher_target is not None:
        return f"[[{publisher_target.resolved_page_id()}]]"
    return fetch_result.sitename or urlparse(entry.url).netloc.replace("www.", "")


def _render_applied_section(applied: dict[str, Any] | None) -> list[str]:
    """Legacy wrapper — delegates to shared renderer, returns list[str] for backward compat."""
    rendered = render_applied_to_you(applied)
    if not rendered:
        return []
    return rendered.rstrip("\n").split("\n") + [""]


def _render_attribution_section(stance_change_note: str | None) -> list[str]:
    note = (stance_change_note or "").strip()
    if not note:
        return []
    return ["## Author Stance Update", "", note, ""]


def _render_discovered_via_line(entry: ArticleDropEntry) -> str:
    if entry.source_type == "substack-link":
        if entry.source_page_id:
            return "_Discovered via [[%s|the linking Substack post]]._\n" % entry.source_page_id
        if entry.source_post_url:
            return f"_Discovered via [the linking Substack post]({entry.source_post_url})._\n"
        return f"_Discovered via Substack post {entry.source_post_id}._\n"
    source_label = entry.source_label or "links import"
    return f"_Discovered via {source_label}._\n"


def write_article_page(
    entry: ArticleDropEntry,
    *,
    fetch_result: ArticleFetchResult,
    summary: dict[str, Any],
    repo_root: Path,
    applied: dict[str, Any] | None = None,
    stance_change_note: str | None = None,
    creator_target: MaterializationCandidate | None = None,
    publisher_target: MaterializationCandidate | None = None,
    force: bool = False,
) -> Path:
    """Write a single article page. Skips if it already exists."""
    target = article_page_path(repo_root, entry)
    if target.exists() and not force:
        return target

    body_parts: list[str] = []
    title = fetch_result.title or entry.anchor_text or "Untitled"
    author_label = creator_target.name if creator_target is not None else fetch_result.author
    outlet_label = publisher_target.name if publisher_target is not None else fetch_result.sitename
    body_parts.append(f"# {title}\n")
    if author_label:
        if creator_target is not None:
            body_parts.append(f"**Author:** [[{creator_target.resolved_page_id()}|{author_label}]]  ")
        else:
            body_parts.append(f"**Author:** {author_label}  ")
    if outlet_label:
        if publisher_target is not None:
            body_parts.append(f"**Outlet:** [[{publisher_target.resolved_page_id()}|{outlet_label}]]  ")
        else:
            body_parts.append(f"**Outlet:** {outlet_label}  ")
    if fetch_result.published:
        body_parts.append(f"**Published:** {fetch_result.published[:10]}  ")
    body_parts.append(f"**Source:** [{entry.url}]({entry.url})\n")
    body_parts.append(_render_discovered_via_line(entry))

    # Render via shared section renderers
    rendered_sections = list(filter(None, [
        render_tldr(summary),
        render_core_argument(summary),
        render_argument_structure(summary),
        render_key_claims(summary),
        render_memorable_examples(summary),
        render_notable_quotes(summary),
        render_strongest_fight(summary),
        render_in_conversation_with(summary),
        render_entities(summary),
    ]))
    # Takeaways (article-specific, not in shared renderers)
    takeaways = summary.get("takeaways") or []
    if takeaways:
        tk_parts = ["## Takeaways\n"]
        for t in takeaways:
            tk_parts.append(f"- {t}")
        tk_parts.append("")
        rendered_sections.append("\n".join(tk_parts).rstrip() + "\n")
    rendered_sections.extend(filter(None, [
        render_applied_to_you(applied),
        render_socratic_questions(applied),
    ]))
    stance_lines = _render_attribution_section(stance_change_note)
    if stance_lines:
        rendered_sections.append("\n".join(stance_lines).rstrip() + "\n")
    for section in rendered_sections:
        if section:
            body_parts.extend(section.rstrip("\n").split("\n"))
            body_parts.append("")

    body_text = (fetch_result.body_text or "").strip()
    if body_text:
        body_parts.append("## Full Article\n")
        body_parts.append(body_text + "\n")

    if entry.source_type == "substack-link":
        discovered_via = entry.source_post_url or f"substack-link:{entry.source_post_id}"
    else:
        discovered_via = entry.source_label or entry.source_type or "links-import"

    created_on = _date_prefix(entry.discovered_at)
    source_date = _date_prefix(fetch_result.published) if fetch_result.published else created_on
    legacy_summary_id = summary_page_path(repo_root, entry).stem
    body_text = sanitize_wikilinks("\n".join(body_parts), repo_root=repo_root)
    write_contract_page(
        target,
        page_type="article",
        title=title,
        body=body_text,
        status="active",
        created=created_on,
        last_updated=created_on,
        aliases=[legacy_summary_id],
        tags=list(summary.get("topics") or []),
        domains=["learning", "craft"],
        sources=[DurableLinkTarget(page_type="article", page_id=entry.source_page_id)] if entry.source_page_id else [],
        extra_frontmatter={
            "source_type": "article",
            "source_date": source_date,
            "ingested": _date_type.today().isoformat(),
            "author": _author_value(fetch_result, creator_target),
            "outlet": _outlet_value(fetch_result, publisher_target, entry),
            "published": (fetch_result.published or "")[:10],
            "source_url": entry.url,
            "discovered_via": discovered_via,
            "discovered_at": entry.discovered_at,
        },
        force=force,
    )
    return target


def write_summary_page(
    entry: ArticleDropEntry,
    *,
    fetch_result: ArticleFetchResult,
    summary: dict[str, Any],
    repo_root: Path,
    applied: dict[str, Any] | None = None,
    stance_change_note: str | None = None,
    creator_target: MaterializationCandidate | None = None,
    publisher_target: MaterializationCandidate | None = None,
    force: bool = False,
) -> Path:
    """Compatibility wrapper that now returns the canonical article page."""
    return write_article_page(
        entry,
        fetch_result=fetch_result,
        summary=summary,
        repo_root=repo_root,
        applied=applied,
        stance_change_note=stance_change_note,
        creator_target=creator_target,
        publisher_target=publisher_target,
        force=force,
    )


def ensure_author_page(
    *,
    fetch_result: ArticleFetchResult,
    repo_root: Path,
    creator_target: MaterializationCandidate | None,
    publisher_target: MaterializationCandidate | None,
    source_link: str,
) -> Path | None:
    if creator_target is None or creator_target.page_type != "person":
        return None
    today_iso = _date_type.today().isoformat()
    target = wiki_path(repo_root, "people", f"{creator_target.resolved_page_id()}.md")
    if target.exists():
        return target
    publication_page_id = publisher_target.resolved_page_id() if publisher_target else ""
    publication_label = publisher_target.name if publisher_target else ""
    body = f"# {creator_target.name}\n\nPrimary article author materialized from [[{source_link}]].\n"
    if publication_page_id and publication_label:
        body += f"\nWrites for [[{publication_page_id}|{publication_label}]].\n"
    write_contract_page(
        target,
        page_type="person",
        title=creator_target.name,
        body=body,
        status="active",
        created=today_iso,
        last_updated=today_iso,
        aliases=[],
        domains=default_domains("person"),
        sources=[DurableLinkTarget(page_type="article", page_id=source_link)],
        relates_to=[DurableLinkTarget(page_type="company", page_id=publication_page_id)] if publication_page_id else [],
        extra_frontmatter={"name": creator_target.name},
    )
    return target


def ensure_outlet_page(
    *,
    fetch_result: ArticleFetchResult,
    repo_root: Path,
    publisher_target: MaterializationCandidate | None,
    source_link: str,
) -> Path | None:
    if publisher_target is None or publisher_target.page_type != "company":
        return None
    today_iso = _date_type.today().isoformat()
    target = wiki_path(repo_root, "companies", f"{publisher_target.resolved_page_id()}.md")
    if target.exists():
        return target
    body = f"# {publisher_target.name}\n\nPrimary article publisher materialized from [[{source_link}]].\n"
    write_contract_page(
        target,
        page_type="company",
        title=publisher_target.name,
        body=body,
        status="active",
        created=today_iso,
        last_updated=today_iso,
        aliases=[],
        domains=default_domains("company"),
        sources=[DurableLinkTarget(page_type="article", page_id=source_link)],
        extra_frontmatter={"name": publisher_target.name},
    )
    return target
