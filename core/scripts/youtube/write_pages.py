"""Generate contract-valid wiki pages for an enriched YouTube video.

For each video, writes one durable file:
1. wiki/sources/youtube/<category>/<id>-<slug>.md  (type: video)
   where <category> is either 'business' or 'personal' (never 'ignore' — those
   are dropped before they reach the writer).

The category drives the directory and is preserved as an extra tag so the wiki
continues to cluster business vs. personal videos together.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Literal

from mind.services.content_policy import canonical_policy_fields, content_policy_from_classification
from scripts.common import env
from scripts.common.frontmatter import read_page
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
from scripts.common.vault import raw_path, relative_markdown_path, wiki_path
from mind.services.durable_write import DurableLinkTarget, write_contract_page
from mind.services.materialization import MaterializationCandidate
from scripts.youtube.parse import YouTubeRecord

Category = Literal["business", "personal"]


def slugify(text: str, max_len: int = 60) -> str:
    """Thin wrapper around scripts.common.slugify."""
    from scripts.common.slugify import slugify as _slugify
    return _slugify(text, max_len=max_len)


def video_page_path(repo_root: Path, record: YouTubeRecord, category: Category) -> Path:
    existing = _existing_video_page_path(repo_root, record)
    if existing is not None:
        return existing
    base_slug = slugify(record.title) or record.video_id.lower()
    if _youtube_title_slug_is_unique(repo_root, base_slug, record.video_id):
        stem = base_slug
    else:
        stem = f"{base_slug}--youtube-{record.video_id.lower()}"
    return wiki_path(repo_root, "sources", "youtube", category, f"{stem}.md")


def canonical_page_id(repo_root: Path, record: YouTubeRecord) -> str:
    return video_page_path(repo_root, record, "business").stem


def summary_page_path(repo_root: Path, record: YouTubeRecord) -> Path:
    existing = _existing_summary_page_path(repo_root, record)
    if existing is not None:
        return existing
    return wiki_path(repo_root, "summaries", f"summary-{canonical_page_id(repo_root, record)}.md")


def channel_page_path(repo_root: Path, record: YouTubeRecord, creator_target: MaterializationCandidate | None = None) -> Path:
    channel_slug = creator_target.resolved_page_id() if creator_target is not None else slugify(record.channel)
    return wiki_path(repo_root, "channels", f"{channel_slug}.md")


def _existing_video_page_path(repo_root: Path, record: YouTubeRecord) -> Path | None:
    root = wiki_path(repo_root, "sources", "youtube")
    if not root.exists():
        return None
    external_id = f"youtube-{record.video_id}"
    for path in sorted(root.rglob("*.md")):
        text = path.read_text(encoding="utf-8")
        if f"external_id: {external_id}" in text:
            return path
    return None


def _existing_summary_page_path(repo_root: Path, record: YouTubeRecord) -> Path | None:
    root = wiki_path(repo_root, "summaries")
    if not root.exists():
        return None
    external_id = f"youtube-{record.video_id}"
    for path in sorted(root.glob("summary-*.md")):
        text = path.read_text(encoding="utf-8")
        if f"external_id: {external_id}" in text:
            return path
    legacy = root / f"summary-yt-{record.video_id}.md"
    if legacy.exists():
        return legacy
    return None


def _youtube_title_slug_is_unique(repo_root: Path, slug: str, video_id: str) -> bool:
    root = wiki_path(repo_root, "sources", "youtube")
    if not root.exists():
        return True
    collision_prefix = f"{slug}."
    for path in root.rglob(f"{slug}.md"):
        if f"youtube-{video_id}" not in path.read_text(encoding="utf-8"):
            return False
    for path in root.rglob(f"{slug}--youtube-*.md"):
        if f"youtube-{video_id}" not in path.read_text(encoding="utf-8"):
            return False
    return True


def _domains_for_category(category: Category) -> list[str]:
    if category == "business":
        return ["business"]
    return ["personal"]


def _frontmatter_policy_fields(policy: dict[str, Any] | None) -> dict[str, Any]:
    fields = canonical_policy_fields(policy)
    fields.pop("domains", None)
    return fields


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
    return ["## Channel Memory Delta", "", note, ""]


def _artifact_rel_paths(target: Path, repo_root: Path, record: YouTubeRecord) -> tuple[str, str, str]:
    source_rel = relative_markdown_path(
        target,
        raw_path(repo_root, "transcripts", "youtube", f"{record.video_id}.transcription.json"),
    )
    transcript_rel = relative_markdown_path(
        target,
        raw_path(repo_root, "transcripts", "youtube", f"{record.video_id}.transcript.txt"),
    )
    summary_rel = relative_markdown_path(
        target,
        raw_path(repo_root, "transcripts", "youtube", f"{record.video_id}.json"),
    )
    return source_rel, transcript_rel, summary_rel


def _source_date(record: YouTubeRecord) -> str:
    if record.watched_at:
        return record.watched_at[:10]
    if record.published_at:
        return record.published_at[:10]
    return ""


def _preserved_source_write_dates(target: Path, *, today: str) -> tuple[str, str]:
    if not target.exists():
        return today, today
    frontmatter, _body = read_page(target)
    created = str(frontmatter.get("created") or "").strip() or today
    ingested = str(frontmatter.get("ingested") or "").strip() or today
    return created, ingested


def write_video_page(
    record: YouTubeRecord,
    enriched: dict[str, Any],
    duration_minutes: float | None,
    category: Category,
    policy: dict[str, Any] | None = None,
    applied: dict[str, Any] | None = None,
    stance_change_note: str | None = None,
    creator_target: MaterializationCandidate | None = None,
    force: bool = False,
) -> Path:
    cfg = env.load()
    today = date.today().isoformat()
    target = video_page_path(cfg.repo_root, record, category)
    if target.exists() and not force:
        return target  # idempotent — never overwrite an existing page
    created_on, ingested_on = _preserved_source_write_dates(target, today=today)
    frontmatter_domains = list(content_policy_from_classification(policy).domains) if policy is not None else _domains_for_category(category)
    source_rel, transcript_rel, summary_rel = _artifact_rel_paths(target, cfg.repo_root, record)
    channel_page_id = creator_target.resolved_page_id() if creator_target is not None else ""
    legacy_aliases = [summary_page_path(cfg.repo_root, record).stem, f"summary-yt-{record.video_id}"]
    legacy_source_alias = f"{record.video_id}-{slugify(record.title) or record.video_id.lower()}"
    if legacy_source_alias not in legacy_aliases:
        legacy_aliases.append(legacy_source_alias)
    # Build body via shared renderers (filter empties, join with double newlines)
    rendered_sections = list(filter(None, [
        render_tldr(enriched),
        render_core_argument(enriched),
        render_argument_structure(enriched),
        render_key_claims(enriched),
        render_memorable_examples(enriched),
        render_notable_quotes(enriched),
        render_strongest_fight(enriched),
        render_in_conversation_with(enriched),
        render_entities(enriched),
        render_applied_to_you(applied),
        render_socratic_questions(applied),
    ]))
    # Takeaways (not in shared renderers — video-specific)
    takeaways = enriched.get("takeaways") or []
    if takeaways:
        tk_parts = ["## Takeaways\n"]
        for tk in takeaways:
            tk_parts.append(f"- {tk}")
        tk_parts.append("")
        rendered_sections.append("\n".join(tk_parts).rstrip() + "\n")
    # Attribution section (stance change note)
    rendered_sections.extend(filter(None, [
        "\n".join(_render_attribution_section(stance_change_note)) if _render_attribution_section(stance_change_note) else "",
    ]))
    # Article synthesis
    article = (enriched.get("article") or "").strip()
    if article:
        rendered_sections.append(f"## Article\n\n{article}\n")
    body_parts = []
    for section in rendered_sections:
        if section:
            body_parts.extend(section.rstrip("\n").split("\n"))
            body_parts.append("")
    body_text = sanitize_wikilinks("\n".join(body_parts), repo_root=cfg.repo_root)
    extra_frontmatter = {
        **_frontmatter_policy_fields(policy),
        "external_id": f"youtube-{record.video_id}",
        "source_type": "video",
        "source_date": _source_date(record),
        "ingested": ingested_on,
        "source_path": source_rel,
        "summary_path": summary_rel,
        "youtube_id": record.video_id,
        "channel": f"[[{channel_page_id}]]" if channel_page_id else record.channel,
        "channel_id": record.channel_id,
        "channel_url": record.channel_url,
        "category": category,
        "published": record.published_at[:10] if record.published_at else "",
        "watched_on": record.watched_at[:10] if record.watched_at else "",
        "transcript_path": transcript_rel,
        "thumbnail_url": record.thumbnail_url,
        "youtube_url": record.title_url or f"https://www.youtube.com/watch?v={record.video_id}",
        "topics": enriched.get("topics", []),
        "highlights": enriched.get("notable_quotes", []),
    }
    if duration_minutes is not None:
        extra_frontmatter["duration_minutes"] = int(duration_minutes)
    write_contract_page(
        target,
        page_type="video",
        title=record.title,
        body=body_text,
        status="active",
        created=created_on,
        last_updated=today,
        aliases=legacy_aliases,
        tags=list(enriched.get("topics") or []),
        domains=frontmatter_domains,
        sources=[],
        extra_frontmatter=extra_frontmatter,
        force=force,
    )
    return target


def write_summary_page(
    record: YouTubeRecord,
    enriched: dict[str, Any],
    category: Category,
    policy: dict[str, Any] | None = None,
    applied: dict[str, Any] | None = None,
    stance_change_note: str | None = None,
    creator_target: MaterializationCandidate | None = None,
    force: bool = False,
) -> Path:
    """Compatibility wrapper that now returns the canonical video page."""
    return write_video_page(
        record,
        enriched,
        duration_minutes=0.0,
        category=category,
        policy=policy,
        applied=applied,
        stance_change_note=stance_change_note,
        creator_target=creator_target,
        force=force,
    )


def ensure_channel_page(
    record: YouTubeRecord,
    *,
    repo_root: Path,
    creator_target: MaterializationCandidate | None,
    source_link: str,
) -> Path | None:
    if creator_target is None:
        return None
    if creator_target.page_type != "channel":
        raise ValueError("youtube channel materialization requires a channel target")
    target = channel_page_path(repo_root, record, creator_target)
    if target.exists():
        return target
    today = date.today().isoformat()
    body = f"# {creator_target.name}\n\nPrimary YouTube channel materialized from [[{source_link}]].\n"
    write_contract_page(
        target,
        page_type="channel",
        title=creator_target.name,
        body=body,
        status="active",
        created=today,
        last_updated=today,
        aliases=[],
        domains=["learning"],
        sources=[DurableLinkTarget(page_type="video", page_id=source_link)],
        extra_frontmatter={
            "name": creator_target.name,
            "channel_id": record.channel_id,
            "channel_url": record.channel_url,
        },
    )
    return target
