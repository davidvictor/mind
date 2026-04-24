"""Shared durable write helpers for contract-backed pages."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Literal

from scripts.common.contract import canonicalize_page_type, page_types
from scripts.common.default_tags import default_tags, normalize_topic_tags
from scripts.common.wiki_writer import write_page

AllowedLinkTargetType = Literal[
    "person",
    "company",
    "channel",
    "article",
    "web-discovery",
    "video",
    "book",
    "summary",
    "concept",
    "playbook",
    "stance",
    "inquiry",
    "tool",
]

ALLOWED_LINK_TARGET_TYPES: tuple[AllowedLinkTargetType, ...] = (
    "person",
    "company",
    "channel",
    "article",
    "web-discovery",
    "video",
    "book",
    "summary",
    "concept",
    "playbook",
    "stance",
    "inquiry",
    "tool",
)
_PROTECTED_FRONTMATTER_KEYS = {
    "id",
    "type",
    "title",
    "status",
    "created",
    "last_updated",
    "aliases",
    "tags",
    "domains",
    "relates_to",
    "sources",
}


@dataclass(frozen=True)
class DurableLinkTarget:
    """Typed durable link target used at the write boundary."""

    page_type: AllowedLinkTargetType
    page_id: str
    label: str | None = None


def _normalize_page_type(page_type: str) -> str:
    canonical = canonicalize_page_type(page_type)
    if canonical not in page_types():
        raise ValueError(f"Unknown contract-backed page type: {page_type!r}")
    return canonical


def ensure_tag_order(page_type: str, tags: Iterable[str] | None = None) -> list[str]:
    """Return deterministic tags with default axes first and extras second."""

    canonical = _normalize_page_type(page_type)
    ordered = list(default_tags(canonical))
    seen = set(ordered)
    for tag in normalize_topic_tags(list(tags or [])):
        if tag in seen:
            continue
        ordered.append(tag)
        seen.add(tag)
    return ordered


def render_link_target(target: DurableLinkTarget) -> str:
    """Render a typed link target as a wiki-link after validation."""

    if target.page_type not in ALLOWED_LINK_TARGET_TYPES:
        raise ValueError(f"Unsupported durable link target type: {target.page_type!r}")
    if not target.page_id:
        raise ValueError("Durable link target requires a non-empty page_id")
    if target.label:
        return f"[[{target.page_id}|{target.label}]]"
    return f"[[{target.page_id}]]"


def _render_link_list(targets: Iterable[DurableLinkTarget] | None) -> list[str]:
    return [render_link_target(target) for target in (targets or [])]


def build_frontmatter(
    *,
    page_type: str,
    page_id: str,
    title: str,
    status: str,
    created: str,
    last_updated: str,
    aliases: list[str] | None = None,
    tags: Iterable[str] | None = None,
    domains: list[str] | None = None,
    relates_to: Iterable[DurableLinkTarget] | None = None,
    sources: Iterable[DurableLinkTarget] | None = None,
    extra_frontmatter: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build contract-aligned frontmatter for a durable page write."""

    canonical = _normalize_page_type(page_type)
    frontmatter: dict[str, Any] = {
        "id": page_id,
        "type": canonical,
        "title": title,
        "status": status,
        "created": created,
        "last_updated": last_updated,
        "aliases": aliases or [],
        "tags": ensure_tag_order(canonical, tags),
        "domains": domains or [],
        "relates_to": _render_link_list(relates_to),
        "sources": _render_link_list(sources),
    }
    if extra_frontmatter:
        overlap = sorted(_PROTECTED_FRONTMATTER_KEYS & set(extra_frontmatter))
        if overlap:
            raise ValueError(
                "extra_frontmatter cannot override protected contract fields: "
                f"{overlap}"
            )
        frontmatter.update(extra_frontmatter)
    required = page_types()[canonical].get("required_fields") or []
    missing = [field for field in required if not frontmatter.get(field)]
    if missing:
        raise ValueError(
            f"Frontmatter for page type {canonical!r} missing required fields: {missing}"
        )
    return frontmatter


def write_contract_page(
    target,
    *,
    page_type: str,
    title: str,
    body: str,
    status: str = "active",
    created: str,
    last_updated: str,
    aliases: list[str] | None = None,
    tags: Iterable[str] | None = None,
    domains: list[str] | None = None,
    relates_to: Iterable[DurableLinkTarget] | None = None,
    sources: Iterable[DurableLinkTarget] | None = None,
    extra_frontmatter: dict[str, Any] | None = None,
    force: bool = False,
):
    """Write a page through the shared contract-aware write boundary."""

    frontmatter = build_frontmatter(
        page_type=page_type,
        page_id=target.stem,
        title=title,
        status=status,
        created=created,
        last_updated=last_updated,
        aliases=aliases,
        tags=tags,
        domains=domains,
        relates_to=relates_to,
        sources=sources,
        extra_frontmatter=extra_frontmatter,
    )
    write_page(target, frontmatter=frontmatter, body=body, force=force)
    return target
