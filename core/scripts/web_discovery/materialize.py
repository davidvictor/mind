from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mind.services.durable_write import DurableLinkTarget, write_contract_page
from mind.services.materialization import MaterializationCandidate, materialize_primary_target
from scripts.common.frontmatter import split_frontmatter as _split_frontmatter
from scripts.common.vault import wiki_path
from scripts.web_discovery.contracts import WebCandidate, WebDiscoveryRecord


def page_path(repo_root: Path, record: WebDiscoveryRecord) -> Path:
    return wiki_path(repo_root, "sources", "web-discovery", f"{record.page_id}.md")


def load_existing_record(repo_root: Path, record: WebDiscoveryRecord) -> dict[str, Any]:
    target = page_path(repo_root, record)
    if not target.exists():
        return {}
    frontmatter, _body = _split_frontmatter(target.read_text(encoding="utf-8"))
    return frontmatter


def merge_candidate(
    existing: dict[str, Any],
    candidate: WebCandidate,
    *,
    crawl_markdown: str = "",
) -> WebDiscoveryRecord:
    key = existing.get("web_discovery_key") or record_key(candidate)
    title = candidate.title or str(existing.get("title") or candidate.canonical_url)
    summary = crawl_markdown.strip()[:1200] if crawl_markdown.strip() else str(existing.get("summary") or candidate.triage.reason)
    why_it_matters = candidate.triage.why_it_matters or str(existing.get("why_it_matters") or "")
    topics = sorted(set(list(existing.get("topics") or []) + list(candidate.triage.topics or [])))
    bookmark_signals = sorted(
        {
            *list(existing.get("bookmark_folder_signals") or []),
            *[edge.bookmark_folder_path for edge in candidate.evidence_edges if edge.bookmark_folder_path],
        }
    )
    query_refs = sorted({*list(existing.get("query_refs") or []), *[edge.query_id for edge in candidate.evidence_edges if edge.query_id]})
    source_channels = sorted({*list(existing.get("source_channels") or []), *[edge.edge_type for edge in candidate.evidence_edges]})
    existing_event_ids = set(existing.get("evidence_event_ids") or [])
    new_event_ids = {edge.event_id for edge in candidate.evidence_edges if edge.event_id}
    merged_event_ids = sorted(existing_event_ids | new_event_ids)
    existing_history_ids = set(existing.get("history_event_ids") or [])
    new_history_ids = {edge.event_id for edge in candidate.evidence_edges if edge.edge_type == "history" and edge.event_id}
    merged_history_ids = sorted(existing_history_ids | new_history_ids)
    first_seen_candidates = [str(existing.get("first_seen_at") or "")] + [edge.occurred_at for edge in candidate.evidence_edges if edge.occurred_at]
    first_seen = min(value for value in first_seen_candidates if value) if any(first_seen_candidates) else ""
    last_seen_candidates = [str(existing.get("last_seen_at") or "")] + [edge.occurred_at for edge in candidate.evidence_edges if edge.occurred_at]
    last_seen = max(value for value in last_seen_candidates if value) if any(last_seen_candidates) else ""
    crawl_status = "crawled" if crawl_markdown.strip() else str(existing.get("crawl_status") or "not_crawled")
    entity_refs = list(existing.get("entity_refs") or [])
    return WebDiscoveryRecord(
        web_discovery_key=key,
        canonical_url=candidate.canonical_url,
        title=title,
        object_type=candidate.triage.object_type or str(existing.get("object_type") or "other"),
        summary=summary,
        why_it_matters=why_it_matters,
        topics=topics,
        entity_refs=entity_refs,
        source_channels=source_channels,
        evidence_edge_count=len(merged_event_ids),
        evidence_event_ids=merged_event_ids,
        bookmark_folder_signals=bookmark_signals,
        query_refs=query_refs,
        visit_count_total=len(merged_history_ids),
        history_event_ids=merged_history_ids,
        first_seen_at=first_seen,
        last_seen_at=last_seen,
        crawl_status=crawl_status,
        last_crawled_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if crawl_markdown.strip() else str(existing.get("last_crawled_at") or ""),
        merge_version=1,
    )


def record_key(candidate: WebCandidate) -> str:
    return candidate.candidate_id or candidate.canonical_url


def _propagate_entity(repo_root: Path, record: WebDiscoveryRecord) -> list[str]:
    mapping = {
        "tool": "tool",
        "company": "company",
    }
    page_type = mapping.get(record.object_type)
    if not page_type:
        return []
    candidate = MaterializationCandidate(
        page_type=page_type,  # type: ignore[arg-type]
        name=record.title,
        role="tool" if page_type == "tool" else "creator",
        confidence=0.95,
        deterministic=True,
        source="web-discovery",
        central_subject=True,
    )
    target = materialize_primary_target(
        candidate,
        repo_root=repo_root,
        source_link=DurableLinkTarget(page_type="web-discovery", page_id=record.page_id),
    )
    if target is None:
        return []
    return [target.stem]


def write_web_discovery_page(repo_root: Path, record: WebDiscoveryRecord) -> Path:
    target = page_path(repo_root, record)
    entity_refs = _propagate_entity(repo_root, record) if record.crawl_status == "crawled" else list(record.entity_refs)
    body_lines = [
        f"# {record.title}\n",
        f"**URL:** [{record.canonical_url}]({record.canonical_url})  ",
        f"**Object Type:** {record.object_type}  ",
        f"**Crawl Status:** {record.crawl_status}\n",
        "## Summary\n",
        (record.summary or "Signal-only discovery retained from Chrome activity.") + "\n",
        "## Why It Matters\n",
        (record.why_it_matters or "Retained as a potentially meaningful web discovery.") + "\n",
    ]
    if record.topics:
        body_lines.append("## Topics\n")
        body_lines.extend(f"- {topic}" for topic in record.topics)
        body_lines.append("")
    if record.bookmark_folder_signals or record.query_refs:
        body_lines.append("## Evidence\n")
        for folder in record.bookmark_folder_signals:
            body_lines.append(f"- Bookmark folder: {folder}")
        for query_ref in record.query_refs:
            body_lines.append(f"- Search signal: {query_ref}")
        body_lines.append("")
    write_contract_page(
        target,
        page_type="web-discovery",
        title=record.title,
        body="\n".join(body_lines),
        created=(record.first_seen_at[:10] if record.first_seen_at else datetime.now(timezone.utc).strftime("%Y-%m-%d")),
        last_updated=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        tags=list(record.topics),
        domains=["learning"],
        extra_frontmatter={
            "canonical_url": record.canonical_url,
            "web_discovery_key": record.web_discovery_key,
            "object_type": record.object_type,
            "summary": record.summary,
            "why_it_matters": record.why_it_matters,
            "topics": record.topics,
            "entity_refs": entity_refs or record.entity_refs,
            "source_channels": record.source_channels,
            "evidence_edge_count": record.evidence_edge_count,
            "evidence_event_ids": record.evidence_event_ids,
            "bookmark_folder_signals": record.bookmark_folder_signals,
            "query_refs": record.query_refs,
            "visit_count_total": record.visit_count_total,
            "history_event_ids": record.history_event_ids,
            "first_seen_at": record.first_seen_at,
            "last_seen_at": record.last_seen_at,
            "crawl_status": record.crawl_status,
            "last_crawled_at": record.last_crawled_at,
            "merge_version": record.merge_version,
        },
        force=True,
    )
    return target
