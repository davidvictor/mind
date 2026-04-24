from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from scripts.chrome.contracts import canonicalize_url, discovery_key_for_url


@dataclass(frozen=True)
class DiscoveryEventEdge:
    edge_type: str
    event_id: str
    occurred_at: str
    bookmark_folder_path: str = ""
    query_id: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "edge_type": self.edge_type,
            "event_id": self.event_id,
            "occurred_at": self.occurred_at,
            "bookmark_folder_path": self.bookmark_folder_path,
            "query_id": self.query_id,
        }


@dataclass(frozen=True)
class TriageResult:
    decision: str
    confidence: float
    reason: str
    object_type: str
    topics: list[str] = field(default_factory=list)
    why_it_matters: str = ""
    cost_sensitivity: str = "medium"

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision": self.decision,
            "confidence": self.confidence,
            "reason": self.reason,
            "object_type": self.object_type,
            "topics": self.topics,
            "why_it_matters": self.why_it_matters,
            "cost_sensitivity": self.cost_sensitivity,
        }


@dataclass(frozen=True)
class WebCandidate:
    candidate_id: str
    canonical_url: str
    url: str
    title: str
    domain: str
    evidence_edges: list[DiscoveryEventEdge]
    triage: TriageResult
    crawl: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "canonical_url": self.canonical_url,
            "url": self.url,
            "title": self.title,
            "domain": self.domain,
            "evidence_edges": [edge.to_dict() for edge in self.evidence_edges],
            "triage": self.triage.to_dict(),
            "crawl": self.crawl,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "WebCandidate":
        return cls(
            candidate_id=str(data["candidate_id"]),
            canonical_url=str(data["canonical_url"]),
            url=str(data["url"]),
            title=str(data.get("title") or ""),
            domain=str(data.get("domain") or ""),
            evidence_edges=[DiscoveryEventEdge(**edge) for edge in list(data.get("evidence_edges") or [])],
            triage=TriageResult(**dict(data.get("triage") or {})),
            crawl=dict(data.get("crawl") or {}),
        )


@dataclass(frozen=True)
class WebDiscoveryRecord:
    web_discovery_key: str
    canonical_url: str
    title: str
    object_type: str
    summary: str
    why_it_matters: str
    topics: list[str] = field(default_factory=list)
    entity_refs: list[str] = field(default_factory=list)
    source_channels: list[str] = field(default_factory=list)
    evidence_edge_count: int = 0
    evidence_event_ids: list[str] = field(default_factory=list)
    bookmark_folder_signals: list[str] = field(default_factory=list)
    query_refs: list[str] = field(default_factory=list)
    visit_count_total: int = 0
    history_event_ids: list[str] = field(default_factory=list)
    first_seen_at: str = ""
    last_seen_at: str = ""
    crawl_status: str = "not_crawled"
    last_crawled_at: str = ""
    merge_version: int = 1

    @property
    def page_id(self) -> str:
        return f"web-discovery-{self.web_discovery_key[:16]}"

    @classmethod
    def empty_for_url(cls, url: str, *, title: str = "") -> "WebDiscoveryRecord":
        canonical = canonicalize_url(url)
        return cls(
            web_discovery_key=discovery_key_for_url(canonical),
            canonical_url=canonical,
            title=title or canonical,
            object_type="other",
            summary="",
            why_it_matters="",
        )
