from __future__ import annotations

from dataclasses import dataclass
from ipaddress import ip_address
from typing import Any
from urllib.parse import urlparse

from mind.services.llm_service import get_llm_service
from scripts.chrome.contracts import ChromeEvent, canonicalize_url
from scripts.web_discovery.contracts import TriageResult, WebCandidate


SEARCH_HOSTS = {"google.com", "bing.com", "duckduckgo.com", "search.brave.com"}
TRACKING_HOSTS = {"l.facebook.com", "t.co"}
PRIVATE_HOSTS = {"localhost", "127.0.0.1", "::1"}
INBOX_HOSTS = {"mail.google.com", "gmail.com", "inbox.google.com"}


@dataclass(frozen=True)
class ExclusionDecision:
    excluded: bool
    reason: str = ""


def is_query_private(query_text: str) -> bool:
    lowered = (query_text or "").lower()
    if not lowered.strip():
        return True
    if any(token in lowered for token in ("password", "token", "secret", "invoice", "order ", "receipt", "@")):
        return True
    if any(token in lowered for token in ("billing", "support", "account", "login")):
        return True
    return False


def should_exclude_url(url: str) -> ExclusionDecision:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    naked_host = host[4:] if host.startswith("www.") else host
    path = parsed.path or "/"
    scheme = (parsed.scheme or "").lower()
    if scheme in {"", "data", "javascript", "file", "mailto"}:
        return ExclusionDecision(True, "unsupported or private scheme")
    if parsed.username or parsed.password:
        return ExclusionDecision(True, "authenticated URL")
    if host in PRIVATE_HOSTS:
        return ExclusionDecision(True, "local URL")
    try:
        if host:
            addr = ip_address(host)
            if addr.is_private or addr.is_loopback:
                return ExclusionDecision(True, "private network URL")
    except ValueError:
        pass
    if host in INBOX_HOSTS or path.startswith("/mail"):
        return ExclusionDecision(True, "inbox or dashboard")
    if host in TRACKING_HOSTS:
        return ExclusionDecision(True, "redirect or tracking host")
    if naked_host in SEARCH_HOSTS and path in {"/search", "/"}:
        return ExclusionDecision(True, "raw search result page")
    if any(segment in path.lower() for segment in ("/checkout", "/cart", "/billing", "/account", "/orders", "/admin", "/dashboard")):
        return ExclusionDecision(True, "transactional or operational page")
    if path.lower().endswith((".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".css", ".js", ".ico", ".pdf")):
        return ExclusionDecision(True, "asset URL")
    return ExclusionDecision(False)


def _fallback_triage(*, title: str, canonical_url: str, evidence_count: int) -> TriageResult:
    lowered = f"{title} {canonical_url}".lower()
    object_type = "other"
    if "github.com" in lowered:
        object_type = "repo"
    elif any(token in lowered for token in ("tool", "mcp", "sdk")):
        object_type = "tool"
    elif any(token in lowered for token in ("docs", "documentation")):
        object_type = "docs"
    elif any(token in lowered for token in ("portfolio", "dribbble", "behance")):
        object_type = "portfolio"
    decision = "signal_only"
    if evidence_count >= 2 and object_type in {"docs", "tool", "repo"}:
        decision = "crawl"
    return TriageResult(
        decision=decision,
        confidence=0.8 if decision == "crawl" else 0.76,
        reason="Fallback heuristic triage",
        object_type=object_type,
        topics=[part for part in lowered.replace("/", " ").split() if len(part) > 3][:5],
        why_it_matters="Repeated research or bookmark signal" if evidence_count >= 2 else "Potential discovery worth retaining",
        cost_sensitivity="medium",
    )


def triage_candidate(candidate: WebCandidate, *, confidence_threshold: float) -> TriageResult:
    prompt = (
        "You are classifying whether a public URL should become durable memory in a private knowledge base.\n"
        "Return strict JSON with keys: decision, confidence, reason, object_type, topics, why_it_matters, cost_sensitivity.\n"
        "Valid decision values: drop, signal_only, crawl.\n"
        "Prefer precision over recall. False positives are worse than false negatives.\n"
        f"URL: {candidate.canonical_url}\n"
        f"Title: {candidate.title}\n"
        f"Evidence edges: {[edge.to_dict() for edge in candidate.evidence_edges]}\n"
    )
    try:
        data = get_llm_service().generate_json_prompt(prompt)
        triage = TriageResult(
            decision=str(data.get("decision") or "drop"),
            confidence=float(data.get("confidence") or 0.0),
            reason=str(data.get("reason") or ""),
            object_type=str(data.get("object_type") or "other"),
            topics=[str(item) for item in list(data.get("topics") or [])][:8],
            why_it_matters=str(data.get("why_it_matters") or ""),
            cost_sensitivity=str(data.get("cost_sensitivity") or "medium"),
        )
    except Exception:
        triage = _fallback_triage(
            title=candidate.title,
            canonical_url=candidate.canonical_url,
            evidence_count=len(candidate.evidence_edges),
        )
    if triage.confidence < confidence_threshold:
        return TriageResult(
            decision="drop",
            confidence=triage.confidence,
            reason=triage.reason or "Confidence below threshold",
            object_type=triage.object_type,
            topics=triage.topics,
            why_it_matters=triage.why_it_matters,
            cost_sensitivity=triage.cost_sensitivity,
        )
    return triage


def build_candidate_seed(event: ChromeEvent, *, query_id: str = "") -> WebCandidate | None:
    exclusion = should_exclude_url(event.url)
    if exclusion.excluded:
        return None
    canonical = canonicalize_url(event.url)
    if not canonical:
        return None
    parsed = urlparse(canonical)
    return WebCandidate(
        candidate_id="",
        canonical_url=canonical,
        url=event.url,
        title=event.title or canonical,
        domain=(parsed.hostname or "").lower(),
        evidence_edges=[],
        triage=TriageResult(
            decision="signal_only",
            confidence=0.0,
            reason="untriaged",
            object_type="other",
        ),
        crawl={"status": "not_attempted", "last_crawled_at": "", "cooldown_until": ""},
    )
