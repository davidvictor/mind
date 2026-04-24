from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from scripts.chrome.contracts import ChromeEvent, canonicalize_url, stable_hash


def _normalize_query(text: str) -> str:
    return " ".join((text or "").strip().lower().split())


def _intent_for_query(query_text: str) -> str:
    lowered = _normalize_query(query_text)
    if any(token in lowered for token in ("buy ", "pricing", "price", "vs ", "compare")):
        return "shopping"
    if any(token in lowered for token in ("tool", "github", "repo", "software", "mcp")):
        return "tool-discovery"
    return "research"


def _topics_for_query(query_text: str) -> list[str]:
    lowered = _normalize_query(query_text)
    words = [part for part in lowered.split() if len(part) > 2]
    seen: list[str] = []
    for word in words:
        if word not in seen:
            seen.append(word)
    return seen[:6]


@dataclass(frozen=True)
class SearchSignal:
    query_id: str
    query_text: str
    chrome_profile: str
    engine_domain: str
    searched_at: str
    clicked_canonical_urls: list[str] = field(default_factory=list)
    topics: list[str] = field(default_factory=list)
    intent: str = "research"
    privacy_class: str = "retained"

    def to_dict(self) -> dict[str, Any]:
        return {
            "query_id": self.query_id,
            "query_text": self.query_text,
            "chrome_profile": self.chrome_profile,
            "engine_domain": self.engine_domain,
            "searched_at": self.searched_at,
            "clicked_canonical_urls": self.clicked_canonical_urls,
            "topics": self.topics,
            "intent": self.intent,
            "privacy_class": self.privacy_class,
        }


def build_search_signals(events: list[ChromeEvent]) -> list[SearchSignal]:
    clicks_by_key: dict[tuple[str, str, str, str], set[str]] = defaultdict(set)
    for event in events:
        if event.event_type != "query_click" or not event.query_text or not event.url:
            continue
        key = (
            event.chrome_profile,
            event.occurred_at,
            _normalize_query(event.query_text),
            (event.engine_domain or "").lower(),
        )
        canonical = canonicalize_url(event.url)
        if canonical:
            clicks_by_key[key].add(canonical)

    signals: list[SearchSignal] = []
    for event in events:
        if event.event_type != "search_query" or not event.query_text:
            continue
        normalized = _normalize_query(event.query_text)
        key = (
            event.chrome_profile,
            event.occurred_at,
            normalized,
            (event.engine_domain or "").lower(),
        )
        query_id = stable_hash(
            event.chrome_profile,
            event.occurred_at[:13] if event.occurred_at else "",
            normalized,
            (event.engine_domain or "").lower(),
        )
        signals.append(
            SearchSignal(
                query_id=query_id,
                query_text=event.query_text,
                chrome_profile=event.chrome_profile,
                engine_domain=event.engine_domain,
                searched_at=event.occurred_at,
                clicked_canonical_urls=sorted(clicks_by_key.get(key, set())),
                topics=_topics_for_query(event.query_text),
                intent=_intent_for_query(event.query_text),
            )
        )
    return signals
