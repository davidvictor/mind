"""Helpers for queueing extracted links into the article drop queue."""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from scripts.articles.fetch import is_supported_article_url
from scripts.common.anti_sales import is_sales_chrome
from scripts.common.vault import raw_path

_URL_RE = re.compile(r"https?://[^\s<>()\[\]{}\"']+")


def extract_urls_with_context(text: str) -> list[dict[str, str]]:
    """Extract external URLs from plain text with lightweight context."""
    seen: set[str] = set()
    results: list[dict[str, str]] = []
    for match in _URL_RE.finditer(text or ""):
        raw_url = match.group(0).rstrip(".,);:!?")
        if raw_url in seen:
            continue
        seen.add(raw_url)
        start = max(0, match.start() - 120)
        end = min(len(text), match.end() + 120)
        context = text[start:end].replace("\n", " ").strip()
        results.append(
            {
                "url": raw_url,
                "anchor_text": raw_url,
                "context_snippet": context[:220],
            }
        )
    return results


def filter_article_links_for_queue(links: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        link
        for link in links
        if link.get("category") in {"business", "personal"}
        and is_supported_article_url(str(link.get("url") or ""))
        and not is_sales_chrome(
            str(link.get("url") or ""),
            str(link.get("anchor_text") or ""),
            str(link.get("context_snippet") or ""),
        )
    ]


def append_article_links_to_drop_queue(
    *,
    repo_root: Path,
    today: str,
    source_id: str,
    source_url: str,
    source_type: str,
    discovered_at: str,
    links: list[dict[str, Any]],
    source_label: str = "",
) -> Path:
    """Append non-ignored article links to the shared drop queue format."""
    drop_dir = raw_path(repo_root, "drops")
    drop_dir.mkdir(parents=True, exist_ok=True)
    target = drop_dir / f"articles-from-{source_type}-{today}.jsonl"

    keep = filter_article_links_for_queue(links)
    if not keep:
        return target

    existing_keys: set[tuple[str, str]] = set()
    if target.exists():
        for line in target.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            existing_keys.add((str(entry.get("source_post_id") or ""), str(entry.get("url") or "")))

    with target.open("a", encoding="utf-8") as handle:
        for link in keep:
            key = (source_id, str(link["url"]))
            if key in existing_keys:
                continue
            existing_keys.add(key)
            handle.write(json.dumps({
                "url": link["url"],
                "source_post_id": source_id,
                "source_post_url": source_url,
                "anchor_text": link.get("anchor_text", ""),
                "context_snippet": link.get("context_snippet", ""),
                "category": link["category"],
                "discovered_at": discovered_at,
                "source_type": source_type,
                "source_label": source_label,
            }, ensure_ascii=False) + "\n")

    return target
