"""Parse Plan A's articles drop queue (JSONL) into ArticleDropEntry records.

The drop queue is written by scripts.substack.write_pages.append_links_to_drop_queue
and lives at raw/drops/articles-from-substack-YYYY-MM-DD.jsonl.

Dedupe semantics: when the same URL appears multiple times across the same
drop file (e.g. multiple Substack posts linked to the same external article),
the first occurrence wins — later entries inherit nothing. The original
source_post_id is preserved as the canonical "where we first saw this".
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator


@dataclass(frozen=True)
class ArticleDropEntry:
    url: str
    source_post_id: str
    source_post_url: str
    anchor_text: str
    context_snippet: str
    category: str           # "business" or "personal" — Plan A filters out "ignore"
    discovered_at: str      # ISO-8601
    source_type: str        # "substack-link" in v1
    source_page_id: str = ""
    source_label: str = ""


def parse_drop_file(path: Path) -> Iterator[ArticleDropEntry]:
    """Yield ArticleDropEntry per line, deduped by URL (first wins).

    Skips blank lines and malformed JSON lines silently.
    """
    seen_urls: set[str] = set()
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue
        url = data.get("url", "")
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        yield ArticleDropEntry(
            url=url,
            source_post_id=str(data.get("source_post_id", "")),
            source_post_url=data.get("source_post_url", ""),
            source_page_id=str(data.get("source_page_id", "")),
            anchor_text=data.get("anchor_text", ""),
            context_snippet=data.get("context_snippet", ""),
            category=data.get("category", "personal"),
            discovered_at=data.get("discovered_at", ""),
            source_type=data.get("source_type", "substack-link"),
            source_label=data.get("source_label", ""),
        )
