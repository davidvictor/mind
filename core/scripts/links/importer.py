"""Zero-auth links import path.

Normalizes user-provided JSON bookmarks/links into the same drop-queue style
that the articles pipeline can drain.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from scripts.common.vault import raw_path


@dataclass(frozen=True)
class LinkRecord:
    url: str
    title: str = ""
    description: str = ""
    image_url: str = ""
    saved_at: str = ""
    source: str = "links-import"
    source_app: str = "links"
    folder: str = ""
    tags: list[str] | None = None
    notes: str = ""


def _walk_payload(node: Any, *, folder: str = "") -> Iterable[LinkRecord]:
    if isinstance(node, list):
        for item in node:
            yield from _walk_payload(item, folder=folder)
        return

    if not isinstance(node, dict):
        return

    next_folder = folder
    name = node.get("name") or node.get("folder") or ""
    if name and not node.get("url"):
        next_folder = f"{folder}/{name}".strip("/")

    if node.get("url"):
        yield LinkRecord(
            url=str(node.get("url", "")),
            title=str(node.get("title") or node.get("name") or ""),
            description=str(node.get("description") or ""),
            image_url=str(node.get("image_url") or node.get("imageUrl") or node.get("image") or ""),
            saved_at=str(node.get("saved_at") or node.get("date_added") or ""),
            source=str(node.get("source") or "links-import"),
            source_app=str(node.get("source_app") or node.get("sourceApp") or "links"),
            folder=next_folder,
            tags=list(node.get("tags") or []),
            notes=str(node.get("notes") or node.get("note") or ""),
        )

    children = node.get("children")
    if isinstance(children, list):
        for child in children:
            yield from _walk_payload(child, folder=next_folder)

    bookmarks = node.get("bookmarks")
    if bookmarks is not None:
        yield from _walk_payload(bookmarks, folder=next_folder)


def load_links(path: Path) -> list[LinkRecord]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return [record for record in _walk_payload(payload) if record.url]


def append_links_drop(
    repo_root: Path,
    *,
    links: list[LinkRecord],
    today_str: str,
) -> Path:
    drop_dir = raw_path(repo_root, "drops")
    drop_dir.mkdir(parents=True, exist_ok=True)
    target = drop_dir / f"articles-from-links-{today_str}.jsonl"

    existing: set[tuple[str, str]] = set()
    if target.exists():
        for line in target.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue
            existing.add((str(data.get("source_type", "")), str(data.get("url", ""))))

    with target.open("a", encoding="utf-8") as fh:
        for item in links:
            key = ("links-import", item.url)
            if key in existing:
                continue
            existing.add(key)
            fh.write(json.dumps({
                "url": item.url,
                "source_post_id": "links-import",
                "source_post_url": "",
                "anchor_text": item.title or item.url,
                "context_snippet": item.description or item.notes,
                "category": "personal",
                "discovered_at": item.saved_at or f"{today_str}T00:00:00Z",
                "source_type": "links-import",
                "source_label": item.source_app or item.source or "links",
                "folder": item.folder,
                "image_url": item.image_url,
                "notes": item.notes,
            }, ensure_ascii=False) + "\n")
    return target
