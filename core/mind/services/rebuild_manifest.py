from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

import yaml

from scripts.common.vault import Vault


LANES: tuple[str, ...] = ("books", "youtube", "substack", "articles")


@dataclass(frozen=True)
class RebuildManifestItem:
    lane: str
    source_id: str
    title: str
    relative_path: str
    external_id: str = ""


@dataclass(frozen=True)
class RebuildManifest:
    generated_at: str
    wiki_root: str
    lanes: dict[str, list[RebuildManifestItem]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "wiki_root": self.wiki_root,
            "lanes": {
                lane: [asdict(item) for item in items]
                for lane, items in self.lanes.items()
            },
        }


def _parse_frontmatter(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if not text.startswith("---\n"):
        return {}
    end = text.find("\n---\n", 4)
    if end == -1:
        return {}
    return yaml.safe_load(text[4:end]) or {}


def _source_id_for_page(*, lane: str, path: Path, frontmatter: dict[str, Any]) -> str:
    external_id = str(frontmatter.get("external_id") or "").strip()
    if lane == "youtube":
        if external_id.startswith("youtube-"):
            return external_id
        youtube_id = str(frontmatter.get("youtube_id") or "").strip()
        return f"youtube-{youtube_id or path.stem}"
    if lane == "substack":
        if external_id.startswith("substack-"):
            return external_id
        return f"substack-{path.stem}"
    if lane == "articles":
        return f"article-{path.stem}"
    return f"book-{path.stem}"


def build_rebuild_manifest(repo_root: Path) -> RebuildManifest:
    vault = Vault.load(repo_root)
    lane_items: dict[str, list[RebuildManifestItem]] = {lane: [] for lane in LANES}
    for lane in LANES:
        source_root = vault.wiki / "sources" / lane
        if not source_root.exists():
            continue
        for path in sorted(source_root.rglob("*.md")):
            frontmatter = _parse_frontmatter(path)
            lane_items[lane].append(
                RebuildManifestItem(
                    lane=lane,
                    source_id=_source_id_for_page(lane=lane, path=path, frontmatter=frontmatter),
                    title=str(frontmatter.get("title") or path.stem),
                    relative_path=vault.logical_path(path),
                    external_id=str(frontmatter.get("external_id") or "").strip(),
                )
            )
    return RebuildManifest(
        generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        wiki_root=str(vault.wiki.relative_to(repo_root)),
        lanes=lane_items,
    )


def write_rebuild_manifest(*, repo_root: Path, output_path: Path) -> RebuildManifest:
    manifest = build_rebuild_manifest(repo_root)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(manifest.to_dict(), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return manifest


def load_rebuild_manifest(path: Path, *, lane: str | None = None) -> tuple[str, ...]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    lanes = payload.get("lanes") or {}
    if lane is not None:
        items = lanes.get(lane) or []
        return tuple(str(item.get("source_id") or "").strip() for item in items if str(item.get("source_id") or "").strip())
    source_ids: list[str] = []
    for lane_name in LANES:
        for item in lanes.get(lane_name) or []:
            source_id = str(item.get("source_id") or "").strip()
            if source_id:
                source_ids.append(source_id)
    return tuple(source_ids)
