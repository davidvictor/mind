from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import re
from typing import Iterable

from scripts.common.default_tags import default_tags
from scripts.common.frontmatter import split_frontmatter, today_str
from scripts.common.slugify import slugify
from scripts.common.vault import SYSTEM_SKIP_NAMES, Vault, project_root
from scripts.common.wiki_writer import write_page
from mind.services.graph_registry import GraphRegistry
TOKEN_RE = re.compile(r"[a-z0-9]+")
FALLBACK_TAGS = {
    "profile": ["domain/identity", "function/identity", "signal/canon"],
    "project": ["domain/work", "function/note", "signal/working"],
}


@dataclass(frozen=True)
class PageMatch:
    path: Path
    title: str
    score: float
    snippet: str
    annotations: list[str] = field(default_factory=list)

    @property
    def page_id(self) -> str:
        return self.path.stem


def vault() -> Vault:
    return Vault.load(project_root())


def now_heading() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M")


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def extract_title(path: Path) -> str:
    frontmatter, body = split_frontmatter(_read_text(path))
    if frontmatter.get("title"):
        return frontmatter["title"]
    for line in body.splitlines():
        stripped = line.strip()
        if stripped.startswith("# "):
            return stripped[2:].strip()
    return path.stem.replace("-", " ").title()


def extract_body_snippet(text: str, *, max_chars: int = 280) -> str:
    _frontmatter, body = split_frontmatter(text)
    paragraphs = [
        line.strip()
        for line in body.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    snippet = " ".join(paragraphs[:3]).strip()
    return snippet[:max_chars]


def wiki_pages(v: Vault) -> list[Path]:
    paths: list[Path] = []
    for path in v.wiki.rglob("*.md"):
        if path.name in SYSTEM_SKIP_NAMES:
            continue
        rel = path.relative_to(v.wiki)
        if rel.parts and rel.parts[0] in {"templates", ".archive"}:
            continue
        paths.append(path)
    return paths


def score_pages(question: str, v: Vault, *, limit: int = 8) -> list[PageMatch]:
    try:
        registry = GraphRegistry.for_repo_root(v.root)
        matches = registry.query_pages(question, limit=limit)
        if matches:
            return [
                PageMatch(
                    path=v.resolve_logical_path(match.path),
                    title=match.title,
                    score=match.score,
                    snippet=match.snippet,
                    annotations=match.annotations,
                )
                for match in matches
            ]
    except Exception:
        pass
    tokens = [tok for tok in TOKEN_RE.findall(question.lower()) if len(tok) > 2]
    matches: list[PageMatch] = []
    for path in wiki_pages(v):
        text = _read_text(path)
        haystack = f"{path.stem} {extract_title(path)} {text}".lower()
        score = 0
        for token in tokens:
            if token in haystack:
                score += haystack.count(token)
        if score == 0:
            continue
        matches.append(
            PageMatch(
                path=path,
                title=extract_title(path),
                score=score,
                snippet=extract_body_snippet(text),
            )
        )
    matches.sort(key=lambda item: (-item.score, item.path.as_posix()))
    return matches[:limit]


def append_changelog(v: Vault, title: str, lines: Iterable[str]) -> None:
    if not v.changelog.exists():
        v.changelog.write_text("# CHANGELOG\n", encoding="utf-8")
    with v.changelog.open("a", encoding="utf-8") as handle:
        handle.write(f"\n## {now_heading()} — {title}\n")
        for line in lines:
            handle.write(f"{line}\n")


def ensure_index_entries(v: Vault, entries: Iterable[str]) -> None:
    existing = v.index.read_text(encoding="utf-8") if v.index.exists() else "# INDEX\n"
    lines = existing.splitlines()
    present = set(lines)
    additions = [f"- [[{entry}]]" for entry in entries if f"- [[{entry}]]" not in present]
    if not additions:
        return
    if not lines:
        lines = ["# INDEX"]
    if lines[-1] != "":
        lines.append("")
    lines.extend(additions)
    v.index.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def write_note_page(
    target: Path,
    *,
    page_type: str,
    title: str,
    body: str,
    domains: list[str],
    relates_to: list[str] | None = None,
    sources: list[str] | None = None,
    tags_extra: list[str] | None = None,
    extra_frontmatter: dict[str, object] | None = None,
    force: bool = False,
) -> Path:
    today = today_str()
    tags = FALLBACK_TAGS[page_type] if page_type in FALLBACK_TAGS else default_tags(page_type)
    frontmatter = {
        "id": target.stem,
        "type": page_type,
        "title": title,
        "status": "active",
        "created": today,
        "last_updated": today,
        "aliases": [],
        "tags": tags + (tags_extra or []),
        "domains": domains,
        "relates_to": relates_to or [],
        "sources": sources or [],
    }
    if extra_frontmatter:
        frontmatter.update(extra_frontmatter)
    write_page(target, frontmatter=frontmatter, body=body, force=force)
    return target


def ingest_lane_hint(path: Path) -> str:
    parent = path.parent.name.lower()
    if parent in {"web", "exports", "drops"}:
        return parent.rstrip("s")
    suffix = path.suffix.lower().lstrip(".")
    return suffix or "file"


def source_page_id(path: Path) -> str:
    return f"summary-{slugify(path.stem)}"
