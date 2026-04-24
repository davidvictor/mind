"""Walk a wiki and count tag usage per controlled-vocab axis."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Iterator

from scripts.common.contract import tag_taxonomy
from scripts.common.default_tags import is_valid_topic_tag


class TagAxis(str, Enum):
    DOMAIN = "domain"
    FUNCTION = "function"
    SIGNAL = "signal"


_TAXONOMY = tag_taxonomy()
DOMAIN_VOCAB: set[str] = set(_TAXONOMY.get("domain") or [])
FUNCTION_VOCAB: set[str] = set(_TAXONOMY.get("function") or [])
SIGNAL_VOCAB: set[str] = set(_TAXONOMY.get("signal") or [])
VOCABS: dict[TagAxis, set[str]] = {
    TagAxis.DOMAIN: DOMAIN_VOCAB,
    TagAxis.FUNCTION: FUNCTION_VOCAB,
    TagAxis.SIGNAL: SIGNAL_VOCAB,
}


@dataclass
class AuditReport:
    usage: dict[TagAxis, dict[str, int]] = field(
        default_factory=lambda: {
            TagAxis.DOMAIN: defaultdict(int),
            TagAxis.FUNCTION: defaultdict(int),
            TagAxis.SIGNAL: defaultdict(int),
        }
    )
    unused: dict[TagAxis, set[str]] = field(
        default_factory=lambda: {
            TagAxis.DOMAIN: set(),
            TagAxis.FUNCTION: set(),
            TagAxis.SIGNAL: set(),
        }
    )
    topic_tags: set[str] = field(default_factory=set)
    pages_scanned: int = 0


def _iter_pages(root: Path) -> Iterator[Path]:
    for path in root.rglob("*.md"):
        parts = path.parts
        if "templates" in parts or "inbox" in parts or ".archive" in parts:
            continue
        if path.name in ("INDEX.md", "CHANGELOG.md"):
            continue
        if path.name.startswith(".lint-report"):
            continue
        yield path


def _extract_frontmatter_tags(text: str) -> list[str]:
    if not text.startswith("---\n"):
        return []
    end = text.find("\n---\n", 4)
    if end == -1:
        return []
    lines = text[4:end].splitlines()
    tags: list[str] = []
    in_tags = False
    for line in lines:
        stripped = line.strip()
        if stripped == "tags:":
            in_tags = True
            continue
        if in_tags:
            if stripped.startswith("- "):
                tags.append(stripped[2:].strip().strip('"').strip("'"))
                continue
            if line and not line.startswith(" "):
                break
    return tags


def audit(root: Path) -> AuditReport:
    report = AuditReport()
    for page in _iter_pages(root):
        report.pages_scanned += 1
        tags = _extract_frontmatter_tags(page.read_text(encoding="utf-8"))
        for tag in tags:
            lowered = tag.lower()
            if lowered.startswith("domain/"):
                value = lowered.split("/", 1)[1]
                if value in DOMAIN_VOCAB:
                    report.usage[TagAxis.DOMAIN][value] += 1
                continue
            if lowered.startswith("function/"):
                value = lowered.split("/", 1)[1]
                if value in FUNCTION_VOCAB:
                    report.usage[TagAxis.FUNCTION][value] += 1
                continue
            if lowered.startswith("signal/"):
                value = lowered.split("/", 1)[1]
                if value in SIGNAL_VOCAB:
                    report.usage[TagAxis.SIGNAL][value] += 1
                continue
            if is_valid_topic_tag(lowered):
                report.topic_tags.add(lowered)

    for axis, vocab in VOCABS.items():
        used = set(report.usage[axis].keys())
        report.unused[axis] = vocab - used
    return report
