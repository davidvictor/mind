from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
import json
import re
from typing import Any, Iterable

import yaml

from scripts.common.default_tags import default_domains, default_tags, normalize_topic_tags
from scripts.common.slugify import slugify
from scripts.common.vault import SYSTEM_SKIP_NAMES, Vault
from scripts.common.wikilinks import NESTED_WIKILINK_RE, WIKILINK_RE
from scripts.common.wiki_writer import write_page


VALID_SCOPES = ("templates", "tags", "links", "stubs")


@dataclass
class RepairItem:
    scope: str
    path: str
    message: str


@dataclass
class UnresolvedLink:
    path: str
    target: str
    replacement: str


@dataclass
class GraphRepairReport:
    applied: bool
    scopes: list[str]
    generated_at: str
    templates_moved: int = 0
    pages_rewritten: int = 0
    tags_rewritten: int = 0
    links_rewritten: int = 0
    links_downgraded: int = 0
    stubs_rewritten: int = 0
    items: list[RepairItem] = field(default_factory=list)
    unresolved_links: list[UnresolvedLink] = field(default_factory=list)
    report_path: str = ""

    def render(self) -> str:
        lines = [
            "Repair graph summary",
            f"Mode: {'apply' if self.applied else 'dry-run'}",
            f"Scopes: {', '.join(self.scopes)}",
            f"Templates moved: {self.templates_moved}",
            f"Pages rewritten: {self.pages_rewritten}",
            f"Tags rewritten: {self.tags_rewritten}",
            f"Links rewritten: {self.links_rewritten}",
            f"Links downgraded: {self.links_downgraded}",
            f"Stub domains rewritten: {self.stubs_rewritten}",
            f"Report: {self.report_path}",
        ]
        if self.unresolved_links:
            lines.append(f"Unresolved links: {len(self.unresolved_links)}")
        return "\n".join(lines)


def _report_path(repo_root: Path) -> Path:
    return Vault.load(repo_root).reports_root / "repair-graph-report.json"


def _content_pages(vault: Vault) -> list[Path]:
    pages: list[Path] = []
    for path in sorted(vault.wiki.rglob("*.md")):
        rel = path.relative_to(vault.wiki)
        if rel.parts and rel.parts[0] in {".archive"}:
            continue
        if path.name in SYSTEM_SKIP_NAMES:
            continue
        pages.append(path)
    return pages


def _frontmatter_block(text: str) -> tuple[str | None, str]:
    if not text.startswith("---\n"):
        return None, text
    end = text.find("\n---\n", 4)
    if end == -1:
        return None, text
    return text[4:end], text[end + 5 :]


def _parse_page(path: Path) -> tuple[dict[str, Any], str]:
    text = path.read_text(encoding="utf-8")
    block, body = _frontmatter_block(text)
    if block is None:
        return {}, text
    return yaml.safe_load(block) or {}, body


def _write_page(path: Path, frontmatter: dict[str, Any], body: str) -> None:
    write_page(path, frontmatter=frontmatter, body=body, force=True)


def _normalize_lookup_key(value: str) -> str:
    return slugify(value.strip().replace("_", "-"), max_len=120)


def _valid_target_map(paths: Iterable[Path]) -> dict[str, str | None]:
    normalized: dict[str, set[str]] = {}
    for path in paths:
        frontmatter, _body = _parse_page(path)
        page_id = str(frontmatter.get("id") or path.stem).strip()
        if not page_id:
            continue
        for key in {page_id, path.stem}:
            norm = _normalize_lookup_key(key)
            if not norm:
                continue
            normalized.setdefault(norm, set()).add(page_id)
    return {
        key: next(iter(values)) if len(values) == 1 else None
        for key, values in normalized.items()
    }


def _extract_target_and_rest(inner: str) -> tuple[str, str]:
    for index, char in enumerate(inner):
        if char in {"|", "#"}:
            return inner[:index], inner[index:]
    return inner, ""


def _downgrade_text(inner: str) -> str:
    target, rest = _extract_target_and_rest(inner)
    if "|" in rest:
        return rest.split("|", 1)[1] or target
    return target


def _repair_wikilinks(
    text: str,
    *,
    target_map: dict[str, str | None],
    rel_path: str,
    report: GraphRepairReport,
) -> tuple[str, bool]:
    changed = False
    while True:
        updated, count = NESTED_WIKILINK_RE.subn(r"[[\1]]", text)
        if count == 0:
            break
        text = updated
        report.links_rewritten += count
        report.items.append(RepairItem(scope="links", path=rel_path, message=f"collapsed {count} nested wikilinks"))
        changed = True

    def _replace(match: re.Match[str]) -> str:
        nonlocal changed
        inner = match.group(1)
        target, rest = _extract_target_and_rest(inner)
        canonical = target_map.get(_normalize_lookup_key(target))
        if canonical is None:
            replacement = _downgrade_text(inner)
            if replacement != match.group(0):
                report.links_downgraded += 1
                report.unresolved_links.append(
                    UnresolvedLink(path=rel_path, target=target, replacement=replacement)
                )
                changed = True
            return replacement
        resolved = f"[[{canonical}{rest}]]"
        if resolved != match.group(0):
            report.links_rewritten += 1
            report.items.append(RepairItem(scope="links", path=rel_path, message=f"rewrote [[{target}]] -> [[{canonical}]]"))
            changed = True
        return resolved

    return WIKILINK_RE.sub(_replace, text), changed


def _repair_tags(frontmatter: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    if not frontmatter:
        return frontmatter, False
    page_type = str(frontmatter.get("type") or "").strip()
    if not page_type:
        return frontmatter, False
    combined_topics: list[str] = []
    for raw in frontmatter.get("tags") or []:
        if isinstance(raw, str):
            combined_topics.append(raw)
    for raw in frontmatter.get("topics") or []:
        if isinstance(raw, str):
            combined_topics.append(raw)
    new_tags = list(default_tags(page_type)) + normalize_topic_tags(combined_topics)
    if list(frontmatter.get("tags") or []) == new_tags:
        return frontmatter, False
    updated = dict(frontmatter)
    updated["tags"] = new_tags
    return updated, True


def _repair_stub_domains(frontmatter: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    if not frontmatter:
        return frontmatter, False
    page_type = str(frontmatter.get("type") or "").strip()
    if page_type not in {"person", "company", "channel", "tool"}:
        return frontmatter, False
    new_domains = default_domains(page_type)
    if list(frontmatter.get("domains") or []) == new_domains:
        return frontmatter, False
    updated = dict(frontmatter)
    updated["domains"] = new_domains
    return updated, True


def _move_templates(vault: Vault, *, apply: bool, report: GraphRepairReport) -> None:
    source_dir = vault.wiki / "templates"
    if not source_dir.exists():
        return
    target_dir = vault.root / "templates"
    for template in sorted(source_dir.glob("*.md")):
        target = target_dir / template.name
        report.templates_moved += 1
        report.items.append(
            RepairItem(
                scope="templates",
                path=str(template.relative_to(vault.root)),
                message=f"move to {target.relative_to(vault.root)}",
            )
        )
        if not apply:
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(template.read_text(encoding="utf-8"), encoding="utf-8")
        template.unlink()
    if apply and source_dir.exists() and not any(source_dir.iterdir()):
        source_dir.rmdir()


def run_graph_repair(
    repo_root: Path,
    *,
    apply: bool,
    scopes: Iterable[str] | None = None,
) -> GraphRepairReport:
    chosen_scopes = list(scopes or VALID_SCOPES)
    invalid = sorted(set(chosen_scopes) - set(VALID_SCOPES))
    if invalid:
        raise ValueError(f"unsupported repair scopes: {invalid}")

    vault = Vault.load(repo_root)
    report = GraphRepairReport(
        applied=apply,
        scopes=chosen_scopes,
        generated_at=datetime.now(timezone.utc).isoformat(),
    )

    if "templates" in chosen_scopes:
        _move_templates(vault, apply=apply, report=report)

    pages = _content_pages(vault)
    target_map = _valid_target_map(path for path in pages if "templates" not in path.parts)
    for path in pages:
        rel_path = str(path.relative_to(vault.root))
        original_text = path.read_text(encoding="utf-8")
        text = original_text
        page_changed = False

        if "links" in chosen_scopes:
            text, links_changed = _repair_wikilinks(text, target_map=target_map, rel_path=rel_path, report=report)
            page_changed = page_changed or links_changed

        frontmatter, body = _parse_page(path) if text == original_text else _parse_page_from_text(text)
        if not frontmatter:
            continue

        if "tags" in chosen_scopes:
            frontmatter, tags_changed = _repair_tags(frontmatter)
            if tags_changed:
                report.tags_rewritten += 1
                report.items.append(RepairItem(scope="tags", path=rel_path, message="normalized tag list"))
                page_changed = True

        if "stubs" in chosen_scopes:
            frontmatter, stubs_changed = _repair_stub_domains(frontmatter)
            if stubs_changed:
                report.stubs_rewritten += 1
                report.items.append(RepairItem(scope="stubs", path=rel_path, message="normalized stub domains"))
                page_changed = True

        if page_changed:
            report.pages_rewritten += 1
            if apply:
                _write_page(path, frontmatter, body)

    report_path = _report_path(repo_root)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report.report_path = str(report_path)
    payload = asdict(report)
    payload["items"] = [asdict(item) for item in report.items]
    payload["unresolved_links"] = [asdict(item) for item in report.unresolved_links]
    report_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return report


def _parse_page_from_text(text: str) -> tuple[dict[str, Any], str]:
    block, body = _frontmatter_block(text)
    if block is None:
        return {}, text
    return yaml.safe_load(block) or {}, body
