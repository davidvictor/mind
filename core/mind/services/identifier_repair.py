from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from scripts.atoms import cache as atoms_cache
from scripts.atoms.canonical import ATOM_TYPES, canonicalize_atom_page
from scripts.common.frontmatter import read_page
from scripts.common.slugify import normalize_identifier
from scripts.common.vault import Vault
from scripts.common.wikilinks import WIKILINK_RE
from scripts.common.wiki_writer import write_page

from .graph_registry import GraphRegistry


CANONICAL_ID_DIRS = (
    "projects",
    "companies",
    "people",
    "channels",
    "tools",
    "concepts",
    "playbooks",
    "stances",
    "inquiries",
)


@dataclass(frozen=True)
class IdentifierRepairPlan:
    source_path: str
    target_path: str
    old_id: str
    new_id: str
    merged_into: str | None = None


@dataclass
class IdentifierRepairReport:
    apply: bool
    scanned_pages: int = 0
    renamed_pages: int = 0
    merged_pages: int = 0
    links_rewritten: int = 0
    rebuilt_atom_cache: bool = False
    rebuilt_graph: bool = False
    plans: list[IdentifierRepairPlan] = field(default_factory=list)
    details: list[str] = field(default_factory=list)

    def render(self) -> str:
        lines = [
            f"Identifier repair ({'apply' if self.apply else 'dry-run'})",
            f"Scanned pages: {self.scanned_pages}",
            f"Renamed pages: {self.renamed_pages}",
            f"Merged pages: {self.merged_pages}",
            f"Links rewritten: {self.links_rewritten}",
        ]
        if self.apply:
            lines.append(f"Atom cache rebuilt: {'yes' if self.rebuilt_atom_cache else 'no'}")
            lines.append(f"Graph rebuilt: {'yes' if self.rebuilt_graph else 'no'}")
        if self.plans:
            lines.append("")
            lines.append("Plans:")
            lines.extend(
                f"- {plan.source_path} -> {plan.target_path} ({plan.old_id} -> {plan.new_id})"
                + (f" merged into {plan.merged_into}" if plan.merged_into else "")
                for plan in self.plans[:50]
            )
        return "\n".join(lines)


@dataclass(frozen=True)
class _Page:
    path: Path
    frontmatter: dict[str, Any]
    body: str

    @property
    def page_id(self) -> str:
        return str(self.frontmatter.get("id") or self.path.stem).strip()

    @property
    def normalized_id(self) -> str:
        return normalize_identifier(self.page_id)

    @property
    def page_type(self) -> str:
        return str(self.frontmatter.get("type") or self.path.parent.name.rstrip("s")).strip() or "note"

    @property
    def is_probationary(self) -> bool:
        return "probationary" in self.path.parts


def run_identifier_repair(repo_root: Path, *, apply: bool) -> IdentifierRepairReport:
    vault = Vault.load(repo_root)
    pages = _identifier_pages(vault)
    report = IdentifierRepairReport(apply=apply, scanned_pages=len(pages))
    link_map = _link_map(pages)
    winner_map = _winner_pages(pages)
    all_markdown = _all_markdown_pages(vault)

    for page in pages:
        winner = winner_map[(page.path.parent, page.normalized_id)]
        target_path = _target_path(winner, winner.normalized_id)
        merged_into = (
            vault.logical_path(target_path)
            if winner.path != page.path and page.normalized_id == winner.normalized_id
            else None
        )
        if page.path != target_path or page.page_id != page.normalized_id or merged_into is not None:
            report.plans.append(
                IdentifierRepairPlan(
                    source_path=vault.logical_path(page.path),
                    target_path=vault.logical_path(target_path),
                    old_id=page.page_id,
                    new_id=page.normalized_id,
                    merged_into=merged_into,
                )
            )

    if not apply:
        report.renamed_pages = sum(1 for plan in report.plans if plan.merged_into is None)
        report.merged_pages = sum(1 for plan in report.plans if plan.merged_into is not None)
        rewritten = 0
        for path in all_markdown:
            frontmatter, body = read_page(path)
            if _rewrite_frontmatter(frontmatter, link_map) != frontmatter:
                rewritten += 1
            if _rewrite_body(body, link_map) != body:
                rewritten += 1
        report.links_rewritten = rewritten
        return report

    obsolete_paths: set[Path] = set()
    for key, winner in winner_map.items():
        group = [page for page in pages if (page.path.parent, page.normalized_id) == key]
        target_path = _target_path(winner, winner.normalized_id)
        if len(group) == 1 and winner.path == target_path and winner.page_id == winner.normalized_id:
            continue
        merged_frontmatter, merged_body, merged_old_ids = _merge_group(group=group, winner=winner, link_map=link_map)
        merged_frontmatter["id"] = winner.normalized_id
        merged_frontmatter["type"] = winner.page_type
        merged_frontmatter["aliases"] = _dedupe(
            [
                *_coerce_list(merged_frontmatter.get("aliases")),
            ]
        )
        merged_frontmatter["aliases"] = [alias for alias in merged_frontmatter["aliases"] if alias != winner.normalized_id]
        if winner.page_type in ATOM_TYPES:
            rendered = canonicalize_atom_page(frontmatter=merged_frontmatter, body=merged_body)
            merged_frontmatter = rendered.frontmatter
            merged_body = rendered.body
        write_page(target_path, frontmatter=merged_frontmatter, body=merged_body, force=True)
        if winner.path != target_path:
            obsolete_paths.add(winner.path)
            report.renamed_pages += 1
        for page in group:
            if page.path != winner.path:
                obsolete_paths.add(page.path)
                report.merged_pages += 1

    for path in all_markdown:
        if path in obsolete_paths:
            continue
        frontmatter, body = read_page(path)
        if (path.parent.name in CANONICAL_ID_DIRS) or (path.parent.parent.name in CANONICAL_ID_DIRS):
            normalized_id = normalize_identifier(str(frontmatter.get("id") or path.stem))
            if normalized_id and str(frontmatter.get("id") or "") != normalized_id:
                frontmatter["id"] = normalized_id
        rewritten_frontmatter = _rewrite_frontmatter(frontmatter, link_map)
        rewritten_body = _rewrite_body(body, link_map)
        if rewritten_frontmatter != frontmatter or rewritten_body != body:
            write_page(path, frontmatter=rewritten_frontmatter, body=rewritten_body, force=True)
            report.links_rewritten += 1

    for path in sorted(obsolete_paths, reverse=True):
        if path.exists():
            path.unlink()

    atoms_cache.rebuild(repo_root)
    report.rebuilt_atom_cache = True
    GraphRegistry.for_repo_root(repo_root).rebuild()
    report.rebuilt_graph = True
    return report


def _identifier_pages(vault: Vault) -> list[_Page]:
    pages: list[_Page] = []
    for dirname in CANONICAL_ID_DIRS:
        root = vault.wiki / dirname
        if root.exists():
            for path in sorted(root.glob("*.md")):
                frontmatter, body = read_page(path)
                pages.append(_Page(path=path, frontmatter=frontmatter, body=body))
        probationary_root = vault.wiki / "inbox" / "probationary" / dirname
        if probationary_root.exists():
            for path in sorted(probationary_root.glob("*.md")):
                frontmatter, body = read_page(path)
                pages.append(_Page(path=path, frontmatter=frontmatter, body=body))
    return pages


def _all_markdown_pages(vault: Vault) -> list[Path]:
    return [
        path
        for path in sorted(vault.wiki.rglob("*.md"))
        if ".archive" not in path.parts
    ]


def _link_map(pages: list[_Page]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for page in pages:
        if page.page_id != page.normalized_id:
            mapping[page.page_id] = page.normalized_id
        if page.path.stem != page.normalized_id:
            mapping[page.path.stem] = page.normalized_id
    return mapping


def _target_path(page: _Page, normalized_id: str) -> Path:
    if not page.is_probationary:
        return page.path.with_name(f"{normalized_id}.md")
    stem = page.path.stem
    page_id = page.page_id
    if stem.endswith(page_id):
        prefix = stem[: -len(page_id)]
        if prefix:
            return page.path.with_name(f"{prefix}{normalized_id}.md")
    created = str(page.frontmatter.get("created") or "").strip()
    if created:
        return page.path.with_name(f"{created}-{normalized_id}.md")
    return page.path.with_name(f"{normalized_id}.md")


def _winner_pages(pages: list[_Page]) -> dict[tuple[Path, str], _Page]:
    grouped: dict[tuple[Path, str], list[_Page]] = {}
    for page in pages:
        grouped.setdefault((page.path.parent, page.normalized_id), []).append(page)
    winners: dict[tuple[Path, str], _Page] = {}
    for key, group in grouped.items():
        ordered = sorted(
            group,
            key=lambda page: (
                page.page_id != page.normalized_id,
                -len(page.body),
                page.path.as_posix(),
            ),
        )
        winners[key] = ordered[0]
    return winners


def _merge_group(
    *,
    group: list[_Page],
    winner: _Page,
    link_map: dict[str, str],
) -> tuple[dict[str, Any], str, list[str]]:
    merged_frontmatter = dict(winner.frontmatter)
    merged_body = winner.body
    merged_old_ids = [page.page_id for page in group]
    for page in group:
        if page.path == winner.path:
            continue
        merged_frontmatter = _merge_frontmatter(merged_frontmatter, page.frontmatter, winner.normalized_id, page.page_id)
        if winner.page_type in ATOM_TYPES:
            merged_body = _merge_atom_body(merged_body, page.body)
        else:
            merged_body = _merge_body(merged_body, page.body)
    merged_frontmatter = _rewrite_frontmatter(merged_frontmatter, link_map)
    merged_body = _rewrite_body(merged_body, link_map)
    return merged_frontmatter, merged_body, merged_old_ids


def _merge_frontmatter(
    left: dict[str, Any],
    right: dict[str, Any],
    canonical_id: str,
    old_id: str,
) -> dict[str, Any]:
    merged = dict(left)
    merged["id"] = canonical_id
    merged["aliases"] = _dedupe([*_coerce_list(left.get("aliases")), *_coerce_list(right.get("aliases")), old_id])
    for key in ("domains", "sources", "relates_to", "tags"):
        merged[key] = _dedupe([*_coerce_list(left.get(key)), *_coerce_list(right.get(key))])
    merged["typed_relations"] = _merge_typed_relations(left.get("typed_relations"), right.get("typed_relations"))
    for key in ("created",):
        merged[key] = _pick_min(left.get(key), right.get(key))
    for key in ("last_updated", "last_evidence_date", "last_dream_pass"):
        merged[key] = _pick_max(left.get(key), right.get(key))
    if "evidence_count" in left or "evidence_count" in right:
        merged["evidence_count"] = max(_coerce_int(left.get("evidence_count")), _coerce_int(right.get("evidence_count")))
    return merged


def _merge_body(left: str, right: str) -> str:
    left_clean = left.strip()
    right_clean = right.strip()
    if not right_clean or right_clean in left_clean:
        return left
    if not left_clean or left_clean in right_clean:
        return right
    marker = "## Merged duplicate content"
    addition = _strip_title_heading(right_clean)
    if addition in left_clean:
        return left
    if marker in left_clean:
        return left_clean.rstrip() + "\n\n" + addition.rstrip() + "\n"
    return left_clean.rstrip() + f"\n\n{marker}\n\n" + addition.rstrip() + "\n"


def _merge_atom_body(left: str, right: str) -> str:
    title = _page_title(left) or _page_title(right) or "Merged Atom"
    tldr = _section_text(left, "TL;DR") or _section_text(right, "TL;DR")
    evidence = _dedupe_lines(_section_lines(left, "Evidence log") + _section_lines(right, "Evidence log"))
    contradictions = _dedupe_lines(_section_lines(left, "Contradictions") + _section_lines(right, "Contradictions"))
    steps = _dedupe_lines(_section_lines(left, "Steps") + _section_lines(right, "Steps"))
    parts = [f"# {title}"]
    if tldr:
        parts.extend(["", "## TL;DR", "", tldr])
    if steps:
        parts.extend(["", "## Steps", "", *steps])
    if evidence:
        parts.extend(["", "## Evidence log", "", *evidence])
    if contradictions:
        parts.extend(["", "## Contradictions", "", *contradictions])
    return "\n".join(parts).rstrip() + "\n"


def _page_title(body: str) -> str:
    for line in body.splitlines():
        stripped = line.strip()
        if stripped.startswith("# "):
            return stripped[2:].strip()
    return ""


def _section_text(body: str, heading: str) -> str:
    lines = _section_lines(body, heading)
    return "\n".join(lines).strip()


def _section_lines(body: str, heading: str) -> list[str]:
    lines = body.splitlines()
    target = f"## {heading}"
    collecting = False
    collected: list[str] = []
    for line in lines:
        stripped = line.rstrip()
        if stripped.startswith("## "):
            if stripped == target:
                collecting = True
                continue
            if collecting:
                break
        if collecting:
            if stripped:
                collected.append(stripped)
    return collected


def _strip_title_heading(body: str) -> str:
    lines = body.splitlines()
    if lines and lines[0].startswith("# "):
        return "\n".join(lines[1:]).lstrip()
    return body


def _rewrite_frontmatter(frontmatter: dict[str, Any], link_map: dict[str, str]) -> dict[str, Any]:
    rewritten: dict[str, Any] = {}
    for key, value in frontmatter.items():
        if key == "aliases":
            aliases = _coerce_list(value)
            rewritten_aliases = _dedupe([link_map.get(alias, alias) for alias in aliases])
            rewritten[key] = rewritten_aliases
            continue
        rewritten[key] = _rewrite_value(value, link_map)
    return rewritten


def _rewrite_value(value: Any, link_map: dict[str, str]) -> Any:
    if isinstance(value, dict):
        return {key: _rewrite_value(item, link_map) for key, item in value.items()}
    if isinstance(value, list):
        return [_rewrite_value(item, link_map) for item in value]
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if stripped in link_map:
        return link_map[stripped]
    return _rewrite_body(value, link_map)


def _rewrite_body(body: str, link_map: dict[str, str]) -> str:
    def _replace(match):
        inner = match.group(1)
        for index, char in enumerate(inner):
            if char in {"|", "#"}:
                target, rest = inner[:index], inner[index:]
                break
        else:
            target, rest = inner, ""
        replacement = link_map.get(target.strip())
        if replacement is None:
            return match.group(0)
        return f"[[{replacement}{rest}]]"

    return WIKILINK_RE.sub(_replace, body)


def _coerce_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def _dedupe(items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        cleaned = str(item).strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        out.append(cleaned)
    return out


def _dedupe_lines(items: list[str]) -> list[str]:
    return _dedupe(items)


def _merge_typed_relations(left: Any, right: Any) -> dict[str, list[str]]:
    merged: dict[str, list[str]] = {}
    for source in (left, right):
        if not isinstance(source, dict):
            continue
        for key, value in source.items():
            merged[key] = _dedupe([*(merged.get(key) or []), *_coerce_list(value)])
    return merged


def _coerce_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _pick_min(left: Any, right: Any) -> str:
    values = [str(item).strip() for item in (left, right) if str(item).strip()]
    return min(values) if values else ""


def _pick_max(left: Any, right: Any) -> str:
    values = [str(item).strip() for item in (left, right) if str(item).strip()]
    return max(values) if values else ""
