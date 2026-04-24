from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Any, Iterable

import yaml

from scripts.common.slugify import slugify
from scripts.common.vault import Vault
from scripts.common.wikilinks import WIKILINK_RE
from scripts.common.wiki_writer import write_page


PRIMARY_ROOTS: tuple[str, ...] = (
    "sources",
    "summaries",
    "concepts",
    "playbooks",
    "stances",
    "inquiries",
)
SUPPORT_FILES: tuple[str, ...] = ("INDEX.md", "CHANGELOG.md")
SUMMARY_PREFIXES: tuple[str, ...] = (
    "summary-book-",
    "summary-yt-",
    "summary-article-",
    "summary-substack-",
)


@dataclass
class PairIssue:
    lane: str
    issue: str
    key: str
    path: str


@dataclass
class DuplicateIssue:
    lane: str
    title: str
    paths: list[str]
    auto_resolvable: bool


@dataclass
class RenamePlan:
    kind: str
    old_path: str
    new_path: str
    old_id: str
    new_id: str


@dataclass
class VaultHousekeepingReport:
    applied: bool
    generated_at: str
    pair_issues: list[PairIssue] = field(default_factory=list)
    duplicate_issues: list[DuplicateIssue] = field(default_factory=list)
    filename_noise: dict[str, int] = field(default_factory=dict)
    rename_plans: list[RenamePlan] = field(default_factory=list)
    deleted_paths: list[str] = field(default_factory=list)
    updated_paths: list[str] = field(default_factory=list)
    report_path: str = ""

    def render(self) -> str:
        lines = [
            "Vault housekeeping summary",
            f"Mode: {'apply' if self.applied else 'dry-run'}",
            f"Pair issues: {len(self.pair_issues)}",
            f"Duplicate issues: {len(self.duplicate_issues)}",
            f"Filename noise: {self.filename_noise}",
            f"Renames planned: {len(self.rename_plans)}",
            f"Updated paths: {len(self.updated_paths)}",
            f"Deleted paths: {len(self.deleted_paths)}",
            f"Report: {self.report_path}",
        ]
        if self.pair_issues:
            lines.append("Pair issue samples:")
            lines.extend(
                f"- [{issue.lane}] {issue.issue}: {issue.key} -> {issue.path}"
                for issue in self.pair_issues[:10]
            )
        if self.duplicate_issues:
            lines.append("Duplicate samples:")
            for issue in self.duplicate_issues[:10]:
                lines.append(
                    f"- [{issue.lane}] {issue.title} ({'auto' if issue.auto_resolvable else 'review'})"
                )
                lines.extend(f"  - {path}" for path in issue.paths[:4])
        return "\n".join(lines)


@dataclass
class _Page:
    path: Path
    frontmatter: dict[str, Any]
    body: str

    @property
    def stem(self) -> str:
        return self.path.stem

    @property
    def title(self) -> str:
        return str(self.frontmatter.get("title") or "").strip()

    @property
    def type(self) -> str:
        return str(self.frontmatter.get("type") or "").strip()


def _report_path(repo_root: Path) -> Path:
    return Vault.load(repo_root).reports_root / "repair-vault-housekeeping-report.json"


def _rel(path: Path, *, repo_root: Path) -> str:
    return Vault.load(repo_root).logical_path(path)


def _frontmatter_block(text: str) -> tuple[str | None, str]:
    if not text.startswith("---\n"):
        return None, text
    end = text.find("\n---\n", 4)
    if end == -1:
        return None, text
    return text[4:end], text[end + 5 :]


def _parse_page(path: Path) -> _Page:
    text = path.read_text(encoding="utf-8")
    block, body = _frontmatter_block(text)
    if block is None:
        return _Page(path=path, frontmatter={}, body=text)
    return _Page(path=path, frontmatter=yaml.safe_load(block) or {}, body=body)


def _write_page(path: Path, frontmatter: dict[str, Any], body: str) -> None:
    write_page(path, frontmatter=frontmatter, body=body, force=True)


def _primary_graph_pages(vault: Vault) -> list[_Page]:
    pages: list[_Page] = []
    for rel in PRIMARY_ROOTS:
        root = vault.wiki / rel
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.md")):
            pages.append(_parse_page(path))
    for support in SUPPORT_FILES:
        path = vault.wiki / support
        if path.exists():
            pages.append(_parse_page(path))
    return pages


def _source_pages(vault: Vault, lane: str) -> list[_Page]:
    root = vault.wiki / "sources" / lane
    if not root.exists():
        return []
    return [_parse_page(path) for path in sorted(root.rglob("*.md"))]


def _summary_pages(vault: Vault) -> list[_Page]:
    root = vault.wiki / "summaries"
    if not root.exists():
        return []
    return [_parse_page(path) for path in sorted(root.glob("*.md"))]


def _first_author_slug(frontmatter: dict[str, Any]) -> str:
    author = frontmatter.get("author") or []
    if isinstance(author, list) and author:
        first = str(author[0]).strip().strip('"')
        first = first.replace("[[", "").replace("]]", "")
        return slugify(first, max_len=120)
    return ""


def _primary_book_slug(page: _Page) -> str:
    return slugify(f"{_first_author_slug(page.frontmatter)}-{page.title}", max_len=120)


def _video_id(page: _Page) -> str:
    raw = str(page.frontmatter.get("youtube_id") or "")
    if raw:
        return raw
    ext = str(page.frontmatter.get("external_id") or "")
    if ext.startswith("youtube-"):
        return ext.removeprefix("youtube-")
    return page.stem[:11]


def _youtube_title_slug(page: _Page) -> str:
    return slugify(page.title, max_len=80) or page.stem


def _substack_source_slug(page: _Page) -> str:
    return page.stem


def _article_source_slug(page: _Page) -> str:
    return page.stem


def _book_summary_slug(page: _Page) -> str:
    return page.stem.removeprefix("summary-book-").removeprefix("summary-")


def _youtube_summary_key(page: _Page) -> str:
    ext = str(page.frontmatter.get("external_id") or "")
    if ext.startswith("youtube-"):
        return ext.removeprefix("youtube-")
    source_path = str(page.frontmatter.get("source_path") or "")
    match = re.search(r"([A-Za-z0-9_-]{10,12})\.json", source_path)
    if match:
        return match.group(1)
    return page.stem.removeprefix("summary-yt-").removeprefix("summary-")


def _book_summary_key(page: _Page) -> str:
    ext = str(page.frontmatter.get("external_id") or "")
    if ext:
        return ext
    source_path = str(page.frontmatter.get("source_path") or "")
    return Path(source_path).stem or page.stem.removeprefix("summary-")


def _substack_summary_key(page: _Page) -> str:
    ext = str(page.frontmatter.get("external_id") or "")
    if ext.startswith("substack-"):
        return ext.removeprefix("substack-")
    source_path = str(page.frontmatter.get("source_path") or "")
    if source_path:
        return Path(source_path).stem
    return page.stem.removeprefix("summary-substack-").removeprefix("summary-")


def _article_summary_key(page: _Page) -> str:
    source_path = str(page.frontmatter.get("source_path") or "")
    if source_path:
        return Path(source_path).stem
    return page.stem.removeprefix("summary-article-").removeprefix("summary-")


def _book_source_key(page: _Page) -> str:
    ext = str(page.frontmatter.get("external_id") or "")
    if ext:
        return ext
    return page.stem


def _youtube_source_key(page: _Page) -> str:
    ext = str(page.frontmatter.get("external_id") or "")
    if ext.startswith("youtube-"):
        return ext.removeprefix("youtube-")
    return _video_id(page)


def _substack_source_key(page: _Page) -> str:
    source_id = str(page.frontmatter.get("external_id") or "")
    if source_id.startswith("substack-"):
        return source_id.removeprefix("substack-")
    return page.stem


def _article_source_key(page: _Page) -> str:
    return page.stem


def _count_filename_noise(pages: Iterable[_Page]) -> dict[str, int]:
    metrics = {
        "leading_external_id": 0,
        "summary_prefix": 0,
        "filename_gt_80": 0,
    }
    for page in pages:
        stem = page.stem
        if re.match(r"^[A-Za-z0-9_-]{10,12}-", stem):
            metrics["leading_external_id"] += 1
        if stem.startswith(SUMMARY_PREFIXES):
            metrics["summary_prefix"] += 1
        if len(page.path.name) > 80:
            metrics["filename_gt_80"] += 1
    return metrics


def _pair_issues(vault: Vault, *, repo_root: Path) -> list[PairIssue]:
    issues: list[PairIssue] = []

    youtube_sources = {_youtube_source_key(page): page for page in _source_pages(vault, "youtube")}
    youtube_summaries = {_youtube_summary_key(page): page for page in _summary_pages(vault) if page.stem.startswith(("summary-yt-", "summary-")) and str(page.frontmatter.get("source_type") or "") == "video"}
    for key, page in youtube_summaries.items():
        if key not in youtube_sources:
            issues.append(PairIssue("youtube", "legacy summary without canonical source", key, _rel(page.path, repo_root=repo_root)))

    book_sources = {_book_source_key(page): page for page in _source_pages(vault, "books")}
    book_summaries = {_book_summary_key(page): page for page in _summary_pages(vault) if str(page.frontmatter.get("source_type") or "") == "book"}
    for key, page in book_summaries.items():
        if key not in book_sources:
            issues.append(PairIssue("books", "legacy summary without canonical source", key, _rel(page.path, repo_root=repo_root)))

    substack_sources = {_substack_source_key(page): page for page in _source_pages(vault, "substack")}
    substack_summaries = {_substack_summary_key(page): page for page in _summary_pages(vault) if page.stem.startswith(("summary-substack-", "summary-")) and str(page.frontmatter.get("source_type") or "") in {"substack", "article"} and str(page.frontmatter.get("external_id") or "").startswith("substack-")}
    for key, page in substack_summaries.items():
        if key not in substack_sources:
            issues.append(PairIssue("substack", "legacy summary without canonical source", key, _rel(page.path, repo_root=repo_root)))
    for key, source_page in substack_sources.items():
        summary_page = substack_summaries.get(key)
        if summary_page is None:
            continue
        expected = _summary_target_for_source(source_page.path)
        if summary_page.path != expected:
            issues.append(PairIssue("substack", "legacy summary slug drift", key, _rel(summary_page.path, repo_root=repo_root)))

    article_sources = {_article_source_key(page): page for page in _source_pages(vault, "articles")}
    article_summaries = {_article_summary_key(page): page for page in _summary_pages(vault) if str(page.frontmatter.get("source_type") or "") == "article"}
    for key, page in article_summaries.items():
        if key not in article_sources:
            issues.append(PairIssue("articles", "legacy summary without canonical source", key, _rel(page.path, repo_root=repo_root)))

    return issues


def _duplicate_issues(vault: Vault, *, repo_root: Path) -> list[DuplicateIssue]:
    issues: list[DuplicateIssue] = []
    for lane in ("youtube", "books", "articles", "substack"):
        groups: dict[str, list[_Page]] = {}
        for page in _source_pages(vault, lane):
            title = page.title
            if not title:
                continue
            groups.setdefault(title, []).append(page)
        for title, pages in sorted(groups.items()):
            if len(pages) < 2:
                continue
            auto_resolvable = lane == "youtube" or (lane == "books" and len(pages) == 2)
            issues.append(
                DuplicateIssue(
                    lane=lane,
                    title=title,
                    paths=[_rel(page.path, repo_root=repo_root) for page in pages],
                    auto_resolvable=auto_resolvable,
                )
            )
    return issues


def _youtube_canonical_sources(vault: Vault) -> tuple[dict[Path, Path], dict[Path, Path]]:
    pages = _source_pages(vault, "youtube")
    by_key: dict[str, list[_Page]] = {}
    for page in pages:
        by_key.setdefault(_youtube_source_key(page), []).append(page)

    canonical_by_old: dict[Path, Path] = {}
    duplicate_to_canonical: dict[Path, Path] = {}
    kept_pages: list[_Page] = []
    for _key, group in by_key.items():
        group.sort(key=lambda page: (str(page.frontmatter.get("category") or "") != "business", page.path.as_posix()))
        canonical = group[0]
        kept_pages.append(canonical)
        for page in group:
            duplicate_to_canonical[page.path] = canonical.path

    slug_groups: dict[str, list[_Page]] = {}
    for page in kept_pages:
        slug_groups.setdefault(_youtube_title_slug(page), []).append(page)

    for slug, group in slug_groups.items():
        for page in group:
            category_dir = page.path.parent
            if len(group) == 1:
                new_stem = slug
            else:
                new_stem = f"{slug}--youtube-{_video_id(page)}"
            canonical_by_old[page.path] = category_dir / f"{new_stem}.md"
    return canonical_by_old, duplicate_to_canonical


def _book_canonical_sources(vault: Vault) -> tuple[dict[Path, Path], dict[Path, Path]]:
    pages = _source_pages(vault, "books")
    by_title: dict[str, list[_Page]] = {}
    for page in pages:
        by_title.setdefault(page.title, []).append(page)

    canonical_by_old: dict[Path, Path] = {}
    duplicate_to_canonical: dict[Path, Path] = {}
    for _title, group in by_title.items():
        if len(group) == 1:
            page = group[0]
            canonical_by_old[page.path] = page.path.parent / f"{_primary_book_slug(page)}.md"
            duplicate_to_canonical[page.path] = page.path
            continue
        group.sort(
            key=lambda page: (
                len(page.frontmatter.get("author") or [] if isinstance(page.frontmatter.get("author"), list) else [page.frontmatter.get("author")]) != 1,
                -len(page.body),
                page.path.as_posix(),
            )
        )
        canonical = group[0]
        canonical_target = canonical.path.parent / f"{_primary_book_slug(canonical)}.md"
        canonical_by_old[canonical.path] = canonical_target
        for page in group:
            duplicate_to_canonical[page.path] = canonical.path
            if page.path != canonical.path:
                canonical_by_old[page.path] = canonical_target
    return canonical_by_old, duplicate_to_canonical


def _substack_canonical_sources(vault: Vault) -> dict[Path, Path]:
    return {page.path: page.path for page in _source_pages(vault, "substack")}


def _article_canonical_sources(vault: Vault) -> dict[Path, Path]:
    return {page.path: page.path for page in _source_pages(vault, "articles")}


def _summary_target_for_source(source_target: Path) -> Path:
    current = source_target.parent
    while current.name != "memory" and current != current.parent:
        current = current.parent
    return current / "summaries" / f"summary-{source_target.stem}.md"


def _summary_renames(
    vault: Vault,
    *,
    source_targets: dict[Path, Path],
) -> dict[Path, Path]:
    renames: dict[Path, Path] = {}
    summary_pages = _summary_pages(vault)
    # Map current sources to current summary pages by stem relationships / frontmatter
    source_by_current_path = {path: target for path, target in source_targets.items()}
    source_by_stem = {path.stem: target for path, target in source_targets.items()}
    for page in summary_pages:
        source_type = str(page.frontmatter.get("source_type") or "")
        if source_type == "video":
            key = _youtube_summary_key(page)
            source_path = next((p for p in source_targets if _youtube_source_key(_parse_page(p)) == key), None)
        elif source_type == "book":
            key = _book_summary_key(page)
            source_path = next((p for p in source_targets if _book_source_key(_parse_page(p)) == key), None)
        elif source_type == "article":
            key = _article_summary_key(page)
            source_path = next((p for p in source_targets if _article_source_key(_parse_page(p)) == key), None)
        elif source_type in {"substack", ""} and str(page.frontmatter.get("external_id") or "").startswith("substack-"):
            key = _substack_summary_key(page)
            source_path = next((p for p in source_targets if _substack_source_key(_parse_page(p)) == key), None)
        else:
            source_path = None
        if source_path is None:
            continue
        renames[page.path] = _summary_target_for_source(source_targets[source_path])
    return renames


def _primary_support_paths(vault: Vault) -> list[Path]:
    paths = []
    for name in SUPPORT_FILES:
        path = vault.wiki / name
        if path.exists():
            paths.append(path)
    return paths


def _replace_string(value: str, *, link_map: dict[str, str]) -> str:
    def _sub(match: re.Match[str]) -> str:
        inner = match.group(1)
        for idx, char in enumerate(inner):
            if char in {"|", "#"}:
                target, rest = inner[:idx], inner[idx:]
                break
        else:
            target, rest = inner, ""
        replacement = link_map.get(target)
        if replacement is None:
            return match.group(0)
        return f"[[{replacement}{rest}]]"

    replaced = WIKILINK_RE.sub(_sub, value)
    if replaced in link_map:
        return link_map[replaced]
    return replaced


def _summary_title_for_source(page: _Page, *, lane: str) -> str:
    if lane == "books":
        return f"Book: {page.title}"
    if lane == "youtube":
        return f"YouTube: {page.title}"
    return f"Summary — {page.title}"


def _source_type_for_lane(lane: str) -> str:
    return {
        "books": "book",
        "youtube": "video",
        "articles": "article",
        "substack": "substack",
    }[lane]


def _build_missing_summary(
    *,
    source_page: _Page,
    lane: str,
    source_target: Path,
    summary_target: Path,
    repo_root: Path,
) -> tuple[dict[str, Any], str]:
    frontmatter = {
        "id": summary_target.stem,
        "type": "summary",
        "title": _summary_title_for_source(source_page, lane=lane),
        "status": str(source_page.frontmatter.get("status") or "active"),
        "created": str(source_page.frontmatter.get("created") or datetime.now(timezone.utc).date().isoformat()),
        "last_updated": str(source_page.frontmatter.get("last_updated") or datetime.now(timezone.utc).date().isoformat()),
        "aliases": [],
        "tags": list(source_page.frontmatter.get("tags") or []),
        "domains": list(source_page.frontmatter.get("domains") or []),
        "relates_to": [f"[[{source_target.stem}]]"],
        "sources": [],
        "source_path": str(source_target.relative_to(repo_root)),
        "source_type": _source_type_for_lane(lane),
    }
    external_id = str(source_page.frontmatter.get("external_id") or "").strip()
    if external_id:
        frontmatter["external_id"] = external_id
    source_date = str(source_page.frontmatter.get("finished") or source_page.frontmatter.get("watched_on") or source_page.frontmatter.get("published") or source_page.frontmatter.get("created") or "")
    if source_date:
        frontmatter["source_date"] = source_date
    body = source_page.body if source_page.body.strip() else f"# Summary — {source_page.title}\n"
    return frontmatter, body


def _rewrite_frontmatter(value: Any, *, link_map: dict[str, str]) -> Any:
    if isinstance(value, str):
        return _replace_string(value, link_map=link_map)
    if isinstance(value, list):
        return [_rewrite_frontmatter(item, link_map=link_map) for item in value]
    if isinstance(value, dict):
        rewritten: dict[str, Any] = {}
        for key, item in value.items():
            if key == "aliases":
                rewritten[key] = item
            else:
                rewritten[key] = _rewrite_frontmatter(item, link_map=link_map)
        return rewritten
    return value


def run_vault_housekeeping(
    repo_root: Path,
    *,
    apply: bool,
) -> VaultHousekeepingReport:
    vault = Vault.load(repo_root)
    report = VaultHousekeepingReport(
        applied=apply,
        generated_at=datetime.now(timezone.utc).isoformat(),
    )

    primary_pages = _primary_graph_pages(vault)
    report.filename_noise = _count_filename_noise(primary_pages)
    report.pair_issues = _pair_issues(vault, repo_root=repo_root)
    report.duplicate_issues = _duplicate_issues(vault, repo_root=repo_root)

    youtube_targets, youtube_dupes = _youtube_canonical_sources(vault)
    book_targets, book_dupes = _book_canonical_sources(vault)
    substack_targets = _substack_canonical_sources(vault)
    article_targets = _article_canonical_sources(vault)
    source_targets = {**youtube_targets, **book_targets, **substack_targets, **article_targets}
    summary_targets = _summary_renames(vault, source_targets=source_targets)

    link_map: dict[str, str] = {}
    deleted_paths: set[Path] = set()
    for old_path, new_path in {**source_targets, **summary_targets}.items():
        old_stem = old_path.stem
        new_stem = new_path.stem
        if old_stem != new_stem:
            report.rename_plans.append(
                RenamePlan(
                    kind="summary" if old_path.parent.name == "summaries" else "source",
                    old_path=_rel(old_path, repo_root=repo_root),
                    new_path=_rel(new_path, repo_root=repo_root),
                    old_id=old_stem,
                    new_id=new_stem,
                )
            )
            link_map[old_stem] = new_stem
    for old_path, canonical_path in {**youtube_dupes, **book_dupes}.items():
        if old_path != canonical_path:
            deleted_paths.add(old_path)
            if old_path.stem != canonical_path.stem:
                link_map[old_path.stem] = source_targets.get(canonical_path, canonical_path).stem
            deleted_page = _parse_page(old_path)
            for source_link in deleted_page.frontmatter.get("sources") or []:
                if isinstance(source_link, str):
                    link_map[source_link.replace("[[", "").replace("]]", "").strip()] = _summary_target_for_source(source_targets.get(canonical_path, canonical_path)).stem

    all_relevant_paths = {page.path for page in primary_pages}
    all_relevant_paths.update(_primary_support_paths(vault))
    updated_payloads: dict[Path, tuple[dict[str, Any], str]] = {}

    for path in sorted(all_relevant_paths):
        if path in deleted_paths:
            continue
        page = _parse_page(path)
        target_path = source_targets.get(path) or summary_targets.get(path) or path
        new_frontmatter = _rewrite_frontmatter(dict(page.frontmatter), link_map=link_map)
        new_body = _replace_string(page.body, link_map=link_map)
        if target_path != path:
            aliases = list(new_frontmatter.get("aliases") or [])
            if path.stem not in aliases:
                aliases.append(path.stem)
            new_frontmatter["aliases"] = aliases
            new_frontmatter["id"] = target_path.stem
        updated_payloads[target_path] = (new_frontmatter, new_body)
        if target_path != path or new_frontmatter != page.frontmatter or new_body != page.body:
            report.updated_paths.append(_rel(target_path, repo_root=repo_root))

    existing_summary_targets = set(summary_targets.values()) | {path for path in updated_payloads if path.parent.name == "summaries"}
    lane_roots = {"books": "books", "youtube": "youtube", "articles": "articles", "substack": "substack"}
    for lane, root_name in lane_roots.items():
        for source_page in _source_pages(vault, root_name):
            canonical_source = source_targets.get(source_page.path)
            if canonical_source is None or source_page.path in deleted_paths:
                continue
            summary_target = _summary_target_for_source(canonical_source)
            if summary_target in existing_summary_targets:
                continue
            frontmatter, body = _build_missing_summary(
                source_page=source_page,
                lane=lane,
                source_target=canonical_source,
                summary_target=summary_target,
                repo_root=repo_root,
            )
            updated_payloads[summary_target] = (frontmatter, body)
            existing_summary_targets.add(summary_target)
            report.updated_paths.append(_rel(summary_target, repo_root=repo_root))
            for source_link in source_page.frontmatter.get("sources") or []:
                if isinstance(source_link, str):
                    link_map[source_link.replace("[[", "").replace("]]", "").strip()] = summary_target.stem

    updated_payloads = {
        path: (
            _rewrite_frontmatter(frontmatter, link_map=link_map),
            _replace_string(body, link_map=link_map),
        )
        for path, (frontmatter, body) in updated_payloads.items()
    }

    if apply:
        for target_path, (frontmatter, body) in updated_payloads.items():
            _write_page(target_path, frontmatter, body)
        final_paths = set(updated_payloads)
        for old_path in sorted(set(list(source_targets) + list(summary_targets) + list(deleted_paths))):
            if old_path not in final_paths and old_path.exists():
                old_path.unlink()
                report.deleted_paths.append(_rel(old_path, repo_root=repo_root))
        for root in (
            vault.wiki / "sources" / "youtube" / "business",
            vault.wiki / "sources" / "youtube" / "personal",
            vault.wiki / "summaries",
        ):
            current = root
            while current != vault.wiki and current.exists():
                try:
                    current.rmdir()
                except OSError:
                    break
                current = current.parent

    artifact_path = _report_path(repo_root)
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(
        json.dumps(
            {
                **asdict(report),
                "report_path": _rel(artifact_path, repo_root=repo_root),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    report.report_path = _rel(artifact_path, repo_root=repo_root)
    return report
