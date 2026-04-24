from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Iterable, Literal, Mapping, cast

import yaml

from scripts.common.vault import Vault
from scripts.common.wiki_writer import write_page


PolicyRetention = Literal["keep", "exclude"]
PolicyDomain = Literal["business", "personal"]
PolicySynthesisMode = Literal["deep", "light", "none"]
YouTubeCategory = Literal["business", "personal", "ignore"]
BookCategory = Literal["business", "personal", "fiction", "ignore"]
MigrationLane = Literal["youtube", "books"]

_POLICY_DOMAIN_ORDER: tuple[PolicyDomain, ...] = ("business", "personal")
_YOUTUBE_REVIEW_HINTS = (
    "history",
    "archaeology",
    "science",
    "biology",
    "chemistry",
    "physics",
    "philosophy",
    "economics",
    "biography",
    "memoir",
    "design",
    "product",
    "engineering",
    "founder",
    "builder",
)
_BOOK_REVIEW_HINTS = (
    "history",
    "science",
    "biography",
    "memoir",
    "philosophy",
    "economics",
    "design",
    "product",
    "engineering",
    "founder",
    "startup",
    "operator",
)


@dataclass(frozen=True)
class ContentPolicy:
    retention: PolicyRetention
    domains: tuple[PolicyDomain, ...]
    synthesis_mode: PolicySynthesisMode

    def to_dict(self) -> dict[str, Any]:
        return {
            "retention": self.retention,
            "domains": list(self.domains),
            "synthesis_mode": self.synthesis_mode,
        }


def _normalize_policy_domains(value: Iterable[object] | None) -> tuple[PolicyDomain, ...]:
    seen: set[PolicyDomain] = set()
    ordered: list[PolicyDomain] = []
    for candidate in value or ():
        text = str(candidate or "").strip().lower()
        if text not in {"business", "personal"}:
            continue
        domain = cast(PolicyDomain, text)
        if domain in seen:
            continue
        seen.add(domain)
        ordered.append(domain)
    ordered.sort(key=_POLICY_DOMAIN_ORDER.index)
    return tuple(ordered)


def _coerce_retention(value: object, *, default: PolicyRetention) -> PolicyRetention:
    text = str(value or "").strip().lower()
    if text in {"keep", "exclude"}:
        return cast(PolicyRetention, text)
    return default


def _coerce_synthesis_mode(value: object, *, default: PolicySynthesisMode) -> PolicySynthesisMode:
    text = str(value or "").strip().lower()
    if text in {"deep", "light", "none"}:
        return cast(PolicySynthesisMode, text)
    return default


def _compat_youtube_category(policy: ContentPolicy) -> YouTubeCategory:
    if policy.retention == "exclude":
        return "ignore"
    if "business" in policy.domains:
        return "business"
    return "personal"


def _compat_book_category(policy: ContentPolicy, *, hint: object = "") -> BookCategory:
    hint_text = str(hint or "").strip().lower()
    if policy.retention == "exclude":
        return "ignore"
    if hint_text == "fiction":
        return "fiction"
    if "business" in policy.domains:
        return "business"
    return "personal"


def normalize_youtube_classification(raw: Mapping[str, Any] | None) -> dict[str, Any]:
    payload = dict(raw or {})
    legacy_category = str(payload.get("category") or "").strip().lower()
    if legacy_category in {"business", "personal", "ignore"}:
        if legacy_category == "business":
            policy = ContentPolicy(retention="keep", domains=("business",), synthesis_mode="deep")
        elif legacy_category == "personal":
            policy = ContentPolicy(retention="keep", domains=("personal",), synthesis_mode="light")
        else:
            policy = ContentPolicy(retention="exclude", domains=("personal",), synthesis_mode="none")
    else:
        domains = _normalize_policy_domains(payload.get("domains"))
        retention = _coerce_retention(payload.get("retention"), default="keep")
        synthesis_mode = _coerce_synthesis_mode(payload.get("synthesis_mode"), default="deep" if "business" in domains else "light")
        if not domains:
            domains = ("personal",)
        if retention == "exclude":
            synthesis_mode = "none"
            if not domains:
                domains = ("personal",)
        policy = ContentPolicy(retention=retention, domains=domains, synthesis_mode=synthesis_mode)
    normalized = {
        **payload,
        **policy.to_dict(),
        "category": _compat_youtube_category(policy),
        "confidence": str(payload.get("confidence") or "medium").strip().lower() or "medium",
        "reasoning": str(payload.get("reasoning") or "").strip(),
    }
    return normalized


def normalize_book_classification(raw: Mapping[str, Any] | None) -> dict[str, Any]:
    payload = dict(raw or {})
    legacy_category = str(payload.get("category") or "").strip().lower()
    if legacy_category in {"business", "personal", "fiction", "ignore"}:
        if legacy_category == "business":
            policy = ContentPolicy(retention="keep", domains=("business",), synthesis_mode="deep")
        elif legacy_category == "fiction":
            policy = ContentPolicy(retention="keep", domains=("personal",), synthesis_mode="light")
        elif legacy_category == "personal":
            policy = ContentPolicy(retention="keep", domains=("personal",), synthesis_mode="light")
        else:
            policy = ContentPolicy(retention="exclude", domains=("personal",), synthesis_mode="none")
    else:
        domains = _normalize_policy_domains(payload.get("domains"))
        retention = _coerce_retention(payload.get("retention"), default="keep")
        synthesis_mode = _coerce_synthesis_mode(payload.get("synthesis_mode"), default="deep" if "business" in domains else "light")
        if not domains:
            domains = ("personal",)
        if retention == "exclude":
            synthesis_mode = "none"
            if not domains:
                domains = ("personal",)
        policy = ContentPolicy(retention=retention, domains=domains, synthesis_mode=synthesis_mode)
    normalized = {
        **payload,
        **policy.to_dict(),
        "category": _compat_book_category(policy, hint=payload.get("category")),
        "subcategory": payload.get("subcategory"),
        "confidence": str(payload.get("confidence") or "medium").strip().lower() or "medium",
        "reasoning": str(payload.get("reasoning") or "").strip(),
    }
    if normalized["category"] != "personal":
        normalized["subcategory"] = None
    return normalized


def content_policy_from_classification(classification: Mapping[str, Any] | None) -> ContentPolicy:
    payload = dict(classification or {})
    domains = _normalize_policy_domains(payload.get("domains"))
    retention = _coerce_retention(payload.get("retention"), default="keep")
    synthesis_mode = _coerce_synthesis_mode(payload.get("synthesis_mode"), default="light")
    if not domains:
        category = str(payload.get("category") or "").strip().lower()
        if category == "business":
            domains = ("business",)
        else:
            domains = ("personal",)
        if category == "ignore":
            retention = "exclude"
            synthesis_mode = "none"
        elif category == "business":
            retention = "keep"
            synthesis_mode = "deep"
        elif category in {"personal", "fiction"}:
            retention = "keep"
            synthesis_mode = "light"
    if retention == "exclude":
        synthesis_mode = "none"
    return ContentPolicy(retention=retention, domains=domains, synthesis_mode=synthesis_mode)


def canonical_policy_fields(classification: Mapping[str, Any] | None) -> dict[str, Any]:
    return content_policy_from_classification(classification).to_dict()


def should_materialize(classification: Mapping[str, Any] | None) -> bool:
    return content_policy_from_classification(classification).retention == "keep"


def should_run_deep_synthesis(classification: Mapping[str, Any] | None) -> bool:
    policy = content_policy_from_classification(classification)
    return policy.retention == "keep" and policy.synthesis_mode == "deep"


def compatibility_category(
    classification: Mapping[str, Any] | None,
    *,
    lane: MigrationLane,
) -> str:
    policy = content_policy_from_classification(classification)
    if lane == "youtube":
        return _compat_youtube_category(policy)
    raw = dict(classification or {})
    return _compat_book_category(policy, hint=raw.get("category"))


def working_set_domains(classification: Mapping[str, Any] | None) -> list[str]:
    policy = content_policy_from_classification(classification)
    mapped: list[str] = []
    if "business" in policy.domains:
        mapped.append("work")
    if "personal" in policy.domains:
        mapped.append("identity")
    if not mapped:
        mapped.append("learning")
    return mapped


def is_review_candidate(*, lane: MigrationLane, frontmatter: Mapping[str, Any], body: str, path: Path) -> bool:
    category = str(frontmatter.get("category") or path.parent.name).strip().lower()
    if lane == "youtube" and category != "personal":
        return False
    if lane == "books" and category not in {"personal", "fiction"}:
        return False
    if lane == "books" and str(frontmatter.get("subcategory") or "").strip().lower() in {"history", "science", "biography", "memoir", "culture"}:
        return True
    haystack = " ".join(
        [
            str(frontmatter.get("title") or ""),
            str(frontmatter.get("channel") or ""),
            str(frontmatter.get("author") or ""),
            str(frontmatter.get("subcategory") or ""),
            " ".join(str(item) for item in frontmatter.get("tags") or []),
            body[:3000],
        ]
    ).lower()
    hints = _YOUTUBE_REVIEW_HINTS if lane == "youtube" else _BOOK_REVIEW_HINTS
    return any(hint in haystack for hint in hints)


@dataclass
class ContentPolicyRepairReport:
    applied: bool
    generated_at: str
    youtube_counts: dict[str, int] = field(default_factory=dict)
    book_counts: dict[str, int] = field(default_factory=dict)
    ignore_source_paths: list[str] = field(default_factory=list)
    ignore_summary_paths: list[str] = field(default_factory=list)
    downstream_inbox_paths: list[str] = field(default_factory=list)
    downstream_noninbox_paths: list[str] = field(default_factory=list)
    deleted_paths: list[str] = field(default_factory=list)
    report_path: str = ""

    def render(self) -> str:
        lines = [
            "Repair content-policy summary",
            f"Mode: {'apply' if self.applied else 'dry-run'}",
            f"YouTube sources: {self.youtube_counts}",
            f"Book sources: {self.book_counts}",
            f"Ignore source pages: {len(self.ignore_source_paths)}",
            f"Ignore summaries: {len(self.ignore_summary_paths)}",
            f"Derived inbox artifacts: {len(self.downstream_inbox_paths)}",
            f"Non-inbox references left for review: {len(self.downstream_noninbox_paths)}",
            f"Deleted paths: {len(self.deleted_paths)}",
            f"Report: {self.report_path}",
        ]
        if self.downstream_noninbox_paths:
            lines.append("Manual review:")
            lines.extend(f"- {path}" for path in self.downstream_noninbox_paths[:10])
        return "\n".join(lines)


@dataclass
class ContentPolicyMigrationReport:
    lane: MigrationLane
    applied: bool
    generated_at: str
    sources_scanned: int = 0
    summaries_scanned: int = 0
    projected_policy_buckets: dict[str, int] = field(default_factory=dict)
    review_candidates: list[str] = field(default_factory=list)
    updated_paths: list[str] = field(default_factory=list)
    report_path: str = ""

    def render(self) -> str:
        lines = [
            f"Repair content-policy-migrate[{self.lane}]",
            f"Mode: {'apply' if self.applied else 'dry-run'}",
            f"Sources scanned: {self.sources_scanned}",
            f"Summaries scanned: {self.summaries_scanned}",
            f"Projected buckets: {self.projected_policy_buckets}",
            f"Review candidates: {len(self.review_candidates)}",
            f"Updated paths: {len(self.updated_paths)}",
            f"Report: {self.report_path}",
        ]
        if self.review_candidates:
            lines.append("Needs review:")
            lines.extend(f"- {path}" for path in self.review_candidates[:15])
        return "\n".join(lines)


def _report_path(repo_root: Path) -> Path:
    return Vault.load(repo_root).reports_root / "repair-content-policy-report.json"


def _migration_report_path(repo_root: Path, lane: MigrationLane) -> Path:
    return Vault.load(repo_root).reports_root / f"repair-content-policy-migrate-{lane}-report.json"


def _count_markdown_children(root: Path) -> dict[str, int]:
    if not root.exists():
        return {}
    counts: dict[str, int] = {}
    for child in sorted(path for path in root.iterdir() if path.is_dir()):
        counts[child.name] = len(list(child.glob("*.md")))
    return counts


def _video_id_from_source_path(path: Path) -> str:
    return path.stem[:11]


def _summary_ids_for_ignore_sources(paths: list[Path]) -> set[str]:
    return {f"summary-yt-{_video_id_from_source_path(path)}" for path in paths}


def _summary_paths_for_ignore_sources(vault: Vault, paths: list[Path]) -> list[Path]:
    summary_dir = vault.wiki / "summaries"
    results: list[Path] = []
    for source in paths:
        external_id = f"youtube-{_video_id_from_source_path(source)}"
        legacy_candidate = summary_dir / f"summary-yt-{_video_id_from_source_path(source)}.md"
        if legacy_candidate.exists():
            results.append(legacy_candidate)
            continue
        for candidate in sorted(summary_dir.glob("summary-*.md")):
            text = candidate.read_text(encoding="utf-8")
            if f"external_id: {external_id}" in text:
                results.append(candidate)
                break
    return sorted(results)


def _youtube_source_is_excluded(repo_root: Path, path: Path) -> bool:
    frontmatter, _body = _parse_page(path)
    if path.parent.name == "ignore":
        return True
    if not frontmatter:
        return False
    if str(frontmatter.get("retention") or "").strip().lower() == "exclude":
        return True
    if str(frontmatter.get("category") or "").strip().lower() == "ignore":
        return True
    video_id = _video_id_from_source_path(path)
    cache_path = Vault.load(repo_root).raw / "transcripts" / "youtube" / f"{video_id}.classification.json"
    if not cache_path.exists():
        return False
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return False
    data = payload.get("data")
    if not isinstance(data, dict):
        return False
    normalized = normalize_youtube_classification(data)
    return normalized.get("retention") == "exclude"


def _excluded_youtube_source_paths(vault: Vault, *, repo_root: Path) -> list[Path]:
    root = vault.wiki / "sources" / "youtube"
    if not root.exists():
        return []
    paths: list[Path] = []
    for path in sorted(root.rglob("*.md")):
        if _youtube_source_is_excluded(repo_root, path):
            paths.append(path)
    return paths


def _relative(path: Path, *, repo_root: Path) -> str:
    return Vault.load(repo_root).logical_path(path)


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
    try:
        return yaml.safe_load(block) or {}, body
    except yaml.YAMLError:
        return _parse_frontmatter_loose(block), body


def _parse_frontmatter_loose(block: str) -> dict[str, Any]:
    frontmatter: dict[str, Any] = {}
    current_list_key: str | None = None
    for raw_line in block.splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("- ") and current_list_key is not None:
            frontmatter.setdefault(current_list_key, []).append(stripped[2:].strip().strip('"'))
            continue
        current_list_key = None
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()
        if not value:
            frontmatter[key] = []
            current_list_key = key
            continue
        if value == "[]":
            frontmatter[key] = []
            continue
        if value in {"null", "None"}:
            frontmatter[key] = None
            continue
        frontmatter[key] = value.strip('"')
    return frontmatter


def _write_page(path: Path, frontmatter: dict[str, Any], body: str) -> None:
    write_page(path, frontmatter=frontmatter, body=body, force=True)


def _scan_references(
    vault: Vault,
    *,
    repo_root: Path,
    ignored_summary_ids: set[str],
    exclude_paths: set[Path],
) -> tuple[list[str], list[str]]:
    inbox_paths: list[str] = []
    noninbox_paths: list[str] = []
    for path in sorted(vault.wiki.rglob("*.md")):
        if path in exclude_paths:
            continue
        text = path.read_text(encoding="utf-8")
        if not any(summary_id in text for summary_id in ignored_summary_ids):
            continue
        rel = _relative(path, repo_root=repo_root)
        if rel.startswith("memory/inbox/"):
            inbox_paths.append(rel)
        else:
            noninbox_paths.append(rel)
    return inbox_paths, noninbox_paths


def _unlink_if_exists(path: Path, *, repo_root: Path, deleted_paths: list[str]) -> None:
    if not path.exists():
        return
    path.unlink()
    deleted_paths.append(_relative(path, repo_root=repo_root))


def _prune_empty_dirs(start: Path, *, stop: Path) -> None:
    current = start
    while current != stop and current.exists():
        try:
            current.rmdir()
        except OSError:
            break
        current = current.parent


def _infer_existing_classification(*, lane: MigrationLane, frontmatter: Mapping[str, Any], path: Path) -> dict[str, Any]:
    category = str(frontmatter.get("category") or path.parent.name).strip().lower()
    payload: dict[str, Any] = {"category": category}
    if lane == "books":
        payload["subcategory"] = frontmatter.get("subcategory")
        return normalize_book_classification(payload)
    return normalize_youtube_classification(payload)


def _policy_bucket_name(classification: Mapping[str, Any]) -> str:
    policy = content_policy_from_classification(classification)
    domains = "+".join(policy.domains)
    return f"{policy.retention}:{domains}:{policy.synthesis_mode}"


def _iter_lane_source_pages(vault: Vault, *, lane: MigrationLane) -> list[Path]:
    if lane == "youtube":
        root = vault.wiki / "sources" / "youtube"
        categories = ("business", "personal", "ignore")
    else:
        root = vault.wiki / "sources" / "books"
        categories = ("business", "personal", "fiction")
    paths: list[Path] = []
    for category in categories:
        category_dir = root / category
        if not category_dir.exists():
            continue
        paths.extend(sorted(category_dir.glob("*.md")))
    return paths


def _summary_path_for_source(vault: Vault, *, lane: MigrationLane, path: Path) -> Path | None:
    if lane == "youtube":
        external_id = f"youtube-{_video_id_from_source_path(path)}"
        legacy = vault.wiki / "summaries" / f"summary-yt-{_video_id_from_source_path(path)}.md"
        if legacy.exists():
            return legacy
        for candidate in sorted((vault.wiki / "summaries").glob("summary-*.md")):
            text = candidate.read_text(encoding="utf-8")
            if f"external_id: {external_id}" in text:
                return candidate
        return None
    stem = path.stem
    for candidate in (
        vault.wiki / "summaries" / f"summary-{stem}.md",
        vault.wiki / "summaries" / f"summary-book-{stem}.md",
    ):
        if candidate.exists():
            return candidate
    return None


def run_content_policy_repair(
    repo_root: Path,
    *,
    apply: bool,
) -> ContentPolicyRepairReport:
    vault = Vault.load(repo_root)
    ignore_sources = _excluded_youtube_source_paths(vault, repo_root=repo_root)
    ignore_summaries = _summary_paths_for_ignore_sources(vault, ignore_sources)
    ignored_summary_ids = _summary_ids_for_ignore_sources(ignore_sources)
    exclude_paths = set(ignore_sources) | set(ignore_summaries)
    inbox_refs, noninbox_refs = _scan_references(
        vault,
        repo_root=repo_root,
        ignored_summary_ids=ignored_summary_ids,
        exclude_paths=exclude_paths,
    )

    report = ContentPolicyRepairReport(
        applied=apply,
        generated_at=datetime.now(timezone.utc).isoformat(),
        youtube_counts=_count_markdown_children(vault.wiki / "sources" / "youtube"),
        book_counts=_count_markdown_children(vault.wiki / "sources" / "books"),
        ignore_source_paths=[_relative(path, repo_root=repo_root) for path in ignore_sources],
        ignore_summary_paths=[_relative(path, repo_root=repo_root) for path in ignore_summaries],
        downstream_inbox_paths=inbox_refs,
        downstream_noninbox_paths=noninbox_refs,
    )

    if apply:
        for path in ignore_sources:
            _unlink_if_exists(path, repo_root=repo_root, deleted_paths=report.deleted_paths)
        for path in ignore_summaries:
            _unlink_if_exists(path, repo_root=repo_root, deleted_paths=report.deleted_paths)
        for rel in inbox_refs:
            _unlink_if_exists(vault.resolve_logical_path(rel), repo_root=repo_root, deleted_paths=report.deleted_paths)
        ignore_source_dir = vault.wiki / "sources" / "youtube" / "ignore"
        if ignore_source_dir.exists():
            _prune_empty_dirs(ignore_source_dir, stop=vault.wiki)

    artifact_path = _report_path(repo_root)
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(
        json.dumps(
            {
                **asdict(report),
                "report_path": _relative(artifact_path, repo_root=repo_root),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    report.report_path = _relative(artifact_path, repo_root=repo_root)
    return report


def run_content_policy_migration(
    repo_root: Path,
    *,
    lane: MigrationLane,
    apply: bool,
) -> ContentPolicyMigrationReport:
    vault = Vault.load(repo_root)
    source_paths = _iter_lane_source_pages(vault, lane=lane)
    report = ContentPolicyMigrationReport(
        lane=lane,
        applied=apply,
        generated_at=datetime.now(timezone.utc).isoformat(),
    )

    for source_path in source_paths:
        frontmatter, body = _parse_page(source_path)
        classification = _infer_existing_classification(lane=lane, frontmatter=frontmatter, path=source_path)
        report.sources_scanned += 1
        bucket = _policy_bucket_name(classification)
        report.projected_policy_buckets[bucket] = report.projected_policy_buckets.get(bucket, 0) + 1
        if is_review_candidate(lane=lane, frontmatter=frontmatter, body=body, path=source_path):
            report.review_candidates.append(_relative(source_path, repo_root=repo_root))

        updated_frontmatter = dict(frontmatter)
        updated_frontmatter.update(canonical_policy_fields(classification))
        updated_frontmatter["category"] = compatibility_category(classification, lane=lane)
        if lane == "books" and updated_frontmatter["category"] != "personal":
            updated_frontmatter["subcategory"] = frontmatter.get("subcategory") if updated_frontmatter["category"] == "fiction" else ""

        summary_path = _summary_path_for_source(vault, lane=lane, path=source_path)
        if apply:
            _write_page(source_path, updated_frontmatter, body)
            report.updated_paths.append(_relative(source_path, repo_root=repo_root))
        if summary_path is not None:
            summary_frontmatter, summary_body = _parse_page(summary_path)
            updated_summary_frontmatter = dict(summary_frontmatter)
            updated_summary_frontmatter.update(canonical_policy_fields(classification))
            if apply:
                _write_page(summary_path, updated_summary_frontmatter, summary_body)
                report.updated_paths.append(_relative(summary_path, repo_root=repo_root))
            report.summaries_scanned += 1

    artifact_path = _migration_report_path(repo_root, lane)
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(
        json.dumps(
            {
                **asdict(report),
                "report_path": _relative(artifact_path, repo_root=repo_root),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    report.report_path = _relative(artifact_path, repo_root=repo_root)
    return report
