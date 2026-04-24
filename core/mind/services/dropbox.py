from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from fnmatch import fnmatch
import json
from pathlib import Path
import shutil
from typing import Callable, Iterable

# TODO: these ingest operations should live in mind/services/, not mind/commands/
from mind.commands.ingest import (
    ingest_books_export,
    ingest_file_with_details,
    ingest_substack_export,
    ingest_youtube_export,
    preflight_file_ingest,
)
from mind.services.graph_registry import GraphRegistry
from scripts.common.vault import Vault, raw_path


DROPBOX_DIRNAME = "dropbox"
DROPBOX_ARCHIVE_DIRS = (".processed", ".failed", ".reports", ".review")
MACHINE_QUEUE_PATTERNS = (
    "articles-from-*.jsonl",
    "search-signals-from-*.jsonl",
    "web-discovery-candidates-from-*.jsonl",
)
BOOK_EXPORT_PATTERNS = (
    "goodreads-*.csv",
    "audible-library-*.json",
    "audible-*.csv",
    "books-*.csv",
    "books-*.md",
)
SAFE_GENERIC_EXTENSIONS = {".md", ".txt", ".pdf", ".csv"}


@dataclass(frozen=True)
class DropboxPendingItem:
    path: Path
    classification: str
    route: str | None
    detail: str

    @property
    def type_label(self) -> str:
        if self.route:
            return self.route
        if self.classification == "machine-queue":
            return "machine-queue"
        return "unsupported"


@dataclass(frozen=True)
class DropboxItemOutcome:
    source_path: str
    classification: str
    route: str | None
    disposition: str
    detail: str
    destination_path: str | None = None
    outputs: list[str] = field(default_factory=list)
    extra: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class DropboxSweepResult:
    dry_run: bool
    dropbox_root: Path
    scanned_count: int
    pending_count_before: int
    pending_count_after: int
    outcomes: list[DropboxItemOutcome]
    report_json_path: Path
    report_markdown_path: Path
    mirror_json_path: Path
    mirror_markdown_path: Path

    @property
    def processed_count(self) -> int:
        return sum(1 for item in self.outcomes if item.disposition == "processed")

    @property
    def failed_count(self) -> int:
        return sum(1 for item in self.outcomes if item.disposition == "failed")

    @property
    def skipped_count(self) -> int:
        return sum(1 for item in self.outcomes if item.disposition == "skipped")

    @property
    def unsupported_count(self) -> int:
        return sum(1 for item in self.outcomes if item.disposition == "unsupported")

    @property
    def review_count(self) -> int:
        return sum(1 for item in self.outcomes if item.disposition == "review")

    @property
    def predicted_process_count(self) -> int:
        return sum(1 for item in self.outcomes if item.disposition == "would_process")

    @property
    def predicted_review_count(self) -> int:
        return sum(1 for item in self.outcomes if item.disposition == "would_review")

    @property
    def predicted_fail_count(self) -> int:
        return sum(1 for item in self.outcomes if item.disposition == "would_fail")

    @property
    def predicted_patch_existing_count(self) -> int:
        return sum(1 for item in self.outcomes if bool(item.extra.get("would_patch_existing_node")))

    @property
    def predicted_create_canonical_count(self) -> int:
        return sum(1 for item in self.outcomes if bool(item.extra.get("would_create_canonical_page")))

    @property
    def has_failures(self) -> bool:
        return (self.failed_count + self.unsupported_count) > 0

    @property
    def last_item_ref(self) -> str | None:
        if not self.outcomes:
            return None
        return self.outcomes[-1].source_path

    @property
    def metadata(self) -> dict[str, object]:
        failed_items = [
            {
                "source_path": item.source_path,
                "classification": item.classification,
                "disposition": item.disposition,
                "detail": item.detail,
            }
            for item in self.outcomes
            if item.disposition in {"failed", "unsupported"}
        ]
        review_items = [
            {
                "source_path": item.source_path,
                "classification": item.classification,
                "disposition": item.disposition,
                "detail": item.detail,
                "review_reasons": list(item.extra.get("review_reasons") or []),
                "candidate_summaries": list(item.extra.get("candidate_summaries") or []),
            }
            for item in self.outcomes
            if item.disposition in {"review", "would_review"}
        ]
        routes: dict[str, int] = {}
        for item in self.outcomes:
            key = item.route or item.classification
            routes[key] = routes.get(key, 0) + 1
        payload = {
            "dry_run": self.dry_run,
            "scanned_count": self.scanned_count,
            "pending_count_before": self.pending_count_before,
            "pending_count_after": self.pending_count_after,
            "processed_count": self.processed_count,
            "failed_count": self.failed_count,
            "unsupported_count": self.unsupported_count,
            "review_count": self.review_count,
            "skipped_count": self.skipped_count,
            "routes": routes,
            "failed_items": failed_items[:10],
            "review_items": review_items[:10],
            "report_json_path": str(self.report_json_path),
            "report_markdown_path": str(self.report_markdown_path),
        }
        if self.dry_run:
            payload.update(
                {
                    "would_process_count": self.predicted_process_count,
                    "would_review_count": self.predicted_review_count,
                    "would_fail_count": self.predicted_fail_count,
                    "would_patch_existing_node_count": self.predicted_patch_existing_count,
                    "would_create_canonical_page_count": self.predicted_create_canonical_count,
                }
            )
        return payload

    def render(self) -> str:
        lines = [
            "dropbox-sweep:",
        ]
        if self.dry_run:
            lines.extend(
                [
                    "- mode=dry-run",
                    f"- scanned={self.scanned_count}",
                    f"- pending_before={self.pending_count_before}",
                    f"- would_process={self.predicted_process_count}",
                    f"- would_review={self.predicted_review_count}",
                    f"- would_fail={self.predicted_fail_count}",
                    f"- would_patch_existing_node={self.predicted_patch_existing_count}",
                    f"- would_create_canonical_page={self.predicted_create_canonical_count}",
                    f"- pending_after={self.pending_count_after}",
                    f"- report_json={self.report_json_path}",
                    f"- report_md={self.report_markdown_path}",
                ]
            )
        else:
            lines.extend(
                [
                    f"- scanned={self.scanned_count}",
                    f"- pending_before={self.pending_count_before}",
                    f"- processed={self.processed_count}",
                    f"- failed={self.failed_count}",
                    f"- unsupported={self.unsupported_count}",
                    f"- review={self.review_count}",
                    f"- pending_after={self.pending_count_after}",
                    f"- report_json={self.report_json_path}",
                    f"- report_md={self.report_markdown_path}",
                ]
            )
        return "\n".join(lines)


@dataclass(frozen=True)
class DropboxStatus:
    pending_count: int
    pending_by_type: dict[str, int]
    last_sweep_at: str | None
    last_sweep_summary: str | None
    recent_failed_items: list[dict[str, object]]
    recent_review_items: list[dict[str, object]]

    def render(self) -> str:
        lines = [f"dropbox-status: pending={self.pending_count}"]
        if self.pending_by_type:
            lines.append(
                "pending_by_type: "
                + ", ".join(f"{name}={count}" for name, count in sorted(self.pending_by_type.items()))
            )
        if self.last_sweep_at:
            lines.append(f"last_sweep_at: {self.last_sweep_at}")
        if self.last_sweep_summary:
            lines.append(f"last_sweep_summary: {self.last_sweep_summary}")
        if self.recent_failed_items:
            lines.append("recent_failed_items:")
            for item in self.recent_failed_items[:5]:
                lines.append(
                    f"- {item.get('source_path')} — {item.get('disposition')} — {item.get('detail')}"
                )
        if self.recent_review_items:
            lines.append("recent_review_items:")
            for item in self.recent_review_items[:5]:
                reasons = list(item.get("review_reasons") or [])
                suffix = f" — reasons={'; '.join(reasons[:2])}" if reasons else ""
                lines.append(
                    f"- {item.get('source_path')} — {item.get('disposition')} — {item.get('detail')}{suffix}"
                )
        return "\n".join(lines)


@dataclass(frozen=True)
class DropboxMigrationMove:
    source_path: str
    disposition: str
    destination_path: str | None
    detail: str


@dataclass(frozen=True)
class DropboxMigrationResult:
    moved_count: int
    kept_count: int
    moves: list[DropboxMigrationMove]
    report_json_path: Path
    report_markdown_path: Path
    mirror_json_path: Path
    mirror_markdown_path: Path

    @property
    def metadata(self) -> dict[str, object]:
        return {
            "moved_count": self.moved_count,
            "kept_count": self.kept_count,
            "moves": [asdict(item) for item in self.moves[:20]],
            "report_json_path": str(self.report_json_path),
            "report_markdown_path": str(self.report_markdown_path),
        }

    def render(self) -> str:
        return "\n".join(
            [
                "dropbox-migrate-legacy:",
                f"- moved={self.moved_count}",
                f"- kept_machine_queue={self.kept_count}",
                f"- report_json={self.report_json_path}",
                f"- report_md={self.report_markdown_path}",
            ]
        )


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")


def dropbox_root(repo_root: Path) -> Path:
    return Vault.load(repo_root).dropbox


def ensure_dropbox_layout(repo_root: Path) -> Path:
    root = dropbox_root(repo_root)
    root.mkdir(parents=True, exist_ok=True)
    for dirname in DROPBOX_ARCHIVE_DIRS:
        (root / dirname).mkdir(parents=True, exist_ok=True)
    return root


def _path_under_dropbox(root: Path, path: Path) -> Path:
    try:
        relative = path.resolve().relative_to(root.resolve())
    except ValueError:
        return Path(path.name)
    if relative.parts and relative.parts[0] in DROPBOX_ARCHIVE_DIRS:
        if len(relative.parts) == 1:
            return Path(path.name)
        return Path(*relative.parts[1:])
    return relative


def _unique_destination(base: Path) -> Path:
    if not base.exists():
        return base
    stem = base.stem
    suffix = base.suffix
    counter = 2
    while True:
        candidate = base.with_name(f"{stem}-{counter}{suffix}")
        if not candidate.exists():
            return candidate
        counter += 1


def _archive_destination(root: Path, bucket: str, source_path: Path) -> Path:
    relative = _path_under_dropbox(root, source_path)
    destination = root / bucket / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    return _unique_destination(destination)


def _inbox_destination(root: Path, source_path: Path) -> Path:
    relative = _path_under_dropbox(root, source_path)
    destination = root / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    return _unique_destination(destination)


def _move_file(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source), str(destination))


def _is_reserved_dropbox_path(root: Path, path: Path) -> bool:
    try:
        relative = path.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return bool(relative.parts) and relative.parts[0] in DROPBOX_ARCHIVE_DIRS


def _iter_visible_dropbox_files(root: Path) -> Iterable[Path]:
    if not root.exists():
        return []
    visible: list[Path] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        if path.name.startswith("."):
            continue
        if _is_reserved_dropbox_path(root, path):
            continue
        visible.append(path)
    return visible


def _is_within_root(root: Path, path: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True


def _iter_targeted_dropbox_files(root: Path, target: Path) -> list[Path]:
    if target.is_file():
        return [] if target.name.startswith(".") else [target]

    include_reserved = _is_reserved_dropbox_path(root, target)
    visible: list[Path] = []
    for path in sorted(target.rglob("*")):
        if not path.is_file() or path.name.startswith("."):
            continue
        if not include_reserved and _is_reserved_dropbox_path(root, path):
            continue
        visible.append(path)
    return visible


def classify_dropbox_file(path: Path) -> DropboxPendingItem:
    name = path.name.lower()
    for pattern in MACHINE_QUEUE_PATTERNS:
        if fnmatch(name, pattern):
            return DropboxPendingItem(
                path=path,
                classification="machine-queue",
                route=None,
                detail="machine queue artifacts belong in raw/drops, not dropbox/",
            )

    if fnmatch(name, "youtube-*.json"):
        return DropboxPendingItem(
            path=path,
            classification="structured-export",
            route="youtube",
            detail="matched YouTube export filename",
        )
    if fnmatch(name, "substack-saved-*.json"):
        return DropboxPendingItem(
            path=path,
            classification="structured-export",
            route="substack",
            detail="matched Substack export filename",
        )
    if any(fnmatch(name, pattern) for pattern in BOOK_EXPORT_PATTERNS):
        return DropboxPendingItem(
            path=path,
            classification="structured-export",
            route="books",
            detail="matched books export filename",
        )

    suffix = path.suffix.lower()
    if suffix in SAFE_GENERIC_EXTENSIONS:
        return DropboxPendingItem(
            path=path,
            classification="generic-document",
            route="file",
            detail=f"safe generic extension {suffix}",
        )
    if suffix == ".json":
        return DropboxPendingItem(
            path=path,
            classification="unsupported",
            route=None,
            detail="JSON files must match a supported export filename such as youtube-*.json or substack-saved-*.json",
        )
    return DropboxPendingItem(
        path=path,
        classification="unsupported",
        route=None,
        detail=f"unsupported file type {suffix or '(no extension)'}",
    )


def scan_dropbox_pending(repo_root: Path) -> list[DropboxPendingItem]:
    root = ensure_dropbox_layout(repo_root)
    return [classify_dropbox_file(path) for path in _iter_visible_dropbox_files(root)]


def _run_route(item: DropboxPendingItem, *, graph_registry: GraphRegistry | None = None) -> tuple[list[str], dict[str, object]]:
    path = item.path
    assert item.route is not None
    if item.route == "file":
        target, details = ingest_file_with_details(path, graph_aware=True, graph_registry=graph_registry)
        return [str(target)], details
    if item.route == "books":
        result = ingest_books_export(path)
        return result.page_ids, {}
    if item.route == "youtube":
        result = ingest_youtube_export(path)
        details = [
            f"selected={result.selected_count}",
            f"executed={result.executed}",
            f"failed={result.failed}",
            f"pages_written={result.pages_written}",
        ]
        return details, {}
    if item.route == "substack":
        result = ingest_substack_export(export_path=path, drain_articles=False)
        return [
            f"selected={result.selected_count}",
            f"executed={result.executed}",
            f"failed={result.failed}",
            f"paywalled={result.paywalled}",
            f"posts_written={result.posts_written}",
        ], {}
    raise ValueError(f"unsupported route {item.route!r}")


def _preflight_route(item: DropboxPendingItem, *, graph_registry: GraphRegistry | None = None) -> tuple[str, str, dict[str, object]]:
    if item.route == "file":
        plan = preflight_file_ingest(item.path, graph_aware=True, graph_registry=graph_registry)
        if bool(plan.details.get("review_required")):
            reasons = list(plan.details.get("review_reasons") or [])
            detail = "graph review required"
            if reasons:
                detail = f"{detail}: {reasons[0]}"
            return "would_review", detail, plan.details
        detail = "would process via graph-aware file ingest"
        return "would_process", detail, plan.details
    if item.route in {"books", "youtube", "substack"}:
        return "would_process", f"would route via {item.route}", {}
    return "would_fail", item.detail, {}


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _write_markdown(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _report_paths(repo_root: Path, *, prefix: str, timestamp: str) -> tuple[Path, Path, Path, Path]:
    root = ensure_dropbox_layout(repo_root)
    report_name = f"{prefix}-{timestamp}"
    raw_root = raw_path(repo_root, "reports", "dropbox")
    return (
        raw_root / f"{report_name}.json",
        raw_root / f"{report_name}.md",
        root / ".reports" / f"{report_name}.json",
        root / ".reports" / f"{report_name}.md",
    )


def _render_sweep_markdown(result: DropboxSweepResult) -> list[str]:
    lines = [
        "# Dropbox Sweep Report",
        "",
        f"- Dry run: {'yes' if result.dry_run else 'no'}",
        f"- Scanned: {result.scanned_count}",
        f"- Pending before: {result.pending_count_before}",
        f"- Processed: {result.processed_count}",
        f"- Failed: {result.failed_count}",
        f"- Unsupported: {result.unsupported_count}",
        f"- Review: {result.review_count}",
        f"- Pending after: {result.pending_count_after}",
        "",
        "## Outcomes",
        "",
    ]
    if result.dry_run:
        lines[4:8] = [
            f"- Would process: {result.predicted_process_count}",
            f"- Would review: {result.predicted_review_count}",
            f"- Would fail: {result.predicted_fail_count}",
            f"- Would patch existing node: {result.predicted_patch_existing_count}",
            f"- Would create canonical page: {result.predicted_create_canonical_count}",
        ]
    if not result.outcomes:
        lines.append("- No dropbox files were found.")
        return lines
    for item in result.outcomes:
        lines.append(
            f"- `{item.source_path}` -> `{item.disposition}`"
            + (f" via `{item.route}`" if item.route else "")
            + f" — {item.detail}"
        )
        if item.destination_path:
            lines.append(f"  destination: `{item.destination_path}`")
        if item.outputs:
            lines.append(f"  outputs: {', '.join(item.outputs)}")
        if item.extra.get("canonical_page_target"):
            lines.append(f"  canonical_page_target: `{item.extra['canonical_page_target']}`")
        if item.extra.get("review_reasons"):
            lines.append(f"  review_reasons: {' | '.join(list(item.extra['review_reasons'])[:3])}")
        if item.extra.get("candidate_summaries"):
            lines.append(f"  candidates: {' | '.join(list(item.extra['candidate_summaries'])[:4])}")
    return lines


def _render_migration_markdown(result: DropboxMigrationResult) -> list[str]:
    lines = [
        "# Dropbox Legacy Migration Report",
        "",
        f"- Moved: {result.moved_count}",
        f"- Kept machine queue files: {result.kept_count}",
        "",
        "## Decisions",
        "",
    ]
    if not result.moves:
        lines.append("- No legacy files were found.")
        return lines
    for item in result.moves:
        lines.append(f"- `{item.source_path}` -> `{item.disposition}` — {item.detail}")
        if item.destination_path:
            lines.append(f"  destination: `{item.destination_path}`")
    return lines


def sweep_dropbox(
    repo_root: Path,
    *,
    dry_run: bool = False,
    limit: int | None = None,
    target_path: Path | None = None,
    phase_callback: Callable[[str], None] | None = None,
) -> DropboxSweepResult:
    if phase_callback is not None:
        phase_callback("scanning dropbox")
    root = ensure_dropbox_layout(repo_root)
    pending_before = scan_dropbox_pending(repo_root)
    if target_path is None:
        selected_paths = [item.path for item in pending_before]
    else:
        resolved = target_path.resolve()
        if not resolved.exists():
            raise FileNotFoundError(f"dropbox target not found: {target_path}")
        if not _is_within_root(root, resolved):
            raise ValueError("dropbox target must stay within dropbox/")
        selected_paths = _iter_targeted_dropbox_files(root, resolved)

    if limit is not None:
        selected_paths = selected_paths[: max(limit, 0)]

    outcomes: list[DropboxItemOutcome] = []
    needs_graph = any(classify_dropbox_file(path).route == "file" for path in selected_paths)
    graph_registry = GraphRegistry.for_repo_root(repo_root) if needs_graph else None
    if graph_registry is not None:
        graph_registry.rebuild()
    if phase_callback is not None and selected_paths:
        phase_callback("routing files to ingest lanes")
    for path in selected_paths:
        item = classify_dropbox_file(path)
        if item.route is None:
            destination: Path | None = None
            if not dry_run:
                destination = _archive_destination(root, ".failed", path)
                _move_file(path, destination)
            outcomes.append(
                DropboxItemOutcome(
                    source_path=str(path),
                    classification=item.classification,
                    route=item.route,
                    disposition="would_fail" if dry_run else "unsupported",
                    detail=item.detail,
                    destination_path=str(destination) if destination else None,
                )
            )
            continue

        if dry_run:
            try:
                disposition, detail, extra = _preflight_route(item, graph_registry=graph_registry)
            except Exception as exc:
                disposition, detail, extra = "would_fail", f"{type(exc).__name__}: {exc}", {}
            outcomes.append(
                DropboxItemOutcome(
                    source_path=str(path),
                    classification=item.classification,
                    route=item.route,
                    disposition=disposition,
                    detail=detail,
                    extra=extra,
                )
            )
            continue

        try:
            outputs, details = _run_route(item, graph_registry=graph_registry)
            destination_bucket = ".review" if details.get("review_required") else ".processed"
            destination = _archive_destination(root, destination_bucket, path)
            _move_file(path, destination)
            outcomes.append(
                DropboxItemOutcome(
                    source_path=str(path),
                    classification=item.classification,
                    route=item.route,
                    disposition="review" if details.get("review_required") else "processed",
                    detail="graph review required" if details.get("review_required") else item.detail,
                    destination_path=str(destination),
                    outputs=outputs + list(details.get("review_artifacts") or []),
                    extra=details,
                )
            )
            if graph_registry is not None:
                graph_registry.rebuild()
        except Exception as exc:
            destination = _archive_destination(root, ".failed", path)
            _move_file(path, destination)
            outcomes.append(
                DropboxItemOutcome(
                    source_path=str(path),
                    classification=item.classification,
                    route=item.route,
                    disposition="failed",
                    detail=f"{type(exc).__name__}: {exc}",
                    destination_path=str(destination),
                )
            )

    timestamp = _utc_timestamp()
    report_json_path, report_markdown_path, mirror_json_path, mirror_markdown_path = _report_paths(
        repo_root,
        prefix="dropbox-sweep",
        timestamp=timestamp,
    )
    pending_after = scan_dropbox_pending(repo_root)
    result = DropboxSweepResult(
        dry_run=dry_run,
        dropbox_root=root,
        scanned_count=len(selected_paths),
        pending_count_before=len(pending_before),
        pending_count_after=len(pending_after),
        outcomes=outcomes,
        report_json_path=report_json_path,
        report_markdown_path=report_markdown_path,
        mirror_json_path=mirror_json_path,
        mirror_markdown_path=mirror_markdown_path,
    )
    payload = {
        "generated_at": timestamp,
        "dry_run": dry_run,
        "dropbox_root": str(root),
        "summary": result.metadata,
        "outcomes": [asdict(item) for item in result.outcomes],
    }
    markdown_lines = _render_sweep_markdown(result)
    for path, writer in (
        (report_json_path, lambda target: _write_json(target, payload)),
        (mirror_json_path, lambda target: _write_json(target, payload)),
        (report_markdown_path, lambda target: _write_markdown(target, markdown_lines)),
        (mirror_markdown_path, lambda target: _write_markdown(target, markdown_lines)),
    ):
        writer(path)
    return result


def build_dropbox_status(repo_root: Path, *, queue_metadata: dict[str, object] | None = None) -> DropboxStatus:
    pending = scan_dropbox_pending(repo_root)
    pending_by_type: dict[str, int] = {}
    for item in pending:
        pending_by_type[item.type_label] = pending_by_type.get(item.type_label, 0) + 1
    metadata = queue_metadata or {}
    summary = None
    if metadata:
        if metadata.get("dry_run"):
            summary = (
                f"would_process={metadata.get('would_process_count', 0)} "
                f"would_review={metadata.get('would_review_count', 0)} "
                f"would_fail={metadata.get('would_fail_count', 0)} "
                f"pending_after={metadata.get('pending_count_after', len(pending))}"
            )
        else:
            summary = (
                f"processed={metadata.get('processed_count', 0)} "
                f"failed={metadata.get('failed_count', 0)} "
                f"unsupported={metadata.get('unsupported_count', 0)} "
                f"review={metadata.get('review_count', 0)} "
                f"pending_after={metadata.get('pending_count_after', len(pending))}"
            )
    return DropboxStatus(
        pending_count=len(pending),
        pending_by_type=pending_by_type,
        last_sweep_at=str(metadata.get("updated_at") or metadata.get("last_sweep_at") or "") or None,
        last_sweep_summary=summary,
        recent_failed_items=list(metadata.get("failed_items") or []),
        recent_review_items=list(metadata.get("review_items") or []),
    )


def dropbox_queue_status(result: DropboxSweepResult) -> str:
    if result.pending_count_after > 0:
        return "queued"
    if result.review_count > 0 or result.has_failures:
        return "degraded"
    return "ready"


def dropbox_phase_status(result: DropboxSweepResult) -> str:
    if result.has_failures or result.review_count > 0:
        return "failed"
    if result.scanned_count == 0:
        return "skipped"
    return "completed"


def dropbox_phase_message(result: DropboxSweepResult) -> str:
    return (
        f"scanned={result.scanned_count} processed={result.processed_count} "
        f"failed={result.failed_count} unsupported={result.unsupported_count} review={result.review_count}"
    )


def migrate_legacy_dropbox_files(repo_root: Path) -> DropboxMigrationResult:
    root = ensure_dropbox_layout(repo_root)
    legacy_root = raw_path(repo_root, "drops")
    moves: list[DropboxMigrationMove] = []
    moved_count = 0
    kept_count = 0
    if legacy_root.exists():
        for path in sorted(legacy_root.glob("*")):
            if not path.is_file():
                continue
            item = classify_dropbox_file(path)
            if item.classification == "machine-queue":
                kept_count += 1
                moves.append(
                    DropboxMigrationMove(
                        source_path=str(path),
                        disposition="kept",
                        destination_path=None,
                        detail=item.detail,
                    )
                )
                continue
            destination = _inbox_destination(root, path).resolve()
            _move_file(path, destination)
            moved_count += 1
            moves.append(
                DropboxMigrationMove(
                    source_path=str(path),
                    disposition="moved",
                    destination_path=str(destination),
                    detail="moved from legacy raw/drops/ into dropbox/",
                )
            )

    timestamp = _utc_timestamp()
    report_json_path, report_markdown_path, mirror_json_path, mirror_markdown_path = _report_paths(
        repo_root,
        prefix="dropbox-migrate-legacy",
        timestamp=timestamp,
    )
    result = DropboxMigrationResult(
        moved_count=moved_count,
        kept_count=kept_count,
        moves=moves,
        report_json_path=report_json_path,
        report_markdown_path=report_markdown_path,
        mirror_json_path=mirror_json_path,
        mirror_markdown_path=mirror_markdown_path,
    )
    payload = {
        "generated_at": timestamp,
        "legacy_root": str(legacy_root),
        "summary": result.metadata,
        "moves": [asdict(item) for item in result.moves],
    }
    markdown_lines = _render_migration_markdown(result)
    for path, writer in (
        (report_json_path, lambda target: _write_json(target, payload)),
        (mirror_json_path, lambda target: _write_json(target, payload)),
        (report_markdown_path, lambda target: _write_markdown(target, markdown_lines)),
        (mirror_markdown_path, lambda target: _write_markdown(target, markdown_lines)),
    ):
        writer(path)
    return result
