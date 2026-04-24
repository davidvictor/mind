from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
from typing import Any

from mind.runtime_state import RuntimeState
from mind.services.graph_registry import GraphRegistry
from scripts.common.frontmatter import read_page
from scripts.common.vault import Vault
from scripts.common.wiki_writer import write_page


WEAVE_FRONTMATTER_FIELDS = ("weave_cluster_refs", "last_weaved_at")


@dataclass
class WeaveCleanupReport:
    applied: bool
    generated_at: str
    scanned_pages: int = 0
    pages_to_update: list[str] = field(default_factory=list)
    pages_updated: list[str] = field(default_factory=list)
    removed_frontmatter_fields: int = 0
    removed_relation_refs: int = 0
    archived_from: str = ""
    archived_to: str = ""
    runtime_state_cleared: bool = False
    runtime_locks_cleared: int = 0
    graph_rebuilt: bool = False
    report_path: str = ""

    def render(self) -> str:
        lines = [
            "Weave cleanup summary",
            f"Mode: {'apply' if self.applied else 'dry-run'}",
            f"Scanned pages: {self.scanned_pages}",
            f"Pages to update: {len(self.pages_to_update)}",
            f"Pages updated: {len(self.pages_updated)}",
            f"Removed frontmatter fields: {self.removed_frontmatter_fields}",
            f"Removed relation refs: {self.removed_relation_refs}",
            f"Archived current Weave pages: {'yes' if self.archived_to else 'no'}",
            f"Runtime Weave state cleared: {'yes' if self.runtime_state_cleared else 'no'}",
            f"Runtime Weave locks cleared: {self.runtime_locks_cleared}",
            f"Graph rebuilt: {'yes' if self.graph_rebuilt else 'no'}",
            f"Report: {self.report_path}",
        ]
        if self.archived_from or self.archived_to:
            lines.append(f"Archive: {self.archived_from or '-'} -> {self.archived_to or '-'}")
        if self.pages_to_update:
            lines.append("Samples:")
            lines.extend(f"- {path}" for path in self.pages_to_update[:20])
        return "\n".join(lines)


def run_weave_cleanup(repo_root: Path, *, apply: bool) -> WeaveCleanupReport:
    vault = Vault.load(repo_root)
    report = WeaveCleanupReport(applied=apply, generated_at=_utc_now())

    for path in sorted(vault.wiki.rglob("*.md")):
        if "/.archive/" in path.as_posix():
            continue
        frontmatter, body = read_page(path)
        if not frontmatter:
            continue
        report.scanned_pages += 1
        cleaned, removed_fields, removed_refs = _clean_frontmatter(frontmatter)
        if removed_fields == 0 and removed_refs == 0:
            continue
        logical = vault.logical_path(path)
        report.pages_to_update.append(logical)
        report.removed_frontmatter_fields += removed_fields
        report.removed_relation_refs += removed_refs
        if apply:
            write_page(path, frontmatter=cleaned, body=body, force=True)
            report.pages_updated.append(logical)

    weave_dir = vault.wiki / "dreams" / "weave"
    if weave_dir.exists():
        report.archived_from = vault.logical_path(weave_dir)
        archive_dir = _next_archive_dir(vault)
        report.archived_to = vault.logical_path(archive_dir)
        if apply:
            archive_dir.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(weave_dir), str(archive_dir))

    if apply:
        report.runtime_state_cleared, report.runtime_locks_cleared = _clear_legacy_runtime_state(repo_root)
        if report.pages_updated or report.archived_to:
            GraphRegistry.for_repo_root(repo_root).rebuild()
            report.graph_rebuilt = True

    report_path = _report_path(vault)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report.report_path = vault.logical_path(report_path)
    report_path.write_text(json.dumps(asdict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report


def _clean_frontmatter(frontmatter: dict[str, Any]) -> tuple[dict[str, Any], int, int]:
    cleaned = dict(frontmatter)
    removed_fields = 0
    for key in WEAVE_FRONTMATTER_FIELDS:
        if key in cleaned:
            cleaned.pop(key, None)
            removed_fields += 1
    removed_refs = 0
    relates_to = cleaned.get("relates_to")
    if isinstance(relates_to, list):
        kept: list[Any] = []
        for item in relates_to:
            if _is_weave_reference(str(item)):
                removed_refs += 1
                continue
            kept.append(item)
        if removed_refs:
            cleaned["relates_to"] = kept
    return cleaned, removed_fields, removed_refs


def _is_weave_reference(value: str) -> bool:
    lowered = value.lower()
    if "dreams/weave" in lowered:
        return True
    if "[[weave-" in lowered or "[[window-" in lowered:
        return True
    return lowered.startswith("weave-") or lowered.startswith("window-")


def _next_archive_dir(vault: Vault) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    base = vault.wiki / ".archive" / "weave-experiments" / stamp
    if not base.exists():
        return base
    index = 2
    while True:
        candidate = base.with_name(f"{base.name}-{index}")
        if not candidate.exists():
            return candidate
        index += 1


def _clear_legacy_runtime_state(repo_root: Path) -> tuple[bool, int]:
    state = RuntimeState.for_repo_root(repo_root)
    cleared_state = False
    cleared_locks = 0
    with state.connect() as conn:
        columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(dream_state)").fetchall()}
        if "last_weave" in columns:
            row = conn.execute("SELECT last_weave FROM dream_state WHERE id = 1").fetchone()
            if row is not None and row["last_weave"]:
                conn.execute("UPDATE dream_state SET last_weave = NULL WHERE id = 1")
                cleared_state = True
        lock_rows = conn.execute(
            """
            SELECT name FROM locks
            WHERE lower(name) LIKE '%weave%' OR lower(holder) LIKE '%weave%'
            """
        ).fetchall()
        cleared_locks = len(lock_rows)
        if cleared_locks:
            conn.execute(
                """
                DELETE FROM locks
                WHERE lower(name) LIKE '%weave%' OR lower(holder) LIKE '%weave%'
                """
            )
        if {"last_lock_holder", "last_lock_acquired_at"}.issubset(columns):
            row = conn.execute("SELECT last_lock_holder FROM dream_state WHERE id = 1").fetchone()
            if row is not None and "weave" in str(row["last_lock_holder"] or "").lower():
                conn.execute(
                    "UPDATE dream_state SET last_lock_holder = NULL, last_lock_acquired_at = NULL WHERE id = 1"
                )
                cleared_state = True
    return cleared_state, cleared_locks


def _report_path(vault: Vault) -> Path:
    return vault.reports_root / "repair-weave-cleanup-report.json"


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
