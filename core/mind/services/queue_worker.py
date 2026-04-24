from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
import traceback
from typing import Callable

# TODO: these ingest operations should live in mind/services/, not mind/commands/
from mind.commands.ingest import (
    ingest_articles_queue,
    ingest_audible_library,
    ingest_books_export,
    cmd_ingest_repair_articles,
    ingest_file,
    cmd_ingest_reingest,
    import_links,
    ingest_substack_export,
    ingest_youtube_export,
)
from mind.commands.onboard import cmd_onboard
from mind.commands.skill import cmd_skill_generate
from mind.runtime_state import (
    DEFAULT_LOCK_NAME,
    DEFAULT_RETRYABLE_LOCK_ATTEMPTS,
    RuntimeState,
    RuntimeStateLockBusy,
)


@dataclass(frozen=True)
class QueueProcessResult:
    processed: int
    failures: int

    @property
    def exit_code(self) -> int:
        return 0 if self.failures == 0 else 1


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _retry_backoff_seconds(retry_count: int) -> int:
    return min(2 * max(retry_count, 1), 10)


def queue_name_for(kind: str, metadata: dict | None = None) -> str | None:
    metadata = metadata or {}
    if kind.startswith("mcp.start_ingest."):
        return f"ingest:{kind.rsplit('.', 1)[-1]}"
    if kind == "mcp.start_reingest":
        return "ingest:reingest"
    if kind == "mcp.start_article_repair":
        return "ingest:repair-articles"
    if kind.startswith("mcp.start_dream."):
        return f"dream:{kind.rsplit('.', 1)[-1]}"
    if kind in {"mcp.generate_skill", "mcp.set_skill_status"}:
        return "skills"
    if kind == "mcp.run_onboard":
        return "onboard"
    if kind == "mcp.clear_stale_lock":
        return "admin"
    if kind == "mcp.retry_queue_item":
        return str(metadata.get("queue_name") or "admin")
    if kind == "mcp.enqueue_links":
        return "links"
    return None


def _split_frontmatter(text: str) -> tuple[dict, str]:
    if not text.startswith("---\n"):
        return {}, text
    end = text.find("\n---\n", 4)
    if end == -1:
        return {}, text
    import yaml

    return yaml.safe_load(text[4:end]) or {}, text[end + 5 :]


def _write_frontmatter(path: Path, frontmatter: dict, body: str) -> None:
    lines = ["---"]
    for key, value in frontmatter.items():
        if isinstance(value, list):
            if not value:
                lines.append(f"{key}: []")
            else:
                lines.append(f"{key}:")
                lines.extend(f"  - {item}" for item in value)
        else:
            lines.append(f"{key}: {value}")
    lines.append("---")
    path.write_text("\n".join(lines) + "\n\n" + body.rstrip() + "\n", encoding="utf-8")


def set_skill_status(repo_root: Path, skill_id: str, status: str) -> int:
    path = repo_root / "skills" / skill_id / "SKILL.md"
    if not path.exists():
        return 1
    frontmatter, body = _split_frontmatter(path.read_text(encoding="utf-8"))
    frontmatter["status"] = status
    _write_frontmatter(path, frontmatter, body)
    return 0


def retry_queue_item(state: RuntimeState, *, run_id: int) -> int:
    details = state.get_run(run_id)
    if details is None:
        return 1
    original = details.run
    if original.kind == "mcp.retry_queue_item" or original.status not in {"failed", "blocked"}:
        return 1
    metadata = json.loads(original.metadata_json) if original.metadata_json else {}
    state.enqueue_run(
        queue_name=original.queue_name or queue_name_for(original.kind, metadata) or "admin",
        kind=original.kind,
        holder=original.holder or "mcp",
        notes=f"retry for run {run_id}",
        metadata=metadata,
        last_item_ref=original.item_ref,
    )
    return 0


def _append_enqueued_links(*, path: Path, links: list[dict[str, object]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for link in links:
            handle.write(json.dumps(link, ensure_ascii=False) + "\n")
    return len(links)


def dispatch_run(repo_root: Path, run_kind: str, metadata: dict | None) -> int:
    metadata = metadata or {}
    if run_kind == "mcp.enqueue_links":
        _append_enqueued_links(path=Path(metadata["path"]), links=list(metadata.get("links") or []))
        return 0
    if run_kind == "mcp.start_ingest.file":
        ingest_file(Path(metadata["path"]))
        return 0
    if run_kind == "mcp.start_ingest.youtube":
        options = metadata.get("options") or {}
        result = ingest_youtube_export(
            Path(metadata["path"]),
            default_duration_minutes=float(options.get("default_duration_minutes", 30.0) or 30.0),
            resume=bool(options.get("resume", True)),
            skip_materialized=bool(options.get("skip_materialized", True)),
            refresh_stale=bool(options.get("refresh_stale", False)),
            recompute_missing=bool(options.get("recompute_missing", False)),
            from_stage=options.get("from_stage"),
            through=str(options.get("through", "propagate")),
            source_ids=tuple(options.get("source_ids") or ()),
            external_ids=tuple(options.get("external_ids") or ()),
            selection=tuple(options.get("selection") or ("all",)),
        )
        return 0 if result.failed == 0 else 1
    if run_kind == "mcp.start_ingest.books":
        options = metadata.get("options") or {}
        ingest_books_export(
            Path(metadata["path"]),
            force_deep=bool(options.get("force_deep")),
            resume=bool(options.get("resume", True)),
            skip_materialized=bool(options.get("skip_materialized", True)),
            refresh_stale=bool(options.get("refresh_stale", False)),
            recompute_missing=bool(options.get("recompute_missing", False)),
            from_stage=options.get("from_stage"),
            through=str(options.get("through", "propagate")),
            source_ids=tuple(options.get("source_ids") or ()),
            external_ids=tuple(options.get("external_ids") or ()),
            selection=tuple(options.get("selection") or ("all",)),
        )
        return 0
    if run_kind == "mcp.start_ingest.substack":
        options = metadata.get("options") or {}
        result = ingest_substack_export(
            export_path=Path(metadata["path"]) if metadata.get("path") else None,
            today=metadata.get("today"),
            drain_articles=bool(options.get("drain_articles", True)),
            resume=bool(options.get("resume", True)),
            skip_materialized=bool(options.get("skip_materialized", True)),
            refresh_stale=bool(options.get("refresh_stale", False)),
            recompute_missing=bool(options.get("recompute_missing", False)),
            from_stage=options.get("from_stage"),
            through=str(options.get("through", "propagate")),
            source_ids=tuple(options.get("source_ids") or ()),
            external_ids=tuple(options.get("external_ids") or ()),
            selection=tuple(options.get("selection") or ("all",)),
        )
        return 0 if result.failures == 0 else 1
    if run_kind == "mcp.start_ingest.audible":
        options = metadata.get("options") or {}
        ingest_audible_library(
            library_only=bool(options.get("library_only")),
            sleep=options.get("sleep"),
            force_deep=bool(options.get("force_deep")),
            resume=bool(options.get("resume", True)),
            skip_materialized=bool(options.get("skip_materialized", True)),
            refresh_stale=bool(options.get("refresh_stale", False)),
            recompute_missing=bool(options.get("recompute_missing", False)),
            from_stage=options.get("from_stage"),
            through=str(options.get("through", "propagate")),
            source_ids=tuple(options.get("source_ids") or ()),
            external_ids=tuple(options.get("external_ids") or ()),
            selection=tuple(options.get("selection") or ("all",)),
        )
        return 0
    if run_kind == "mcp.start_ingest.articles":
        result = ingest_articles_queue(today=metadata.get("today"))
        return 0 if result.failed == 0 else 1
    if run_kind == "mcp.start_ingest.links":
        options = metadata.get("options") or {}
        _imported, result = import_links(
            Path(metadata["path"]),
            today=metadata.get("today"),
            ingest=bool(options.get("ingest")),
        )
        if result is None:
            return 0
        return 0 if result.failed == 0 else 1
    if run_kind == "mcp.start_reingest":
        return cmd_ingest_reingest(
            SimpleNamespace(
                lane=metadata["lane"],
                path=metadata.get("path"),
                today=metadata.get("today"),
                stage=metadata.get("stage", "acquire"),
                through=metadata.get("through", "propagate"),
                limit=metadata.get("limit"),
                source_ids=metadata.get("source_ids") or [],
                dry_run=bool(metadata.get("dry_run", True)),
            )
        )
    if run_kind == "mcp.start_article_repair":
        return cmd_ingest_repair_articles(
            SimpleNamespace(
                path=metadata.get("path"),
                today=metadata.get("today"),
                limit=metadata.get("limit"),
                source_ids=metadata.get("source_ids") or [],
                apply=bool(metadata.get("apply", False)),
                dry_run=not bool(metadata.get("apply", False)),
            )
        )
    if run_kind == "mcp.generate_skill":
        return cmd_skill_generate(
            SimpleNamespace(
                prompt=metadata["prompt"],
                name=metadata.get("name"),
                description=metadata.get("description"),
                context=metadata.get("context", ""),
                stdout=False,
                force=True,
            )
        )
    if run_kind == "mcp.set_skill_status":
        return set_skill_status(repo_root, metadata["skill_id"], metadata["status"])
    if run_kind == "mcp.run_onboard":
        return cmd_onboard(
            SimpleNamespace(
                from_json=metadata["input_path"],
                force=bool(metadata.get("force")),
            )
        )
    if run_kind == "mcp.clear_stale_lock":
        state = RuntimeState.for_repo_root(repo_root)
        state.clear_stale_lock(name=metadata.get("lock_name", DEFAULT_LOCK_NAME))
        return 0
    if run_kind == "mcp.retry_queue_item":
        state = RuntimeState.for_repo_root(repo_root)
        return retry_queue_item(state, run_id=int(metadata["run_id"]))
    return 1


def _process_claimed_run(
    state: RuntimeState,
    repo_root: Path,
    run_id: int,
    run_kind: str,
    metadata: dict | None,
    *,
    acquire_lock: bool = True,
) -> int:
    queue_name = queue_name_for(run_kind, metadata)
    lock_required = acquire_lock and run_kind != "mcp.clear_stale_lock"
    holder = f"worker:{run_id}"
    state.add_run_event(
        run_id,
        stage="worker",
        event_type="started",
        message=f"worker started {run_kind}",
        payload={"queue_name": queue_name, "run_id": run_id},
    )
    try:
        if lock_required:
            state.acquire_lock(holder=holder)
        rc = dispatch_run(repo_root, run_kind, metadata)
    except RuntimeStateLockBusy as exc:
        details = state.get_run(run_id)
        retry_count = (details.run.retry_count if details is not None else 0) + 1
        if retry_count <= DEFAULT_RETRYABLE_LOCK_ATTEMPTS and run_kind != "mcp.clear_stale_lock":
            next_attempt_at = (_utc_now() + timedelta(seconds=_retry_backoff_seconds(retry_count))).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            state.retry_queued_run(
                run_id,
                retry_count=retry_count,
                next_attempt_at=next_attempt_at,
                notes=str(exc),
            )
            state.add_run_event(
                run_id,
                stage="worker",
                event_type="retry_scheduled",
                message=str(exc),
                payload={"retry_count": retry_count, "next_attempt_at": next_attempt_at},
            )
            return 0
        state.add_run_event(run_id, stage="worker", event_type="blocked", message=str(exc))
        state.add_error(run_id=run_id, stage="worker", error_type=type(exc).__name__, message=str(exc))
        state.complete_queued_run(run_id, status="blocked", notes=str(exc), queue_name=queue_name)
        return 1
    except Exception as exc:
        tb = traceback.format_exc()
        state.add_run_event(run_id, stage="worker", event_type="failed", message=str(exc))
        state.add_error(
            run_id=run_id,
            stage="worker",
            error_type=type(exc).__name__,
            message=str(exc),
            traceback=tb,
        )
        state.complete_queued_run(run_id, status="failed", notes=str(exc), queue_name=queue_name)
        return 1
    finally:
        if lock_required:
            state.release_lock(holder=holder)
    state.add_run_event(
        run_id,
        stage="worker",
        event_type="completed" if rc == 0 else "failed",
        message=f"exit_code={rc}",
    )
    if rc != 0:
        state.add_error(run_id=run_id, stage="worker", error_type="CommandFailed", message=f"exit_code={rc}")
    state.complete_queued_run(
        run_id,
        status="completed" if rc == 0 else "failed",
        notes=f"exit_code={rc}",
        queue_name=queue_name,
    )
    return rc


def process_one_queued_run(
    repo_root: Path,
    *,
    acquire_lock: bool = True,
    allowed_queue_prefixes: tuple[str, ...] | None = None,
    phase_callback: Callable[[str], None] | None = None,
) -> tuple[int, str]:
    state = RuntimeState.for_repo_root(repo_root)
    if phase_callback is not None:
        phase_callback("claiming queued work")
    run = state.claim_oldest_queued_run(allowed_queue_prefixes=allowed_queue_prefixes)
    if run is None:
        return 0, "worker: no queued runs"
    if phase_callback is not None:
        phase_callback(f"processing {run.kind}")
    state.add_run_event(
        run.id,
        stage="queue",
        event_type="claimed",
        message=f"worker claimed {run.kind}",
        payload={"queue_name": run.queue_name, "item_ref": run.item_ref},
    )
    metadata = json.loads(run.metadata_json) if run.metadata_json else {}
    rc = _process_claimed_run(
        state,
        repo_root,
        run.id,
        run.kind,
        metadata,
        acquire_lock=acquire_lock,
    )
    details = state.get_run(run.id)
    if details is not None and details.run.status == "retry_scheduled":
        return 0, f"worker: retry scheduled for run {run.id} ({run.kind}) at {details.run.next_attempt_at}"
    if rc == 0:
        return 0, f"worker: processed run {run.id} ({run.kind}) -> 0"
    note = details.run.notes if details is not None else f"exit_code={rc}"
    return 1, f"worker: processed run {run.id} ({run.kind}) -> error: {note}"


def drain_until_empty(
    repo_root: Path,
    *,
    acquire_lock: bool = True,
    allowed_queue_prefixes: tuple[str, ...] | None = None,
    phase_callback: Callable[[str], None] | None = None,
) -> QueueProcessResult:
    processed = 0
    failures = 0
    while True:
        rc, message = process_one_queued_run(
            repo_root,
            acquire_lock=acquire_lock,
            allowed_queue_prefixes=allowed_queue_prefixes,
            phase_callback=phase_callback,
        )
        if message == "worker: no queued runs":
            break
        processed += 1
        if rc != 0:
            failures += 1
    return QueueProcessResult(processed=processed, failures=failures)
