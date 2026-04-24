from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Iterator

from mind.services.cli_progress import progress_for_args
from mind.runtime_state import RuntimeState
from mind.services.dropbox import (
    build_dropbox_status,
    dropbox_phase_message,
    dropbox_phase_status,
    dropbox_queue_status,
    migrate_legacy_dropbox_files,
    sweep_dropbox,
)

from . import common as command_common


def _project_root() -> Path:
    return command_common.project_root()


def _runtime_state() -> RuntimeState:
    return RuntimeState.for_repo_root(_project_root())


def _utc_now_string() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@contextmanager
def _tracked_run(*, kind: str, holder: str, metadata: dict[str, object] | None = None) -> Iterator[tuple[RuntimeState, int]]:
    state = _runtime_state()
    run_id = state.create_run(kind=kind, holder=holder, metadata=metadata)
    state.add_run_event(run_id, stage="dropbox", event_type="started", message=f"{holder} started")
    try:
        yield state, run_id
    except Exception as exc:
        state.add_run_event(
            run_id,
            stage="dropbox",
            event_type="failed",
            message=f"{holder} failed: {type(exc).__name__}",
        )
        state.add_error(
            run_id=run_id,
            stage="dropbox",
            error_type=type(exc).__name__,
            message=str(exc),
        )
        state.finish_run(run_id, status="failed", notes=str(exc))
        raise


def _finalize_run(
    state: RuntimeState,
    run_id: int,
    *,
    holder: str,
    status: str,
    notes: str,
) -> None:
    event_type = "completed" if status == "completed" else "failed"
    state.add_run_event(run_id, stage="dropbox", event_type=event_type, message=notes)
    if status != "completed":
        state.add_error(run_id=run_id, stage="dropbox", error_type="CommandFailed", message=notes)
    state.finish_run(run_id, status=status, notes=notes)


def _load_queue_metadata(queue_item) -> dict[str, object]:
    metadata: dict[str, object] = {}
    if queue_item and queue_item.metadata_json:
        try:
            loaded = json.loads(queue_item.metadata_json)
        except json.JSONDecodeError:
            loaded = {}
        if isinstance(loaded, dict):
            metadata = loaded
    if queue_item is not None:
        metadata.setdefault("updated_at", queue_item.updated_at)
    return metadata


def cmd_dropbox_sweep(args: argparse.Namespace) -> int:
    target_path = Path(args.path).resolve() if args.path else None
    metadata = {
        "dry_run": bool(args.dry_run),
        "limit": args.limit,
        "path": str(target_path) if target_path else None,
    }
    with progress_for_args(args, message="sweeping dropbox", default=True) as progress:
        progress.phase("scanning dropbox")
        with _tracked_run(kind="dropbox.sweep", holder="mind-dropbox-sweep", metadata=metadata) as (state, run_id):
            try:
                result = sweep_dropbox(
                    _project_root(),
                    dry_run=bool(args.dry_run),
                    limit=args.limit,
                    target_path=target_path,
                    phase_callback=progress.phase,
                )
            except (FileNotFoundError, ValueError) as exc:
                _finalize_run(
                    state,
                    run_id,
                    holder="mind-dropbox-sweep",
                    status="failed",
                    notes=str(exc),
                )
                print(str(exc))
                return 1
            queue_metadata = dict(result.metadata)
            queue_metadata["last_sweep_at"] = _utc_now_string()
            state.upsert_queue_state(
                name="dropbox",
                status=dropbox_queue_status(result),
                pending_count=result.pending_count_after,
                last_item_ref=result.last_item_ref,
                last_run_id=run_id,
                metadata=queue_metadata,
            )
            state.add_run_event(
                run_id,
                stage="dropbox",
                event_type="swept",
                message=dropbox_phase_message(result),
                payload=result.metadata,
            )
            status = dropbox_phase_status(result)
            _finalize_run(state, run_id, holder="mind-dropbox-sweep", status=status, notes=result.render())
            print(result.render())
            return 0 if not result.has_failures and result.review_count == 0 else 1


def cmd_dropbox_status(_args: argparse.Namespace) -> int:
    state = _runtime_state()
    queue_item = next((item for item in state.list_queue() if item.name == "dropbox"), None)
    status = build_dropbox_status(_project_root(), queue_metadata=_load_queue_metadata(queue_item))
    run_id = state.create_run(kind="dropbox.status", holder="mind-dropbox-status")
    state.add_run_event(run_id, stage="dropbox", event_type="status", message=f"pending={status.pending_count}")
    state.finish_run(run_id, status="completed", notes=status.render())
    print(status.render())
    return 0


def cmd_dropbox_migrate_legacy(_args: argparse.Namespace) -> int:
    with _tracked_run(kind="dropbox.migrate_legacy", holder="mind-dropbox-migrate-legacy") as (state, run_id):
        result = migrate_legacy_dropbox_files(_project_root())
        state.add_run_event(
            run_id,
            stage="dropbox",
            event_type="migrated",
            message=f"moved={result.moved_count} kept={result.kept_count}",
            payload=result.metadata,
        )
        state.upsert_queue_state(
            name="dropbox",
            status="queued",
            pending_count=build_dropbox_status(_project_root()).pending_count,
            last_item_ref=None,
            last_run_id=run_id,
            metadata={
                "last_migration_at": _utc_now_string(),
                "moved_count": result.moved_count,
                "kept_count": result.kept_count,
                "report_json_path": str(result.report_json_path),
                "report_markdown_path": str(result.report_markdown_path),
            },
        )
        _finalize_run(
            state,
            run_id,
            holder="mind-dropbox-migrate-legacy",
            status="completed",
            notes=result.render(),
        )
        print(result.render())
        return 0
