"""Canonical CLI entrypoint for the Brain runtime."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import date
import json
from pathlib import Path
from typing import Any, Iterator, Sequence

from mind.commands.registry import register_additional_commands
from mind.dream.quality import QUALITY_ADAPTER, CANONICAL_LANES, LANE_DISPLAY
from mind.runtime_state import RuntimeState
from mind.services.cli_progress import progress_for_args
from mind.services.provider_ops import run_audible_pull, run_substack_pull, run_youtube_pull
from scripts.common.vault import Vault, project_root
from scripts.links.importer import append_links_drop, load_links
from mind.commands.ingest import (
    drain_web_discovery,
    ingest_articles_queue,
    ingest_chrome,
    ingest_search_signals,
    scan_chrome,
)


def _project_root() -> Path:
    return project_root()


def _repo_root() -> Path:
    """Return the content root for CLI commands (repo root in standard layout)."""
    project_root = _project_root()
    vault = Vault.load(project_root)
    if vault.wiki.parent == vault.raw.parent:
        return vault.wiki.parent
    return project_root


def _runtime_state() -> RuntimeState:
    return RuntimeState.for_repo_root(_project_root())


def _add_quiet_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--quiet", action="store_true")


@contextmanager
def _tracked_run(*, kind: str, holder: str, metadata: dict[str, object] | None = None) -> Iterator[tuple[RuntimeState, int]]:
    state = _runtime_state()
    run_id = state.create_run(kind=kind, holder=holder, metadata=metadata)
    state.add_run_event(run_id, stage="cli", event_type="started", message=f"{holder} started")
    try:
        yield state, run_id
    except Exception as exc:
        state.add_run_event(
            run_id,
            stage="cli",
            event_type="failed",
            message=f"{holder} failed: {type(exc).__name__}",
        )
        state.add_error(
            run_id=run_id,
            stage="cli",
            error_type=type(exc).__name__,
            message=str(exc),
        )
        state.finish_run(run_id, status="failed", notes=str(exc))
        raise


def _finalize_tracked_run(
    state: RuntimeState,
    run_id: int,
    *,
    holder: str,
    status: str,
    notes: str | None = None,
) -> None:
    event_type = "completed" if status == "completed" else "failed"
    message = notes or f"{holder} {event_type}"
    state.add_run_event(run_id, stage="cli", event_type=event_type, message=message)
    if status != "completed":
        state.add_error(run_id=run_id, stage="cli", error_type="CommandFailed", message=message)
    state.finish_run(run_id, status=status, notes=notes)


def cmd_lint(args: argparse.Namespace) -> int:
    from scripts import lint as lint_module

    argv: list[str] = []
    if args.path:
        argv.append(args.path)
    if args.verbose:
        argv.append("-v")
    return lint_module.main(argv)


def cmd_check_env(args: argparse.Namespace) -> int:
    from scripts.common import env

    try:
        cfg = env.load()
    except Exception as exc:  # pragma: no cover - exercised via tests as behavior
        print(str(exc))
        return 1

    if args.substack_cookie and not cfg.substack_session_cookie:
        print(
            "SUBSTACK_SESSION_COOKIE is missing. Follow README.md to export your "
            "session cookie into .env."
        )
        return 1

    print("env check: ok")
    return 0


def cmd_check_audible_auth(_args: argparse.Namespace) -> int:
    from scripts.audible.auth import load_authenticator

    try:
        load_authenticator()
    except Exception as exc:  # pragma: no cover - exercised via tests as behavior
        print(str(exc))
        return 1
    print("audible auth: ok")
    return 0


def cmd_youtube_pull(args: argparse.Namespace) -> int:
    with progress_for_args(args, message="pulling YouTube history", default=True) as progress:
        progress.phase("pulling YouTube history")
        with _tracked_run(
            kind="youtube.pull",
            holder="mind-youtube-pull",
            metadata={"dry_run": bool(args.dry_run), "limit": args.limit},
        ) as (state, run_id):
            result = run_youtube_pull(
                _project_root(),
                dry_run=bool(args.dry_run),
                limit=args.limit,
            )
            rc = result.exit_code
            _finalize_tracked_run(
                state,
                run_id,
                holder="mind-youtube-pull",
                status="completed" if rc == 0 else "failed",
                notes=result.detail,
            )
            return rc


def cmd_audible_pull(args: argparse.Namespace) -> int:
    with progress_for_args(args, message="pulling Audible library export", default=True) as progress:
        progress.phase("pulling Audible library export")
        with _tracked_run(
            kind="audible.pull",
            holder="mind-audible-pull",
            metadata={
                "dry_run": bool(args.dry_run),
                "library_only": bool(args.library_only),
                "sleep": args.sleep,
            },
        ) as (state, run_id):
            result = run_audible_pull(
                _project_root(),
                dry_run=bool(args.dry_run),
                library_only=bool(args.library_only),
                sleep=args.sleep,
            )
            rc = result.exit_code
            _finalize_tracked_run(
                state,
                run_id,
                holder="mind-audible-pull",
                status="completed" if rc == 0 else "failed",
                notes=result.detail,
            )
            return rc


def cmd_substack_pull(args: argparse.Namespace) -> int:
    with progress_for_args(args, message="pulling Substack saved posts", default=True) as progress:
        progress.phase("pulling Substack saved posts")
        with _tracked_run(
            kind="substack.pull",
            holder="mind-substack-pull",
            metadata={"today": args.today},
        ) as (state, run_id):
            result = run_substack_pull(_project_root(), today=args.today)
            export_path = result.export_path
            assert export_path is not None
            state.add_run_event(
                run_id,
                stage="substack",
                event_type="exported",
                message=str(export_path),
            )
            _finalize_tracked_run(
                state,
                run_id,
                holder="mind-substack-pull",
                status="completed",
                notes=str(export_path),
            )
            print(export_path)
            return 0


def cmd_articles_drain(args: argparse.Namespace) -> int:
    repo_root = _repo_root()
    today_str = args.today or date.today().isoformat()
    with progress_for_args(args, message="draining article queue", default=True) as progress:
        progress.phase("draining article queue")
        with _tracked_run(
            kind="articles.drain",
            holder="mind-articles-drain",
            metadata={"today": today_str},
        ) as (state, run_id):
            result = ingest_articles_queue(today=today_str, repo_root=repo_root)
            state.add_run_event(
                run_id,
                stage="articles",
                event_type="drained",
                message=(
                    f"processed={result.drop_files_processed} "
                    f"fetched={result.fetched_summarized} "
                    f"failed={result.failed}"
                ),
            )
            state.upsert_queue_state(
                name="articles",
                status="ready" if result.failed == 0 else "degraded",
                pending_count=0,
                last_item_ref=today_str,
                last_run_id=run_id,
                metadata={
                    "drop_files_processed": result.drop_files_processed,
                    "urls_in_queue": result.urls_in_queue,
                    "skipped_existing": result.skipped_existing,
                    "fetched_summarized": result.fetched_summarized,
                    "paywalled": result.paywalled,
                    "failed": result.failed,
                },
            )
            rc = 0 if result.failed == 0 else 1
            _finalize_tracked_run(
                state,
                run_id,
                holder="mind-articles-drain",
                status="completed" if rc == 0 else "failed",
                notes=f"failed={result.failed}",
            )
            print(
                "ingest-articles: "
                f"{result.drop_files_processed} drop files -> "
                f"{result.fetched_summarized} fetched, "
                f"{result.paywalled} paywalled, "
                f"{result.failed} failed, "
                f"{result.skipped_existing} skipped"
            )
            return rc


def cmd_links_import(args: argparse.Namespace) -> int:
    repo_root = _repo_root()
    today_str = args.today or date.today().isoformat()
    with _tracked_run(
        kind="links.import",
        holder="mind-links-import",
        metadata={"today": today_str, "path": args.path},
    ) as (state, run_id):
        links = load_links(Path(args.path))
        target = append_links_drop(repo_root, links=links, today_str=today_str)
        state.add_run_event(
            run_id,
            stage="links",
            event_type="imported",
            message=f"{len(links)} links -> {target}",
        )
        state.upsert_queue_state(
            name="articles",
            status="pending" if links else "ready",
            pending_count=len(links),
            last_item_ref=str(target),
            last_run_id=run_id,
            metadata={"source": "links-import"},
        )
        _finalize_tracked_run(
            state,
            run_id,
            holder="mind-links-import",
            status="completed",
            notes=f"imported={len(links)}",
        )
        print(f"links-import: {len(links)} links -> {target}")
        return 0


def cmd_links_ingest(args: argparse.Namespace) -> int:
    repo_root = _repo_root()
    today_str = args.today or date.today().isoformat()

    with progress_for_args(args, message="importing links and draining articles", default=True) as progress:
        progress.phase("importing links")
        with _tracked_run(
            kind="links.ingest",
            holder="mind-links-ingest",
            metadata={"today": today_str, "path": args.path},
        ) as (state, run_id):
            links = load_links(Path(args.path))
            target = append_links_drop(repo_root, links=links, today_str=today_str)
            state.add_run_event(
                run_id,
                stage="links",
                event_type="imported",
                message=f"{len(links)} links -> {target}",
            )
            progress.phase("draining article queue")
            result = ingest_articles_queue(today=today_str, repo_root=repo_root)
            state.add_run_event(
                run_id,
                stage="articles",
                event_type="drained",
                message=(
                    f"fetched={result.fetched_summarized} "
                    f"failed={result.failed} "
                    f"skipped={result.skipped_existing}"
                ),
            )
            state.upsert_queue_state(
                name="articles",
                status="ready" if result.failed == 0 else "degraded",
                pending_count=0,
                last_item_ref=str(target),
                last_run_id=run_id,
                metadata={
                    "imported_links": len(links),
                    "fetched_summarized": result.fetched_summarized,
                    "failed": result.failed,
                    "skipped_existing": result.skipped_existing,
                },
            )
            rc = 0 if result.failed == 0 else 1
            _finalize_tracked_run(
                state,
                run_id,
                holder="mind-links-ingest",
                status="completed" if rc == 0 else "failed",
                notes=f"failed={result.failed}",
            )
            print(
                "links-ingest: "
                f"{len(links)} links imported -> "
                f"{result.fetched_summarized} fetched, "
                f"{result.failed} failed, "
                f"{result.skipped_existing} skipped "
                f"({target})"
            )
            return rc


def cmd_chrome_scan(args: argparse.Namespace) -> int:
    repo_root = _repo_root()
    today_str = args.today or date.today().isoformat()
    profiles = list(args.profile or [])
    with progress_for_args(args, message="scanning Chrome profiles", default=True) as progress:
        progress.phase("scanning Chrome profiles")
        with _tracked_run(
            kind="chrome.scan",
            holder="mind-chrome-scan",
            metadata={"today": today_str, "profiles": profiles, "since_days": args.since_days},
        ) as (state, run_id):
            result = scan_chrome(
                today=today_str,
                repo_root=repo_root,
                selected_profiles=profiles or None,
                since_days=args.since_days,
            )
            state.add_run_event(
                run_id,
                stage="chrome",
                event_type="scanned",
                message=f"events={result.events_scanned}",
                payload={"event_files": [str(path) for path in result.event_files]},
            )
            _finalize_tracked_run(
                state,
                run_id,
                holder="mind-chrome-scan",
                status="completed",
                notes=f"events={result.events_scanned}",
            )
            print(f"chrome-scan: {result.events_scanned} events -> {len(result.event_files)} files")
            return 0


def cmd_chrome_ingest(args: argparse.Namespace) -> int:
    repo_root = _repo_root()
    today_str = args.today or date.today().isoformat()
    profiles = list(args.profile or [])
    with progress_for_args(args, message="building Chrome-derived ingest drops", default=True) as progress:
        progress.phase("building Chrome-derived ingest drops")
        with _tracked_run(
            kind="chrome.ingest",
            holder="mind-chrome-ingest",
            metadata={"today": today_str, "profiles": profiles, "since_days": args.since_days},
        ) as (state, run_id):
            result = ingest_chrome(
                today=today_str,
                repo_root=repo_root,
                selected_profiles=profiles or None,
                since_days=args.since_days,
            )
            state.add_run_event(
                run_id,
                stage="chrome",
                event_type="ingested",
                message=(
                    f"events={result.raw_events_seen} candidates={result.candidates_written} "
                    f"search_signals={result.search_signals_written}"
                ),
            )
            state.upsert_queue_state(
                name="web-discovery",
                status="pending" if result.candidates_written else "ready",
                pending_count=result.candidates_written,
                last_item_ref=str(result.candidate_drop_path),
                last_run_id=run_id,
                metadata={"source": "chrome"},
            )
            state.upsert_queue_state(
                name="search-signals",
                status="pending" if result.search_signals_written else "ready",
                pending_count=result.search_signals_written,
                last_item_ref=str(result.search_signal_drop_path),
                last_run_id=run_id,
                metadata={"source": "chrome"},
            )
            _finalize_tracked_run(
                state,
                run_id,
                holder="mind-chrome-ingest",
                status="completed",
                notes=f"candidates={result.candidates_written}",
            )
            print(
                "chrome-ingest: "
                f"{result.raw_events_seen} events -> "
                f"{result.candidates_written} candidates, "
                f"{result.search_signals_written} search signals"
            )
            return 0


def cmd_search_signals_ingest(args: argparse.Namespace) -> int:
    repo_root = _repo_root()
    today_str = args.today or date.today().isoformat()
    with progress_for_args(args, message="materializing search signals", default=True) as progress:
        progress.phase("materializing search signals")
        with _tracked_run(
            kind="search-signals.ingest",
            holder="mind-search-signals-ingest",
            metadata={"today": today_str},
        ) as (state, run_id):
            result = ingest_search_signals(today=today_str, repo_root=repo_root)
            state.add_run_event(
                run_id,
                stage="search-signals",
                event_type="materialized",
                message=f"signals={result.signals_materialized} pages={result.pages_written}",
            )
            state.upsert_queue_state(
                name="search-signals",
                status="ready",
                pending_count=0,
                last_item_ref=today_str,
                last_run_id=run_id,
                metadata={"signals_materialized": result.signals_materialized},
            )
            _finalize_tracked_run(
                state,
                run_id,
                holder="mind-search-signals-ingest",
                status="completed",
                notes=f"signals={result.signals_materialized}",
            )
            print(
                "search-signals-ingest: "
                f"{result.drop_files_processed} drop files -> "
                f"{result.signals_materialized} signals, "
                f"{result.pages_written} pages"
            )
            return 0


def cmd_web_discovery_drain(args: argparse.Namespace) -> int:
    repo_root = _repo_root()
    today_str = args.today or date.today().isoformat()
    with progress_for_args(args, message="draining web discovery queue", default=True) as progress:
        progress.phase("draining web discovery queue")
        with _tracked_run(
            kind="web-discovery.drain",
            holder="mind-web-discovery-drain",
            metadata={"today": today_str},
        ) as (state, run_id):
            result = drain_web_discovery(today=today_str, repo_root=repo_root)
            state.add_run_event(
                run_id,
                stage="web-discovery",
                event_type="drained",
                message=(
                    f"candidates={result.candidates_processed} pages={result.pages_written} "
                    f"crawled={result.crawled} failed={result.failed}"
                ),
            )
            state.upsert_queue_state(
                name="web-discovery",
                status="ready" if result.failed == 0 else "degraded",
                pending_count=0,
                last_item_ref=today_str,
                last_run_id=run_id,
                metadata={
                    "drop_files_processed": result.drop_files_processed,
                    "candidates_processed": result.candidates_processed,
                    "pages_written": result.pages_written,
                    "crawled": result.crawled,
                    "failed": result.failed,
                },
            )
            rc = 0 if result.failed == 0 else 1
            _finalize_tracked_run(
                state,
                run_id,
                holder="mind-web-discovery-drain",
                status="completed" if rc == 0 else "failed",
                notes=f"failed={result.failed}",
            )
            print(
                "web-discovery-drain: "
                f"{result.drop_files_processed} drop files -> "
                f"{result.pages_written} pages, "
                f"{result.crawled} crawled, "
                f"{result.failed} failed"
            )
            return rc


def _json_object(raw: str | None) -> dict[str, Any] | None:
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _display_scalar(value: object, *, max_len: int = 72) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        text = "true" if value else "false"
    elif isinstance(value, (int, float)):
        text = f"{value:g}"
    elif isinstance(value, str):
        text = value.strip()
    else:
        text = str(value).strip()
    if not text:
        return None
    if len(text) <= max_len:
        return text
    if "://" in text:
        return f"{text[:32]}...{text[-20:]}"
    if "/" in text or "\\" in text:
        parts = [part for part in Path(text).parts if part not in {"/", "\\"}]
        tail = "/".join(parts[-3:])
        if tail and len(tail) + 4 <= max_len:
            return f".../{tail}"
    return f"{text[: max_len - 3]}..."


def _payload_focus_segments(payload: dict[str, Any], *, include_queue: bool = False) -> list[str]:
    specs: list[tuple[str, tuple[str, ...]]] = [
        ("source", ("source_id", "summary_id", "video_id", "post_id", "source")),
        ("item", ("item_ref", "item")),
        ("path", ("source_path", "path", "export_path", "artifact_ref", "report_path")),
        ("url", ("url", "canonical_url")),
        ("bundle", ("bundle_id",)),
        ("lock", ("lock_name",)),
    ]
    if include_queue:
        specs.append(("queue", ("queue_name",)))
    segments: list[str] = []
    seen: set[str] = set()
    for label, keys in specs:
        for key in keys:
            text = _display_scalar(payload.get(key))
            if not text:
                continue
            segment = f"{label}={text}"
            if segment in seen:
                continue
            seen.add(segment)
            segments.append(segment)
            break
    return segments


def _progress_segment(payload: dict[str, Any] | None) -> str | None:
    if not payload:
        return None
    progress_payload = payload.get("progress")
    candidates = [progress_payload] if isinstance(progress_payload, dict) else []
    candidates.append(payload)
    for candidate in candidates:
        for current_key, total_key in (
            ("current", "total"),
            ("completed", "total"),
            ("processed", "total"),
            ("index", "total"),
            ("current_index", "total"),
            ("processed_count", "total_count"),
        ):
            current = candidate.get(current_key)
            total = candidate.get(total_key)
            current_text = _display_scalar(current, max_len=24)
            total_text = _display_scalar(total, max_len=24)
            if current_text is not None and total_text is not None:
                return f"progress={current_text}/{total_text}"
    return None


def _counter_segments(payload: dict[str, Any] | None) -> list[str]:
    if not payload:
        return []
    containers = [payload]
    for key in ("counts", "counters", "totals", "final_counts", "final_counters"):
        nested = payload.get(key)
        if isinstance(nested, dict):
            containers.append(nested)

    excluded = {
        "source_id",
        "summary_id",
        "video_id",
        "post_id",
        "source",
        "item_ref",
        "item",
        "source_path",
        "path",
        "artifact_ref",
        "report_path",
        "url",
        "canonical_url",
        "queue_name",
        "bundle_id",
        "lock_name",
        "progress",
        "current",
        "total",
        "completed",
        "processed",
        "index",
        "current_index",
        "processed_count",
        "total_count",
        "counts",
        "counters",
        "totals",
        "final_counts",
        "final_counters",
    }
    priority = (
        "selected",
        "skipped",
        "failed",
        "blocked",
        "executed",
        "processed",
        "completed",
        "pages_written",
        "posts_written",
        "linked_articles_fetched",
        "drop_files_processed",
        "fetched_summarized",
        "candidates_written",
        "search_signals_written",
        "signals_materialized",
        "events_scanned",
        "paywalled",
        "unsaved_refs",
        "resumable",
        "stale",
        "retry_count",
        "evidence_updates",
        "probationary_updates",
        "cache_reused",
    )
    counter_values: dict[str, str] = {}
    for container in containers:
        for key, value in container.items():
            if key in excluded or key in counter_values:
                continue
            rendered: str | None = None
            if isinstance(value, bool):
                rendered = "true" if value else "false"
            elif isinstance(value, (int, float)):
                rendered = _display_scalar(value, max_len=24)
            elif isinstance(value, list) and key.endswith(("_ids", "_items", "_files", "_samples")):
                rendered = _display_scalar(len(value), max_len=24)
            if rendered is not None:
                counter_values[key] = rendered
    ordered_keys = [key for key in priority if key in counter_values]
    ordered_keys.extend(sorted(key for key in counter_values if key not in priority))
    return [f"{key}={counter_values[key]}" for key in ordered_keys[:4]]


def _payload_summary(payload: dict[str, Any] | None, *, include_queue: bool = False) -> str | None:
    if not payload:
        return None
    segments = _payload_focus_segments(payload, include_queue=include_queue)
    progress = _progress_segment(payload)
    if progress:
        segments.append(progress)
    segments.extend(_counter_segments(payload))
    deduped: list[str] = []
    seen: set[str] = set()
    for segment in segments:
        if segment in seen:
            continue
        seen.add(segment)
        deduped.append(segment)
    return " ".join(deduped[:6]) or None


def _event_detail_text(event) -> str:
    message = event.message or ""
    payload_summary = _payload_summary(_json_object(event.payload_json), include_queue=True)
    if message and payload_summary:
        return f"{message} | {payload_summary}"
    if message:
        return message
    return payload_summary or "-"


def _stage_signal_event(events: Sequence[object]):
    for event in reversed(events):
        if getattr(event, "event_type", "") in {"retry_scheduled", "blocked", "failed"}:
            return event
    for excluded_stages in ({"cli", "worker"}, {"cli"}, set()):
        for event in reversed(events):
            if getattr(event, "stage", "") in excluded_stages:
                continue
            return event
    return None


def _stage_signal_text(events: Sequence[object]) -> str:
    event = _stage_signal_event(events)
    if event is None:
        return "-"
    signal = f"{event.stage}/{event.event_type}"
    progress = _progress_segment(_json_object(event.payload_json))
    if progress:
        return f"{signal} {progress}"
    return signal


def _focus_signal_text(run, events: Sequence[object]) -> str:
    for event in reversed(events):
        payload = _json_object(getattr(event, "payload_json", None))
        focus_segments = _payload_focus_segments(payload or {})
        if focus_segments:
            return focus_segments[0]
    if run.item_ref:
        text = _display_scalar(run.item_ref)
        if text:
            return f"item={text}"
    metadata_focus = _payload_focus_segments(_json_object(run.metadata_json) or {})
    if metadata_focus:
        return metadata_focus[0]
    if run.queue_name:
        return f"queue={run.queue_name}"
    return "-"


def cmd_state_summary(_args: argparse.Namespace) -> int:
    state = _runtime_state()
    summary = state.summary()
    dream = summary.dream_state
    quality = state.get_adapter_state(QUALITY_ADAPTER) or {}
    runs = state.list_runs(limit=50)
    locks = state.list_locks()
    latest_daily = next((run for run in runs if run.kind == "orchestrate.daily"), None)
    latest_failed = next((run for run in runs if run.status == "failed"), None)
    print(f"Runtime DB: {summary.db_path}")
    print(f"Schema version: {summary.schema_version}")
    print(f"Active locks: {summary.active_locks}")
    print(f"Runs recorded: {summary.run_count}")
    print(f"Queue entries: {summary.queue_entries}")
    print(f"Tracked skills: {summary.tracked_skills}")
    print(
        "Dream state: "
        f"light={dream.last_light or '-'} "
        f"deep={dream.last_deep or '-'} "
        f"rem={dream.last_rem or '-'} "
        f"light_since_deep={dream.light_passes_since_deep} "
        f"deep_since_rem={dream.deep_passes_since_rem}"
    )
    if quality:
        print(
            "Dream lane trust: "
            f"evaluated_at={quality.get('evaluated_at') or '-'} "
            f"report={quality.get('report_path') or '-'}"
        )
        for lane in CANONICAL_LANES:
            lane_payload = (quality.get("lanes") or {}).get(lane) or {}
            reasons = ",".join(str(item) for item in lane_payload.get("reasons") or []) or "-"
            metrics = lane_payload.get("metrics") or {}
            print(
                f"  {LANE_DISPLAY.get(lane, lane)}\tstate={lane_payload.get('state') or '-'}\t"
                f"recent={lane_payload.get('recent_sources') or 0}\t"
                f"pass_d={metrics.get('pass_d_success_rate') if metrics.get('pass_d_success_rate') is not None else '-'}\t"
                f"route={metrics.get('route_policy_compliance') if metrics.get('route_policy_compliance') is not None else '-'}\t"
                f"quote={metrics.get('quote_verification_coverage') if metrics.get('quote_verification_coverage') is not None else '-'}\t"
                f"entity={metrics.get('entity_log_yield') if metrics.get('entity_log_yield') is not None else '-'}\t"
                f"fanout={metrics.get('fanout_yield') if metrics.get('fanout_yield') is not None else '-'}\t"
                f"grounding={metrics.get('source_grounded_coverage') if metrics.get('source_grounded_coverage') is not None else '-'}\t"
                f"parity={lane_payload.get('parity_status') or '-'}\t"
                f"reasons={reasons}"
            )
    if locks:
        print("Locks held:")
        for lock in locks:
            print(f"  {lock.name}\t{lock.holder}\t{lock.acquired_at}")
    elif dream.last_lock_holder:
        print(f"Last lock: {dream.last_lock_holder} at {dream.last_lock_acquired_at or '-'}")
    if dream.last_skip_reason:
        print(f"Last skip reason: {dream.last_skip_reason}")
    if latest_daily is not None:
        print(
            "Last daily orchestrator: "
            f"id={latest_daily.id} status={latest_daily.status} "
            f"started={latest_daily.started_at} finished={latest_daily.finished_at or '-'}"
        )
        daily_details = state.get_run(latest_daily.id)
        if daily_details is not None:
            skipped = [event for event in daily_details.events if event.event_type == "skipped"]
            blocked = [event for event in daily_details.events if event.event_type == "blocked"]
            if skipped:
                print("Last orchestrator skips:")
                for event in skipped:
                    print(f"  {event.stage}: {event.message or '-'}")
            if blocked:
                print("Last orchestrator blocks:")
                for event in blocked:
                    print(f"  {event.stage}: {event.message or '-'}")
    if latest_failed is not None:
        print(
            "Last failed run: "
            f"id={latest_failed.id} kind={latest_failed.kind} "
            f"started={latest_failed.started_at} notes={latest_failed.notes or '-'}"
        )
    return 0



def cmd_state_runs(args: argparse.Namespace) -> int:
    state = _runtime_state()
    runs = state.list_runs(limit=args.limit)
    if not runs:
        print("No runs recorded.")
        return 0
    print("id\tkind\tstatus\tholder\tstage\tfocus\tstarted\tfinished")
    for run in runs:
        details = state.get_run(run.id)
        events = details.events if details is not None else []
        finished = run.finished_at or "-"
        holder = run.holder or "-"
        stage_signal = _stage_signal_text(events)
        focus_signal = _focus_signal_text(run, events)
        print(
            f"{run.id}\t{run.kind}\t{run.status}\t{holder}\t"
            f"{stage_signal}\t{focus_signal}\t{run.started_at}\t{finished}"
        )
    return 0


def cmd_state_run(args: argparse.Namespace) -> int:
    details = _runtime_state().get_run(args.run_id)
    if details is None:
        print(f"Run {args.run_id} not found.")
        return 1
    run = details.run
    print(f"Run: {run.id}")
    print(f"Kind: {run.kind}")
    print(f"Status: {run.status}")
    print(f"Holder: {run.holder or '-'}")
    print(f"Started: {run.started_at}")
    print(f"Finished: {run.finished_at or '-'}")
    print(f"Notes: {run.notes or '-'}")
    print(f"Queue: {run.queue_name or '-'}")
    print(f"Focus: {_focus_signal_text(run, details.events)}")
    print(f"Latest stage: {_stage_signal_text(details.events)}")
    print("Events:")
    if not details.events:
        print("  (none)")
    else:
        print("  id\tstage\ttype\tat\tdetail")
        for event in details.events:
            print(
                f"  {event.id}\t{event.stage}\t{event.event_type}\t"
                f"{event.created_at}\t{_event_detail_text(event)}"
            )
    print("Errors:")
    if not details.errors:
        print("  (none)")
    else:
        print("  id\tstage\ttype\tat\tmessage")
        for error in details.errors:
            print(
                f"  {error.id}\t{error.stage or '-'}\t{error.error_type}\t{error.created_at}\t{error.message}"
            )
    return 0


def cmd_state_queue(_args: argparse.Namespace) -> int:
    queue = _runtime_state().list_queue()
    if not queue:
        print("No queue state recorded.")
        return 0
    for item in queue:
        last_item_ref = item.last_item_ref or "-"
        last_run_id = str(item.last_run_id) if item.last_run_id is not None else "-"
        print(
            f"{item.name}\t{item.status}\t{item.pending_count}\t{last_item_ref}\t{last_run_id}\t{item.updated_at}"
        )
    return 0


def cmd_state_locks(_args: argparse.Namespace) -> int:
    locks = _runtime_state().list_locks()
    if not locks:
        print("No locks held.")
        return 0
    for lock in locks:
        print(f"{lock.name}\t{lock.holder}\t{lock.acquired_at}")
    return 0


def cmd_state_skills(_args: argparse.Namespace) -> int:
    skills = _runtime_state().list_skill_usage()
    if not skills:
        print("No skill usage recorded.")
        return 0
    for skill in skills:
        last_used = skill.last_used_at or "-"
        print(
            f"{skill.skill_name}\tusage={skill.usage_count}\tartifacts={skill.artifact_count}\tlast_used={last_used}"
        )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mind",
        description="Canonical CLI wrappers for the Brain runtime",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    lint_p = sub.add_parser("lint", help="Run the Brain linter")
    lint_p.add_argument("path", nargs="?", default=None)
    lint_p.add_argument("-v", "--verbose", action="store_true")
    lint_p.set_defaults(func=cmd_lint)

    check_p = sub.add_parser("check", help="Run environment/auth checks")
    check_sub = check_p.add_subparsers(dest="check_command", required=True)

    env_p = check_sub.add_parser("env", help="Verify required env vars")
    env_p.add_argument("--substack-cookie", action="store_true")
    env_p.set_defaults(func=cmd_check_env)

    audible_auth_p = check_sub.add_parser("audible-auth", help="Verify Audible auth file")
    audible_auth_p.set_defaults(func=cmd_check_audible_auth)

    youtube_p = sub.add_parser("youtube", help="YouTube operations")
    youtube_sub = youtube_p.add_subparsers(dest="youtube_command", required=True)
    youtube_pull_p = youtube_sub.add_parser("pull", help="Run the YouTube history puller")
    youtube_pull_p.add_argument("--dry-run", action="store_true")
    youtube_pull_p.add_argument("--limit", type=int, default=None)
    _add_quiet_argument(youtube_pull_p)
    youtube_pull_p.set_defaults(func=cmd_youtube_pull, progress_enabled=True)

    audible_p = sub.add_parser("audible", help="Audible operations")
    audible_sub = audible_p.add_subparsers(dest="audible_command", required=True)
    audible_pull_p = audible_sub.add_parser("pull", help="Run the Audible puller")
    audible_pull_p.add_argument("--dry-run", action="store_true")
    audible_pull_p.add_argument("--library-only", action="store_true")
    audible_pull_p.add_argument("--sleep", type=float, default=None)
    _add_quiet_argument(audible_pull_p)
    audible_pull_p.set_defaults(func=cmd_audible_pull, progress_enabled=True)

    substack_p = sub.add_parser("substack", help="Substack operations")
    substack_sub = substack_p.add_subparsers(dest="substack_command", required=True)
    substack_pull_p = substack_sub.add_parser("pull", help="Pull the saved-posts export")
    substack_pull_p.add_argument("--today", default=None)
    _add_quiet_argument(substack_pull_p)
    substack_pull_p.set_defaults(func=cmd_substack_pull, progress_enabled=True)

    articles_p = sub.add_parser("articles", help="Article queue operations")
    articles_sub = articles_p.add_subparsers(dest="articles_command", required=True)
    articles_drain_p = articles_sub.add_parser("drain", help="Drain the articles drop queue")
    articles_drain_p.add_argument("--today", default=None)
    _add_quiet_argument(articles_drain_p)
    articles_drain_p.set_defaults(func=cmd_articles_drain, progress_enabled=True)

    links_p = sub.add_parser("links", help="Zero-auth links import operations")
    links_sub = links_p.add_subparsers(dest="links_command", required=True)
    links_import_p = links_sub.add_parser("import", help="Import links JSON into the drop queue")
    links_import_p.add_argument("path")
    links_import_p.add_argument("--today", default=None)
    links_import_p.set_defaults(func=cmd_links_import)
    links_ingest_p = links_sub.add_parser("ingest", help="Import links JSON and drain articles")
    links_ingest_p.add_argument("path")
    links_ingest_p.add_argument("--today", default=None)
    _add_quiet_argument(links_ingest_p)
    links_ingest_p.set_defaults(func=cmd_links_ingest, progress_enabled=True)

    chrome_p = sub.add_parser("chrome", help="Chrome web-discovery operations")
    chrome_sub = chrome_p.add_subparsers(dest="chrome_command", required=True)
    chrome_scan_p = chrome_sub.add_parser("scan", help="Read Chrome profiles and write raw event logs")
    chrome_scan_p.add_argument("--today", default=None)
    chrome_scan_p.add_argument("--since-days", type=int, default=None)
    chrome_scan_p.add_argument("--profile", action="append", default=[])
    _add_quiet_argument(chrome_scan_p)
    chrome_scan_p.set_defaults(func=cmd_chrome_scan, progress_enabled=True)
    chrome_ingest_p = chrome_sub.add_parser("ingest", help="Build web-discovery and search-signal drops from Chrome")
    chrome_ingest_p.add_argument("--today", default=None)
    chrome_ingest_p.add_argument("--since-days", type=int, default=None)
    chrome_ingest_p.add_argument("--profile", action="append", default=[])
    _add_quiet_argument(chrome_ingest_p)
    chrome_ingest_p.set_defaults(func=cmd_chrome_ingest, progress_enabled=True)

    search_signals_p = sub.add_parser("search-signals", help="Search signal materialization operations")
    search_signals_sub = search_signals_p.add_subparsers(dest="search_signals_command", required=True)
    search_signals_ingest_p = search_signals_sub.add_parser("ingest", help="Materialize search signal rollups")
    search_signals_ingest_p.add_argument("--today", default=None)
    _add_quiet_argument(search_signals_ingest_p)
    search_signals_ingest_p.set_defaults(func=cmd_search_signals_ingest, progress_enabled=True)

    web_discovery_p = sub.add_parser("web-discovery", help="Web discovery queue operations")
    web_discovery_sub = web_discovery_p.add_subparsers(dest="web_discovery_command", required=True)
    web_discovery_drain_p = web_discovery_sub.add_parser("drain", help="Drain the web-discovery drop queue")
    web_discovery_drain_p.add_argument("--today", default=None)
    _add_quiet_argument(web_discovery_drain_p)
    web_discovery_drain_p.set_defaults(func=cmd_web_discovery_drain, progress_enabled=True)

    state_p = sub.add_parser("state", help="Inspect SQLite-backed operational state")
    state_sub = state_p.add_subparsers(dest="state_command")
    state_p.set_defaults(func=cmd_state_summary)

    state_runs_p = state_sub.add_parser("runs", help="List recorded runs")
    state_runs_p.add_argument("--limit", type=int, default=20)
    state_runs_p.set_defaults(func=cmd_state_runs)

    state_run_p = state_sub.add_parser("run", help="Show one recorded run")
    state_run_p.add_argument("run_id", type=int)
    state_run_p.set_defaults(func=cmd_state_run)

    state_queue_p = state_sub.add_parser("queue", help="Show ingest queue state")
    state_queue_p.set_defaults(func=cmd_state_queue)

    state_locks_p = state_sub.add_parser("locks", help="Show active locks")
    state_locks_p.set_defaults(func=cmd_state_locks)

    state_skills_p = state_sub.add_parser("skills", help="Show tracked skill usage")
    state_skills_p.set_defaults(func=cmd_state_skills)

    state_health_p = state_sub.add_parser("health", help="Summarize runtime + dream lane trust")
    state_health_p.set_defaults(func=cmd_state_summary)

    register_additional_commands(sub)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    return int(args.func(args))
