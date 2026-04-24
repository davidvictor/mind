from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

# TODO: these ingest operations should live in mind/services/, not mind/commands/
from mind.commands.ingest import (
    ingest_articles_queue,
    ingest_books_export,
    ingest_substack_export,
    ingest_youtube_export,
)
from mind.dream.common import DreamBlockedError, DreamPreconditionError
from mind.dream.v2.runtime import run_dream_v2_stage
from mind.dream.v2.weave_stage import run_weave_v2_shadow
from mind.runtime_state import RuntimeState, RuntimeStateLockBusy
from mind.services.dropbox import dropbox_phase_message, dropbox_phase_status, dropbox_queue_status, sweep_dropbox
from mind.services.provider_ops import PullResult, run_audible_pull, run_substack_pull, run_youtube_pull
from mind.services.queue_worker import drain_until_empty
from scripts.common.vault import Vault

ORCHESTRATOR_QUEUE_PREFIXES = ("links", "ingest:file", "ingest:links", "ingest:articles")


def run_light(*, dry_run: bool = False, acquire_lock: bool = True):
    return run_dream_v2_stage(stage="light", dry_run=dry_run, acquire_lock=acquire_lock)


def run_deep(*, dry_run: bool = False, acquire_lock: bool = True):
    return run_dream_v2_stage(stage="deep", dry_run=dry_run, acquire_lock=acquire_lock)


def run_rem(*, dry_run: bool = False, acquire_lock: bool = True):
    return run_dream_v2_stage(stage="rem", dry_run=dry_run, acquire_lock=acquire_lock)


def run_weave(*, dry_run: bool = False, acquire_lock: bool = True):
    return run_dream_v2_stage(stage="weave", dry_run=dry_run, acquire_lock=acquire_lock)


@dataclass(frozen=True)
class OrchestratorPhaseResult:
    stage: str
    status: str
    message: str


@dataclass(frozen=True)
class DailyOrchestratorResult:
    status: str
    phases: list[OrchestratorPhaseResult] = field(default_factory=list)

    @property
    def exit_code(self) -> int:
        return 0 if self.status == "completed" else 1

    def render(self) -> str:
        lines = [f"Orchestrator status: {self.status}"]
        for phase in self.phases:
            lines.append(f"- {phase.stage}: {phase.status} — {phase.message}")
        return "\n".join(lines)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(value, fmt)
            if fmt == "%Y-%m-%d":
                parsed = parsed.replace(hour=0, minute=0, second=0)
            return parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _is_due(last_run: str | None, *, minimum_age: timedelta) -> bool:
    if not last_run:
        return True
    parsed = _parse_timestamp(last_run)
    if parsed is None:
        return True
    return (_utc_now() - parsed) >= minimum_age


def _record_phase(
    state: RuntimeState,
    run_id: int,
    *,
    stage: str,
    status: str,
    message: str,
    phases: list[OrchestratorPhaseResult],
    phase_callback: Callable[[str], None] | None = None,
) -> None:
    phases.append(OrchestratorPhaseResult(stage=stage, status=status, message=message))
    state.add_run_event(run_id, stage=stage, event_type=status, message=message)
    if phase_callback is not None:
        phase_callback(f"{stage}: {status}")
    if status in {"failed", "blocked"}:
        state.add_error(run_id=run_id, stage=stage, error_type=status.title(), message=message)


def _phase_status_from_exit(rc: int) -> str:
    return "completed" if rc == 0 else "failed"


def _phase_message_from_pull(result: PullResult) -> str:
    return result.detail


def _phase_result_summary(phases: list[OrchestratorPhaseResult]) -> str:
    counts: dict[str, int] = {}
    for phase in phases:
        counts[phase.status] = counts.get(phase.status, 0) + 1
    return ", ".join(f"{key}={counts[key]}" for key in sorted(counts))


def run_daily_orchestrator(repo_root: Path, *, phase_callback: Callable[[str], None] | None = None) -> DailyOrchestratorResult:
    state = RuntimeState.for_repo_root(repo_root)
    run_id = state.create_run(kind="orchestrate.daily", holder="orchestrate-daily")
    phases: list[OrchestratorPhaseResult] = []
    try:
        state.acquire_lock(holder="orchestrate-daily")
    except RuntimeStateLockBusy as exc:
        _record_phase(
            state,
            run_id,
            stage="orchestrator",
            status="blocked",
            message=str(exc),
            phases=phases,
            phase_callback=phase_callback,
        )
        state.finish_run(run_id, status="blocked", notes=str(exc))
        return DailyOrchestratorResult(status="blocked", phases=phases)

    try:
        vault = Vault.load(repo_root)
        enabled = set(vault.config.ingestors.enabled)
        _record_phase(
            state,
            run_id,
            stage="config",
            status="completed",
            message=f"enabled={','.join(sorted(enabled)) or '(none)'}",
            phases=phases,
            phase_callback=phase_callback,
        )

        try:
            dropbox_result = sweep_dropbox(repo_root=repo_root, dry_run=False)
            state.upsert_queue_state(
                name="dropbox",
                status=dropbox_queue_status(dropbox_result),
                pending_count=dropbox_result.pending_count_after,
                last_item_ref=dropbox_result.last_item_ref,
                last_run_id=run_id,
                metadata={
                    **dropbox_result.metadata,
                    "last_sweep_at": _utc_now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                },
            )
            _record_phase(
                state,
                run_id,
                stage="dropbox:sweep",
                status=dropbox_phase_status(dropbox_result),
                message=dropbox_phase_message(dropbox_result),
                phases=phases,
                phase_callback=phase_callback,
            )
        except Exception as exc:
            _record_phase(state, run_id, stage="dropbox:sweep", status="failed", message=str(exc), phases=phases, phase_callback=phase_callback)

        pull_results: dict[str, PullResult] = {}
        for provider, runner in (
            ("youtube", lambda: run_youtube_pull(repo_root)),
            ("audible", lambda: run_audible_pull(repo_root)),
            ("substack", lambda: run_substack_pull(repo_root)),
        ):
            stage = f"provider:{provider}"
            if provider not in enabled:
                _record_phase(state, run_id, stage=stage, status="skipped", message="disabled in config", phases=phases)
                if phase_callback is not None:
                    phase_callback(f"{stage}: skipped")
                continue
            try:
                result = runner()
            except Exception as exc:
                _record_phase(state, run_id, stage=stage, status="failed", message=str(exc), phases=phases, phase_callback=phase_callback)
                continue
            pull_results[provider] = result
            _record_phase(
                state,
                run_id,
                stage=stage,
                status=_phase_status_from_exit(result.exit_code),
                message=_phase_message_from_pull(result),
                phases=phases,
                phase_callback=phase_callback,
            )

        drain_result = drain_until_empty(
            repo_root,
            acquire_lock=False,
            allowed_queue_prefixes=ORCHESTRATOR_QUEUE_PREFIXES,
        )
        drain_status = "completed"
        drain_message = f"processed={drain_result.processed} failed={drain_result.failures}"
        if drain_result.processed == 0:
            drain_status = "skipped"
            drain_message = "no queued work"
        elif drain_result.failures > 0:
            drain_status = "failed"
        _record_phase(state, run_id, stage="queue-drain", status=drain_status, message=drain_message, phases=phases, phase_callback=phase_callback)

        if "youtube" in enabled:
            export_path = pull_results.get("youtube", PullResult("youtube", 1, "")).export_path
            if export_path is None:
                _record_phase(state, run_id, stage="ingest:youtube", status="skipped", message="no export available", phases=phases)
                if phase_callback is not None:
                    phase_callback("ingest:youtube: skipped")
            else:
                try:
                    result = ingest_youtube_export(export_path)
                    status = "completed" if result.failed == 0 else "failed"
                    _record_phase(
                        state,
                        run_id,
                        stage="ingest:youtube",
                        status=status,
                        message=(
                            f"selected={result.selected_count} "
                            f"skipped_materialized={result.skipped_materialized} "
                            f"blocked={result.blocked} "
                            f"executed={result.executed} "
                            f"failed={result.failed} "
                            f"pages_written={result.pages_written}"
                        ),
                        phases=phases,
                        phase_callback=phase_callback,
                    )
                except Exception as exc:
                    _record_phase(state, run_id, stage="ingest:youtube", status="failed", message=str(exc), phases=phases, phase_callback=phase_callback)

        if "audible" in enabled:
            export_path = pull_results.get("audible", PullResult("audible", 1, "")).export_path
            if export_path is None:
                _record_phase(state, run_id, stage="ingest:audible", status="skipped", message="no export available", phases=phases)
                if phase_callback is not None:
                    phase_callback("ingest:audible: skipped")
            else:
                try:
                    result = ingest_books_export(export_path)
                    _record_phase(
                        state,
                        run_id,
                        stage="ingest:audible",
                        status="completed",
                        message=(
                            f"selected={result.selected_count} "
                            f"skipped_materialized={result.skipped_materialized} "
                            f"blocked={result.blocked} "
                            f"executed={result.executed} "
                            f"failed={result.failed} "
                            f"pages_written={result.pages_written}"
                        ),
                        phases=phases,
                        phase_callback=phase_callback,
                    )
                except Exception as exc:
                    _record_phase(state, run_id, stage="ingest:audible", status="failed", message=str(exc), phases=phases, phase_callback=phase_callback)

        if "substack" in enabled:
            export_path = pull_results.get("substack", PullResult("substack", 1, "")).export_path
            if export_path is None:
                _record_phase(state, run_id, stage="ingest:substack", status="skipped", message="no export available", phases=phases)
                if phase_callback is not None:
                    phase_callback("ingest:substack: skipped")
            else:
                try:
                    result = ingest_substack_export(export_path=export_path, drain_articles=False)
                    status = "completed" if result.failed == 0 else "failed"
                    _record_phase(
                        state,
                        run_id,
                        stage="ingest:substack",
                        status=status,
                        message=(
                            f"selected={result.selected_count} "
                            f"paywalled={result.paywalled} "
                            f"blocked={result.blocked} "
                            f"executed={result.executed} "
                            f"failed={result.failed} "
                            f"posts_written={result.posts_written}"
                        ),
                        phases=phases,
                        phase_callback=phase_callback,
                    )
                except Exception as exc:
                    _record_phase(state, run_id, stage="ingest:substack", status="failed", message=str(exc), phases=phases, phase_callback=phase_callback)

        if "articles" in enabled:
            try:
                articles_result = ingest_articles_queue(repo_root=vault.memory_root)
                status = "completed" if articles_result.failed == 0 else "failed"
                if articles_result.drop_files_processed == 0 and articles_result.fetched_summarized == 0:
                    status = "skipped"
                _record_phase(
                    state,
                    run_id,
                    stage="ingest:articles",
                    status=status,
                    message=(
                        f"drop_files={articles_result.drop_files_processed} "
                        f"fetched={articles_result.fetched_summarized} failed={articles_result.failed}"
                    ),
                    phases=phases,
                    phase_callback=phase_callback,
                )
            except Exception as exc:
                _record_phase(state, run_id, stage="ingest:articles", status="failed", message=str(exc), phases=phases, phase_callback=phase_callback)

        dream_state = state.get_dream_state()
        if not vault.config.dream.enabled:
            for stage_name in ("light", "deep", "rem", "weave"):
                _record_phase(state, run_id, stage=f"dream:{stage_name}", status="skipped", message="dream disabled in config", phases=phases, phase_callback=phase_callback)
        else:
            rem_status = "skipped"
            for stage_name, last_run, minimum_age, runner in (
                ("light", dream_state.last_light, timedelta(days=1), run_light),
                ("deep", dream_state.last_deep, timedelta(days=7), run_deep),
                ("rem", dream_state.last_rem, timedelta(days=30), run_rem),
            ):
                if not _is_due(last_run, minimum_age=minimum_age):
                    _record_phase(
                        state,
                        run_id,
                        stage=f"dream:{stage_name}",
                        status="skipped",
                        message=f"cadence not due (last={last_run})",
                        phases=phases,
                        phase_callback=phase_callback,
                    )
                    if stage_name == "rem":
                        rem_status = "skipped"
                    continue
                try:
                    result = runner(dry_run=False, acquire_lock=False)
                    message = result.summary
                    if result.warnings:
                        message = f"{message} warnings={' | '.join(result.warnings[:3])}"
                    _record_phase(
                        state,
                        run_id,
                        stage=f"dream:{stage_name}",
                        status=result.status,
                        message=message,
                        phases=phases,
                        phase_callback=phase_callback,
                    )
                    if stage_name == "rem":
                        rem_status = result.status
                except DreamBlockedError as exc:
                    _record_phase(state, run_id, stage=f"dream:{stage_name}", status="blocked", message=str(exc), phases=phases, phase_callback=phase_callback)
                    if stage_name == "rem":
                        rem_status = "blocked"
                except DreamPreconditionError as exc:
                    _record_phase(state, run_id, stage=f"dream:{stage_name}", status="blocked", message=str(exc), phases=phases, phase_callback=phase_callback)
                    if stage_name == "rem":
                        rem_status = "blocked"
                except Exception as exc:
                    _record_phase(state, run_id, stage=f"dream:{stage_name}", status="failed", message=str(exc), phases=phases, phase_callback=phase_callback)
                    if stage_name == "rem":
                        rem_status = "failed"

            weave_cfg = vault.config.dream.weave
            if not weave_cfg.enabled:
                _record_phase(state, run_id, stage="dream:weave", status="skipped", message="disabled in config", phases=phases, phase_callback=phase_callback)
            elif not weave_cfg.run_after_rem:
                _record_phase(state, run_id, stage="dream:weave", status="skipped", message="run_after_rem disabled in config", phases=phases, phase_callback=phase_callback)
            elif rem_status != "completed":
                _record_phase(
                    state,
                    run_id,
                    stage="dream:weave",
                    status="skipped",
                    message=f"REM did not complete ({rem_status})",
                    phases=phases,
                    phase_callback=phase_callback,
                )
            else:
                try:
                    result = run_weave(dry_run=False, acquire_lock=False)
                    message = result.summary
                    if result.warnings:
                        message = f"{message} warnings={' | '.join(result.warnings[:3])}"
                    _record_phase(
                        state,
                        run_id,
                        stage="dream:weave",
                        status=result.status,
                        message=message,
                        phases=phases,
                        phase_callback=phase_callback,
                    )
                except DreamBlockedError as exc:
                    _record_phase(state, run_id, stage="dream:weave", status="blocked", message=str(exc), phases=phases, phase_callback=phase_callback)
                except DreamPreconditionError as exc:
                    _record_phase(state, run_id, stage="dream:weave", status="blocked", message=str(exc), phases=phases, phase_callback=phase_callback)
                except Exception as exc:
                    _record_phase(state, run_id, stage="dream:weave", status="failed", message=str(exc), phases=phases, phase_callback=phase_callback)

        overall_status = "completed"
        if any(phase.status == "failed" for phase in phases):
            overall_status = "failed"
        elif any(phase.status == "blocked" for phase in phases):
            overall_status = "blocked"
        summary = _phase_result_summary(phases)
        state.finish_run(run_id, status=overall_status, notes=summary)
        return DailyOrchestratorResult(status=overall_status, phases=phases)
    finally:
        state.release_lock(holder="orchestrate-daily")
