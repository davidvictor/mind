from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import sys
from types import SimpleNamespace

from mind.cli import main
from mind.dream.common import DreamResult
from mind.runtime_state import RuntimeState
from mind.services.provider_ops import PullResult
from mind.commands.ingest import BooksIngestResult, SubstackIngestResult, YouTubeIngestResult
from mind.services.queue_worker import QueueProcessResult
from tests.support import write_repo_config


def _write_config(root: Path) -> None:
    write_repo_config(
        root,
        create_me=True,
        create_indexes=True,
        create_exports=True,
        create_digests=True,
        ingestors_enabled=["substack", "articles", "youtube", "books", "audible"],
        dream_enabled=True,
    )


def _patch_roots(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("mind.commands.common.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.config.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.doctor.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.digest.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.worker.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.orchestrate.project_root", lambda: root)


@contextmanager
def _fake_progress(*_args, **_kwargs):
    class _Progress:
        def phase(self, message: str) -> None:
            print(f"[progress] {message}", file=sys.stderr)

        def update(self, message: str) -> None:
            print(f"[progress] {message}", file=sys.stderr)

        def clear(self, *, newline: bool = False) -> None:
            if newline:
                print("", file=sys.stderr)

    yield _Progress()


def test_digest_command_writes_digest(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_roots(monkeypatch, tmp_path)

    assert main(["digest", "--today", "2026-04-09"]) == 0
    out = capsys.readouterr().out.strip()
    target = tmp_path / "memory" / "me" / "digests" / "2026-04-09.md"
    assert out.endswith(str(target))
    assert target.exists()


def test_worker_drain_until_empty_continues_past_failures(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_roots(monkeypatch, tmp_path)
    state = RuntimeState.for_repo_root(tmp_path)
    state.enqueue_run(
        queue_name="links",
        kind="mcp.enqueue_links",
        metadata={
            "count": 1,
            "path": str(tmp_path / "raw" / "drops" / "articles-from-mcp-2026-04-08.jsonl"),
            "links": [{"url": "https://example.com", "title": "Example"}],
        },
        last_item_ref="ok",
    )
    state.enqueue_run(
        queue_name="skills",
        kind="mcp.set_skill_status",
        metadata={"skill_id": "missing-skill", "status": "archived"},
        last_item_ref="missing-skill",
    )

    assert main(["worker", "drain-until-empty"]) == 1
    out = capsys.readouterr().out
    assert "processed=2" in out
    assert "failed=1" in out

    runs = state.list_runs(limit=10)
    assert any(run.kind == "mcp.enqueue_links" and run.status == "completed" for run in runs)
    assert any(run.kind == "mcp.set_skill_status" and run.status == "failed" for run in runs)


def test_worker_drain_progress_goes_to_stderr(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_roots(monkeypatch, tmp_path)
    monkeypatch.setattr("mind.commands.worker.progress_for_args", _fake_progress)
    monkeypatch.setattr("mind.commands.worker.drain_until_empty", lambda *args, **kwargs: QueueProcessResult(processed=1, failures=0))

    assert main(["worker", "drain-until-empty"]) == 0
    captured = capsys.readouterr()
    assert "processed=1 failed=0" in captured.out
    assert "[progress] draining queued work" in captured.err


def test_orchestrate_daily_runs_phases_and_records_run(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_roots(monkeypatch, tmp_path)
    calls: list[str] = []

    youtube_export = tmp_path / "raw" / "exports" / "youtube-recent-2026-04-09.json"
    audible_export = tmp_path / "raw" / "exports" / "audible-library-2026-04-09.json"
    substack_export = tmp_path / "raw" / "exports" / "substack-saved-2026-04-09.json"
    for export in (youtube_export, audible_export, substack_export):
        export.write_text("[]", encoding="utf-8")

    monkeypatch.setattr(
        "mind.services.orchestrator.run_youtube_pull",
        lambda repo_root: calls.append("provider:youtube") or PullResult(label="youtube", exit_code=0, detail=str(youtube_export), export_path=youtube_export),
    )
    monkeypatch.setattr(
        "mind.services.orchestrator.run_audible_pull",
        lambda repo_root: calls.append("provider:audible") or PullResult(label="audible", exit_code=0, detail=str(audible_export), export_path=audible_export),
    )
    monkeypatch.setattr(
        "mind.services.orchestrator.run_substack_pull",
        lambda repo_root: calls.append("provider:substack") or PullResult(label="substack", exit_code=0, detail=str(substack_export), export_path=substack_export),
    )
    monkeypatch.setattr(
        "mind.services.orchestrator.sweep_dropbox",
        lambda repo_root, dry_run=False: calls.append("dropbox:sweep")
        or SimpleNamespace(
            scanned_count=0,
            processed_count=0,
            failed_count=0,
            unsupported_count=0,
            review_count=0,
            pending_count_after=0,
            last_item_ref=None,
            has_failures=False,
            metadata={},
        ),
    )
    monkeypatch.setattr(
        "mind.services.orchestrator.ingest_youtube_export",
        lambda path: YouTubeIngestResult(pages_written=2),
    )
    monkeypatch.setattr(
        "mind.services.orchestrator.ingest_books_export",
        lambda path: BooksIngestResult(pages_written=4, page_ids=["a", "b", "c", "d"]),
    )
    monkeypatch.setattr(
        "mind.services.orchestrator.ingest_substack_export",
        lambda export_path=None, drain_articles=True: SubstackIngestResult(
            posts_written=3,
            paywalled=0,
            failures=0,
            unsaved_refs=1,
            linked_articles_fetched=2,
            export_path=substack_export,
        ),
    )
    monkeypatch.setattr(
        "mind.services.orchestrator.ingest_articles_queue",
        lambda repo_root=None, today=None: SimpleNamespace(
            drop_files_processed=0,
            fetched_summarized=0,
            failed=0,
        ),
    )
    monkeypatch.setattr(
        "mind.services.orchestrator.drain_until_empty",
        lambda repo_root, acquire_lock=False, allowed_queue_prefixes=None: QueueProcessResult(processed=0, failures=0),
    )
    monkeypatch.setattr(
        "mind.services.orchestrator.run_light",
        lambda dry_run=False, acquire_lock=False: DreamResult(stage="light", dry_run=False, summary="Light done"),
    )
    monkeypatch.setattr(
        "mind.services.orchestrator.run_deep",
        lambda dry_run=False, acquire_lock=False: DreamResult(stage="deep", dry_run=False, summary="Deep done"),
    )
    monkeypatch.setattr(
        "mind.services.orchestrator.run_rem",
        lambda dry_run=False, acquire_lock=False: DreamResult(stage="rem", dry_run=False, summary="REM done"),
    )
    assert main(["orchestrate", "daily"]) == 0
    out = capsys.readouterr().out
    assert "Orchestrator status: completed" in out
    assert "dropbox:sweep: skipped" in out
    assert "provider:youtube: completed" in out
    assert "dream:light: completed" in out
    assert "dream:weave" not in out
    assert calls[0] == "dropbox:sweep"

    state = RuntimeState.for_repo_root(tmp_path)
    daily = next(run for run in state.list_runs(limit=10) if run.kind == "orchestrate.daily")
    assert daily.status == "completed"
    details = state.get_run(daily.id)
    assert details is not None
    assert any(event.stage == "dropbox:sweep" and event.event_type == "skipped" for event in details.events)
    assert any(event.stage == "provider:youtube" and event.event_type == "completed" for event in details.events)


def test_orchestrate_daily_progress_goes_to_stderr(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_roots(monkeypatch, tmp_path)
    monkeypatch.setattr("mind.commands.orchestrate.progress_for_args", _fake_progress)
    monkeypatch.setattr(
        "mind.commands.orchestrate.run_daily_orchestrator",
        lambda repo_root, phase_callback=None: phase_callback("provider:youtube: completed")
        or SimpleNamespace(render=lambda: "Orchestrator status: completed", exit_code=0),
    )

    assert main(["orchestrate", "daily"]) == 0
    captured = capsys.readouterr()
    assert "Orchestrator status: completed" in captured.out
    assert "[progress] running daily orchestrator" in captured.err
    assert "[progress] provider:youtube: completed" in captured.err


def test_orchestrate_daily_surfaces_dream_quality_warnings(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_roots(monkeypatch, tmp_path)
    export = tmp_path / "raw" / "exports" / "youtube-recent-2026-04-09.json"
    export.write_text("[]", encoding="utf-8")

    monkeypatch.setattr(
        "mind.services.orchestrator.run_youtube_pull",
        lambda repo_root: PullResult(label="youtube", exit_code=0, detail=str(export), export_path=export),
    )
    monkeypatch.setattr("mind.services.orchestrator.run_audible_pull", lambda repo_root: PullResult(label="audible", exit_code=1, detail="disabled", export_path=None))
    monkeypatch.setattr("mind.services.orchestrator.run_substack_pull", lambda repo_root: PullResult(label="substack", exit_code=1, detail="disabled", export_path=None))
    monkeypatch.setattr(
        "mind.services.orchestrator.sweep_dropbox",
        lambda repo_root, dry_run=False: SimpleNamespace(
            scanned_count=0,
            processed_count=0,
            failed_count=0,
            unsupported_count=0,
            review_count=0,
            pending_count_after=0,
            last_item_ref=None,
            has_failures=False,
            metadata={},
        ),
    )
    monkeypatch.setattr("mind.services.orchestrator.ingest_youtube_export", lambda path: YouTubeIngestResult(pages_written=0))
    monkeypatch.setattr(
        "mind.services.orchestrator.ingest_articles_queue",
        lambda repo_root=None, today=None: SimpleNamespace(drop_files_processed=0, fetched_summarized=0, failed=0),
    )
    monkeypatch.setattr(
        "mind.services.orchestrator.drain_until_empty",
        lambda repo_root, acquire_lock=False, allowed_queue_prefixes=None: QueueProcessResult(processed=0, failures=0),
    )
    monkeypatch.setattr(
        "mind.services.orchestrator.run_light",
        lambda dry_run=False, acquire_lock=False: DreamResult(
            stage="light",
            dry_run=False,
            summary="Light done",
            warnings=["lane quality degraded: YouTube: partial-fidelity (quote_coverage_low)"],
        ),
    )
    monkeypatch.setattr(
        "mind.services.orchestrator.run_deep",
        lambda dry_run=False, acquire_lock=False: DreamResult(stage="deep", dry_run=False, summary="Deep done"),
    )
    monkeypatch.setattr(
        "mind.services.orchestrator.run_rem",
        lambda dry_run=False, acquire_lock=False: DreamResult(stage="rem", dry_run=False, summary="REM done"),
    )
    assert main(["orchestrate", "daily"]) == 1
    out = capsys.readouterr().out
    assert "quote_coverage_low" in out

    state = RuntimeState.for_repo_root(tmp_path)
    daily = next(run for run in state.list_runs(limit=10) if run.kind == "orchestrate.daily")
    details = state.get_run(daily.id)
    assert details is not None
    light_event = next(event for event in details.events if event.stage == "dream:light" and event.event_type == "completed")
    assert "quote_coverage_low" in (light_event.message or "")


def test_orchestrate_daily_ignores_legacy_weave_config_after_cutover(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_roots(monkeypatch, tmp_path)
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        cfg.read_text(encoding="utf-8").replace(
            "dream:\n  enabled: true\n",
            "dream:\n  enabled: true\n  v2:\n    weave_shadow_enabled: true\n",
            1,
        ),
        encoding="utf-8",
    )
    export = tmp_path / "raw" / "exports" / "youtube-recent-2026-04-09.json"
    export.write_text("[]", encoding="utf-8")

    monkeypatch.setattr(
        "mind.services.orchestrator.run_youtube_pull",
        lambda repo_root: PullResult(label="youtube", exit_code=0, detail=str(export), export_path=export),
    )
    monkeypatch.setattr("mind.services.orchestrator.run_audible_pull", lambda repo_root: PullResult(label="audible", exit_code=1, detail="disabled", export_path=None))
    monkeypatch.setattr("mind.services.orchestrator.run_substack_pull", lambda repo_root: PullResult(label="substack", exit_code=1, detail="disabled", export_path=None))
    monkeypatch.setattr(
        "mind.services.orchestrator.sweep_dropbox",
        lambda repo_root, dry_run=False: SimpleNamespace(
            scanned_count=0,
            processed_count=0,
            failed_count=0,
            unsupported_count=0,
            review_count=0,
            pending_count_after=0,
            last_item_ref=None,
            has_failures=False,
            metadata={},
        ),
    )
    monkeypatch.setattr("mind.services.orchestrator.ingest_youtube_export", lambda path: YouTubeIngestResult(pages_written=0))
    monkeypatch.setattr(
        "mind.services.orchestrator.ingest_articles_queue",
        lambda repo_root=None, today=None: SimpleNamespace(drop_files_processed=0, fetched_summarized=0, failed=0),
    )
    monkeypatch.setattr(
        "mind.services.orchestrator.drain_until_empty",
        lambda repo_root, acquire_lock=False, allowed_queue_prefixes=None: QueueProcessResult(processed=0, failures=0),
    )
    monkeypatch.setattr("mind.services.orchestrator.run_light", lambda dry_run=False, acquire_lock=False: DreamResult(stage="light", dry_run=False, summary="Light done"))
    monkeypatch.setattr("mind.services.orchestrator.run_deep", lambda dry_run=False, acquire_lock=False: DreamResult(stage="deep", dry_run=False, summary="Deep done"))
    monkeypatch.setattr("mind.services.orchestrator.run_rem", lambda dry_run=False, acquire_lock=False: DreamResult(stage="rem", dry_run=False, summary="REM done"))

    assert main(["orchestrate", "daily"]) == 1
    out = capsys.readouterr().out
    assert "dream:weave" not in out


def test_orchestrate_daily_marks_dropbox_review_as_failed_and_degraded_queue(tmp_path: Path, monkeypatch, capsys):
    _write_config(tmp_path)
    _patch_roots(monkeypatch, tmp_path)
    export = tmp_path / "raw" / "exports" / "youtube-recent-2026-04-09.json"
    export.write_text("[]", encoding="utf-8")

    monkeypatch.setattr(
        "mind.services.orchestrator.run_youtube_pull",
        lambda repo_root: PullResult(label="youtube", exit_code=0, detail=str(export), export_path=export),
    )
    monkeypatch.setattr("mind.services.orchestrator.run_audible_pull", lambda repo_root: PullResult(label="audible", exit_code=1, detail="disabled", export_path=None))
    monkeypatch.setattr("mind.services.orchestrator.run_substack_pull", lambda repo_root: PullResult(label="substack", exit_code=1, detail="disabled", export_path=None))
    monkeypatch.setattr(
        "mind.services.orchestrator.sweep_dropbox",
        lambda repo_root, dry_run=False: SimpleNamespace(
            scanned_count=1,
            processed_count=0,
            failed_count=0,
            unsupported_count=0,
            review_count=1,
            pending_count_after=0,
            last_item_ref=None,
            has_failures=False,
            metadata={"review_count": 1, "review_items": [{"source_path": "dropbox/note.md", "detail": "graph review required"}]},
        ),
    )
    monkeypatch.setattr("mind.services.orchestrator.ingest_youtube_export", lambda path: YouTubeIngestResult(pages_written=0))
    monkeypatch.setattr(
        "mind.services.orchestrator.ingest_articles_queue",
        lambda repo_root=None, today=None: SimpleNamespace(drop_files_processed=0, fetched_summarized=0, failed=0),
    )
    monkeypatch.setattr(
        "mind.services.orchestrator.drain_until_empty",
        lambda repo_root, acquire_lock=False, allowed_queue_prefixes=None: QueueProcessResult(processed=0, failures=0),
    )
    monkeypatch.setattr(
        "mind.services.orchestrator.run_light",
        lambda dry_run=False, acquire_lock=False: DreamResult(stage="light", dry_run=False, summary="Light done"),
    )
    monkeypatch.setattr(
        "mind.services.orchestrator.run_deep",
        lambda dry_run=False, acquire_lock=False: DreamResult(stage="deep", dry_run=False, summary="Deep done"),
    )
    monkeypatch.setattr(
        "mind.services.orchestrator.run_rem",
        lambda dry_run=False, acquire_lock=False: DreamResult(stage="rem", dry_run=False, summary="REM done"),
    )
    assert main(["orchestrate", "daily"]) == 1
    out = capsys.readouterr().out
    assert "dropbox:sweep: failed" in out
    assert "review=1" in out

    queue = {item.name: item for item in RuntimeState.for_repo_root(tmp_path).list_queue()}
    assert queue["dropbox"].status == "degraded"
