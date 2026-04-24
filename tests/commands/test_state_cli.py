from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from mind.cli import main
from mind.runtime_state import RuntimeState
from tests.support import write_repo_config


def _write_config(root: Path) -> None:
    write_repo_config(root, create_me=True)


def test_mind_state_summary(tmp_path: Path, capsys):
    _write_config(tmp_path)
    state = RuntimeState.for_repo_root(tmp_path)
    state.update_dream_state(
        last_light="2026-04-08T12:00:00Z",
        light_passes_since_deep=4,
        deep_passes_since_rem=1,
    )

    with patch("mind.cli._project_root", return_value=tmp_path):
        rc = main(["state"])

    assert rc == 0
    out = capsys.readouterr().out
    assert "Runtime DB:" in out
    assert "Schema version: 1" in out
    assert "light=2026-04-08T12:00:00Z" in out
    assert "weave=" not in out


def test_mind_state_subcommands_render_records(tmp_path: Path, capsys):
    _write_config(tmp_path)
    state = RuntimeState.for_repo_root(tmp_path)
    run_id = state.create_run(
        kind="ingest.youtube",
        holder="worker-1",
        queue_name="ingest:youtube",
        item_ref="exports/youtube-history.json",
    )
    state.add_run_event(
        run_id,
        stage="queue",
        event_type="claimed",
        message="worker claimed ingest.youtube",
        payload={"queue_name": "ingest:youtube", "item_ref": "exports/youtube-history.json"},
    )
    state.add_run_event(
        run_id,
        stage="ingest",
        event_type="progress",
        message="processing source",
        payload={"source_id": "youtube:abc123xyz00", "current": 2, "total": 5},
    )
    state.add_run_event(
        run_id,
        stage="ingest",
        event_type="completed",
        message="finished batch",
        payload={"counts": {"selected": 5, "skipped": 1, "failed": 0, "pages_written": 4}},
    )
    state.finish_run(run_id, status="completed", notes="selected=5 failed=0 pages_written=4")
    state.add_error(run_id=run_id, stage="ingest", error_type="ValueError", message="bad page")
    active_run_id = state.create_run(kind="ingest.substack", holder="worker-2")
    state.add_run_event(
        active_run_id,
        stage="ingest",
        event_type="progress",
        message="processing saved posts",
        payload={"source": "substack:post-42", "current": 2, "total": 5},
    )
    export_only_run_id = state.create_run(
        kind="youtube.ingest",
        holder="worker-3",
        metadata={"export_path": str(tmp_path / "raw" / "exports" / "youtube-recent-2026-04-09.json")},
    )
    state.upsert_queue_state(name="articles", status="pending", pending_count=2, last_run_id=run_id)
    state.acquire_lock(holder="dream-light")
    state.record_skill_usage(skill_name="skill-creator", run_id=run_id)
    state.record_skill_artifact(
        skill_name="skill-creator",
        artifact_type="markdown",
        artifact_ref="skills/foo/SKILL.md",
        run_id=run_id,
    )

    with patch("mind.cli._project_root", return_value=tmp_path):
        assert main(["state", "runs"]) == 0
        runs_out = capsys.readouterr().out
        assert "id\tkind\tstatus\tholder\tstage\tfocus\tstarted\tfinished" in runs_out
        assert f"{active_run_id}\tingest.substack\trunning\tworker-2\tingest/progress progress=2/5" in runs_out
        assert "source=substack:post-42" in runs_out
        assert f"{export_only_run_id}\tyoutube.ingest\trunning\tworker-3\t-\tpath=" in runs_out

        assert main(["state", "run", str(run_id)]) == 0
        run_out = capsys.readouterr().out
        assert "Queue: ingest:youtube" in run_out
        assert "Focus: source=youtube:abc123xyz00" in run_out
        assert "Latest stage: ingest/completed" in run_out
        assert "Events:" in run_out
        assert "processing source | source=youtube:abc123xyz00 progress=2/5" in run_out
        assert "finished batch | selected=5 skipped=1 failed=0 pages_written=4" in run_out
        assert "Errors:" in run_out

        assert main(["state", "queue"]) == 0
        queue_out = capsys.readouterr().out
        assert "articles" in queue_out

        assert main(["state", "locks"]) == 0
        locks_out = capsys.readouterr().out
        assert "dream-light" in locks_out

        assert main(["state", "skills"]) == 0
        skills_out = capsys.readouterr().out
        assert "skill-creator" in skills_out
