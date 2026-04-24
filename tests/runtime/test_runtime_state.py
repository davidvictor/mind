from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path

import pytest

from mind.runtime_state import RuntimeState, RuntimeStateLockBusy


def _write_config(root: Path, *, split: bool = False) -> None:
    wiki_dir = "memory" if split else "memory"
    raw_dir = "raw"
    (root / "config.yaml").write_text(
        "vault:\n"
        f"  wiki_dir: {wiki_dir}\n"
        f"  raw_dir: {raw_dir}\n"
        "  owner_profile: me/profile.md\n"
        "llm:\n"
        "  model: google/gemini-2.5-pro\n",
        encoding="utf-8",
    )


def _write_legacy_state(root: Path, *, split: bool = False) -> None:
    wiki_root = root / "memory"
    wiki_root.mkdir(parents=True, exist_ok=True)
    (wiki_root / ".brain-state.json").write_text(
        "{\n"
        "  \"schema_version\": \"2.3\",\n"
        "  \"last_light_dream_at\": \"2026-04-07T10:00:00Z\",\n"
        "  \"last_deep_dream_at\": null,\n"
        "  \"last_rem_dream_at\": null,\n"
        "  \"light_passes_since_deep\": 2,\n"
        "  \"deep_passes_since_rem\": 1,\n"
        "  \"last_lock_holder\": \"legacy-holder\",\n"
        "  \"last_lock_acquired_at\": \"2026-04-07T11:00:00Z\",\n"
        "  \"last_skip_reason\": \"legacy-skip\"\n"
        "}\n",
        encoding="utf-8",
    )


def test_bootstrap_creates_schema_and_imports_legacy_dream_state(tmp_path: Path):
    _write_config(tmp_path, split=True)
    _write_legacy_state(tmp_path, split=True)

    state = RuntimeState.for_repo_root(tmp_path)

    assert state.db_path == tmp_path / ".brain-runtime.sqlite3"
    assert state.db_path.exists()
    assert state.schema_version() == "1"

    with sqlite3.connect(state.db_path) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    assert {
        "runtime_meta",
        "locks",
        "dream_state",
        "runs",
        "run_events",
        "errors",
        "ingest_queue",
        "adapter_state",
        "skill_artifacts",
        "skill_usage",
        "query_history",
        "mcp_sessions",
    }.issubset(tables)

    dream = state.get_dream_state()
    assert dream.last_light == "2026-04-07T10:00:00Z"
    assert dream.light_passes_since_deep == 2
    assert dream.last_lock_holder == "legacy-holder"
    assert dream.last_skip_reason == "legacy-skip"


def test_lock_acquire_release_and_busy(tmp_path: Path):
    _write_config(tmp_path)
    state = RuntimeState.for_repo_root(tmp_path)

    lock = state.acquire_lock(holder="ingest-substack")
    assert lock.holder == "ingest-substack"
    assert state.read_lock() is not None

    with pytest.raises(RuntimeStateLockBusy, match="ingest-substack"):
        state.acquire_lock(holder="dream-light")

    state.release_lock(holder="ingest-substack")
    assert state.read_lock() is None


def test_concurrent_lock_contention_raises_runtime_busy(tmp_path: Path):
    _write_config(tmp_path)

    barrier = threading.Barrier(2)
    results: list[tuple[str, str]] = []
    lock = threading.Lock()

    def contender(holder: str) -> None:
        state = RuntimeState.for_repo_root(tmp_path)
        barrier.wait()
        try:
            state.acquire_lock(holder=holder)
        except RuntimeStateLockBusy as exc:
            outcome = ("busy", str(exc))
        else:
            outcome = ("acquired", holder)
        with lock:
            results.append(outcome)

    first = threading.Thread(target=contender, args=("holder-a",))
    second = threading.Thread(target=contender, args=("holder-b",))
    first.start()
    second.start()
    first.join()
    second.join()

    assert len(results) == 2
    assert sorted(result[0] for result in results) == ["acquired", "busy"]
    assert any("brain lock held by" in result[1] for result in results if result[0] == "busy")


def test_stale_lock_recovery(tmp_path: Path):
    _write_config(tmp_path)
    state = RuntimeState.for_repo_root(tmp_path)

    with state.connect() as conn:
        conn.execute(
            "INSERT INTO locks(name, holder, acquired_at) VALUES (?, ?, ?)",
            ("brain", "old-holder", "2000-01-01T00:00:00Z"),
        )

    recovered = state.acquire_lock(holder="new-holder", stale_after_seconds=1)
    assert recovered.holder == "new-holder"
    assert state.read_lock() is not None
    assert state.read_lock().holder == "new-holder"


def test_update_dream_state_can_clear_nullable_fields(tmp_path: Path):
    _write_config(tmp_path)
    state = RuntimeState.for_repo_root(tmp_path)

    state.update_dream_state(last_skip_reason="skip-me", last_light="2026-04-08T12:00:00Z")
    updated = state.update_dream_state(last_skip_reason=None, last_light=None)

    assert updated.last_skip_reason is None
    assert updated.last_light is None


def test_runtime_updates_do_not_mutate_brain_state_atom_cache(tmp_path: Path):
    _write_config(tmp_path, split=True)
    wiki_root = tmp_path / "memory"
    wiki_root.mkdir(parents=True, exist_ok=True)
    state_path = wiki_root / ".brain-state.json"
    state_path.write_text(
        json.dumps(
            {
                "schema_version": "2.3",
                "last_light_dream_at": None,
                "last_deep_dream_at": None,
                "last_rem_dream_at": None,
                "light_passes_since_deep": 0,
                "deep_passes_since_rem": 0,
                "last_lock_holder": None,
                "last_lock_acquired_at": None,
                "last_skip_reason": None,
                "atoms": {
                    "last_built_at": "2026-04-09T00:00:00Z",
                    "count": 1,
                    "by_type": {
                        "concept": 1,
                        "playbook": 0,
                        "stance": 0,
                        "inquiry": 0,
                    },
                    "index": [
                        {
                            "id": "concept-a",
                            "type": "concept",
                            "path": "memory/concepts/concept-a.md",
                            "lifecycle_state": "active",
                            "domains": ["work"],
                            "topics": ["systems"],
                            "last_evidence_date": "2026-04-08",
                            "evidence_count": 1,
                            "tldr": "cached concept",
                        }
                    ],
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    state = RuntimeState.for_repo_root(tmp_path)
    updated = state.update_dream_state(
        last_light="2026-04-09T12:00:00Z",
        last_skip_reason="manual-run",
    )

    file_state = json.loads(state_path.read_text(encoding="utf-8"))
    assert updated.last_light == "2026-04-09T12:00:00Z"
    assert updated.last_skip_reason == "manual-run"
    assert file_state["last_light_dream_at"] is None
    assert file_state["last_skip_reason"] is None
    assert file_state["atoms"]["count"] == 1
    assert file_state["atoms"]["index"][0]["id"] == "concept-a"


def test_run_event_error_queue_and_skill_recording(tmp_path: Path):
    _write_config(tmp_path)
    state = RuntimeState.for_repo_root(tmp_path)

    run_id = state.create_run(kind="ingest", holder="articles")
    state.add_run_event(run_id, stage="fetch", event_type="started", message="fetching")
    state.add_error(run_id=run_id, stage="fetch", error_type="TimeoutError", message="slow source")
    state.finish_run(run_id, status="failed", notes="timed out")
    state.upsert_queue_state(
        name="articles",
        status="ready",
        pending_count=3,
        last_item_ref="drop-1",
        last_run_id=run_id,
    )
    state.record_skill_usage(skill_name="skill-creator", run_id=run_id, context="rem")
    state.record_skill_artifact(
        skill_name="skill-creator",
        artifact_type="markdown",
        artifact_ref="skills/foo/SKILL.md",
        run_id=run_id,
    )

    details = state.get_run(run_id)
    assert details is not None
    assert details.run.status == "failed"
    assert len(details.events) == 1
    assert len(details.errors) == 1

    queue = state.list_queue()
    assert len(queue) == 1
    assert queue[0].pending_count == 3

    skills = state.list_skill_usage()
    assert len(skills) == 1
    assert skills[0].skill_name == "skill-creator"
    assert skills[0].usage_count == 1
    assert skills[0].artifact_count == 1


def test_adapter_state_round_trip_and_clear(tmp_path: Path):
    _write_config(tmp_path)
    state = RuntimeState.for_repo_root(tmp_path)

    assert state.get_adapter_state("dream.bootstrap") is None

    state.upsert_adapter_state(
        adapter="dream.bootstrap",
        state={"status": "running", "completed_source_ids": ["summary-a"]},
    )
    assert state.get_adapter_state("dream.bootstrap") == {
        "status": "running",
        "completed_source_ids": ["summary-a"],
    }

    state.clear_adapter_state(adapter="dream.bootstrap")
    assert state.get_adapter_state("dream.bootstrap") is None
