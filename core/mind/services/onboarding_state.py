"""Chunk-level resumable state for onboarding synthesis phases."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import tempfile
from typing import Literal

from pydantic import BaseModel, Field


ChunkPhase = Literal["graph_nodes", "merge_nodes", "merge_relationships"]
ChunkStatusValue = Literal["pending", "in_flight", "done", "failed"]
STALE_LEASE_SECONDS = 300
MAX_CHUNK_ATTEMPTS = 3


class ChunkState(BaseModel):
    bundle_id: str
    phase: ChunkPhase
    chunk_id: str
    status: ChunkStatusValue = "pending"
    owner_pid: int | None = None
    attempts: int = 0
    last_attempt_started_at: str | None = None
    last_attempt_finished_at: str | None = None
    last_error: str | None = None
    generation_id: str | None = None
    retry_after_seconds: int | None = None
    next_retry_not_before_at: str | None = None
    result_path: str | None = None


class ChunkSummary(BaseModel):
    done: int = 0
    in_flight: int = 0
    pending: int = 0
    failed: int = 0
    cooling_down: int = 0
    total: int = 0
    next_retry_not_before_at: str | None = None

    def render(self) -> str:
        base = (
            f"{self.done}/{self.total} done, "
            f"{self.in_flight} in_flight, "
            f"{self.pending} pending, "
            f"{self.failed} failed"
        )
        if self.cooling_down and self.next_retry_not_before_at:
            return f"{base}, {self.cooling_down} cooling down until {self.next_retry_not_before_at}"
        return base


def chunk_phase_dir(bundle_dir: Path, *, phase: ChunkPhase) -> Path:
    return bundle_dir / "chunks" / phase


def chunk_state_path(bundle_dir: Path, *, phase: ChunkPhase, chunk_id: str) -> Path:
    return chunk_phase_dir(bundle_dir, phase=phase) / f"{chunk_id}.state.json"


def chunk_result_path(bundle_dir: Path, *, phase: ChunkPhase, chunk_id: str) -> Path:
    return chunk_phase_dir(bundle_dir, phase=phase) / f"{chunk_id}.result.json"


def ensure_chunk_state(bundle_dir: Path, *, bundle_id: str, phase: ChunkPhase, chunk_id: str) -> ChunkState:
    path = chunk_state_path(bundle_dir, phase=phase, chunk_id=chunk_id)
    if path.exists():
        return ChunkState.model_validate_json(path.read_text(encoding="utf-8"))
    state = ChunkState(bundle_id=bundle_id, phase=phase, chunk_id=chunk_id)
    _atomic_write_json(path, state.model_dump(mode="json"))
    return state


def load_chunk_states(bundle_dir: Path, *, phase: ChunkPhase) -> list[ChunkState]:
    phase_dir = chunk_phase_dir(bundle_dir, phase=phase)
    if not phase_dir.exists():
        return []
    states: list[ChunkState] = []
    for path in sorted(phase_dir.glob("*.state.json")):
        states.append(ChunkState.model_validate_json(path.read_text(encoding="utf-8")))
    return states


def prune_chunk_phase(bundle_dir: Path, *, phase: ChunkPhase, keep_chunk_ids: set[str]) -> None:
    phase_dir = chunk_phase_dir(bundle_dir, phase=phase)
    if not phase_dir.exists():
        return
    for state in load_chunk_states(bundle_dir, phase=phase):
        if state.chunk_id in keep_chunk_ids:
            continue
        state_path = chunk_state_path(bundle_dir, phase=phase, chunk_id=state.chunk_id)
        result_path = chunk_result_path(bundle_dir, phase=phase, chunk_id=state.chunk_id)
        if state_path.exists():
            state_path.unlink()
        if result_path.exists():
            result_path.unlink()
    for result_path in phase_dir.glob("*.result.json"):
        chunk_id = result_path.name.removesuffix(".result.json")
        if chunk_id not in keep_chunk_ids and result_path.exists():
            result_path.unlink()


def acquire_chunk_lease(bundle_dir: Path, *, bundle_id: str, phase: ChunkPhase, chunk_id: str) -> tuple[bool, ChunkState]:
    state = ensure_chunk_state(bundle_dir, bundle_id=bundle_id, phase=phase, chunk_id=chunk_id)
    if state.status == "done":
        return False, state
    if state.status == "failed" and state.attempts >= MAX_CHUNK_ATTEMPTS:
        return False, state
    if state.status == "failed" and _retry_window_active(state):
        return False, state
    if state.status == "in_flight" and not _lease_is_stale(state):
        return False, state
    state.status = "in_flight"
    state.owner_pid = os.getpid()
    state.attempts += 1
    state.last_attempt_started_at = _utc_now_iso()
    state.last_attempt_finished_at = None
    state.last_error = None
    state.retry_after_seconds = None
    state.next_retry_not_before_at = None
    write_chunk_state(bundle_dir, state=state)
    return True, state


def mark_chunk_done(
    bundle_dir: Path,
    *,
    state: ChunkState,
    payload: dict,
    generation_id: str | None,
) -> ChunkState:
    result_path = chunk_result_path(bundle_dir, phase=state.phase, chunk_id=state.chunk_id)
    _atomic_write_json(result_path, payload)
    state.status = "done"
    state.owner_pid = None
    state.last_attempt_finished_at = _utc_now_iso()
    state.last_error = None
    state.generation_id = generation_id
    state.retry_after_seconds = None
    state.next_retry_not_before_at = None
    state.result_path = result_path.as_posix()
    write_chunk_state(bundle_dir, state=state)
    return state


def mark_chunk_failed(
    bundle_dir: Path,
    *,
    state: ChunkState,
    error_message: str,
    retry_after_seconds: int | None,
) -> ChunkState:
    state.status = "failed"
    state.owner_pid = None
    state.last_attempt_finished_at = _utc_now_iso()
    state.last_error = error_message
    effective_retry = max(_exponential_backoff_seconds(state.attempts), retry_after_seconds or 0)
    state.retry_after_seconds = effective_retry
    state.next_retry_not_before_at = _utc_after_iso(effective_retry)
    write_chunk_state(bundle_dir, state=state)
    return state


def write_chunk_state(bundle_dir: Path, *, state: ChunkState) -> None:
    path = chunk_state_path(bundle_dir, phase=state.phase, chunk_id=state.chunk_id)
    _atomic_write_json(path, state.model_dump(mode="json"))


def load_chunk_result(bundle_dir: Path, *, phase: ChunkPhase, chunk_id: str) -> dict:
    path = chunk_result_path(bundle_dir, phase=phase, chunk_id=chunk_id)
    return json.loads(path.read_text(encoding="utf-8"))


def summarize_chunk_phase(bundle_dir: Path, *, phase: ChunkPhase) -> ChunkSummary | None:
    states = load_chunk_states(bundle_dir, phase=phase)
    if not states:
        return None
    summary = ChunkSummary(total=len(states))
    next_retry_values: list[str] = []
    for state in states:
        if state.status == "done":
            summary.done += 1
        elif state.status == "in_flight" and _lease_is_stale(state):
            summary.pending += 1
        elif state.status == "in_flight":
            summary.in_flight += 1
        elif state.status == "pending":
            summary.pending += 1
        elif state.status == "failed":
            summary.failed += 1
            if _retry_window_active(state):
                summary.cooling_down += 1
                if state.next_retry_not_before_at:
                    next_retry_values.append(state.next_retry_not_before_at)
    if next_retry_values:
        summary.next_retry_not_before_at = min(next_retry_values)
    return summary


def iter_runnable_states(bundle_dir: Path, *, phase: ChunkPhase) -> list[ChunkState]:
    runnable: list[ChunkState] = []
    for state in load_chunk_states(bundle_dir, phase=phase):
        if state.status == "done":
            continue
        if state.status == "failed" and state.attempts >= MAX_CHUNK_ATTEMPTS:
            continue
        if state.status == "failed" and _retry_window_active(state):
            continue
        if state.status == "in_flight" and not _lease_is_stale(state):
            continue
        runnable.append(state)
    return runnable


def next_retry_not_before(bundle_dir: Path, *, phase: ChunkPhase) -> str | None:
    values: list[str] = []
    for state in load_chunk_states(bundle_dir, phase=phase):
        if state.status == "failed" and _retry_window_active(state) and state.next_retry_not_before_at:
            values.append(state.next_retry_not_before_at)
    return min(values) if values else None


def _lease_is_stale(state: ChunkState) -> bool:
    if state.owner_pid is not None and not _process_exists(state.owner_pid):
        return True
    if not state.last_attempt_started_at:
        return True
    try:
        started_at = datetime.fromisoformat(state.last_attempt_started_at.replace("Z", "+00:00"))
    except ValueError:
        return True
    return started_at <= datetime.now(timezone.utc) - timedelta(seconds=STALE_LEASE_SECONDS)


def _process_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _retry_window_active(state: ChunkState) -> bool:
    if state.status != "failed" or not state.next_retry_not_before_at:
        return False
    try:
        not_before = datetime.fromisoformat(state.next_retry_not_before_at.replace("Z", "+00:00"))
    except ValueError:
        return False
    return datetime.now(timezone.utc) < not_before


def _exponential_backoff_seconds(attempts: int) -> int:
    sequence = {1: 30, 2: 120, 3: 300}
    return sequence.get(attempts, 300)


def _utc_now_iso() -> str:
    return _utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")


def _utc_after_iso(seconds: int) -> str:
    return (_utc_now() + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=path.parent) as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
        temp_name = handle.name
    os.replace(temp_name, path)
