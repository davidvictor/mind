"""Typed row models for the operational runtime state store."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LockInfo:
    name: str
    holder: str
    acquired_at: str


@dataclass(frozen=True)
class DreamState:
    last_light: str | None
    last_deep: str | None
    last_rem: str | None
    light_passes_since_deep: int
    deep_passes_since_rem: int
    last_lock_holder: str | None
    last_lock_acquired_at: str | None
    last_skip_reason: str | None
    updated_at: str


@dataclass(frozen=True)
class RunRecord:
    id: int
    kind: str
    status: str
    holder: str | None
    started_at: str
    finished_at: str | None
    notes: str | None
    metadata_json: str | None
    queue_name: str | None
    item_ref: str | None
    retry_count: int
    next_attempt_at: str | None


@dataclass(frozen=True)
class RunEventRecord:
    id: int
    run_id: int
    stage: str
    event_type: str
    message: str | None
    created_at: str
    payload_json: str | None


@dataclass(frozen=True)
class ErrorRecord:
    id: int
    run_id: int | None
    stage: str | None
    error_type: str
    message: str
    traceback: str | None
    created_at: str
    payload_json: str | None


@dataclass(frozen=True)
class QueueRecord:
    name: str
    status: str
    pending_count: int
    last_item_ref: str | None
    last_run_id: int | None
    updated_at: str
    metadata_json: str | None


@dataclass(frozen=True)
class SkillSummary:
    skill_name: str
    usage_count: int
    artifact_count: int
    last_used_at: str | None


@dataclass(frozen=True)
class RunDetails:
    run: RunRecord
    events: list[RunEventRecord]
    errors: list[ErrorRecord]


@dataclass(frozen=True)
class RuntimeSummary:
    db_path: str
    schema_version: str
    active_locks: int
    run_count: int
    queue_entries: int
    tracked_skills: int
    dream_state: DreamState
