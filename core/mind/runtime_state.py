"""SQLite-backed operational state for the Brain runtime."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any, Iterator

from mind.state_models import (
    DreamState,
    ErrorRecord,
    LockInfo,
    QueueRecord,
    RunDetails,
    RunEventRecord,
    RunRecord,
    RuntimeSummary,
    SkillSummary,
)
from scripts.common.vault import Vault

RUNTIME_DB_NAME = ".brain-runtime.sqlite3"
SCHEMA_VERSION = "1"
DEFAULT_LOCK_NAME = "brain"
DEFAULT_STALE_LOCK_SECONDS = 60 * 60
DEFAULT_RETRYABLE_LOCK_ATTEMPTS = 3
UNSET = object()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_string() -> str:
    return _utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_timestamp(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def _json_dumps(value: dict[str, Any] | None) -> str | None:
    if value is None:
        return None
    return json.dumps(value, sort_keys=True)


class RuntimeStateLockBusy(Exception):
    """Raised when the operational lock is already held by another workflow."""


class RuntimeState:
    """Typed API over the root-level SQLite operational state store."""

    def __init__(self, repo_root: Path):
        self.repo_root = repo_root
        self.vault = Vault.load(repo_root)
        self.db_path = self.vault.runtime_db

    @classmethod
    def for_repo_root(cls, repo_root: Path) -> "RuntimeState":
        state = cls(repo_root)
        state.bootstrap()
        return state

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def bootstrap(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS runtime_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS locks (
                    name TEXT PRIMARY KEY,
                    holder TEXT NOT NULL,
                    acquired_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS dream_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    last_light TEXT,
                    last_deep TEXT,
                    last_rem TEXT,
                    light_passes_since_deep INTEGER NOT NULL DEFAULT 0,
                    deep_passes_since_rem INTEGER NOT NULL DEFAULT 0,
                    last_lock_holder TEXT,
                    last_lock_acquired_at TEXT,
                    last_skip_reason TEXT,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    holder TEXT,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    notes TEXT,
                    metadata_json TEXT,
                    queue_name TEXT,
                    item_ref TEXT,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at TEXT
                );

                CREATE TABLE IF NOT EXISTS run_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
                    stage TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    message TEXT,
                    created_at TEXT NOT NULL,
                    payload_json TEXT
                );

                CREATE TABLE IF NOT EXISTS errors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER REFERENCES runs(id) ON DELETE SET NULL,
                    stage TEXT,
                    error_type TEXT NOT NULL,
                    message TEXT NOT NULL,
                    traceback TEXT,
                    created_at TEXT NOT NULL,
                    payload_json TEXT
                );

                CREATE TABLE IF NOT EXISTS ingest_queue (
                    name TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    pending_count INTEGER NOT NULL DEFAULT 0,
                    last_item_ref TEXT,
                    last_run_id INTEGER REFERENCES runs(id) ON DELETE SET NULL,
                    updated_at TEXT NOT NULL,
                    metadata_json TEXT
                );

                CREATE TABLE IF NOT EXISTS adapter_state (
                    adapter TEXT PRIMARY KEY,
                    state_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS skill_artifacts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    skill_name TEXT NOT NULL,
                    artifact_type TEXT NOT NULL,
                    artifact_ref TEXT NOT NULL,
                    run_id INTEGER REFERENCES runs(id) ON DELETE SET NULL,
                    created_at TEXT NOT NULL,
                    metadata_json TEXT
                );

                CREATE TABLE IF NOT EXISTS skill_usage (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    skill_name TEXT NOT NULL,
                    run_id INTEGER REFERENCES runs(id) ON DELETE SET NULL,
                    context TEXT,
                    used_at TEXT NOT NULL,
                    metadata_json TEXT
                );

                CREATE TABLE IF NOT EXISTS query_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    query_text TEXT NOT NULL,
                    run_id INTEGER REFERENCES runs(id) ON DELETE SET NULL,
                    status TEXT,
                    created_at TEXT NOT NULL,
                    metadata_json TEXT
                );

                CREATE TABLE IF NOT EXISTS mcp_sessions (
                    session_id TEXT PRIMARY KEY,
                    kind TEXT,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    metadata_json TEXT
                );
                """
            )
            existing_run_columns = {
                str(row["name"])
                for row in conn.execute("PRAGMA table_info(runs)").fetchall()
            }
            if "queue_name" not in existing_run_columns:
                conn.execute("ALTER TABLE runs ADD COLUMN queue_name TEXT")
            if "item_ref" not in existing_run_columns:
                conn.execute("ALTER TABLE runs ADD COLUMN item_ref TEXT")
            if "retry_count" not in existing_run_columns:
                conn.execute("ALTER TABLE runs ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0")
            if "next_attempt_at" not in existing_run_columns:
                conn.execute("ALTER TABLE runs ADD COLUMN next_attempt_at TEXT")
            now = _utc_now_string()
            conn.execute(
                """
                INSERT INTO runtime_meta(key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
                """,
                ("schema_version", SCHEMA_VERSION, now),
            )
            row = conn.execute("SELECT 1 FROM dream_state WHERE id = 1").fetchone()
            if row is None:
                legacy = self._read_legacy_dream_state()
                conn.execute(
                    """
                    INSERT INTO dream_state(
                        id, last_light, last_deep, last_rem,
                        light_passes_since_deep, deep_passes_since_rem,
                        last_lock_holder, last_lock_acquired_at, last_skip_reason, updated_at
                    ) VALUES(1, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        legacy.get("last_light_dream_at"),
                        legacy.get("last_deep_dream_at"),
                        legacy.get("last_rem_dream_at"),
                        int(legacy.get("light_passes_since_deep") or 0),
                        int(legacy.get("deep_passes_since_rem") or 0),
                        legacy.get("last_lock_holder"),
                        legacy.get("last_lock_acquired_at"),
                        legacy.get("last_skip_reason"),
                        now,
                    ),
                )

    def _read_legacy_dream_state(self) -> dict[str, Any]:
        if not self.vault.brain_state.exists():
            return {}
        try:
            return json.loads(self.vault.brain_state.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}

    def schema_version(self) -> str:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT value FROM runtime_meta WHERE key = 'schema_version'"
            ).fetchone()
        return str(row["value"]) if row else SCHEMA_VERSION

    def acquire_lock(
        self,
        *,
        holder: str,
        name: str = DEFAULT_LOCK_NAME,
        stale_after_seconds: int = DEFAULT_STALE_LOCK_SECONDS,
    ) -> LockInfo:
        self.bootstrap()
        now = _utc_now()
        now_text = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT holder, acquired_at FROM locks WHERE name = ?",
                (name,),
            ).fetchone()
            if row is not None:
                acquired_at = _parse_timestamp(str(row["acquired_at"]))
                if now - acquired_at <= timedelta(seconds=stale_after_seconds):
                    raise RuntimeStateLockBusy(f"brain lock held by {row['holder']}")
                conn.execute("DELETE FROM locks WHERE name = ?", (name,))
            try:
                conn.execute(
                    """
                    INSERT INTO locks(name, holder, acquired_at)
                    VALUES (?, ?, ?)
                    """,
                    (name, holder, now_text),
                )
            except sqlite3.IntegrityError as exc:
                current = conn.execute(
                    "SELECT holder FROM locks WHERE name = ?",
                    (name,),
                ).fetchone()
                current_holder = str(current["holder"]) if current is not None else "<unknown>"
                raise RuntimeStateLockBusy(f"brain lock held by {current_holder}") from exc
            self._update_dream_state(
                conn,
                last_lock_holder=holder,
                last_lock_acquired_at=now_text,
                last_skip_reason=None,
            )
        return LockInfo(name=name, holder=holder, acquired_at=now_text)

    def release_lock(self, *, holder: str | None = None, name: str = DEFAULT_LOCK_NAME) -> None:
        self.bootstrap()
        with self.connect() as conn:
            if holder is None:
                conn.execute("DELETE FROM locks WHERE name = ?", (name,))
                return
            conn.execute(
                "DELETE FROM locks WHERE name = ? AND holder = ?",
                (name, holder),
            )

    def read_lock(self, *, name: str = DEFAULT_LOCK_NAME) -> LockInfo | None:
        self.bootstrap()
        with self.connect() as conn:
            row = conn.execute(
                "SELECT name, holder, acquired_at FROM locks WHERE name = ?",
                (name,),
            ).fetchone()
        if row is None:
            return None
        return LockInfo(
            name=str(row["name"]),
            holder=str(row["holder"]),
            acquired_at=str(row["acquired_at"]),
        )

    def clear_stale_lock(
        self,
        *,
        name: str = DEFAULT_LOCK_NAME,
        stale_after_seconds: int = DEFAULT_STALE_LOCK_SECONDS,
    ) -> bool:
        self.bootstrap()
        lock = self.read_lock(name=name)
        if lock is None:
            return False
        acquired_at = _parse_timestamp(lock.acquired_at)
        if _utc_now() - acquired_at <= timedelta(seconds=stale_after_seconds):
            raise RuntimeStateLockBusy(f"brain lock held by {lock.holder}")
        self.release_lock(holder=None, name=name)
        return True

    def list_locks(self) -> list[LockInfo]:
        self.bootstrap()
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT name, holder, acquired_at FROM locks ORDER BY acquired_at ASC"
            ).fetchall()
        return [
            LockInfo(name=str(row["name"]), holder=str(row["holder"]), acquired_at=str(row["acquired_at"]))
            for row in rows
        ]

    def get_dream_state(self) -> DreamState:
        self.bootstrap()
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM dream_state WHERE id = 1").fetchone()
        assert row is not None
        return DreamState(
            last_light=row["last_light"],
            last_deep=row["last_deep"],
            last_rem=row["last_rem"],
            light_passes_since_deep=int(row["light_passes_since_deep"]),
            deep_passes_since_rem=int(row["deep_passes_since_rem"]),
            last_lock_holder=row["last_lock_holder"],
            last_lock_acquired_at=row["last_lock_acquired_at"],
            last_skip_reason=row["last_skip_reason"],
            updated_at=str(row["updated_at"]),
        )

    def update_dream_state(
        self,
        *,
        last_light: str | None | object = UNSET,
        last_deep: str | None | object = UNSET,
        last_rem: str | None | object = UNSET,
        light_passes_since_deep: int | object = UNSET,
        deep_passes_since_rem: int | object = UNSET,
        last_lock_holder: str | None | object = UNSET,
        last_lock_acquired_at: str | None | object = UNSET,
        last_skip_reason: str | None | object = UNSET,
    ) -> DreamState:
        self.bootstrap()
        with self.connect() as conn:
            self._update_dream_state(
                conn,
                last_light=last_light,
                last_deep=last_deep,
                last_rem=last_rem,
                light_passes_since_deep=light_passes_since_deep,
                deep_passes_since_rem=deep_passes_since_rem,
                last_lock_holder=last_lock_holder,
                last_lock_acquired_at=last_lock_acquired_at,
                last_skip_reason=last_skip_reason,
            )
        return self.get_dream_state()

    def _update_dream_state(self, conn: sqlite3.Connection, **updates: Any) -> None:
        current = conn.execute("SELECT * FROM dream_state WHERE id = 1").fetchone()
        if current is None:
            raise RuntimeError("dream_state row missing after bootstrap")
        payload = {
            "last_light": current["last_light"],
            "last_deep": current["last_deep"],
            "last_rem": current["last_rem"],
            "light_passes_since_deep": int(current["light_passes_since_deep"]),
            "deep_passes_since_rem": int(current["deep_passes_since_rem"]),
            "last_lock_holder": current["last_lock_holder"],
            "last_lock_acquired_at": current["last_lock_acquired_at"],
            "last_skip_reason": current["last_skip_reason"],
            "updated_at": _utc_now_string(),
        }
        for key, value in updates.items():
            if value is not UNSET:
                payload[key] = value
        conn.execute(
            """
            UPDATE dream_state
            SET last_light = ?,
                last_deep = ?,
                last_rem = ?,
                light_passes_since_deep = ?,
                deep_passes_since_rem = ?,
                last_lock_holder = ?,
                last_lock_acquired_at = ?,
                last_skip_reason = ?,
                updated_at = ?
            WHERE id = 1
            """,
            (
                payload["last_light"],
                payload["last_deep"],
                payload["last_rem"],
                payload["light_passes_since_deep"],
                payload["deep_passes_since_rem"],
                payload["last_lock_holder"],
                payload["last_lock_acquired_at"],
                payload["last_skip_reason"],
                payload["updated_at"],
            ),
        )

    def create_run(
        self,
        *,
        kind: str,
        status: str = "running",
        holder: str | None = None,
        notes: str | None = None,
        metadata: dict[str, Any] | None = None,
        queue_name: str | None = None,
        item_ref: str | None = None,
        retry_count: int = 0,
        next_attempt_at: str | None = None,
    ) -> int:
        self.bootstrap()
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO runs(
                    kind, status, holder, started_at, notes, metadata_json,
                    queue_name, item_ref, retry_count, next_attempt_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    kind,
                    status,
                    holder,
                    _utc_now_string(),
                    notes,
                    _json_dumps(metadata),
                    queue_name,
                    item_ref,
                    retry_count,
                    next_attempt_at,
                ),
            )
            return int(cursor.lastrowid)

    def enqueue_run(
        self,
        *,
        queue_name: str,
        kind: str,
        holder: str | None = "mcp",
        notes: str | None = None,
        metadata: dict[str, Any] | None = None,
        last_item_ref: str | None = None,
    ) -> int:
        item_ref = last_item_ref or str(_utc_now().timestamp())
        enriched_metadata = dict(metadata or {})
        enriched_metadata.setdefault("item_ref", item_ref)
        enriched_metadata.setdefault("queue_name", queue_name)
        with self.connect() as conn:
            now = _utc_now_string()
            cursor = conn.execute(
                """
                INSERT INTO runs(
                    kind, status, holder, started_at, notes, metadata_json,
                    queue_name, item_ref, retry_count, next_attempt_at
                )
                VALUES (?, 'queued', ?, ?, ?, ?, ?, ?, 0, NULL)
                """,
                (
                    kind,
                    holder,
                    now,
                    notes,
                    _json_dumps(enriched_metadata),
                    queue_name,
                    item_ref,
                ),
            )
            run_id = int(cursor.lastrowid)
            conn.execute(
                """
                INSERT INTO run_events(run_id, stage, event_type, message, created_at, payload_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    "queue",
                    "queued",
                    notes or kind,
                    now,
                    _json_dumps({"queue_name": queue_name, "item_ref": item_ref}),
                ),
            )
            conn.execute(
                """
                INSERT INTO ingest_queue(name, status, pending_count, last_item_ref, last_run_id, updated_at, metadata_json)
                VALUES (?, 'queued', 1, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    status = 'queued',
                    pending_count = ingest_queue.pending_count + 1,
                    last_item_ref = excluded.last_item_ref,
                    last_run_id = excluded.last_run_id,
                    updated_at = excluded.updated_at,
                    metadata_json = excluded.metadata_json
                """,
                (
                    queue_name,
                    item_ref,
                    run_id,
                    now,
                    _json_dumps(enriched_metadata),
                ),
            )
            return run_id

    def finish_run(
        self,
        run_id: int,
        *,
        status: str,
        notes: str | None = None,
        metadata: dict[str, Any] | None = None,
        retry_count: int | object = UNSET,
        next_attempt_at: str | None | object = UNSET,
    ) -> None:
        self.bootstrap()
        with self.connect() as conn:
            current = conn.execute(
                """
                SELECT metadata_json, retry_count, next_attempt_at
                FROM runs
                WHERE id = ?
                """,
                (run_id,),
            ).fetchone()
            if current is None:
                return
            metadata_json = _json_dumps(metadata) if metadata is not None else current["metadata_json"]
            resolved_retry_count = int(current["retry_count"]) if retry_count is UNSET else int(retry_count)
            resolved_next_attempt_at = current["next_attempt_at"] if next_attempt_at is UNSET else next_attempt_at
            conn.execute(
                """
                UPDATE runs
                SET status = ?,
                    finished_at = ?,
                    notes = COALESCE(?, notes),
                    metadata_json = ?,
                    retry_count = ?,
                    next_attempt_at = ?
                WHERE id = ?
                """,
                (
                    status,
                    _utc_now_string(),
                    notes,
                    metadata_json,
                    resolved_retry_count,
                    resolved_next_attempt_at,
                    run_id,
                ),
            )

    def add_run_event(
        self,
        run_id: int,
        *,
        stage: str,
        event_type: str,
        message: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> int:
        self.bootstrap()
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO run_events(run_id, stage, event_type, message, created_at, payload_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (run_id, stage, event_type, message, _utc_now_string(), _json_dumps(payload)),
            )
            return int(cursor.lastrowid)

    def add_error(
        self,
        *,
        error_type: str,
        message: str,
        run_id: int | None = None,
        stage: str | None = None,
        traceback: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> int:
        self.bootstrap()
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO errors(run_id, stage, error_type, message, traceback, created_at, payload_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (run_id, stage, error_type, message, traceback, _utc_now_string(), _json_dumps(payload)),
            )
            return int(cursor.lastrowid)

    def upsert_queue_state(
        self,
        *,
        name: str,
        status: str,
        pending_count: int,
        last_item_ref: str | None = None,
        last_run_id: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.bootstrap()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO ingest_queue(name, status, pending_count, last_item_ref, last_run_id, updated_at, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    status = excluded.status,
                    pending_count = excluded.pending_count,
                    last_item_ref = excluded.last_item_ref,
                    last_run_id = excluded.last_run_id,
                    updated_at = excluded.updated_at,
                    metadata_json = excluded.metadata_json
                """,
                (
                    name,
                    status,
                    pending_count,
                    last_item_ref,
                    last_run_id,
                    _utc_now_string(),
                    _json_dumps(metadata),
                ),
            )

    def retry_queued_run(
        self,
        run_id: int,
        *,
        retry_count: int,
        next_attempt_at: str,
        notes: str | None = None,
    ) -> None:
        self.finish_run(
            run_id,
            status="retry_scheduled",
            notes=notes,
            retry_count=retry_count,
            next_attempt_at=next_attempt_at,
        )

    def list_queue(self) -> list[QueueRecord]:
        self.bootstrap()
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT name, status, pending_count, last_item_ref, last_run_id, updated_at, metadata_json
                FROM ingest_queue
                ORDER BY name ASC
                """
            ).fetchall()
        return [
            QueueRecord(
                name=str(row["name"]),
                status=str(row["status"]),
                pending_count=int(row["pending_count"]),
                last_item_ref=row["last_item_ref"],
                last_run_id=row["last_run_id"],
                updated_at=str(row["updated_at"]),
                metadata_json=row["metadata_json"],
            )
            for row in rows
        ]

    def get_adapter_state(self, adapter: str) -> dict[str, Any] | None:
        self.bootstrap()
        with self.connect() as conn:
            row = conn.execute(
                "SELECT state_json FROM adapter_state WHERE adapter = ?",
                (adapter,),
            ).fetchone()
        if row is None or row["state_json"] is None:
            return None
        try:
            payload = json.loads(str(row["state_json"]))
        except json.JSONDecodeError:
            return None
        return payload if isinstance(payload, dict) else None

    def upsert_adapter_state(self, *, adapter: str, state: dict[str, Any]) -> None:
        self.bootstrap()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO adapter_state(adapter, state_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(adapter) DO UPDATE SET
                    state_json = excluded.state_json,
                    updated_at = excluded.updated_at
                """,
                (adapter, _json_dumps(state), _utc_now_string()),
            )

    def clear_adapter_state(self, *, adapter: str) -> None:
        self.bootstrap()
        with self.connect() as conn:
            conn.execute("DELETE FROM adapter_state WHERE adapter = ?", (adapter,))

    def record_skill_usage(
        self,
        *,
        skill_name: str,
        run_id: int | None = None,
        context: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> int:
        self.bootstrap()
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO skill_usage(skill_name, run_id, context, used_at, metadata_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (skill_name, run_id, context, _utc_now_string(), _json_dumps(metadata)),
            )
            return int(cursor.lastrowid)

    def record_skill_artifact(
        self,
        *,
        skill_name: str,
        artifact_type: str,
        artifact_ref: str,
        run_id: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> int:
        self.bootstrap()
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO skill_artifacts(skill_name, artifact_type, artifact_ref, run_id, created_at, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (skill_name, artifact_type, artifact_ref, run_id, _utc_now_string(), _json_dumps(metadata)),
            )
            return int(cursor.lastrowid)

    def list_skill_usage(self) -> list[SkillSummary]:
        self.bootstrap()
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    usage.skill_name AS skill_name,
                    COUNT(usage.id) AS usage_count,
                    COALESCE(artifacts.artifact_count, 0) AS artifact_count,
                    MAX(usage.used_at) AS last_used_at
                FROM skill_usage AS usage
                LEFT JOIN (
                    SELECT skill_name, COUNT(*) AS artifact_count
                    FROM skill_artifacts
                    GROUP BY skill_name
                ) AS artifacts
                ON usage.skill_name = artifacts.skill_name
                GROUP BY usage.skill_name, artifacts.artifact_count
                ORDER BY usage.skill_name ASC
                """
            ).fetchall()
        return [
            SkillSummary(
                skill_name=str(row["skill_name"]),
                usage_count=int(row["usage_count"]),
                artifact_count=int(row["artifact_count"]),
                last_used_at=row["last_used_at"],
            )
            for row in rows
        ]

    def list_runs(self, *, limit: int = 20) -> list[RunRecord]:
        self.bootstrap()
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    id, kind, status, holder, started_at, finished_at, notes, metadata_json,
                    queue_name, item_ref, retry_count, next_attempt_at
                FROM runs
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._run_record_from_row(row) for row in rows]

    def list_runs_by_status(self, *, status: str, limit: int = 50) -> list[RunRecord]:
        self.bootstrap()
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    id, kind, status, holder, started_at, finished_at, notes, metadata_json,
                    queue_name, item_ref, retry_count, next_attempt_at
                FROM runs
                WHERE status = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (status, limit),
            ).fetchall()
        return [self._run_record_from_row(row) for row in rows]

    def find_latest_run_for_item(
        self,
        *,
        queue_name: str,
        item_ref: str,
        exclude_kinds: set[str] | None = None,
    ) -> RunRecord | None:
        self.bootstrap()
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    id, kind, status, holder, started_at, finished_at, notes, metadata_json,
                    queue_name, item_ref, retry_count, next_attempt_at
                FROM runs
                WHERE queue_name = ? AND item_ref = ?
                ORDER BY id DESC
                """,
                (queue_name, item_ref),
            ).fetchall()
        for row in rows:
            if exclude_kinds and str(row["kind"]) in exclude_kinds:
                continue
            return self._run_record_from_row(row)
        return None

    def claim_oldest_queued_run(
        self,
        *,
        allowed_queue_prefixes: tuple[str, ...] | None = None,
    ) -> RunRecord | None:
        self.bootstrap()
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                """
                SELECT
                    id, kind, status, holder, started_at, finished_at, notes, metadata_json,
                    queue_name, item_ref, retry_count, next_attempt_at
                FROM runs
                WHERE status IN ('queued', 'retry_scheduled')
                  AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                ORDER BY id ASC
                """,
                (_utc_now_string(),),
            ).fetchall()
            row = None
            for candidate in rows:
                if allowed_queue_prefixes is None:
                    row = candidate
                    break
                queue_name = str(candidate["queue_name"] or "")
                if any(queue_name.startswith(prefix) for prefix in allowed_queue_prefixes):
                    row = candidate
                    break
            if row is None:
                return None
            conn.execute(
                "UPDATE runs SET status = 'running', finished_at = NULL, next_attempt_at = NULL WHERE id = ?",
                (row["id"],),
            )
            return self._run_record_from_row(row)

    def complete_queued_run(
        self,
        run_id: int,
        *,
        status: str,
        notes: str | None = None,
        queue_name: str | None = None,
    ) -> None:
        self.finish_run(run_id, status=status, notes=notes, next_attempt_at=None)
        if queue_name is None:
            return
        with self.connect() as conn:
            now = _utc_now_string()
            current = conn.execute(
                """
                SELECT pending_count, last_item_ref, metadata_json
                FROM ingest_queue
                WHERE name = ?
                """,
                (queue_name,),
            ).fetchone()
            pending = max(0, int(current["pending_count"]) - 1) if current is not None else 0
            conn.execute(
                """
                INSERT INTO ingest_queue(name, status, pending_count, last_item_ref, last_run_id, updated_at, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    status = excluded.status,
                    pending_count = excluded.pending_count,
                    last_item_ref = excluded.last_item_ref,
                    last_run_id = excluded.last_run_id,
                    updated_at = excluded.updated_at,
                    metadata_json = excluded.metadata_json
                """,
                (
                    queue_name,
                    "ready" if pending == 0 else "queued",
                    pending,
                    current["last_item_ref"] if current is not None else str(run_id),
                    run_id,
                    now,
                    current["metadata_json"] if current is not None else None,
                ),
            )

    def upsert_mcp_session(
        self,
        *,
        session_id: str,
        kind: str,
        status: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.bootstrap()
        now = _utc_now_string()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO mcp_sessions(session_id, kind, status, started_at, last_seen_at, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    kind = excluded.kind,
                    status = excluded.status,
                    last_seen_at = excluded.last_seen_at,
                    metadata_json = excluded.metadata_json
                """,
                (session_id, kind, status, now, now, _json_dumps(metadata)),
            )

    def get_run(self, run_id: int) -> RunDetails | None:
        self.bootstrap()
        with self.connect() as conn:
            run_row = conn.execute(
                """
                SELECT
                    id, kind, status, holder, started_at, finished_at, notes, metadata_json,
                    queue_name, item_ref, retry_count, next_attempt_at
                FROM runs
                WHERE id = ?
                """,
                (run_id,),
            ).fetchone()
            if run_row is None:
                return None
            event_rows = conn.execute(
                """
                SELECT id, run_id, stage, event_type, message, created_at, payload_json
                FROM run_events
                WHERE run_id = ?
                ORDER BY id ASC
                """,
                (run_id,),
            ).fetchall()
            error_rows = conn.execute(
                """
                SELECT id, run_id, stage, error_type, message, traceback, created_at, payload_json
                FROM errors
                WHERE run_id = ?
                ORDER BY id ASC
                """,
                (run_id,),
            ).fetchall()
        return RunDetails(
            run=self._run_record_from_row(run_row),
            events=[
                RunEventRecord(
                    id=int(row["id"]),
                    run_id=int(row["run_id"]),
                    stage=str(row["stage"]),
                    event_type=str(row["event_type"]),
                    message=row["message"],
                    created_at=str(row["created_at"]),
                    payload_json=row["payload_json"],
                )
                for row in event_rows
            ],
            errors=[
                ErrorRecord(
                    id=int(row["id"]),
                    run_id=row["run_id"],
                    stage=row["stage"],
                    error_type=str(row["error_type"]),
                    message=str(row["message"]),
                    traceback=row["traceback"],
                    created_at=str(row["created_at"]),
                    payload_json=row["payload_json"],
                )
                for row in error_rows
            ],
        )

    def summary(self) -> RuntimeSummary:
        self.bootstrap()
        dream = self.get_dream_state()
        with self.connect() as conn:
            active_locks = int(conn.execute("SELECT COUNT(*) FROM locks").fetchone()[0])
            run_count = int(conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0])
            queue_entries = int(conn.execute("SELECT COUNT(*) FROM ingest_queue").fetchone()[0])
            tracked_skills = int(
                conn.execute("SELECT COUNT(DISTINCT skill_name) FROM skill_usage").fetchone()[0]
            )
        return RuntimeSummary(
            db_path=str(self.db_path),
            schema_version=self.schema_version(),
            active_locks=active_locks,
            run_count=run_count,
            queue_entries=queue_entries,
            tracked_skills=tracked_skills,
            dream_state=dream,
        )

    def _run_record_from_row(self, row: sqlite3.Row) -> RunRecord:
        return RunRecord(
            id=int(row["id"]),
            kind=str(row["kind"]),
            status=str(row["status"]),
            holder=row["holder"],
            started_at=str(row["started_at"]),
            finished_at=row["finished_at"],
            notes=row["notes"],
            metadata_json=row["metadata_json"],
            queue_name=row["queue_name"],
            item_ref=row["item_ref"],
            retry_count=int(row["retry_count"] or 0),
            next_attempt_at=row["next_attempt_at"],
        )
