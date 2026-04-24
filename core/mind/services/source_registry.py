from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any, Iterator

from scripts.common.vault import Vault


SCHEMA_VERSION = "1"
STAGE_ORDER: tuple[str, ...] = (
    "acquire",
    "pass_a",
    "pass_b",
    "pass_c",
    "pass_d",
    "materialize",
    "propagate",
)


def _utc_now_string() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json_dumps(value: dict[str, Any] | None) -> str | None:
    if value is None:
        return None
    return json.dumps(value, sort_keys=True)


def _json_loads(value: str | None) -> dict[str, Any] | None:
    if value is None:
        return None
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


@dataclass(frozen=True)
class SourceRegistryRow:
    source_key: str
    lane: str
    adapter: str
    title: str
    source_date: str
    status: str
    first_seen_at: str
    last_seen_at: str
    canonical_page_path: str | None
    excluded_reason: str | None
    blocked_reason: str | None
    metadata_json: str | None = None

    @property
    def metadata(self) -> dict[str, Any] | None:
        return _json_loads(self.metadata_json)


@dataclass(frozen=True)
class SourceAliasRow:
    source_key: str
    alias: str
    alias_type: str


@dataclass(frozen=True)
class SourceStageRow:
    source_key: str
    stage: str
    status: str
    freshness: str
    artifact_path: str | None
    summary: str | None
    updated_at: str


@dataclass(frozen=True)
class SourceArtifactRow:
    source_key: str
    artifact_kind: str
    path: str
    fingerprint: str | None
    exists: bool
    updated_at: str


@dataclass(frozen=True)
class SourceRegistryRecord:
    source: SourceRegistryRow
    aliases: list[SourceAliasRow]
    stages: list[SourceStageRow]
    artifacts: list[SourceArtifactRow]


@dataclass(frozen=True)
class SourceRegistryDetails:
    source: SourceRegistryRow
    aliases: list[SourceAliasRow]
    stages: list[SourceStageRow]
    artifacts: list[SourceArtifactRow]


@dataclass(frozen=True)
class SourceRegistryStatus:
    db_path: Path
    schema_version: str
    source_count: int
    alias_count: int
    stage_count: int
    artifact_count: int
    last_built_at: str | None
    status_counts: dict[str, int]
    lane_counts: dict[str, int]

    def render(self) -> str:
        status_summary = ", ".join(f"{key}={value}" for key, value in sorted(self.status_counts.items())) or "-"
        lane_summary = ", ".join(f"{key}={value}" for key, value in sorted(self.lane_counts.items())) or "-"
        return "\n".join(
            [
                f"source-registry-db: {self.db_path}",
                f"schema_version: {self.schema_version}",
                f"sources: {self.source_count}",
                f"aliases: {self.alias_count}",
                f"stage_rows: {self.stage_count}",
                f"artifacts: {self.artifact_count}",
                f"last_built_at: {self.last_built_at or '-'}",
                f"status_counts: {status_summary}",
                f"lane_counts: {lane_summary}",
            ]
        )


class SourceRegistry:
    def __init__(self, repo_root: Path):
        self.repo_root = repo_root
        self.vault = Vault.load(repo_root)
        self.db_path = self.vault.sources_db

    @classmethod
    def for_repo_root(cls, repo_root: Path) -> "SourceRegistry":
        registry = cls(repo_root)
        registry.bootstrap()
        return registry

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
                CREATE TABLE IF NOT EXISTS registry_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS sources (
                    source_key TEXT PRIMARY KEY,
                    lane TEXT NOT NULL,
                    adapter TEXT NOT NULL,
                    title TEXT NOT NULL,
                    source_date TEXT,
                    status TEXT NOT NULL,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    canonical_page_path TEXT,
                    excluded_reason TEXT,
                    blocked_reason TEXT,
                    metadata_json TEXT
                );

                CREATE TABLE IF NOT EXISTS source_aliases (
                    alias TEXT PRIMARY KEY,
                    source_key TEXT NOT NULL REFERENCES sources(source_key) ON DELETE CASCADE,
                    alias_type TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS source_stage_state (
                    source_key TEXT NOT NULL REFERENCES sources(source_key) ON DELETE CASCADE,
                    stage TEXT NOT NULL,
                    status TEXT NOT NULL,
                    freshness TEXT NOT NULL,
                    artifact_path TEXT,
                    summary TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (source_key, stage)
                );

                CREATE TABLE IF NOT EXISTS source_artifacts (
                    source_key TEXT NOT NULL REFERENCES sources(source_key) ON DELETE CASCADE,
                    artifact_kind TEXT NOT NULL,
                    path TEXT NOT NULL,
                    fingerprint TEXT,
                    exists_flag INTEGER NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (source_key, artifact_kind, path)
                );
                """
            )
            now = _utc_now_string()
            conn.execute(
                """
                INSERT INTO registry_meta(key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
                """,
                ("schema_version", SCHEMA_VERSION, now),
            )

    def schema_version(self) -> str:
        self.bootstrap()
        with self.connect() as conn:
            row = conn.execute(
                "SELECT value FROM registry_meta WHERE key = 'schema_version'"
            ).fetchone()
        return str(row["value"]) if row else SCHEMA_VERSION

    def replace_all(self, records: list[SourceRegistryRecord], *, built_at: str | None = None) -> None:
        self.bootstrap()
        built_at = built_at or _utc_now_string()
        with self.connect() as conn:
            conn.execute("DELETE FROM source_artifacts")
            conn.execute("DELETE FROM source_stage_state")
            conn.execute("DELETE FROM source_aliases")
            conn.execute("DELETE FROM sources")
            self._insert_records(conn, records)
            conn.execute(
                """
                INSERT INTO registry_meta(key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
                """,
                ("last_built_at", built_at, built_at),
            )

    def upsert_record(self, record: SourceRegistryRecord, *, seen_at: str | None = None) -> None:
        self.bootstrap()
        seen_at = seen_at or _utc_now_string()
        with self.connect() as conn:
            current = conn.execute(
                "SELECT first_seen_at FROM sources WHERE source_key = ?",
                (record.source.source_key,),
            ).fetchone()
            source_row = SourceRegistryRow(
                source_key=record.source.source_key,
                lane=record.source.lane,
                adapter=record.source.adapter,
                title=record.source.title,
                source_date=record.source.source_date,
                status=record.source.status,
                first_seen_at=str(current["first_seen_at"]) if current is not None else record.source.first_seen_at,
                last_seen_at=seen_at,
                canonical_page_path=record.source.canonical_page_path,
                excluded_reason=record.source.excluded_reason,
                blocked_reason=record.source.blocked_reason,
                metadata_json=record.source.metadata_json,
            )
            self._insert_source(conn, source_row)
            conn.execute("DELETE FROM source_aliases WHERE source_key = ?", (source_row.source_key,))
            conn.execute("DELETE FROM source_stage_state WHERE source_key = ?", (source_row.source_key,))
            conn.execute("DELETE FROM source_artifacts WHERE source_key = ?", (source_row.source_key,))
            self._insert_aliases(conn, record.aliases)
            self._insert_stages(conn, record.stages)
            self._insert_artifacts(conn, record.artifacts)

    def resolve_source_key(self, identifier: str) -> str | None:
        self.bootstrap()
        with self.connect() as conn:
            direct = conn.execute(
                "SELECT source_key FROM sources WHERE source_key = ?",
                (identifier,),
            ).fetchone()
            if direct is not None:
                return str(direct["source_key"])
            row = conn.execute(
                "SELECT source_key FROM source_aliases WHERE alias = ?",
                (identifier,),
            ).fetchone()
        return str(row["source_key"]) if row is not None else None

    def get(self, identifier: str) -> SourceRegistryDetails | None:
        self.bootstrap()
        source_key = self.resolve_source_key(identifier)
        if source_key is None:
            return None
        with self.connect() as conn:
            source_row = conn.execute(
                """
                SELECT
                    source_key, lane, adapter, title, source_date, status,
                    first_seen_at, last_seen_at, canonical_page_path,
                    excluded_reason, blocked_reason, metadata_json
                FROM sources
                WHERE source_key = ?
                """,
                (source_key,),
            ).fetchone()
            if source_row is None:
                return None
            alias_rows = conn.execute(
                "SELECT source_key, alias, alias_type FROM source_aliases WHERE source_key = ? ORDER BY alias ASC",
                (source_key,),
            ).fetchall()
            stage_rows = conn.execute(
                """
                SELECT source_key, stage, status, freshness, artifact_path, summary, updated_at
                FROM source_stage_state
                WHERE source_key = ?
                ORDER BY CASE stage
                    WHEN 'acquire' THEN 1
                    WHEN 'pass_a' THEN 2
                    WHEN 'pass_b' THEN 3
                    WHEN 'pass_c' THEN 4
                    WHEN 'pass_d' THEN 5
                    WHEN 'materialize' THEN 6
                    WHEN 'propagate' THEN 7
                    ELSE 99 END
                """,
                (source_key,),
            ).fetchall()
            artifact_rows = conn.execute(
                """
                SELECT source_key, artifact_kind, path, fingerprint, exists_flag, updated_at
                FROM source_artifacts
                WHERE source_key = ?
                ORDER BY artifact_kind ASC, path ASC
                """,
                (source_key,),
            ).fetchall()
        return SourceRegistryDetails(
            source=self._source_row_from_sql(source_row),
            aliases=[SourceAliasRow(str(row["source_key"]), str(row["alias"]), str(row["alias_type"])) for row in alias_rows],
            stages=[
                SourceStageRow(
                    source_key=str(row["source_key"]),
                    stage=str(row["stage"]),
                    status=str(row["status"]),
                    freshness=str(row["freshness"]),
                    artifact_path=str(row["artifact_path"]) if row["artifact_path"] else None,
                    summary=str(row["summary"]) if row["summary"] else None,
                    updated_at=str(row["updated_at"]),
                )
                for row in stage_rows
            ],
            artifacts=[
                SourceArtifactRow(
                    source_key=str(row["source_key"]),
                    artifact_kind=str(row["artifact_kind"]),
                    path=str(row["path"]),
                    fingerprint=str(row["fingerprint"]) if row["fingerprint"] else None,
                    exists=bool(row["exists_flag"]),
                    updated_at=str(row["updated_at"]),
                )
                for row in artifact_rows
            ],
        )

    def status(self) -> SourceRegistryStatus:
        self.bootstrap()
        with self.connect() as conn:
            source_count = int(conn.execute("SELECT COUNT(*) FROM sources").fetchone()[0])
            alias_count = int(conn.execute("SELECT COUNT(*) FROM source_aliases").fetchone()[0])
            stage_count = int(conn.execute("SELECT COUNT(*) FROM source_stage_state").fetchone()[0])
            artifact_count = int(conn.execute("SELECT COUNT(*) FROM source_artifacts").fetchone()[0])
            last_built_row = conn.execute(
                "SELECT value FROM registry_meta WHERE key = 'last_built_at'"
            ).fetchone()
            status_rows = conn.execute(
                "SELECT status, COUNT(*) AS count FROM sources GROUP BY status ORDER BY status ASC"
            ).fetchall()
            lane_rows = conn.execute(
                "SELECT lane, COUNT(*) AS count FROM sources GROUP BY lane ORDER BY lane ASC"
            ).fetchall()
        return SourceRegistryStatus(
            db_path=self.db_path,
            schema_version=self.schema_version(),
            source_count=source_count,
            alias_count=alias_count,
            stage_count=stage_count,
            artifact_count=artifact_count,
            last_built_at=str(last_built_row["value"]) if last_built_row is not None else None,
            status_counts={str(row["status"]): int(row["count"]) for row in status_rows},
            lane_counts={str(row["lane"]): int(row["count"]) for row in lane_rows},
        )

    def list_sources(self, *, lane: str | None = None) -> list[SourceRegistryRow]:
        self.bootstrap()
        with self.connect() as conn:
            if lane is None:
                rows = conn.execute(
                    """
                    SELECT
                        source_key, lane, adapter, title, source_date, status,
                        first_seen_at, last_seen_at, canonical_page_path,
                        excluded_reason, blocked_reason, metadata_json
                    FROM sources
                    ORDER BY lane ASC, title ASC, source_key ASC
                    """
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT
                        source_key, lane, adapter, title, source_date, status,
                        first_seen_at, last_seen_at, canonical_page_path,
                        excluded_reason, blocked_reason, metadata_json
                    FROM sources
                    WHERE lane = ?
                    ORDER BY title ASC, source_key ASC
                    """,
                    (lane,),
                ).fetchall()
        return [self._source_row_from_sql(row) for row in rows]

    def _insert_records(self, conn: sqlite3.Connection, records: list[SourceRegistryRecord]) -> None:
        for record in records:
            self._insert_source(conn, record.source)
        for record in records:
            self._insert_aliases(conn, record.aliases)
            self._insert_stages(conn, record.stages)
            self._insert_artifacts(conn, record.artifacts)

    def _insert_source(self, conn: sqlite3.Connection, source: SourceRegistryRow) -> None:
        conn.execute(
            """
            INSERT INTO sources(
                source_key, lane, adapter, title, source_date, status,
                first_seen_at, last_seen_at, canonical_page_path,
                excluded_reason, blocked_reason, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_key) DO UPDATE SET
                lane = excluded.lane,
                adapter = excluded.adapter,
                title = excluded.title,
                source_date = excluded.source_date,
                status = excluded.status,
                first_seen_at = excluded.first_seen_at,
                last_seen_at = excluded.last_seen_at,
                canonical_page_path = excluded.canonical_page_path,
                excluded_reason = excluded.excluded_reason,
                blocked_reason = excluded.blocked_reason,
                metadata_json = excluded.metadata_json
            """,
            (
                source.source_key,
                source.lane,
                source.adapter,
                source.title,
                source.source_date,
                source.status,
                source.first_seen_at,
                source.last_seen_at,
                source.canonical_page_path,
                source.excluded_reason,
                source.blocked_reason,
                source.metadata_json,
            ),
        )

    def _insert_aliases(self, conn: sqlite3.Connection, aliases: list[SourceAliasRow]) -> None:
        for alias in aliases:
            conn.execute(
                """
                INSERT INTO source_aliases(alias, source_key, alias_type)
                VALUES (?, ?, ?)
                ON CONFLICT(alias) DO UPDATE SET
                    source_key = excluded.source_key,
                    alias_type = excluded.alias_type
                """,
                (alias.alias, alias.source_key, alias.alias_type),
            )

    def _insert_stages(self, conn: sqlite3.Connection, stages: list[SourceStageRow]) -> None:
        for stage in stages:
            conn.execute(
                """
                INSERT INTO source_stage_state(source_key, stage, status, freshness, artifact_path, summary, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_key, stage) DO UPDATE SET
                    status = excluded.status,
                    freshness = excluded.freshness,
                    artifact_path = excluded.artifact_path,
                    summary = excluded.summary,
                    updated_at = excluded.updated_at
                """,
                (
                    stage.source_key,
                    stage.stage,
                    stage.status,
                    stage.freshness,
                    stage.artifact_path,
                    stage.summary,
                    stage.updated_at,
                ),
            )

    def _insert_artifacts(self, conn: sqlite3.Connection, artifacts: list[SourceArtifactRow]) -> None:
        for artifact in artifacts:
            conn.execute(
                """
                INSERT INTO source_artifacts(source_key, artifact_kind, path, fingerprint, exists_flag, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_key, artifact_kind, path) DO UPDATE SET
                    fingerprint = excluded.fingerprint,
                    exists_flag = excluded.exists_flag,
                    updated_at = excluded.updated_at
                """,
                (
                    artifact.source_key,
                    artifact.artifact_kind,
                    artifact.path,
                    artifact.fingerprint,
                    int(artifact.exists),
                    artifact.updated_at,
                ),
            )

    def _source_row_from_sql(self, row: sqlite3.Row) -> SourceRegistryRow:
        return SourceRegistryRow(
            source_key=str(row["source_key"]),
            lane=str(row["lane"]),
            adapter=str(row["adapter"]),
            title=str(row["title"]),
            source_date=str(row["source_date"] or ""),
            status=str(row["status"]),
            first_seen_at=str(row["first_seen_at"]),
            last_seen_at=str(row["last_seen_at"]),
            canonical_page_path=str(row["canonical_page_path"]) if row["canonical_page_path"] else None,
            excluded_reason=str(row["excluded_reason"]) if row["excluded_reason"] else None,
            blocked_reason=str(row["blocked_reason"]) if row["blocked_reason"] else None,
            metadata_json=str(row["metadata_json"]) if row["metadata_json"] else None,
        )
