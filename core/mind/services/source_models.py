from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, NewType


SourceKey = NewType("SourceKey", str)
SourceStatus = Literal["materialized", "incomplete", "stale", "blocked", "excluded", "unseen"]
StageStatus = Literal["completed", "missing"]
StageFreshness = Literal["fresh", "stale", "missing"]

SOURCE_STATUSES: tuple[SourceStatus, ...] = (
    "materialized",
    "incomplete",
    "stale",
    "blocked",
    "excluded",
    "unseen",
)
STAGE_STATUSES: tuple[StageStatus, ...] = ("completed", "missing")
STAGE_FRESHNESS: tuple[StageFreshness, ...] = ("fresh", "stale", "missing")
SELECTION_VALUES: tuple[str, ...] = (*SOURCE_STATUSES, "all")


@dataclass(frozen=True)
class StageProbeState:
    stage: str
    status: StageStatus
    freshness: StageFreshness
    artifact_path: str | None = None
    summary: str | None = None


@dataclass(frozen=True)
class SourceArtifact:
    artifact_kind: str
    path: str
    fingerprint: str | None
    exists: bool


@dataclass(frozen=True)
class InventoryRequest:
    lane: str
    path: Path | None = None
    today: str | None = None
    source_ids: tuple[str, ...] = ()
    external_ids: tuple[str, ...] = ()
    selection: tuple[str, ...] = ("all",)
    limit: int | None = None
    lane_options: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class InventoryItem:
    source_key: SourceKey
    lane: str
    adapter: str
    title: str
    source_date: str
    status: SourceStatus
    aliases: tuple[str, ...]
    canonical_page_path: str | None
    stage_states: tuple[StageProbeState, ...]
    artifacts: tuple[SourceArtifact, ...]
    source_id: str | None = None
    external_id: str | None = None
    blocked_reason: str | None = None
    excluded_reason: str | None = None
    registry_status: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    payload: Any = None

    @property
    def page_exists(self) -> bool:
        return bool(self.canonical_page_path and Path(self.canonical_page_path).exists())

    @property
    def has_any_artifacts(self) -> bool:
        return any(artifact.exists for artifact in self.artifacts)

    def stage(self, name: str) -> StageProbeState | None:
        for stage in self.stage_states:
            if stage.stage == name:
                return stage
        return None

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_key": str(self.source_key),
            "lane": self.lane,
            "adapter": self.adapter,
            "title": self.title,
            "source_date": self.source_date,
            "status": self.status,
            "aliases": list(self.aliases),
            "source_id": self.source_id,
            "external_id": self.external_id,
            "canonical_page_path": self.canonical_page_path,
            "blocked_reason": self.blocked_reason,
            "excluded_reason": self.excluded_reason,
            "registry_status": self.registry_status,
            "stage_states": [
                {
                    "stage": stage.stage,
                    "status": stage.status,
                    "freshness": stage.freshness,
                    "artifact_path": stage.artifact_path,
                    "summary": stage.summary,
                }
                for stage in self.stage_states
            ],
            "artifacts": [
                {
                    "artifact_kind": artifact.artifact_kind,
                    "path": artifact.path,
                    "fingerprint": artifact.fingerprint,
                    "exists": artifact.exists,
                }
                for artifact in self.artifacts
            ],
        }


@dataclass(frozen=True)
class InventoryResult:
    request: InventoryRequest
    items: tuple[InventoryItem, ...]

    @property
    def counts(self) -> dict[str, int]:
        counts = {status: 0 for status in SOURCE_STATUSES}
        for item in self.items:
            counts[item.status] = counts.get(item.status, 0) + 1
        return counts

    def to_dict(self) -> dict[str, Any]:
        return {
            "lane": self.request.lane,
            "path": str(self.request.path) if self.request.path else None,
            "today": self.request.today,
            "selection": list(self.request.selection),
            "limit": self.request.limit,
            "lane_options": dict(self.request.lane_options),
            "counts": self.counts,
            "items": [item.to_dict() for item in self.items],
        }

    def render(self) -> str:
        count_summary = ", ".join(
            f"{name}={count}"
            for name, count in sorted(self.counts.items())
            if count > 0
        ) or "none"
        lines = [
            f"inventory[{self.request.lane}]: source={self.request.path.name if self.request.path else (self.request.today or 'default')}",
            f"selected={len(self.items)} counts={count_summary}",
        ]
        for item in self.items[:15]:
            detail = item.blocked_reason or item.excluded_reason or item.source_id or item.external_id or "-"
            lines.append(f"- {item.source_key} {item.status} {detail}")
        return "\n".join(lines)


@dataclass(frozen=True)
class PlanRequest:
    lane: str
    path: Path | None = None
    today: str | None = None
    source_ids: tuple[str, ...] = ()
    external_ids: tuple[str, ...] = ()
    selection: tuple[str, ...] = ("all",)
    limit: int | None = None
    resume: bool = True
    skip_materialized: bool = True
    refresh_stale: bool = False
    recompute_missing: bool = False
    from_stage: str | None = None
    through: str = "propagate"
    lane_options: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PlanItem:
    source_key: SourceKey
    lane: str
    title: str
    status: SourceStatus
    action: str
    start_stage: str | None = None
    through_stage: str | None = None
    source_id: str | None = None
    external_id: str | None = None
    blocked_reason: str | None = None
    excluded_reason: str | None = None
    canonical_page_path: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_key": str(self.source_key),
            "lane": self.lane,
            "title": self.title,
            "status": self.status,
            "action": self.action,
            "start_stage": self.start_stage,
            "through_stage": self.through_stage,
            "source_id": self.source_id,
            "external_id": self.external_id,
            "blocked_reason": self.blocked_reason,
            "excluded_reason": self.excluded_reason,
            "canonical_page_path": self.canonical_page_path,
        }


@dataclass(frozen=True)
class PlanResult:
    request: PlanRequest
    inventory: InventoryResult
    items: tuple[PlanItem, ...]

    @property
    def selected_count(self) -> int:
        return len(self.items)

    @property
    def skipped_materialized_count(self) -> int:
        return sum(1 for item in self.items if item.action == "skip_materialized")

    @property
    def resumable_count(self) -> int:
        return sum(1 for item in self.items if item.action.startswith("resume_from_"))

    @property
    def blocked_count(self) -> int:
        return sum(1 for item in self.items if item.action == "blocked_missing_artifacts")

    @property
    def stale_count(self) -> int:
        return sum(1 for item in self.items if item.status == "stale")

    @property
    def blocked_samples(self) -> list[str]:
        samples: list[str] = []
        for item in self.items:
            if item.action != "blocked_missing_artifacts":
                continue
            reason = item.blocked_reason or "missing required reusable artifacts"
            samples.append(f"{item.title}: {reason}")
            if len(samples) >= 5:
                break
        return samples

    def to_dict(self) -> dict[str, Any]:
        return {
            "lane": self.request.lane,
            "path": str(self.request.path) if self.request.path else None,
            "today": self.request.today,
            "selection": list(self.request.selection),
            "resume": self.request.resume,
            "skip_materialized": self.request.skip_materialized,
            "refresh_stale": self.request.refresh_stale,
            "recompute_missing": self.request.recompute_missing,
            "from_stage": self.request.from_stage,
            "through": self.request.through,
            "lane_options": dict(self.request.lane_options),
            "selected_count": self.selected_count,
            "skipped_materialized_count": self.skipped_materialized_count,
            "resumable_count": self.resumable_count,
            "blocked_count": self.blocked_count,
            "stale_count": self.stale_count,
            "blocked_samples": list(self.blocked_samples),
            "items": [item.to_dict() for item in self.items],
        }

    def render(self) -> str:
        lines = [
            (
                f"plan[{self.request.lane}]: selected={self.selected_count} "
                f"skipped_materialized={self.skipped_materialized_count} "
                f"resumable={self.resumable_count} blocked={self.blocked_count} stale={self.stale_count}"
            )
        ]
        if self.blocked_samples:
            lines.append("blocked_samples:")
            for sample in self.blocked_samples:
                lines.append(f"- {sample}")
        for item in self.items[:20]:
            detail = item.start_stage or item.blocked_reason or item.excluded_reason or "-"
            lines.append(f"- {item.source_key} {item.action} {detail}")
        return "\n".join(lines)


@dataclass(frozen=True)
class CompletedExecutionItem:
    source_key: SourceKey
    title: str
    source_id: str | None
    materialized_paths: dict[str, str]
    propagate: dict[str, Any]


@dataclass(frozen=True)
class PlanExecutionResult:
    plan: PlanResult
    executed_count: int
    failed_count: int
    page_ids: tuple[str, ...]
    blocked_samples: tuple[str, ...]
    failed_items: tuple[str, ...]
    completed_items: tuple[CompletedExecutionItem, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.plan.to_dict(),
            "executed_count": self.executed_count,
            "failed_count": self.failed_count,
            "page_ids": list(self.page_ids),
            "blocked_samples": list(self.blocked_samples),
            "failed_items": list(self.failed_items),
            "completed_items": [
                {
                    "source_key": str(item.source_key),
                    "title": item.title,
                    "source_id": item.source_id,
                    "materialized_paths": dict(item.materialized_paths),
                    "propagate": item.propagate,
                }
                for item in self.completed_items
            ],
        }


@dataclass(frozen=True)
class ReconcileResult:
    request: InventoryRequest
    refreshed_count: int
    changed_count: int
    new_count: int
    removed_count: int
    upstream_selected_count: int
    registry_matched_count: int
    page_matched_count: int
    registry_only_count: int
    page_only_count: int
    cache_only_count: int
    registry_only_samples: tuple[str, ...] = ()
    page_only_samples: tuple[str, ...] = ()
    cache_only_samples: tuple[str, ...] = ()
    inventory: InventoryResult | None = None

    def render(self) -> str:
        lines = [
            (
                f"reconcile[{self.request.lane}]: refreshed={self.refreshed_count} "
                f"changed={self.changed_count} new={self.new_count} removed={self.removed_count}"
            ),
            (
                f"upstream_selected={self.upstream_selected_count} registry_matched={self.registry_matched_count} "
                f"page_matched={self.page_matched_count} registry_only={self.registry_only_count} "
                f"page_only={self.page_only_count} cache_only={self.cache_only_count}"
            ),
        ]
        if self.registry_only_samples:
            lines.append("registry_only_samples:")
            lines.extend(f"- {sample}" for sample in self.registry_only_samples)
        if self.page_only_samples:
            lines.append("page_only_samples:")
            lines.extend(f"- {sample}" for sample in self.page_only_samples)
        if self.cache_only_samples:
            lines.append("cache_only_samples:")
            lines.extend(f"- {sample}" for sample in self.cache_only_samples)
        if self.inventory is not None:
            lines.append(self.inventory.render())
        return "\n".join(lines)
