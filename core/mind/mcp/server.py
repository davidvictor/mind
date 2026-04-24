from __future__ import annotations

import os
from pathlib import Path
import uuid

from mind.commands.common import project_root, score_pages
from mind.runtime_state import RuntimeState
from mind.services.ingest_readiness import build_graph_health, run_ingest_readiness
from scripts.common.vault import Vault

from .models import (
    AuthRequest,
    ClearStaleLockRequest,
    EnqueueLinksRequest,
    EnqueueResponse,
    ErrorView,
    GraphHealthRequest,
    GenerateSkillRequest,
    IngestReadinessRequest,
    MemoryMatch,
    QueueItemResponse,
    ReadSkillRequest,
    RetryQueueItemRequest,
    RunDetailsResponse,
    RunEventView,
    RunOnboardRequest,
    RuntimeStatusResponse,
    RunSummary,
    SearchMemoryRequest,
    SetSkillStatusRequest,
    SkillListItem,
    SkillReadResponse,
    StartArticleRepairRequest,
    StartDreamRequest,
    StartDreamBootstrapRequest,
    StartIngestRequest,
    StartReingestRequest,
)


class MCPAuthError(PermissionError):
    """Raised when an MCP call is not authorized."""


class MCPUnsupportedOperationError(RuntimeError):
    """Raised when an MCP write operation is intentionally unsupported."""


class BrainMCPServer:
    """Typed in-process MCP facade over the Brain runtime/services."""

    def __init__(self, *, root: Path | None = None, auth_token: str | None = None):
        self.root = root or project_root()
        self.state = RuntimeState.for_repo_root(self.root)
        self.required_token = auth_token if auth_token is not None else os.environ.get("BRAIN_MCP_TOKEN", "").strip() or None

    def _authorize(self, request: AuthRequest) -> str:
        if self.required_token and request.auth_token != self.required_token:
            raise MCPAuthError("invalid MCP auth token")
        session_id = request.session_id or f"mcp-{uuid.uuid4()}"
        self.state.upsert_mcp_session(session_id=session_id, kind="mcp", status="active")
        return session_id

    def _normalize_repo_path(self, raw_path: str) -> str:
        candidate = Path(raw_path).expanduser()
        resolved = (self.root / candidate).resolve() if not candidate.is_absolute() else candidate.resolve()
        try:
            resolved.relative_to(self.root)
        except ValueError as exc:
            raise ValueError(f"path must stay inside repo root: {raw_path}") from exc
        return resolved.as_posix()

    def _reject_dream_queue(self, stage: str) -> EnqueueResponse:
        raise MCPUnsupportedOperationError(
            f"MCP Dream start for {stage} is unsupported; use direct CLI operator commands instead."
        )

    def get_runtime_status(self, request: AuthRequest | None = None) -> RuntimeStatusResponse:
        self._authorize(request or AuthRequest())
        summary = self.state.summary()
        return RuntimeStatusResponse(
            db_path=summary.db_path,
            schema_version=summary.schema_version,
            active_locks=summary.active_locks,
            run_count=summary.run_count,
            queue_entries=summary.queue_entries,
            tracked_skills=summary.tracked_skills,
            last_light=summary.dream_state.last_light,
            last_deep=summary.dream_state.last_deep,
            last_rem=summary.dream_state.last_rem,
            light_passes_since_deep=summary.dream_state.light_passes_since_deep,
            deep_passes_since_rem=summary.dream_state.deep_passes_since_rem,
        )

    def list_runs(self, request: AuthRequest | None = None, *, limit: int = 20) -> list[RunSummary]:
        self._authorize(request or AuthRequest())
        return [
            RunSummary(
                id=run.id,
                kind=run.kind,
                status=run.status,
                holder=run.holder,
                started_at=run.started_at,
                finished_at=run.finished_at,
                notes=run.notes,
                queue_name=run.queue_name,
                item_ref=run.item_ref,
                retry_count=run.retry_count,
                next_attempt_at=run.next_attempt_at,
            )
            for run in self.state.list_runs(limit=limit)
        ]

    def get_run(self, run_id: int, request: AuthRequest | None = None) -> RunDetailsResponse | None:
        self._authorize(request or AuthRequest())
        details = self.state.get_run(run_id)
        if details is None:
            return None
        return RunDetailsResponse(
            run=RunSummary(
                id=details.run.id,
                kind=details.run.kind,
                status=details.run.status,
                holder=details.run.holder,
                started_at=details.run.started_at,
                finished_at=details.run.finished_at,
                notes=details.run.notes,
                queue_name=details.run.queue_name,
                item_ref=details.run.item_ref,
                retry_count=details.run.retry_count,
                next_attempt_at=details.run.next_attempt_at,
            ),
            events=[
                RunEventView(
                    id=event.id,
                    stage=event.stage,
                    event_type=event.event_type,
                    message=event.message,
                    created_at=event.created_at,
                )
                for event in details.events
            ],
            errors=[
                ErrorView(
                    id=error.id,
                    stage=error.stage,
                    error_type=error.error_type,
                    message=error.message,
                    created_at=error.created_at,
                )
                for error in details.errors
            ],
        )

    def search_memory(self, request: SearchMemoryRequest) -> list[MemoryMatch]:
        self._authorize(request)
        matches = score_pages(request.query, Vault.load(self.root), limit=request.limit)
        return [
            MemoryMatch(
                page_id=match.page_id,
                title=match.title,
                path=str(match.path),
                score=match.score,
                snippet=match.snippet,
            )
            for match in matches
        ]

    def list_skills(self, request: AuthRequest | None = None) -> list[SkillListItem]:
        self._authorize(request or AuthRequest())
        usage = {item.skill_name: item for item in self.state.list_skill_usage()}
        items: list[SkillListItem] = []
        for path in sorted((self.root / "skills").glob("*/SKILL.md")):
            skill_id = path.parent.name
            summary = usage.get(skill_id)
            items.append(
                SkillListItem(
                    skill_id=skill_id,
                    path=str(path),
                    usage_count=summary.usage_count if summary else 0,
                    artifact_count=summary.artifact_count if summary else 0,
                    last_used_at=summary.last_used_at if summary else None,
                )
            )
        return items

    def read_skill(self, request: ReadSkillRequest) -> SkillReadResponse:
        self._authorize(request)
        path = self.root / "skills" / request.skill_id / "SKILL.md"
        if not path.exists():
            raise FileNotFoundError(request.skill_id)
        return SkillReadResponse(skill_id=request.skill_id, path=str(path), content=path.read_text(encoding="utf-8"))

    def list_queue(self, request: AuthRequest | None = None) -> list[QueueItemResponse]:
        self._authorize(request or AuthRequest())
        return [
            QueueItemResponse(
                name=item.name,
                status=item.status,
                pending_count=item.pending_count,
                last_item_ref=item.last_item_ref,
                last_run_id=item.last_run_id,
                updated_at=item.updated_at,
            )
            for item in self.state.list_queue()
        ]

    def get_graph_health(self, request: GraphHealthRequest) -> dict[str, object]:
        self._authorize(request)
        health = build_graph_health(
            repo_root=self.root,
            include_promotion_gate=not request.skip_promotion_gate,
        )
        return {
            "graph_built": health.graph_built,
            "node_count": health.node_count,
            "edge_count": health.edge_count,
            "document_count": health.document_count,
            "embedding_model": health.embedding_model,
            "embedding_count": health.embedding_count,
            "embedding_backend": health.embedding_backend,
            "embedding_backend_count": health.embedding_backend_count,
            "shadow_mode": health.shadow_mode,
            "promotion_gate_passed": health.promotion_gate_passed,
            "promotion_gate_artifact_json": health.promotion_gate_artifact_json,
            "promotion_gate_artifact_markdown": health.promotion_gate_artifact_markdown,
            "issues": list(health.issues),
        }

    def run_ingest_readiness(self, request: IngestReadinessRequest) -> dict[str, object]:
        self._authorize(request)
        result = run_ingest_readiness(
            repo_root=self.root,
            dropbox_limit=request.dropbox_limit,
            lane_limit=request.lane_limit,
            include_promotion_gate=request.include_promotion_gate,
        )
        return {
            "passed": result.passed,
            "issues": list(result.issues),
            "report_json_path": str(result.report_json_path),
            "report_markdown_path": str(result.report_markdown_path),
            "dropbox": result.dropbox.metadata,
            "graph": {
                "graph_built": result.graph.graph_built,
                "node_count": result.graph.node_count,
                "edge_count": result.graph.edge_count,
                "document_count": result.graph.document_count,
                "embedding_model": result.graph.embedding_model,
                "embedding_count": result.graph.embedding_count,
                "embedding_backend": result.graph.embedding_backend,
                "embedding_backend_count": result.graph.embedding_backend_count,
                "shadow_mode": result.graph.shadow_mode,
                "promotion_gate_passed": result.graph.promotion_gate_passed,
                "issues": list(result.graph.issues),
            },
            "lanes": [
                {
                    "lane": lane.lane,
                    "stage": lane.stage,
                    "through": lane.through,
                    "selected_count": lane.selected_count,
                    "blocked_count": lane.blocked_count,
                    "ready": lane.ready,
                    "report": lane.report,
                }
                for lane in result.lanes
            ],
            "article_repair": {
                "ready_count": result.article_repair.plan.ready_count,
                "reacquire_count": result.article_repair.plan.reacquire_count,
                "recompute_count": result.article_repair.plan.recompute_count,
                "blocked_count": result.article_repair.plan.blocked_count,
            },
        }

    def enqueue_links(self, request: EnqueueLinksRequest) -> EnqueueResponse:
        self._authorize(request)
        today = request.today or __import__("datetime").date.today().isoformat()
        target = Vault.load(self.root).raw / "drops" / f"articles-from-mcp-{today}.jsonl"
        run_id = self.state.enqueue_run(
            queue_name="links",
            kind="mcp.enqueue_links",
            notes=f"{len(request.links)} links enqueued",
            metadata={"path": str(target), "count": len(request.links), "links": request.links},
            last_item_ref=str(target),
        )
        return EnqueueResponse(run_id=run_id, queue_name="links")

    def start_ingest(self, request: StartIngestRequest) -> EnqueueResponse:
        self._authorize(request)
        normalized_path = self._normalize_repo_path(request.path) if request.path else None
        metadata = {"path": normalized_path, "today": request.today, "options": request.options or {}}
        run_id = self.state.enqueue_run(
            queue_name=f"ingest:{request.kind}",
            kind=f"mcp.start_ingest.{request.kind}",
            notes=f"ingest {request.kind} queued",
            metadata=metadata,
            last_item_ref=normalized_path or request.kind,
        )
        return EnqueueResponse(run_id=run_id, queue_name=f"ingest:{request.kind}")

    def start_reingest(self, request: StartReingestRequest) -> EnqueueResponse:
        self._authorize(request)
        normalized_path = self._normalize_repo_path(request.path) if request.path else None
        metadata = {
            "lane": request.lane,
            "path": normalized_path,
            "today": request.today,
            "stage": request.stage,
            "through": request.through,
            "limit": request.limit,
            "source_ids": request.source_ids,
            "dry_run": request.dry_run,
        }
        run_id = self.state.enqueue_run(
            queue_name="ingest:reingest",
            kind="mcp.start_reingest",
            notes=f"reingest {request.lane} queued",
            metadata=metadata,
            last_item_ref=normalized_path or request.lane,
        )
        return EnqueueResponse(run_id=run_id, queue_name="ingest:reingest")

    def start_article_repair(self, request: StartArticleRepairRequest) -> EnqueueResponse:
        self._authorize(request)
        normalized_path = self._normalize_repo_path(request.path) if request.path else None
        metadata = {
            "path": normalized_path,
            "today": request.today,
            "limit": request.limit,
            "source_ids": request.source_ids,
            "apply": request.apply,
        }
        run_id = self.state.enqueue_run(
            queue_name="ingest:repair-articles",
            kind="mcp.start_article_repair",
            notes="article repair queued",
            metadata=metadata,
            last_item_ref=normalized_path or "articles",
        )
        return EnqueueResponse(run_id=run_id, queue_name="ingest:repair-articles")

    def start_dream_light(self, request: StartDreamRequest) -> EnqueueResponse:
        self._authorize(request)
        return self._reject_dream_queue("light")

    def start_dream_deep(self, request: StartDreamRequest) -> EnqueueResponse:
        self._authorize(request)
        return self._reject_dream_queue("deep")

    def start_dream_rem(self, request: StartDreamRequest) -> EnqueueResponse:
        self._authorize(request)
        return self._reject_dream_queue("rem")

    def start_dream_bootstrap(self, request: StartDreamBootstrapRequest) -> EnqueueResponse:
        self._authorize(request)
        return self._reject_dream_queue("bootstrap")

    def generate_skill(self, request: GenerateSkillRequest) -> EnqueueResponse:
        self._authorize(request)
        run_id = self.state.enqueue_run(
            queue_name="skills",
            kind="mcp.generate_skill",
            notes=f"generate skill queued for {request.name or 'unnamed-skill'}",
            metadata=request.model_dump(),
            last_item_ref=request.name or request.prompt[:32],
        )
        return EnqueueResponse(run_id=run_id, queue_name="skills")

    def set_skill_status(self, request: SetSkillStatusRequest) -> EnqueueResponse:
        self._authorize(request)
        run_id = self.state.enqueue_run(
            queue_name="skills",
            kind="mcp.set_skill_status",
            notes=f"set skill status queued for {request.skill_id}",
            metadata=request.model_dump(),
            last_item_ref=request.skill_id,
        )
        return EnqueueResponse(run_id=run_id, queue_name="skills")

    def retry_queue_item(self, request: RetryQueueItemRequest) -> EnqueueResponse:
        self._authorize(request)
        run = self.state.get_run(request.run_id)
        if run is None or run.run.queue_name is None:
            raise FileNotFoundError(f"queued run {request.run_id} not found")
        if run.run.status not in {"failed", "blocked"}:
            raise ValueError(f"run {request.run_id} is not retryable from status={run.run.status}")
        run_id = self.state.enqueue_run(
            queue_name=run.run.queue_name,
            kind="mcp.retry_queue_item",
            notes=f"retry queued for run {request.run_id}",
            metadata={**request.model_dump(), "queue_name": run.run.queue_name},
            last_item_ref=str(request.run_id),
        )
        return EnqueueResponse(run_id=run_id, queue_name=run.run.queue_name)

    def clear_stale_lock(self, request: ClearStaleLockRequest) -> EnqueueResponse:
        self._authorize(request)
        run_id = self.state.enqueue_run(
            queue_name="admin",
            kind="mcp.clear_stale_lock",
            notes=f"clear stale lock queued for {request.lock_name}",
            metadata=request.model_dump(),
            last_item_ref=request.lock_name,
        )
        return EnqueueResponse(run_id=run_id, queue_name="admin")

    def run_onboard(self, request: RunOnboardRequest) -> EnqueueResponse:
        self._authorize(request)
        run_id = self.state.enqueue_run(
            queue_name="onboard",
            kind="mcp.run_onboard",
            notes=f"onboard queued for {request.input_path}",
            metadata=request.model_dump(),
            last_item_ref=request.input_path,
        )
        return EnqueueResponse(run_id=run_id, queue_name="onboard")
