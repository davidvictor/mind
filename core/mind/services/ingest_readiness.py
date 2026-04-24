from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import TYPE_CHECKING

from mind.services.embedding_evaluation import evaluate_promotion_gate
from mind.services.embedding_service import get_embedding_service
from mind.services.graph_registry import GraphRegistry
from mind.services.llm_routing import resolve_route
from mind.services.reingest import (
    ArticleRepairResult,
    ReingestRequest,
    ReingestRunResult,
    render_article_repair_report,
    render_reingest_report,
    run_article_repair,
    run_reingest,
)
from mind.services.vector_index import select_vector_backend
from scripts.common.vault import Vault, raw_path

if TYPE_CHECKING:
    from mind.services.dropbox import DropboxSweepResult


@dataclass(frozen=True)
class GraphHealthSnapshot:
    graph_built: bool
    node_count: int
    edge_count: int
    document_count: int
    embedding_model: str
    embedding_count: int
    embedding_backend: str
    embedding_backend_count: int
    shadow_mode: str
    promotion_gate_passed: bool | None
    promotion_gate_artifact_json: str | None
    promotion_gate_artifact_markdown: str | None
    issues: tuple[str, ...]

    def render(self) -> str:
        lines = [
            "graph-health:",
            f"- graph_built={'yes' if self.graph_built else 'no'}",
            f"- nodes={self.node_count}",
            f"- edges={self.edge_count}",
            f"- documents={self.document_count}",
            f"- embedding_model={self.embedding_model}",
            f"- embedding_count={self.embedding_count}",
            f"- embedding_backend={self.embedding_backend}",
            f"- embedding_backend_count={self.embedding_backend_count}",
            f"- shadow_mode={self.shadow_mode}",
        ]
        if self.promotion_gate_passed is not None:
            lines.append(f"- promotion_gate_passed={self.promotion_gate_passed}")
        if self.promotion_gate_artifact_json:
            lines.append(f"- promotion_gate_artifact_json={self.promotion_gate_artifact_json}")
        if self.promotion_gate_artifact_markdown:
            lines.append(f"- promotion_gate_artifact_md={self.promotion_gate_artifact_markdown}")
        if self.issues:
            lines.append("- issues=" + " | ".join(self.issues))
        return "\n".join(lines)


@dataclass(frozen=True)
class LaneReadiness:
    lane: str
    stage: str
    through: str
    selected_count: int
    blocked_count: int
    ready: bool
    report: str


@dataclass(frozen=True)
class IngestReadinessResult:
    passed: bool
    dropbox: DropboxSweepResult
    graph: GraphHealthSnapshot
    lanes: tuple[LaneReadiness, ...]
    article_repair: ArticleRepairResult
    issues: tuple[str, ...]
    report_json_path: Path
    report_markdown_path: Path

    def render(self) -> str:
        lines = [
            f"ingest-readiness: {'pass' if self.passed else 'fail'}",
            f"- dropbox_would_process={self.dropbox.predicted_process_count}",
            f"- dropbox_would_review={self.dropbox.predicted_review_count}",
            f"- dropbox_would_fail={self.dropbox.predicted_fail_count}",
            f"- graph_built={'yes' if self.graph.graph_built else 'no'}",
            f"- embeddings_populated={'yes' if self.graph.embedding_count > 0 and self.graph.embedding_backend_count > 0 else 'no'}",
            f"- article_repair_ready={self.article_repair.plan.ready_count}",
            f"- article_repair_refresh_acquisition={self.article_repair.plan.reacquire_count}",
            f"- article_repair_recompute_downstream={self.article_repair.plan.recompute_count}",
            f"- article_repair_blocked={self.article_repair.plan.blocked_count}",
            f"- report_json={self.report_json_path}",
            f"- report_md={self.report_markdown_path}",
        ]
        for lane in self.lanes:
            lines.append(
                f"- lane={lane.lane} stage={lane.stage}->{lane.through} "
                f"selected={lane.selected_count} blocked={lane.blocked_count} ready={'yes' if lane.ready else 'no'}"
            )
        if self.issues:
            lines.append("issues:")
            for issue in self.issues:
                lines.append(f"- {issue}")
        return "\n".join(lines)


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")


def _report_paths(repo_root: Path, *, timestamp: str) -> tuple[Path, Path]:
    root = raw_path(repo_root, "reports", "ingest-review")
    return (
        root / f"ingest-readiness-{timestamp}.json",
        root / f"ingest-readiness-{timestamp}.md",
    )


def build_graph_health(
    *,
    repo_root: Path,
    include_promotion_gate: bool = False,
) -> GraphHealthSnapshot:
    registry = GraphRegistry.for_repo_root(repo_root)
    status = registry.status()
    route = resolve_route("embedding")
    backend = select_vector_backend(Vault.load(repo_root).vector_db)
    embedding_status = registry.embedding_status(model=route.model)
    backend_status = backend.status(model=route.model)
    issues: list[str] = []
    if status.node_count == 0 or status.document_count == 0:
        issues.append("graph registry is empty or not rebuilt")
    if int(embedding_status.get("count") or 0) == 0:
        issues.append("embedding registry is empty")
    if int(backend_status.get("count") or 0) == 0:
        issues.append("vector backend index is empty")
    gate_passed: bool | None = None
    gate_json: str | None = None
    gate_md: str | None = None
    if include_promotion_gate and int(embedding_status.get("count") or 0) > 0 and int(backend_status.get("count") or 0) > 0:
        try:
            gate = evaluate_promotion_gate(
                repo_root=repo_root,
                registry=registry,
                embedding_service=get_embedding_service(),
                vector_backend=backend,
                model=route.model,
            )
        except Exception as exc:
            issues.append(f"shadow vector promotion gate unavailable: {type(exc).__name__}: {exc}")
        else:
            gate_passed = gate.passed
            gate_json = str(gate.artifact_json_path)
            gate_md = str(gate.artifact_markdown_path)
            if not gate.passed:
                issues.append("shadow vector promotion gate is failing")
    return GraphHealthSnapshot(
        graph_built=status.node_count > 0 and status.document_count > 0,
        node_count=status.node_count,
        edge_count=status.edge_count,
        document_count=status.document_count,
        embedding_model=route.model,
        embedding_count=int(embedding_status.get("count") or 0),
        embedding_backend=str(backend_status.get("backend") or "unknown"),
        embedding_backend_count=int(backend_status.get("count") or 0),
        shadow_mode="advisory-only",
        promotion_gate_passed=gate_passed,
        promotion_gate_artifact_json=gate_json,
        promotion_gate_artifact_markdown=gate_md,
        issues=tuple(issues),
    )


def run_ingest_readiness(
    *,
    repo_root: Path,
    dropbox_limit: int | None = None,
    lane_limit: int | None = None,
    include_promotion_gate: bool = False,
) -> IngestReadinessResult:
    from mind.services.dropbox import sweep_dropbox

    timestamp = _utc_timestamp()
    dropbox = sweep_dropbox(repo_root, dry_run=True, limit=dropbox_limit)
    graph = build_graph_health(repo_root=repo_root, include_promotion_gate=include_promotion_gate)
    lane_specs = (
        ("youtube", "acquire", "propagate"),
        ("books", "summary", "propagate"),
        ("articles", "pass_d", "materialize"),
        ("substack", "summary", "propagate"),
    )
    lane_results: list[LaneReadiness] = []
    issues: list[str] = []
    for lane, stage, through in lane_specs:
        run = run_reingest(
            ReingestRequest(
                lane=lane,
                stage=stage,
                through=through,
                limit=lane_limit,
                dry_run=True,
            ),
            repo_root=repo_root,
        )
        ready = run.plan.blocked_count == 0
        lane_results.append(
            LaneReadiness(
                lane=lane,
                stage=stage,
                through=through,
                selected_count=run.plan.selected_count,
                blocked_count=run.plan.blocked_count,
                ready=ready,
                report=render_reingest_report(run),
            )
        )
        if not ready:
            issues.append(f"{lane} reingest dry-run still has {run.plan.blocked_count} blocked items")
    article_repair = run_article_repair(repo_root=repo_root, limit=lane_limit, apply=False)
    if dropbox.predicted_review_count > 0:
        issues.append(f"dropbox dry-run predicts {dropbox.predicted_review_count} review-required files")
    if dropbox.predicted_fail_count > 0:
        issues.append(f"dropbox dry-run predicts {dropbox.predicted_fail_count} failing files")
    issues.extend(graph.issues)
    if article_repair.plan.blocked_count > 0:
        issues.append(f"article repair still has {article_repair.plan.blocked_count} blocked items")
    report_json_path, report_markdown_path = _report_paths(repo_root, timestamp=timestamp)
    result = IngestReadinessResult(
        passed=not issues,
        dropbox=dropbox,
        graph=graph,
        lanes=tuple(lane_results),
        article_repair=article_repair,
        issues=tuple(dict.fromkeys(issues)),
        report_json_path=report_json_path,
        report_markdown_path=report_markdown_path,
    )
    payload = {
        "generated_at": timestamp,
        "passed": result.passed,
        "issues": list(result.issues),
        "dropbox": {
            **result.dropbox.metadata,
            "outcomes": [asdict(item) for item in result.dropbox.outcomes],
        },
        "graph": asdict(result.graph),
        "lanes": [asdict(item) for item in result.lanes],
        "article_repair": {
            "plan": asdict(result.article_repair.plan),
            "report": render_article_repair_report(result.article_repair),
        },
    }
    markdown_lines = [
        "# Ingest Readiness Report",
        "",
        result.render(),
        "",
        "## Dropbox",
        "",
        result.dropbox.render(),
        "",
        "## Graph",
        "",
        result.graph.render(),
        "",
        "## Lanes",
        "",
    ]
    for lane in result.lanes:
        markdown_lines.extend([f"### {lane.lane}", "", lane.report, ""])
    markdown_lines.extend(["## Article Repair", "", render_article_repair_report(result.article_repair), ""])
    report_json_path.parent.mkdir(parents=True, exist_ok=True)
    report_json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    report_markdown_path.write_text("\n".join(markdown_lines).rstrip() + "\n", encoding="utf-8")
    return result
