from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json

from .embedding_service import EmbeddingService
from .graph_registry import GraphRegistry, ResolutionCandidate
from .vector_index import VectorIndexBackend
from scripts.common.vault import raw_path


EVALUATION_FIXTURES: list[tuple[str, str | None]] = [
    ("Local First Systems", "local-first-systems"),
    ("Weekly Review Loop", "weekly-review-loop"),
    ("User Owned AI", "user-owned-ai"),
]


@dataclass(frozen=True)
class EvaluationRow:
    query: str
    expected_page_id: str | None
    phase1_page_id: str | None
    vector_page_id: str | None
    phase1_ok: bool
    vector_ok: bool


@dataclass(frozen=True)
class PromotionGateResult:
    passed: bool
    phase1_regressions: int
    vector_false_negatives: int
    rows: list[EvaluationRow]
    artifact_json_path: Path
    artifact_markdown_path: Path


def evaluate_shadow_vectors(
    *,
    registry: GraphRegistry,
    embedding_service: EmbeddingService,
    vector_backend: VectorIndexBackend,
    model: str,
    limit: int = 5,
    ) -> list[EvaluationRow]:
    rows: list[EvaluationRow] = []
    for query, expected_page_id in EVALUATION_FIXTURES:
        phase1 = registry.resolve_candidates(query, limit=limit)
        vector = registry.resolve_vector_candidates(
            query,
            embedding_service=embedding_service,
            vector_backend=vector_backend,
            model=model,
            limit=limit,
        )
        phase1_page_id = phase1[0].page_id if phase1 else None
        vector_page_id = vector[0].page_id if vector else None
        rows.append(
            EvaluationRow(
                query=query,
                expected_page_id=expected_page_id,
                phase1_page_id=phase1_page_id,
                vector_page_id=vector_page_id,
                phase1_ok=phase1_page_id == expected_page_id,
                vector_ok=vector_page_id == expected_page_id,
            )
        )
    return rows


def evaluate_promotion_gate(
    *,
    repo_root: Path,
    registry: GraphRegistry,
    embedding_service: EmbeddingService,
    vector_backend: VectorIndexBackend,
    model: str,
) -> PromotionGateResult:
    rows = evaluate_shadow_vectors(
        registry=registry,
        embedding_service=embedding_service,
        vector_backend=vector_backend,
        model=model,
    )
    phase1_regressions = sum(1 for row in rows if not row.phase1_ok)
    vector_false_negatives = sum(1 for row in rows if row.expected_page_id and row.vector_page_id != row.expected_page_id)
    passed = phase1_regressions == 0 and vector_false_negatives == 0
    root = raw_path(repo_root, "reports", "graph-embed")
    root.mkdir(parents=True, exist_ok=True)
    timestamp = __import__("datetime").datetime.utcnow().strftime("%Y-%m-%dT%H%M%SZ")
    json_path = root / f"promotion-gate-{timestamp}.json"
    md_path = root / f"promotion-gate-{timestamp}.md"
    payload = {
        "model": model,
        "passed": passed,
        "phase1_regressions": phase1_regressions,
        "vector_false_negatives": vector_false_negatives,
        "rows": [row.__dict__ for row in rows],
    }
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    lines = [
        "# Graph Embed Promotion Gate",
        "",
        f"- Model: {model}",
        f"- Passed: {'yes' if passed else 'no'}",
        f"- Phase 1 regressions: {phase1_regressions}",
        f"- Vector false negatives: {vector_false_negatives}",
        "",
        "## Rows",
        "",
    ]
    for row in rows:
        lines.append(
            f"- {row.query} — expected={row.expected_page_id or '-'} "
            f"phase1={row.phase1_page_id or '-'} vector={row.vector_page_id or '-'} "
            f"phase1_ok={row.phase1_ok} vector_ok={row.vector_ok}"
        )
    md_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return PromotionGateResult(
        passed=passed,
        phase1_regressions=phase1_regressions,
        vector_false_negatives=vector_false_negatives,
        rows=rows,
        artifact_json_path=json_path,
        artifact_markdown_path=md_path,
    )
