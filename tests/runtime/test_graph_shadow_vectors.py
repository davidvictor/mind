from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from mind.services.embedding_service import EmbeddingExecutionResult
from mind.services.embedding_executor import EmbeddingIdentity
from mind.services.graph_registry import GraphRegistry
from mind.services.graph_resolution import resolve_graph_document
from mind.services.vector_index import FileVectorIndexBackend
from tests.support import write_repo_config


def _write_page(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_resolve_graph_document_records_shadow_vector_candidates(tmp_path: Path, monkeypatch):
    write_repo_config(tmp_path, create_indexes=True)
    _write_page(
        tmp_path / "memory" / "projects" / "the-pick-ai.md",
        "---\n"
        "id: the-pick-ai\n"
        "type: project\n"
        "title: Example Product\n"
        "status: active\n"
        "created: 2026-04-11\n"
        "last_updated: 2026-04-11\n"
        "aliases:\n  - Example Product\n"
        "tags:\n  - domain/work\n  - function/note\n  - signal/working\n"
        "domains:\n  - work\n"
        "relates_to: []\n"
        "sources: []\n"
        "---\n\n# Example Product\n\nA product.\n",
    )
    registry = GraphRegistry.for_repo_root(tmp_path)
    registry.rebuild()
    backend = FileVectorIndexBackend(tmp_path / "raw" / "cache" / "graph-vectors")
    backend.upsert(model="openai/text-embedding-3-small", vectors={"the-pick-ai": [1.0, 0.0]})
    registry.upsert_embeddings(
        model="openai/text-embedding-3-small",
        records=[
            {
                "target_id": "the-pick-ai",
                "target_type": "node",
                "page_id": "the-pick-ai",
                "content_sha256": "abc",
                "vector_dim": 2,
            }
        ],
    )

    monkeypatch.setattr(
        "mind.services.graph_resolution.resolve_route",
        lambda task_class: SimpleNamespace(model="openai/text-embedding-3-small"),
    )
    monkeypatch.setattr(
        "mind.services.graph_resolution.select_vector_backend",
        lambda root: backend,
    )
    monkeypatch.setattr(
        "mind.services.graph_resolution.get_embedding_service",
        lambda: SimpleNamespace(
            embed_query=lambda text: EmbeddingExecutionResult(
                vectors=[[1.0, 0.0]],
                identity=EmbeddingIdentity(
                    provider="openai",
                    model="openai/text-embedding-3-small",
                    transport="ai_gateway",
                    api_family="responses",
                    input_mode="text",
                ),
                response_metadata={},
            )
        ),
    )

    sample = tmp_path / "dropbox" / "The-Pick-Case-Study.md"
    sample.parent.mkdir(parents=True, exist_ok=True)
    sample.write_text("# Example Product — Case Study\n\nExample Product is a product.\n", encoding="utf-8")

    resolved = resolve_graph_document(path=sample, registry=registry)

    assert resolved.primary_decision.resolved_node_id == "the-pick-ai"
    assert any(candidate.page_id == "the-pick-ai" for candidate in resolved.primary_decision.shadow_vector_candidates)
