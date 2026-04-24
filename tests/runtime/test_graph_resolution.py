from __future__ import annotations

from dataclasses import dataclass

import mind.services.graph_resolution as graph_resolution


@dataclass(frozen=True)
class _Candidate:
    registry_node_id: str
    page_id: str
    primary_type: str
    title: str
    path: str
    score: float
    match_kind: str
    aliases: list[str]


class _RecordingLLM:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []

    def generate_json_prompt(self, prompt: str, *, task_class: str, prompt_version: str):
        self.calls.append(
            {
                "task_class": task_class,
                "prompt_version": prompt_version,
                "prompt": prompt,
            }
        )
        return {
            "selected_registry_node_id": "node-1",
            "confidence": "high",
            "rationale": "best match",
        }


def test_llm_pick_candidate_uses_classification_route(monkeypatch) -> None:
    recorder = _RecordingLLM()
    monkeypatch.setattr(graph_resolution, "get_llm_service", lambda: recorder)

    selected, confidence, rationale = graph_resolution._llm_pick_candidate(
        title="Example Product Case Study",
        body="Long form write-up about Example Product product and brand system.",
        mention="Example Product",
        candidates=[
            _Candidate(
                registry_node_id="node-1",
                page_id="the-pick-ai",
                primary_type="project",
                title="Example Product",
                path="memory/projects/the-pick-ai.md",
                score=0.81,
                match_kind="fts_title_alias",
                aliases=["Example Product"],
            )
        ],
    )

    assert selected == "node-1"
    assert confidence == 0.9
    assert rationale == "best match"
    assert recorder.calls == [
        {
            "task_class": "classification",
            "prompt_version": graph_resolution.GRAPH_RESOLUTION_PICK_PROMPT_VERSION,
            "prompt": recorder.calls[0]["prompt"],
        }
    ]


def test_extract_mentions_filters_generic_scaffolding_terms() -> None:
    body = """
# Example Product — Case Study

## Role

Sole Technical Founder

## Stack

React
SwiftUI
Jetpack Compose
LangGraph
Python
FastAPI
NautilusTrader
Dagster
Supabase
Redis
"""
    mentions = graph_resolution._extract_mentions(
        graph_resolution.Path("The-Pick-Case-Study.md"),
        "Example Product — Case Study",
        body,
    )

    assert "Example Product — Case Study" in mentions
    assert "Example Product" in mentions
    assert "Role" not in mentions
    assert "Stack" not in mentions
    assert "Product Design" not in mentions
    assert len(mentions) <= 12


def test_resolve_graph_document_dedupes_related_nodes(tmp_path, monkeypatch) -> None:
    registry = type(
        "_Registry",
        (),
        {
            "repo_root": tmp_path,
            "resolve_candidates": lambda self, mention: [
                _Candidate(
                    registry_node_id="lifejet",
                    page_id="lifejet",
                    primary_type="project",
                    title="Example Health App",
                    path="memory/projects/lifejet.md",
                    score=1.0 if "Example Health App" in mention else 0.82,
                    match_kind="exact" if "Example Health App" in mention else "fts_title_alias",
                    aliases=[],
                )
            ],
            "resolve_vector_candidates": lambda self, *args, **kwargs: [],
        },
    )()
    monkeypatch.setattr(graph_resolution, "get_embedding_service", lambda: None)
    monkeypatch.setattr(graph_resolution, "select_vector_backend", lambda *args, **kwargs: None)
    monkeypatch.setattr(graph_resolution, "resolve_route", lambda task_class: type("_Route", (), {"model": "fake"})())

    path = tmp_path / "Example Health App.md"
    path.write_text(
        "# Example Health App\n\n"
        "**Slug:** lifejet\n\n"
        "Example Health App is a health AI product.\n\n"
        "Example Health App helps people think through symptoms.\n",
        encoding="utf-8",
    )

    resolved = graph_resolution.resolve_graph_document(path=path, registry=registry)

    assert resolved.primary_decision.resolved_node_id == "lifejet"
    assert resolved.related_decisions == []


def test_patch_canonical_node_uses_registry_path_resolver(tmp_path) -> None:
    repo = tmp_path / "repo"
    memory = tmp_path / "private" / "memory"
    page = memory / "projects" / "lifejet.md"
    repo.mkdir()
    page.parent.mkdir(parents=True)
    page.write_text(
        "---\n"
        "id: lifejet\n"
        "type: project\n"
        "title: Example Health App\n"
        "aliases: []\n"
        "sources: []\n"
        "relates_to: []\n"
        "---\n\n"
        "# Example Health App\n\n"
        "## Evidence\n\n",
        encoding="utf-8",
    )
    node = graph_resolution.GraphNode(
        node_id="node-1",
        page_id="lifejet",
        primary_type="project",
        title="Example Health App",
        path="memory/projects/lifejet.md",
        status="active",
        normalized_title="lifejet",
        canonical_slug="lifejet",
        domains=[],
        facets=[],
        aliases=[],
    )
    registry = type(
        "_Registry",
        (),
        {
            "get_node": lambda self, node_id: node if node_id == "node-1" else None,
            "resolve_path": lambda self, path_text: page,
        },
    )()
    decision = graph_resolution.ResolutionDecision(
        mention_text="Example Health App",
        resolved_node_id="lifejet",
        resolved_registry_node_id="node-1",
        resolution_kind="exact",
        confidence=1.0,
        rationale="matched",
        candidates=[],
        shadow_vector_candidates=[],
    )
    resolved = graph_resolution.ResolvedGraphDocument(
        doc_id="doc-1",
        artifact_id="artifact-1",
        title="Example Health App note",
        body="New evidence.",
        source_kind="note",
        mentions=["Example Health App"],
        primary_decision=decision,
        related_decisions=[],
        review_required=False,
        review_payload={},
        derived_aliases=["Example Health App AI"],
    )

    graph_resolution.patch_canonical_node(
        repo_root=repo,
        registry=registry,
        resolved=resolved,
        source_ref="[[summary-lifejet]]",
    )

    text = page.read_text(encoding="utf-8")
    assert "Example Health App AI" in text
    assert "[[summary-lifejet]]" in text
