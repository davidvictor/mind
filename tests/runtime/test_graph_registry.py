from __future__ import annotations

from pathlib import Path

from mind.services.graph_registry import GraphRegistry
from tests.support import write_repo_config


def _write_config(root: Path) -> None:
    write_repo_config(root, create_indexes=True)


def _write_page(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_graph_registry_rebuild_indexes_nodes_aliases_and_edges(tmp_path: Path):
    _write_config(tmp_path)
    _write_page(
        tmp_path / "memory" / "projects" / "the-pick-ai.md",
        "---\n"
        "id: the-pick-ai\n"
        "type: project\n"
        "title: Example Product\n"
        "status: active\n"
        "created: 2026-04-11\n"
        "last_updated: 2026-04-11\n"
        "aliases:\n"
        "  - Example Product\n"
        "tags:\n  - domain/work\n  - function/note\n  - signal/working\n"
        "domains:\n  - work\n"
        "relates_to:\n  - \"[[example-owner]]\"\n"
        "sources: []\n"
        "---\n\n"
        "# Example Product\n\nA conversational sports intelligence product.\n",
    )
    _write_page(
        tmp_path / "memory" / "people" / "example-owner.md",
        "---\n"
        "id: example-owner\n"
        "type: person\n"
        "title: Example Owner\n"
        "status: active\n"
        "created: 2026-04-11\n"
        "last_updated: 2026-04-11\n"
        "aliases: []\n"
        "tags:\n  - domain/relationships\n  - function/reference\n  - signal/canon\n"
        "domains:\n  - relationships\n"
        "relates_to: []\n"
        "sources: []\n"
        "---\n\n"
        "# Example Owner\n\nDesigner-engineer-builder.\n",
    )

    registry = GraphRegistry.for_repo_root(tmp_path)
    result = registry.rebuild()
    status = registry.status()

    assert result.node_count == 2
    assert status.alias_count >= 3
    assert status.edge_count == 1
    candidates = registry.resolve_candidates("Example Product")
    assert candidates
    assert candidates[0].page_id == "the-pick-ai"


def test_graph_registry_uses_logical_paths_for_external_private_roots(tmp_path: Path):
    repo_root = tmp_path / "repo"
    private_root = tmp_path / "private-store"
    memory_root = private_root / "memory"
    raw_root = private_root / "raw"
    dropbox_root = private_root / "dropbox"
    state_root = private_root / "state"
    repo_root.mkdir()
    raw_root.mkdir(parents=True)
    dropbox_root.mkdir(parents=True)
    state_root.mkdir(parents=True)
    (repo_root / "config.yaml").write_text(
        "\n".join(
            [
                "paths:",
                f"  memory_root: {memory_root}",
                f"  raw_root: {raw_root}",
                f"  dropbox_root: {dropbox_root}",
                f"  state_root: {state_root}",
                "vault:",
                f"  wiki_dir: {memory_root}",
                f"  raw_dir: {raw_root}",
                f"  dropbox_dir: {dropbox_root}",
                f"  state_dir: {state_root}",
                "state:",
                f"  graph_db: {state_root / 'brain-graph.sqlite3'}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    _write_page(
        memory_root / "concepts" / "local-first-systems.md",
        "---\n"
        "id: local-first-systems\n"
        "type: concept\n"
        "title: Local-first systems\n"
        "status: active\n"
        "created: 2026-04-11\n"
        "last_updated: 2026-04-11\n"
        "aliases:\n"
        "  - local-first\n"
        "tags:\n  - domain/meta\n  - function/concept\n  - signal/working\n"
        "domains:\n  - meta\n"
        "relates_to: []\n"
        "sources: []\n"
        "---\n\n"
        "# Local-first systems\n\n"
        "Local-first systems keep private data portable and inspectable.\n",
    )

    registry = GraphRegistry.for_repo_root(repo_root)
    result = registry.rebuild()

    assert result.node_count == 1
    node = registry.get_node("local-first-systems")
    assert node is not None
    assert node.path == "memory/concepts/local-first-systems.md"
    targets = registry.list_embedding_targets()
    assert any(target.page_id == "local-first-systems" for target in targets)
    matches = registry.query_pages("portable private data", limit=3)
    assert matches
    assert matches[0].path == "memory/concepts/local-first-systems.md"

    source_path = dropbox_root / "note.md"
    source_path.write_text("Local-first systems note.\n", encoding="utf-8")
    registry.record_document(
        doc_id="doc:test",
        path=source_path,
        title="Note",
        source_kind="document",
        ingest_lane="dropbox-file",
        body="Local-first systems note.",
        resolutions=[],
        candidates=[],
        document_targets=["local-first-systems"],
    )
    with registry.connect() as conn:
        row = conn.execute("SELECT path FROM documents WHERE doc_id = ?", ("doc:test",)).fetchone()
    assert str(row["path"]) == "dropbox/note.md"


def test_graph_registry_embedding_text_is_compact_for_sparse_project_page(tmp_path: Path):
    _write_config(tmp_path)
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
        "---\n\n"
        "# Example Product\n\n"
        "A conversational sports intelligence product.\n",
    )
    registry = GraphRegistry.for_repo_root(tmp_path)
    registry.rebuild()

    targets = registry.list_embedding_targets()
    target = next(item for item in targets if item.page_id == "the-pick-ai")
    assert "Title: Example Product" in target.content
    assert "Aliases: Example Product" in target.content
    assert target.content.count("A conversational sports intelligence product.") == 1


def test_graph_registry_record_document_dedupes_document_targets(tmp_path: Path):
    _write_config(tmp_path)
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
        "---\n\n"
        "# Example Product\n\n"
        "A conversational sports intelligence product.\n",
    )
    registry = GraphRegistry.for_repo_root(tmp_path)
    registry.rebuild()

    registry.record_document(
        doc_id="doc:test",
        path=tmp_path / "dropbox" / "sample.md",
        title="Sample",
        source_kind="md",
        ingest_lane="dropbox-file",
        body="Hello world",
        resolutions=[],
        candidates=[],
        document_targets=["the-pick-ai", "the-pick-ai"],
    )

    with registry.connect() as conn:
        rows = conn.execute(
            "SELECT doc_id, node_id, relation_kind FROM document_targets WHERE doc_id = ?",
            ("doc:test",),
        ).fetchall()
    assert len(rows) == 1


def test_graph_registry_indexes_source_frontmatter_wikilinks_as_edges(tmp_path: Path):
    _write_config(tmp_path)
    _write_page(
        tmp_path / "memory" / "sources" / "substack" / "example" / "on-trust.md",
        "---\n"
        "id: on-trust\n"
        "type: article\n"
        "title: On Trust\n"
        "status: active\n"
        "created: 2026-04-11\n"
        "last_updated: 2026-04-11\n"
        "aliases: []\n"
        "tags:\n  - domain/learning\n  - function/source\n  - signal/canon\n"
        "domains:\n  - learning\n"
        "relates_to: []\n"
        "sources: []\n"
        "author: \"[[mario-gabriele]]\"\n"
        "outlet: \"[[thegeneralist]]\"\n"
        "---\n\n"
        "# On Trust\n\nTrust compounds.\n",
    )
    _write_page(
        tmp_path / "memory" / "people" / "mario-gabriele.md",
        "---\n"
        "id: mario-gabriele\n"
        "type: person\n"
        "title: Mario Gabriele\n"
        "status: active\n"
        "created: 2026-04-11\n"
        "last_updated: 2026-04-11\n"
        "aliases: []\n"
        "tags:\n  - domain/relationships\n  - function/reference\n  - signal/canon\n"
        "domains:\n  - learning\n"
        "relates_to: []\n"
        "sources: []\n"
        "---\n\n"
        "# Mario Gabriele\n\nAuthor.\n",
    )
    _write_page(
        tmp_path / "memory" / "companies" / "thegeneralist.md",
        "---\n"
        "id: thegeneralist\n"
        "type: company\n"
        "title: The Generalist\n"
        "status: active\n"
        "created: 2026-04-11\n"
        "last_updated: 2026-04-11\n"
        "aliases: []\n"
        "tags:\n  - domain/relationships\n  - function/reference\n  - signal/canon\n"
        "domains:\n  - learning\n"
        "relates_to: []\n"
        "sources: []\n"
        "---\n\n"
        "# The Generalist\n\nPublication.\n",
    )

    registry = GraphRegistry.for_repo_root(tmp_path)
    result = registry.rebuild()

    assert result.node_count == 2
    with registry.connect() as conn:
        targets = conn.execute(
            "SELECT node_id, relation_kind FROM document_targets WHERE doc_id = ? ORDER BY node_id",
            ("doc:memory/sources/substack/example/on-trust.md",),
        ).fetchall()
    assert [(str(row["node_id"]), str(row["relation_kind"])) for row in targets] == [
        ("mario-gabriele", "linked"),
        ("thegeneralist", "linked"),
    ]


def test_graph_registry_indexes_typed_relations_and_query_surfaces_tension(tmp_path: Path):
    _write_config(tmp_path)
    _write_page(
        tmp_path / "memory" / "concepts" / "builder-judgment.md",
        "---\n"
        "id: builder-judgment\n"
        "type: concept\n"
        "title: Builder Judgment\n"
        "status: active\n"
        "created: 2026-04-11\n"
        "last_updated: 2026-04-11\n"
        "aliases: []\n"
        "tags:\n  - domain/meta\n  - function/concept\n  - signal/working\n"
        "domains:\n  - work\n"
        "typed_relations:\n"
        "  contradicts:\n"
        "    - \"[[automated-judgment]]\"\n"
        "relates_to:\n  - \"[[automated-judgment]]\"\n"
        "sources: []\n"
        "---\n\n"
        "# Builder Judgment\n\n"
        "Human judgment is the edge in AI product work.\n\n"
        "## TL;DR\n\nHuman judgment is the edge.\n\n"
        "## Evidence log\n\n- local evidence\n",
    )
    _write_page(
        tmp_path / "memory" / "concepts" / "automated-judgment.md",
        "---\n"
        "id: automated-judgment\n"
        "type: concept\n"
        "title: Automated Judgment\n"
        "status: active\n"
        "created: 2026-04-11\n"
        "last_updated: 2026-04-11\n"
        "aliases: []\n"
        "tags:\n  - domain/meta\n  - function/concept\n  - signal/working\n"
        "domains:\n  - work\n"
        "typed_relations:\n"
        "  contradicts:\n"
        "    - \"[[builder-judgment]]\"\n"
        "relates_to:\n  - \"[[builder-judgment]]\"\n"
        "sources: []\n"
        "---\n\n"
        "# Automated Judgment\n\n"
        "Full automation is the edge in AI product work.\n\n"
        "## TL;DR\n\nAutomation is the edge.\n\n"
        "## Evidence log\n\n- local evidence\n",
    )

    registry = GraphRegistry.for_repo_root(tmp_path)
    registry.rebuild()
    with registry.connect() as conn:
        rel_types = [str(row["rel_type"]) for row in conn.execute("SELECT rel_type FROM edges").fetchall()]
    assert "contradicts" in rel_types

    matches = registry.query_pages("judgment edge ai product work", limit=5)
    assert matches
    assert any("tension with [[automated-judgment]]" in match.annotations for match in matches if match.page_id == "builder-judgment")
