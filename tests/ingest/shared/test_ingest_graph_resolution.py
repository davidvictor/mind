from __future__ import annotations

from pathlib import Path

from mind.commands import ingest
from tests.support import write_repo_config


def _write_page(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_graph_aware_ingest_file_resolves_to_existing_project_and_updates_page(tmp_path: Path, monkeypatch):
    write_repo_config(tmp_path, create_indexes=True)
    project_page = tmp_path / "memory" / "projects" / "the-pick-ai.md"
    _write_page(
        project_page,
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
        "relates_to: []\n"
        "sources: []\n"
        "---\n\n"
        "# Example Product\n\nA conversational sports intelligence product.\n",
    )
    sample = tmp_path / "dropbox" / "The-Pick-Case-Study.md"
    sample.parent.mkdir(parents=True, exist_ok=True)
    sample.write_text(
        "# Example Product — Case Study\n\n"
        "**Role:** Sole Technical Founder\n"
        "**Timeline:** 2024–Present\n"
        "**Status:** Live, funded, 500+ subscribers\n"
        "**Stack:** Next.js 15, React 19, SwiftUI\n\n"
        "## Context\n\n"
        "Example Product is a conversational sports intelligence product.\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(ingest, "vault", lambda: __import__("scripts.common.vault", fromlist=["Vault"]).Vault.load(tmp_path))

    captured: dict[str, object] = {}

    def fake_run_ingestion_lifecycle(**kwargs):
        captured.update(kwargs)

        class Result:
            materialized = tmp_path / "raw" / "files" / "file-artifact-abc123.md"

        return Result()

    monkeypatch.setattr(ingest, "run_ingestion_lifecycle", fake_run_ingestion_lifecycle)

    out = ingest.ingest_file(sample, graph_aware=True)

    assert out == tmp_path / "raw" / "files" / "file-artifact-abc123.md"
    source = captured["source"]
    assert source.source_id.startswith("file-artifact-")
    assert "Example Product is a conversational sports intelligence product." in source.primary_content
    assert "the-pick-ai" in source.source_metadata["resolved_nodes"]
    assert captured["distill"] is None
    updated = project_page.read_text(encoding="utf-8")
    assert "raw/files/file-artifact-abc123.md" in updated
    assert "## Roles" in updated
    assert "Sole Technical Founder" in updated
    assert "## Recent Evidence" in updated
