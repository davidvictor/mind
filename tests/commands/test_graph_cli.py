from __future__ import annotations

from pathlib import Path

from mind.cli import build_parser, main
from tests.support import subcommand_names, write_repo_config


def _write_page(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _patch_project_root(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("mind.commands.common.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.config.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.doctor.project_root", lambda: root)


def test_graph_parser_exposes_subcommands():
    assert subcommand_names(build_parser(), "graph") >= {"rebuild", "status", "health", "resolve"}


def test_graph_embed_parser_exposes_subcommands():
    assert subcommand_names(build_parser(), "graph", "embed") >= {"rebuild", "status", "query", "evaluate"}


def test_graph_rebuild_and_resolve(tmp_path: Path, monkeypatch, capsys):
    write_repo_config(tmp_path, create_indexes=True)
    _patch_project_root(monkeypatch, tmp_path)
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

    assert main(["graph", "rebuild"]) == 0
    assert "graph-rebuild:" in capsys.readouterr().out
    assert main(["graph", "resolve", "Example Product"]) == 0
    out = capsys.readouterr().out
    assert "the-pick-ai" in out


def test_graph_embed_evaluate_returns_nonzero_when_gate_fails(tmp_path: Path, monkeypatch, capsys):
    write_repo_config(tmp_path, create_indexes=True)
    _patch_project_root(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "mind.commands.graph.evaluate_promotion_gate",
        lambda **kwargs: __import__("types").SimpleNamespace(
            passed=False,
            phase1_regressions=0,
            vector_false_negatives=1,
            rows=[],
            artifact_json_path=tmp_path / "raw" / "reports" / "graph-embed" / "gate.json",
            artifact_markdown_path=tmp_path / "raw" / "reports" / "graph-embed" / "gate.md",
        ),
    )

    assert main(["graph", "embed", "evaluate"]) == 1
    out = capsys.readouterr().out
    assert "passed=False" in out


def test_graph_health_reports_advisory_shadow_mode(tmp_path: Path, monkeypatch, capsys):
    write_repo_config(tmp_path, create_indexes=True)
    _patch_project_root(monkeypatch, tmp_path)
    monkeypatch.setattr(
        "mind.commands.graph.build_graph_health",
        lambda repo_root, include_promotion_gate=True: __import__("types").SimpleNamespace(
            issues=(),
            render=lambda: "\n".join(
                [
                    "graph-health:",
                    "- graph_built=yes",
                    "- embedding_count=10",
                    "- shadow_mode=advisory-only",
                    "- promotion_gate_passed=True",
                ]
            ),
        ),
    )

    assert main(["graph", "health", "--skip-promotion-gate"]) == 0
    out = capsys.readouterr().out
    assert "shadow_mode=advisory-only" in out
