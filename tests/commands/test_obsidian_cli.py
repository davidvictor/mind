from __future__ import annotations

import json
from pathlib import Path

import pytest

from mind.cli import main
from tests.support import write_repo_config


def _patch_project_root(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("mind.commands.common.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.config.project_root", lambda: root)
    monkeypatch.setattr("mind.commands.doctor.project_root", lambda: root)


def test_mind_obsidian_theme_apply_help_exposes_flags(capsys) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["obsidian", "theme", "apply", "--help"])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert "--dark" in out
    assert "--light" in out
    assert "--force" in out


def test_mind_obsidian_theme_apply_generates_artifacts(tmp_path: Path, monkeypatch, capsys) -> None:
    write_repo_config(tmp_path)
    _patch_project_root(monkeypatch, tmp_path)

    assert main(["obsidian", "theme", "apply"]) == 0
    out = capsys.readouterr().out
    assert "obsidian-theme:" in out
    assert "dark=dragon" in out
    assert "light=lotus" in out
    assert (tmp_path / "memory" / ".obsidian" / "snippets" / "brain-kanagawa.css").exists()

    appearance = json.loads((tmp_path / "memory" / ".obsidian" / "appearance.json").read_text(encoding="utf-8"))
    assert "brain-kanagawa" in appearance["enabledCssSnippets"]
    graph = json.loads((tmp_path / "memory" / ".obsidian" / "graph.json").read_text(encoding="utf-8"))
    assert graph["search"] == "-path:summaries"
    assert any(group["query"].endswith("path:sources/books") for group in graph["colorGroups"])
